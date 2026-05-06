# Copyright (c) 2026, Aurumor and contributors
# License: MIT. See LICENSE

import os
import time
import random
import json
from typing import NoReturn
from filelock import FileLock, Timeout

import frappe
from frappe.utils import get_bench_path, get_sites, now_datetime, cint
from frappe.utils.background_jobs import set_niceness


def start_controller() -> NoReturn:
	"""
	Telemetry Consumer.
	Reads from 'controller:telemetry' Redis stream and updates MariaDB.
	"""
	import logging
	import traceback
	logger = logging.getLogger("frappe_controller.telemetry")
	
	set_niceness()

	lock_path = _get_controller_lock_file()

	try:
		lock = FileLock(lock_path)
		lock.acquire(blocking=False)
	except Timeout:
		logger.info("Controller already running")
		return

	# Setup site connection for background job
	sites = get_sites()
	if not sites:
		logger.error("No sites found")
		return
	site = sites[0]
	
	logger.info(f"Starting telemetry consumer for site {site}")
	frappe.init(site)
	frappe.connect()
	
	cache = frappe.cache()
	streams = ["fs:started:low", "fs:started:medium", "fs:started:high", "fs:finished:low", "fs:failed:low", "fs:finished:medium", "fs:failed:medium", "fs:finished:high", "fs:failed:high"]
	
	for stream in streams:
		try:
			cache.xgroup_create(stream, "telemetry_consumer_group", id="0", mkstream=True)
			logger.info(f"Created consumer group for {stream}")
		except Exception as e:
			if "BUSYGROUP" in str(e):
				pass
			else:
				logger.warning(f"Could not create consumer group for {stream}: {e}")

	while True:
		try:
			messages = cache.xreadgroup(
				"telemetry_consumer_group",
				"consumer-1",
				{s: ">" for s in streams},
				count=500,
				block=5000
			)

			if not messages:
				continue
				
			logger.info(f"Received {len(messages)} stream updates")
			stream_msg_ids = {}
			for stream_name, stream_messages in messages:
				if isinstance(stream_name, bytes):
					stream_name = stream_name.decode("utf-8")
				if stream_name not in stream_msg_ids:
					stream_msg_ids[stream_name] = []
				
				logger.debug(f"Processing {len(stream_messages)} messages from {stream_name}")
				for msg_id, payload in stream_messages:
					stream_msg_ids[stream_name].append(msg_id)
					if b"payload" in payload:
						try:
							payload_data = json.loads(payload[b"payload"])
							payload = payload_data
						except Exception:
							pass
					elif "payload" in payload:
						try:
							payload_data = json.loads(payload["payload"])
							payload = payload_data
						except Exception:
							pass
							
					job_id = payload.get("job_id")
					status = payload.get("status")
					error = payload.get("error")
					job_site = payload.get("site")
					started_at = payload.get("started_at")
					time_taken = payload.get("time_taken", 0)
					
					# Ensure payload strings are parsed correctly if they are bytes
					if isinstance(job_id, bytes): job_id = job_id.decode('utf-8')
					if isinstance(status, bytes): status = status.decode('utf-8')
					if isinstance(error, bytes): error = error.decode('utf-8')
					if isinstance(job_site, bytes): job_site = job_site.decode('utf-8')

					if not job_id:
						continue
						
					# Single db connection handles it
					if job_site and getattr(frappe.local, "site", None) != job_site:
						frappe.init(site=job_site, force=True)
						frappe.connect()
						
					if status == "Started":
						frappe.db.sql("""
							UPDATE `tabFS Job`
							SET status = %s, started_at = %s
							WHERE name = %s
						""", (status, started_at, job_id))
					else:
						frappe.db.sql("""
							UPDATE `tabFS Job`
							SET status = %s, exc_info = %s, ended_at = %s, time_taken = %s
							WHERE name = %s
						""", (status, error, now_datetime(), time_taken, job_id))
					
					# Check if job type wants log
					job_type_name = frappe.db.get_value("FS Job", job_id, "job_type")
					if job_type_name and frappe.db.get_value("Controller Job Type", job_type_name, "create_log"):
						try:
							# Use db_insert to bypass global hooks that might fail on sites
							# without certain apps (like erpnext hooks on a non-erpnext site)
							log = frappe.new_doc("Controller Job Log")
							log.controller_job_type = job_type_name
							log.status = "Failed" if status == "Failed" else "Complete"
							log.details = error if error else "Finished successfully"
							log.set_new_name() # Generate a name since we are bypassing insert()
							log.db_insert()
						except Exception as log_e:
							logger.warning(f"Could not create Controller Job Log for {job_id}: {log_e}")
						
					frappe.db.commit()
				
			if stream_msg_ids:
				for s_name, m_ids in stream_msg_ids.items():
					cache.xack(s_name, "telemetry_consumer_group", *m_ids)

		except Exception as e:
			frappe.db.rollback()
			logger.error(f"Telemetry loop error: {traceback.format_exc()}")
			if "NOGROUP" in str(e):
				for stream in streams:
					try:
						cache.xgroup_create(stream, "telemetry_consumer_group", id="0", mkstream=True)
					except Exception:
						pass
			time.sleep(5)

def sweep_lost_jobs():
	"""
	The Sweeper: Scheduled task running every scheduler tick.
	Finds FS Jobs queued longer than the tick interval and re-pushes to Redis ingestion stream.
	"""
	if not frappe.db.exists("DocType", "FS Job"):
		return
		
	lost_jobs = frappe.db.sql("""
		SELECT name, queue FROM `tabFS Job` 
		WHERE status='Queued'
	""", as_dict=True)
	
	cache = frappe.cache()
	for job_info in lost_jobs:
		lock_key = f"fs:started:{job_info.name}"
		if cache.get(lock_key):
			continue
			
		queue_name = job_info.get("queue")
		if queue_name not in ("low", "medium", "high"):
			continue

		job = frappe.get_doc("FS Job", job_info.name)
		job_payload = job.as_dict()
		job_payload["site"] = frappe.local.site
		msg = {"payload": json.dumps(job_payload, default=str)}
		
		try:
			zscore = cache.execute_command('ZSCORE', f"fs:scheduled:{queue_name}", json.dumps(msg))
			if zscore is not None:
				continue
		except Exception:
			pass
			
		cache.xadd(f"fs:queue:{queue_name}", msg)

def _get_controller_lock_file():
	return os.path.abspath(os.path.join(get_bench_path(), "config", "controller_process"))

def create_job_log(job_type: str, status: str, details: str = None):
	"""Helper function to insert a Controller Job Log"""
	log = frappe.new_doc("Controller Job Log")
	log.controller_job_type = job_type
	log.status = status
	log.details = details
	log.insert(ignore_permissions=True)

def clear_old_logs():
	"""
	Deletes Controller Job Logs that are older than 30 days.
	Intended to be run via daily scheduler event.
	"""
	try:
		frappe.db.sql("""
			DELETE FROM `tabFS Job Log`
			WHERE creation < DATE_SUB(NOW(), INTERVAL 30 DAY)
		""")
		frappe.db.commit()
	except Exception:
		frappe.logger("controller").error("Failed to clean up old Controller Job Logs", exc_info=True)

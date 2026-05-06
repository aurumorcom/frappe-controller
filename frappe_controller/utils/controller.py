# Copyright (c) 2026, Aurumor and contributors
# License: MIT. See LICENSE

import os
import time
import json
import threading
import queue
from typing import NoReturn
from filelock import FileLock, Timeout

import frappe
from frappe.utils import get_bench_path, get_sites, now_datetime, cint
from frappe.utils.background_jobs import set_niceness


def start_controller() -> NoReturn:
	"""
	Telemetry Consumer & Orchestrator.
	Reads from 'controller:telemetry' Redis stream, updates MariaDB,
	and actively monitors for lost jobs using Redis Keyspace events.
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

	sites = get_sites()
	if not sites:
		logger.error("No sites found")
		return
	site = sites[0]
	
	logger.info(f"Starting telemetry consumer for site {site}")
	frappe.init(site)
	frappe.connect()
	
	cache = frappe.cache()
	
	# Enable Redis Keyspace events (Ex) to catch heartbeat timeouts
	try:
		cache.execute_command("CONFIG", "SET", "notify-keyspace-events", "Ex")
		logger.info("Enabled Redis keyspace expiration events.")
	except Exception as e:
		logger.warning(f"Could not configure notify-keyspace-events: {e}")

	# 1. Startup Sweep: Catch any jobs lost while the controller was offline
	logger.info("Running initial startup sweep for orphaned jobs...")
	reconcile_orphaned_jobs()

	expired_jobs_queue = queue.Queue()

	def listen_for_expirations():
		try:
			from redis import Redis
			redis_url = frappe.conf.get("redis_cache") or "redis://localhost:13000"
			r = Redis.from_url(redis_url)
			pubsub = r.pubsub()
			pubsub.psubscribe('__keyevent@*__:expired')
			logger.info("Listening for Redis expiration events...")
			for message in pubsub.listen():
				if message['type'] == 'pmessage':
					key = message['data']
					if isinstance(key, bytes):
						key = key.decode('utf-8')
					if key.startswith('fs:started:'):
						job_id = key.split('fs:started:')[1]
						expired_jobs_queue.put(job_id)
		except Exception as e:
			logger.error(f"Keyspace listener error: {e}")

	# 2. Start background thread to listen for expired heartbeats
	threading.Thread(target=listen_for_expirations, daemon=True).start()

	streams = ["fs:started:low", "fs:started:medium", "fs:started:high", "fs:finished:low", "fs:failed:low", "fs:finished:medium", "fs:failed:medium", "fs:finished:high", "fs:failed:high"]
	
	for stream in streams:
		try:
			cache.xgroup_create(stream, "telemetry_consumer_group", id="0", mkstream=True)
		except Exception as e:
			if "BUSYGROUP" not in str(e):
				logger.warning(f"Could not create consumer group for {stream}: {e}")

	while True:
		try:
			# Process instant re-queuing for expired heartbeats
			while not expired_jobs_queue.empty():
				job_id = expired_jobs_queue.get()
				try:
					if not frappe.db.exists("FS Job", job_id):
						continue
					
					job = frappe.get_doc("FS Job", job_id)
					if job.status in ("Started", "Queued"):
						# Double check heartbeat just in case
						if cache.get(f"fs:started:{job_id}"):
							continue
							
						logger.warning(f"Heartbeat expired for job {job_id}. Re-queuing in real-time.")
						queue_name = job.queue
						job_payload = job.as_dict()
						job_payload["site"] = frappe.local.site
						msg = {"payload": json.dumps(job_payload, default=str)}
						
						try:
							if cache.execute_command('ZSCORE', f"fs:scheduled:{queue_name}", json.dumps(msg)) is not None:
								continue
							if cache.execute_command('ZSCORE', f"fs:deferred:{queue_name}", json.dumps(msg)) is not None:
								continue
						except Exception:
							pass
						
						job.db_set("status", "Queued")
						cache.xadd(f"fs:queue:{queue_name}", msg)
						frappe.db.commit()
				except Exception as e:
					logger.error(f"Error handling expired heartbeat for {job_id}: {traceback.format_exc()}")
					frappe.db.rollback()

			# Block for up to 5 seconds to get telemetry updates
			messages = cache.xreadgroup(
				"telemetry_consumer_group",
				"consumer-1",
				{s: ">" for s in streams},
				count=500,
				block=5000
			)

			if not messages:
				continue
				
			stream_msg_ids = {}
			for stream_name, stream_messages in messages:
				if isinstance(stream_name, bytes):
					stream_name = stream_name.decode("utf-8")
				if stream_name not in stream_msg_ids:
					stream_msg_ids[stream_name] = []
				
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
					total_tried = payload.get("total_tried")
					
					if isinstance(job_id, bytes): job_id = job_id.decode('utf-8')
					if isinstance(status, bytes): status = status.decode('utf-8')
					if isinstance(error, bytes): error = error.decode('utf-8')
					if isinstance(job_site, bytes): job_site = job_site.decode('utf-8')

					if not job_id:
						continue
						
					if job_site and getattr(frappe.local, "site", None) != job_site:
						frappe.init(site=job_site, force=True)
						frappe.connect()
						
					if status == "Started":
						sql = "UPDATE `tabFS Job` SET status = %s, total_tried = %s"
						values = [status, cint(total_tried or 1)]
						
						# Automatically record started_at in the correct site timezone
						# only if it hasn't been set yet, or if it's a retry
						sql += ", started_at = COALESCE(started_at, %s)"
						values.append(now_datetime())
						
						if error:
							sql += ", exc_info = %s"
							values.append(error)
						sql += " WHERE name = %s"
						values.append(job_id)
						frappe.db.sql(sql, tuple(values))
					else:
						frappe.db.sql("""
							UPDATE `tabFS Job`
							SET status = %s, exc_info = %s, ended_at = %s, time_taken = %s, total_tried = %s
							WHERE name = %s
						""", (status, error, now_datetime(), time_taken, cint(total_tried), job_id))
					
					if status in ("Finished", "Failed"):
						job_type_name = frappe.db.get_value("FS Job", job_id, "job_type")
						if job_type_name and frappe.db.get_value("Controller Job Type", job_type_name, "create_log"):
							try:
								log = frappe.new_doc("Controller Job Log")
								log.controller_job_type = job_type_name
								log.status = "Failed" if status == "Failed" else "Complete"
								log.details = error if error else f"Finished successfully after {total_tried} attempts"
								log.set_new_name()
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

def reconcile_orphaned_jobs():
	"""
	The Startup Sweeper (Reconciliation):
	Runs on boot to find 'Queued' jobs that aren't picked up,
	and 'Started' jobs that have missing heartbeats from previous runs.
	"""
	if not frappe.db.exists("DocType", "FS Job"):
		return
		
	potential_lost_jobs = frappe.db.sql("""
		SELECT name, queue, status, modified FROM `tabFS Job` 
		WHERE status IN ('Queued', 'Started')
	""", as_dict=True)
	
	cache = frappe.cache()
	for job_info in potential_lost_jobs:
		# Check Pickup Lock / Heartbeat
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
		
		# Ensure it's not already in delayed retry or rate-limit ZSETs
		try:
			if cache.execute_command('ZSCORE', f"fs:scheduled:{queue_name}", json.dumps(msg)) is not None:
				continue
			if cache.execute_command('ZSCORE', f"fs:deferred:{queue_name}", json.dumps(msg)) is not None:
				continue
		except Exception:
			pass
		
		if job.status == "Started":
			frappe.logger("controller").warning(f"Startup Sweeper found lost started job {job_info.name}. Re-queuing.")
			job.db_set("status", "Queued")
			
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
	"""
	try:
		frappe.db.sql("""
			DELETE FROM `tabFS Job Log`
			WHERE creation < DATE_SUB(NOW(), INTERVAL 30 DAY)
		""")
		frappe.db.commit()
	except Exception:
		frappe.logger("controller").error("Failed to clean up old Controller Job Logs", exc_info=True)

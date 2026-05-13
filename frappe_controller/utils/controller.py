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


class SuspendJob(Exception):
	"""Exception raised to suspend a job and free up the worker slot."""
	def __init__(self, event_key, payload=None):
		self.event_key = event_key
		self.payload = payload
		super().__init__(f"Job suspended waiting for event: {event_key}")


def wait_for_event(event_key: str, condition: str = None, consider_events_since: str = None) -> dict:
	"""
	Registers a wait condition for the current job.
	Performs a retroactive lookback to mitigate race conditions.
	"""
	if not getattr(frappe.flags, "current_job_id", None):
		raise Exception("wait_for_event can only be called within an FS Job context")

	job_id = frappe.flags.current_job_id
	
	# 1. Retroactive Lookback
	filters = {"key": event_key}
	if consider_events_since:
		filters["creation"] = [">=", consider_events_since]
	
	events = frappe.get_all("FS Event", filters=filters, fields=["argument", "creation"], order_by="creation asc")
	
	for event in events:
		argument = frappe.parse_json(event.argument)
		if not condition or frappe.safe_eval(condition, None, {"argument": argument}):
			return argument

	# 2. Register Wait Condition
	match_condition = frappe.get_doc({
		"doctype": "FS Match Condition",
		"job": job_id,
		"event_key": event_key,
		"condition": condition,
		"consider_events_since": consider_events_since,
		"is_satisfied": 0
	})
	match_condition.insert(ignore_permissions=True)
	
	# 3. Suspend Job
	raise SuspendJob(event_key)


def emit_event(key: str, argument: dict = None):
	"""
	Logs an event and notifies the orchestrator.
	"""
	event = frappe.get_doc({
		"doctype": "FS Event",
		"key": key,
		"argument": json.dumps(argument, default=str) if argument else None
	})
	event.insert(ignore_permissions=True)
	
	# Notify orchestrator via Redis Stream
	frappe.cache().xadd("fs:events", {"key": key, "event_id": event.name})


def handle_doc_event(doc, method):
	"""
	Broadcaster for DocType events.
	"""
	if doc.doctype in ("FS Event", "FS Match Condition", "FS Job"):
		return

	if not frappe.db.table_exists("FS Event"):
		return

	# 1. Generic event
	emit_event(f"doc:{doc.doctype}:{method}", doc.as_dict())
	# 2. Specific event
	emit_event(f"doc:{doc.doctype}:{method}:{doc.name}", doc.as_dict())


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
	for s in sites:
		try:
			frappe.init(s)
			frappe.connect()
			reconcile_orphaned_jobs()
		except Exception as e:
			logger.error(f"Error during startup sweep for site {s}: {e}")
		finally:
			frappe.destroy()

	# Re-init back to the first site for general operations
	frappe.init(site)
	frappe.connect()

	expired_jobs_queue = queue.Queue()

	def listen_for_expirations(site_name: str):
		try:
			frappe.init(site_name)
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
						# Key format: fs:started:site_name:job_id
						parts = key.split(':')
						if len(parts) >= 4:
							site_name = parts[2]
							job_id = parts[3]
							expired_jobs_queue.put((site_name, job_id))
		except Exception as e:
			logger.error(f"Keyspace listener error: {e}")

	# 2. Start background thread to listen for expired heartbeats
	threading.Thread(target=listen_for_expirations, args=(site,), daemon=True).start()

	streams = [
		"fs:started:low", "fs:started:medium", "fs:started:high",
		"fs:finished:low", "fs:failed:low",
		"fs:finished:medium", "fs:failed:medium",
		"fs:finished:high", "fs:failed:high",
		"fs:events"
	]
	
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
				job_site, job_id = expired_jobs_queue.get()
				try:
					if job_site and getattr(frappe.local, "site", None) != job_site:
						frappe.init(site=job_site, force=True)
						frappe.connect()

					if not frappe.db.exists("FS Job", job_id):
						continue
					
					job = frappe.get_doc("FS Job", job_id)
					if job.status in ("started", "queued"):
						# Double check heartbeat just in case
						if cache.get(f"fs:started:{job_site}:{job_id}"):
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
						
						job.db_set("status", "queued")
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
						
					if stream_name == "fs:events":
						event_key = payload.get("key")
						event_id = payload.get("event_id")
						
						if not event_key or not event_id:
							continue
							
						# Find matching wait conditions
						conditions = frappe.get_all("FS Match Condition", filters={
							"event_key": event_key,
							"is_satisfied": 0
						}, fields=["name", "job", "condition", "consider_events_since"])
						
						if not conditions:
							continue

						event_doc = frappe.get_doc("FS Event", event_id)
						event_argument = frappe.parse_json(event_doc.argument)
						
						for cond in conditions:
							# Check lookback window
							if cond.consider_events_since and event_doc.creation < cond.consider_events_since:
								continue
								
							# Evaluate condition
							if not cond.condition or frappe.safe_eval(cond.condition, None, {"argument": event_argument}):
								# Satisfy condition
								frappe.db.set_value("FS Match Condition", cond.name, "is_satisfied", 1)
								
								# Promote job from fs:deferred:
								job_doc = frappe.get_doc("FS Job", cond.job)
								queue_name = job_doc.queue
								deferred_key = f"fs:deferred:{queue_name}"
								
								# Find and remove from deferred
								items = cache.zrange(deferred_key, 0, -1)
								for item in items:
									item_str = item.decode('utf-8') if isinstance(item, bytes) else str(item)
									if f'"name": "{job_doc.name}"' in item_str:
										cache.zrem(deferred_key, item)
										# Push to queue
										cache.xadd(f"fs:queue:{queue_name}", {"payload": item_str})
										break
						
						frappe.db.commit()
						continue

					if status in ("started", "queued"):
						sql = "UPDATE `tabFS Job` SET status = %s, total_tried = %s"
						values = [status, cint(total_tried or 1)]
						
						if status == "started":
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
					
					if status in ("finished", "failed"):
						job_type_name = frappe.db.get_value("FS Job", job_id, "job_type")
						if job_type_name and frappe.db.get_value("Controller Job Type", job_type_name, "create_log"):
							try:
								log = frappe.new_doc("Controller Job Log")
								log.controller_job_type = job_type_name
								log.status = "Failed" if status == "failed" else "Complete"
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
			logger.error(f"Telemetry loop error: {traceback.format_exc()}")
			try:
				if frappe.db:
					frappe.db.rollback()
			except Exception as db_e:
				logger.error(f"Database rollback failed: {db_e}. Reconnecting...")
				try:
					frappe.connect()
				except Exception:
					pass
					
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
	Runs on boot to find 'queued' jobs that aren't picked up,
	and 'started' jobs that have missing heartbeats from previous runs.
	"""
	if not frappe.db.exists("DocType", "FS Job"):
		return
		
	potential_lost_jobs = frappe.db.sql("""
		SELECT name, queue, status, modified FROM `tabFS Job` 
		WHERE status IN ('queued', 'started')
	""", as_dict=True)
	
	cache = frappe.cache()
	for job_info in potential_lost_jobs:
		# Check Pickup Lock / Heartbeat
		lock_key = f"fs:started:{frappe.local.site}:{job_info.name}"
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
			def is_in_zset(zset_key):
				items = cache.zrange(zset_key, 0, -1)
				for item in items:
					item_str = item.decode('utf-8') if isinstance(item, bytes) else str(item)
					if f'"job_id": "{job_info.name}"' in item_str or f'"name": "{job_info.name}"' in item_str:
						return True
				return False

			if is_in_zset(f"fs:scheduled:{queue_name}") or is_in_zset(f"fs:deferred:{queue_name}"):
				continue
		except Exception:
			pass
		
		if job.status == "started":
			frappe.logger("controller").warning(f"Startup Sweeper found lost started job {job_info.name}. Re-queuing.")
			job.db_set("status", "queued")
			
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

# Copyright (c) 2026, Aurumor and contributors
# License: MIT. See LICENSE

import json
import os
import queue
import threading
import time
from typing import Any, NoReturn

import frappe
from filelock import FileLock, Timeout
from frappe import _
from frappe.utils import cint, flt, get_bench_path, get_sites, now_datetime
from frappe.utils.background_jobs import set_niceness


class JobResult(frappe._dict):
	"""Standardized Frappe-native result container matching RQJob & ScheduledJobLog schema."""

	def __init__(
		self,
		job_id: str | None = None,
		status: str = "finished",
		result: Any = None,
		exc_info: str | None = None,
		time_taken: float = 0.0,
		started_at: Any = None,
		ended_at: Any = None,
		**kwargs,
	):
		super().__init__(
			job_id=job_id,
			status=status,
			result=result,
			exc_info=exc_info,
			time_taken=time_taken,
			started_at=started_at,
			ended_at=ended_at or now_datetime(),
			**kwargs,
		)

	@property
	def is_success(self) -> bool:
		return self.status == "finished"

	@property
	def is_failure(self) -> bool:
		return self.status in ("failed", "canceled")

	@classmethod
	def ok(cls, result: Any = None, job_id: str | None = None, **kwargs) -> "JobResult":
		return cls(job_id=job_id, status="finished", result=result, **kwargs)

	@classmethod
	def fail(cls, exc_info: Any = None, job_id: str | None = None, **kwargs) -> "JobResult":
		if isinstance(exc_info, Exception):
			err_str = f"{type(exc_info).__name__}: {str(exc_info)}"
		elif exc_info is None:
			err_str = frappe.get_traceback()
		else:
			err_str = str(exc_info)
		return cls(job_id=job_id, status="failed", result=None, exc_info=err_str, **kwargs)


class SuspendJob(Exception):
	"""Exception raised to suspend a job and free up the worker slot."""

	def __init__(self, event_key, payload=None, target_timestamp=None):
		self.event_key = event_key
		self.payload = payload
		self.target_timestamp = target_timestamp
		super().__init__(f"Job suspended waiting for event: {event_key}")


def evaluate_frappe_filters(data: dict, filters: dict | list | tuple | None) -> bool:
	if not filters:
		return True
	if not isinstance(data, dict):
		return False

	if isinstance(filters, dict):
		for key, expected in filters.items():
			actual = data.get(key)
			if isinstance(expected, (list, tuple)) and len(expected) == 2:
				op, val = expected
				if op == "=" and actual != val:
					return False
				elif op == "!=" and actual == val:
					return False
				elif op == ">" and not (actual is not None and actual > val):
					return False
				elif op == "<" and not (actual is not None and actual < val):
					return False
				elif op == ">=" and not (actual is not None and actual >= val):
					return False
				elif op == "<=" and not (actual is not None and actual <= val):
					return False
				elif op == "in" and actual not in val:
					return False
				elif op == "not in" and actual in val:
					return False
				elif op == "like" and str(val).replace("%", "").lower() not in str(actual or "").lower():
					return False
			elif actual != expected:
				return False
		return True

	if isinstance(filters, (list, tuple)):
		for item in filters:
			if isinstance(item, (list, tuple)) and len(item) in (3, 4):
				field = item[1] if len(item) == 4 else item[0]
				op = item[2] if len(item) == 4 else item[1]
				val = item[3] if len(item) == 4 else item[2]
				actual = data.get(field)
				if op == "=" and actual != val:
					return False
				elif op == "!=" and actual == val:
					return False
				elif op == ">" and not (actual is not None and actual > val):
					return False
				elif op == "<" and not (actual is not None and actual < val):
					return False
				elif op == ">=" and not (actual is not None and actual >= val):
					return False
				elif op == "<=" and not (actual is not None and actual <= val):
					return False
				elif op == "in" and actual not in val:
					return False
				elif op == "not in" and actual in val:
					return False
				elif op == "like" and str(val).replace("%", "").lower() not in str(actual or "").lower():
					return False
		return True

	return False


def calculate_target_timestamp(
	date: Any = None,
	years=0,
	months=0,
	weeks=0,
	days=0,
	hours=0,
	minutes=0,
	seconds=0,
):
	from frappe.utils.data import add_to_date, get_datetime

	base_dt = get_datetime(date) if date else now_datetime()
	return add_to_date(
		base_dt,
		years=years,
		months=months,
		weeks=weeks,
		days=days,
		hours=hours,
		minutes=minutes,
		seconds=seconds,
		as_datetime=True,
	)


def publish_event(
	event: str | None = None,
	message: dict | None = None,
	room: str | None = None,
	user: str | None = None,
	doctype: str | None = None,
	docname: str | None = None,
	task_id: str | None = None,
	after_commit: bool = False,
	key: str | None = None,
	argument: dict | None = None,
):
	event_key = event or key
	event_msg = message or argument or {}
	if doctype and docname and isinstance(event_msg, dict) and "doctype" not in event_msg:
		event_msg["doctype"] = doctype
		event_msg["docname"] = docname

	def _do_publish():
		event_doc = frappe.get_doc(
			{
				"doctype": "FS Event",
				"key": event_key,
				"argument": json.dumps(event_msg, default=str) if event_msg else None,
			}
		)
		event_doc.insert(ignore_permissions=True)
		frappe.db.commit()

		payload = {"key": event_key, "event_id": event_doc.name}
		frappe.cache().xadd("fs:events", {"payload": json.dumps(payload)})

	if after_commit:
		frappe.db.after_commit(_do_publish)
	else:
		_do_publish()


emit_event = publish_event


def lookback_for_event(
	event_key: str,
	filters: dict | list | None = None,
	condition: str | None = None,
	consider_events_since: Any = None,
) -> dict | None:
	query_filters = {"key": event_key}
	if consider_events_since:
		query_filters["creation"] = [">=", consider_events_since]

	events = frappe.get_all(
		"FS Event", filters=query_filters, fields=["argument", "creation"], order_by="creation asc"
	)

	for event in events:
		argument = frappe.parse_json(event.argument) if event.argument else {}
		filters_match = evaluate_frappe_filters(argument, filters)
		cond_match = True
		if condition:
			cond_match = bool(frappe.safe_eval(condition, None, {"argument": argument}))
		if filters_match and cond_match:
			return argument

	return None


def wait_for_event(
	event_key: str,
	filters: dict | list | None = None,
	condition: str | None = None,
	consider_events_since: Any = None,
) -> dict:
	current_job_id = getattr(frappe.flags, "current_job_id", None)
	if not current_job_id:
		raise Exception("wait_for_event can only be called within an FS Job context")

	past_event = lookback_for_event(
		event_key, filters=filters, condition=condition, consider_events_since=consider_events_since
	)
	if past_event:
		match_doc_dict = {
			"doctype": "FS Match Condition",
			"job": current_job_id,
			"event_key": event_key,
			"condition": condition,
			"consider_events_since": consider_events_since,
			"is_satisfied": 1,
		}
		if frappe.db.has_column("FS Match Condition", "filters"):
			match_doc_dict["filters"] = json.dumps(filters) if filters else None
		match_condition = frappe.get_doc(match_doc_dict)
		match_condition.insert(ignore_permissions=True)
		frappe.db.commit()
		return past_event

	match_doc_dict = {
		"doctype": "FS Match Condition",
		"job": current_job_id,
		"event_key": event_key,
		"condition": condition,
		"consider_events_since": consider_events_since,
		"is_satisfied": 0,
	}
	if frappe.db.has_column("FS Match Condition", "filters"):
		match_doc_dict["filters"] = json.dumps(filters) if filters else None

	match_condition = frappe.get_doc(match_doc_dict)
	match_condition.insert(ignore_permissions=True)
	frappe.db.commit()

	past_event_toctou = lookback_for_event(
		event_key, filters=filters, condition=condition, consider_events_since=consider_events_since
	)
	if past_event_toctou:
		frappe.db.set_value("FS Match Condition", match_condition.name, "is_satisfied", 1)
		frappe.db.commit()
		return past_event_toctou

	raise SuspendJob(event_key)


def sleep_until(date: Any, as_string=False, as_datetime=False) -> None:
	from frappe.utils.data import get_datetime, get_datetime_str

	target_dt = get_datetime(date)
	if target_dt <= now_datetime():
		return

	current_job_id = getattr(frappe.flags, "current_job_id", None)
	if not current_job_id:
		frappe.throw(_("sleep_until can only be called within an active FS Job context."))

	match_doc_dict = {
		"doctype": "FS Match Condition",
		"job": current_job_id,
		"event_key": f"sleep_until:{get_datetime_str(target_dt)}",
		"is_satisfied": 0,
	}
	if frappe.db.has_column("FS Match Condition", "condition_type"):
		match_doc_dict["condition_type"] = "sleep"
	if frappe.db.has_column("FS Match Condition", "target_timestamp"):
		match_doc_dict["target_timestamp"] = target_dt

	match_condition = frappe.get_doc(match_doc_dict)
	match_condition.insert(ignore_permissions=True)
	frappe.db.commit()

	raise SuspendJob(f"sleep_until:{get_datetime_str(target_dt)}", target_timestamp=target_dt)


def sleep_for(
	years=0,
	months=0,
	weeks=0,
	days=0,
	hours=0,
	minutes=0,
	seconds=0,
) -> None:
	target_dt = calculate_target_timestamp(
		years=years,
		months=months,
		weeks=weeks,
		days=days,
		hours=hours,
		minutes=minutes,
		seconds=seconds,
	)
	sleep_until(target_dt)


def wait_for(
	event_key: str | None = None,
	filters: dict | list | None = None,
	date: Any = None,
	years=0,
	months=0,
	weeks=0,
	days=0,
	hours=0,
	minutes=0,
	seconds=0,
	as_string=False,
	as_datetime=False,
) -> dict | None:
	has_duration = any([years, months, weeks, days, hours, minutes, seconds]) or date is not None

	if not event_key and not has_duration:
		frappe.throw(_("wait_for requires either an event_key, a date, or a duration offset."))

	current_job_id = getattr(frappe.flags, "current_job_id", None)
	if not current_job_id:
		frappe.throw(_("wait_for can only be called within an active FS Job context."))

	if event_key and not has_duration:
		return wait_for_event(event_key=event_key, filters=filters)

	if has_duration and not event_key:
		target_dt = calculate_target_timestamp(
			date=date,
			years=years,
			months=months,
			weeks=weeks,
			days=days,
			hours=hours,
			minutes=minutes,
			seconds=seconds,
		)
		sleep_until(target_dt)
		return None

	target_dt = calculate_target_timestamp(
		date=date,
		years=years,
		months=months,
		weeks=weeks,
		days=days,
		hours=hours,
		minutes=minutes,
		seconds=seconds,
	)

	past_event = lookback_for_event(event_key=event_key, filters=filters)
	if past_event:
		return past_event

	if target_dt <= now_datetime():
		from frappe.utils.data import get_datetime_str

		return {"timed_out": True, "target_timestamp": get_datetime_str(target_dt)}

	match_doc_dict = {
		"doctype": "FS Match Condition",
		"job": current_job_id,
		"event_key": event_key,
		"is_satisfied": 0,
	}
	if frappe.db.has_column("FS Match Condition", "filters"):
		match_doc_dict["filters"] = json.dumps(filters) if filters else None
	if frappe.db.has_column("FS Match Condition", "condition_type"):
		match_doc_dict["condition_type"] = "event_or_timeout"
	if frappe.db.has_column("FS Match Condition", "target_timestamp"):
		match_doc_dict["target_timestamp"] = target_dt

	match_condition = frappe.get_doc(match_doc_dict)
	match_condition.insert(ignore_permissions=True)
	frappe.db.commit()

	raise SuspendJob(event_key, target_timestamp=target_dt)


def _job_matches_in_msg(item_str: str, target_job_id: str) -> bool:
	try:
		msg = json.loads(item_str)
		payload_str = msg.get("payload") if isinstance(msg, dict) else None

		if payload_str:
			payload = json.loads(payload_str)
			job_id = payload.get("name") or payload.get("job_id")
			return job_id == target_job_id
	except Exception:
		pass

	return False


def handle_doc_event(doc, method):
	if doc.doctype in ("FS Event", "FS Match Condition", "FS Job"):
		return

	if not frappe.db.table_exists("FS Event") or not frappe.db.table_exists("FS Match Condition"):
		return

	has_waiting_jobs = frappe.db.exists(
		"FS Match Condition",
		{"is_satisfied": 0, "event_key": method},
	)

	if not has_waiting_jobs and frappe.db.has_column("FS Match Condition", "filters"):
		has_waiting_jobs = frappe.db.sql(
			"SELECT 1 FROM `tabFS Match Condition` WHERE is_satisfied = 0 AND filters LIKE %s LIMIT 1",
			(f'%"doctype": "{doc.doctype}"%',),
		)

	if not has_waiting_jobs:
		return

	publish_event(
		event=method,
		doctype=doc.doctype,
		docname=doc.name,
		message=doc.as_dict(),
	)


def start_controller() -> NoReturn:
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

	try:
		cache.execute_command("CONFIG", "SET", "notify-keyspace-events", "Ex")
		logger.info("Enabled Redis keyspace expiration events.")
	except Exception as e:
		logger.warning(f"Could not configure notify-keyspace-events: {e}")

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
			pubsub.psubscribe("__keyevent@*__:expired")
			logger.info("Listening for Redis expiration events...")
			for message in pubsub.listen():
				if message["type"] == "pmessage":
					key = message["data"]
					if isinstance(key, bytes):
						key = key.decode("utf-8")
					if key.startswith("fs:started:"):
						parts = key.split(":")
						if len(parts) >= 4:
							site_name = parts[2]
							job_id = parts[3]
							expired_jobs_queue.put((site_name, job_id))
		except Exception as e:
			logger.error(f"Keyspace listener error: {e}")

	threading.Thread(target=listen_for_expirations, args=(site,), daemon=True).start()

	streams = [
		"fs:started:low",
		"fs:started:medium",
		"fs:started:high",
		"fs:telemetry:scheduled:low",
		"fs:telemetry:scheduled:medium",
		"fs:telemetry:scheduled:high",
		"fs:telemetry:deferred:low",
		"fs:telemetry:deferred:medium",
		"fs:telemetry:deferred:high",
		"fs:finished:low",
		"fs:failed:low",
		"fs:finished:medium",
		"fs:failed:medium",
		"fs:finished:high",
		"fs:failed:high",
		"fs:events",
	]

	for stream in streams:
		try:
			cache.xgroup_create(stream, "telemetry_consumer_group", id="0", mkstream=True)
		except Exception as e:
			if "BUSYGROUP" not in str(e):
				logger.warning(f"Could not create consumer group for {stream}: {e}")

	for stream in streams:
		if not cache.exists(stream):
			cache.xadd(stream, {"_ping": "1"})

	while True:
		try:
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
						if cache.get(f"fs:started:{job_site}:{job_id}"):
							continue

						logger.warning(f"Heartbeat expired for job {job_id}. Re-queuing in real-time.")
						queue_name = job.queue
						job_payload = job.as_dict()
						job_payload["site"] = frappe.local.site
						msg = {"payload": json.dumps(job_payload, default=str)}

						try:
							if (
								cache.execute_command("ZSCORE", f"fs:scheduled:{queue_name}", json.dumps(msg))
								is not None
							):
								continue
							if (
								cache.execute_command("ZSCORE", f"fs:deferred:{queue_name}", json.dumps(msg))
								is not None
							):
								continue
						except Exception:
							pass

						job.db_set("status", "queued")
						cache.xadd(f"fs:queue:{queue_name}", msg)
						frappe.db.commit()
				except Exception:
					logger.error(f"Error handling expired heartbeat for {job_id}: {traceback.format_exc()}")
					frappe.db.rollback()

			messages = cache.xreadgroup(
				"telemetry_consumer_group", "consumer-1", {s: ">" for s in streams}, count=500, block=5000
			)

			if messages:
				process_telemetry_messages(cache, messages, logger)

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
	if not frappe.db.exists("DocType", "FS Job"):
		return

	potential_lost_jobs = frappe.db.sql(
		"""
		SELECT name, queue, status, modified FROM `tabFS Job`
		WHERE status IN ('queued', 'started', 'scheduled', 'deferred')
	""",
		as_dict=True,
	)

	cache = frappe.cache()
	for job_info in potential_lost_jobs:
		if job_info.status == "started":
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

		try:

			def is_in_zset(zset_key):
				items = cache.zrange(zset_key, 0, -1)
				for item in items:
					item_str = item.decode("utf-8") if isinstance(item, bytes) else str(item)
					if _job_matches_in_msg(item_str, job_info.name):
						return True
				return False

			if is_in_zset(f"fs:scheduled:{queue_name}") or is_in_zset(f"fs:deferred:{queue_name}"):
				continue
		except Exception:
			pass

		if job.status == "scheduled":
			frappe.logger("controller").warning(
				f"Startup Sweeper found lost scheduled job {job_info.name}. Re-inserting to scheduled."
			)
			cache.zadd(f"fs:scheduled:{queue_name}", {json.dumps(msg): time.time() - 1})
		elif job.status == "deferred":
			frappe.logger("controller").warning(
				f"Startup Sweeper found lost deferred job {job_info.name}. Re-inserting to deferred."
			)
			has_pending_condition = frappe.db.exists(
				"FS Match Condition", {"job": job_info.name, "is_satisfied": 0}
			)
			score = 9999999999 if has_pending_condition else (time.time() - 1)
			cache.zadd(f"fs:deferred:{queue_name}", {json.dumps(msg): score})
		else:
			if job.status == "started":
				frappe.logger("controller").warning(
					f"Startup Sweeper found lost started job {job_info.name}. Re-queuing."
				)
				job.db_set("status", "queued")
			cache.xadd(f"fs:queue:{queue_name}", msg)


def process_telemetry_messages(cache, messages, logger=None):
	if logger is None:
		import logging

		logger = logging.getLogger("frappe_controller.telemetry")

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
			result = payload.get("result")
			job_site = payload.get("site")
			_started_at = payload.get("started_at")
			time_taken = payload.get("time_taken", 0)
			total_tried = payload.get("total_tried")

			if isinstance(job_id, bytes):
				job_id = job_id.decode("utf-8")
			if isinstance(status, bytes):
				status = status.decode("utf-8")
			if isinstance(error, bytes):
				error = error.decode("utf-8")
			if isinstance(job_site, bytes):
				job_site = job_site.decode("utf-8")

			if not job_id and stream_name != "fs:events":
				continue

			if job_site and getattr(frappe.local, "site", None) != job_site:
				frappe.init(site=job_site, force=True)
				frappe.connect()

			if job_id and frappe.db.get_value("FS Job", job_id, "status") == "canceled":
				continue

			if stream_name == "fs:events":
				event_key = payload.get("key") or payload.get(b"key")
				event_id = payload.get("event_id") or payload.get(b"event_id")

				if isinstance(event_key, bytes):
					event_key = event_key.decode("utf-8")
				if isinstance(event_id, bytes):
					event_id = event_id.decode("utf-8")

				if not event_key or not event_id:
					continue

				cond_fields = ["name", "job", "condition", "consider_events_since"]
				if frappe.db.has_column("FS Match Condition", "filters"):
					cond_fields.append("filters")

				conditions = frappe.get_all(
					"FS Match Condition",
					filters={"event_key": event_key, "is_satisfied": 0},
					fields=cond_fields,
				)

				if not conditions:
					continue

				event_doc = frappe.get_doc("FS Event", event_id)
				event_argument = frappe.parse_json(event_doc.argument) if event_doc.argument else {}

				for cond in conditions:
					if cond.consider_events_since and event_doc.creation < cond.consider_events_since:
						continue

					parsed_filters = frappe.parse_json(cond.get("filters")) if cond.get("filters") else None
					filters_matched = evaluate_frappe_filters(event_argument, parsed_filters)

					cond_matched = True
					if cond.condition:
						cond_matched = bool(frappe.safe_eval(cond.condition, None, {"argument": event_argument}))

					if filters_matched and cond_matched:
						frappe.db.set_value("FS Match Condition", cond.name, "is_satisfied", 1)

						job_doc = frappe.get_doc("FS Job", cond.job)
						queue_name = job_doc.queue
						deferred_key = f"fs:deferred:{queue_name}"

						cache.execute_command("SETEX", f"fs:promoted:{job_doc.name}", 3600, "1")

						items = cache.zrange(deferred_key, 0, -1)
						for item in items:
							item_str = item.decode("utf-8") if isinstance(item, bytes) else str(item)
							if _job_matches_in_msg(item_str, job_doc.name):
								cache.zrem(deferred_key, item)
								try:
									msg = json.loads(item_str)
									cache.xadd(f"fs:queue:{queue_name}", {"payload": msg.get("payload")})
								except Exception:
									pass
								break

				frappe.db.commit()
				continue

			if status in ("started", "queued", "scheduled", "deferred"):
				sql = "UPDATE `tabFS Job` SET status = %s, total_tried = %s"
				values = [status, cint(total_tried or 1)]

				if status == "started":
					sql += ", started_at = COALESCE(started_at, %s)"
					values.append(now_datetime())

				if error:
					sql += ", exc_info = %s"
					values.append(error)
				sql += " WHERE name = %s"
				values.append(job_id)
				frappe.db.sql(sql, tuple(values))
			else:
				result_str = json.dumps(result, default=str) if status == "finished" and result is not None else None
				frappe.db.sql(
					"""
					UPDATE `tabFS Job`
					SET status = %s, exc_info = %s, result = %s, ended_at = %s, time_taken = %s, total_tried = %s
					WHERE name = %s
				""",
					(status, error, result_str, now_datetime(), flt(time_taken), cint(total_tried), job_id),
				)

			if status in ("finished", "failed"):
				job_info = frappe.db.get_value("FS Job", job_id, ["job_type", "parent_job"])
				job_type_name = job_info[0] if job_info else None
				parent_job = job_info[1] if job_info else None

				if parent_job:
					if status == "finished":
						emit_event(f"fs_job_finished:{job_id}")
					elif status == "failed":
						frappe.db.set_value("FS Job", parent_job, "status", "failed")
						frappe.db.set_value("FS Job", parent_job, "exc_info", f"Child job {job_id} failed.")
						emit_event(f"fs_job_finished:{job_id}")

				try:
					frappe.db.sql("DELETE FROM `tabFS Match Condition` WHERE job = %s", job_id)
				except Exception as e:
					logger.warning(f"Could not delete FS Match Condition for job {job_id}: {e}")

			frappe.db.commit()

	if stream_msg_ids:
		for s_name, m_ids in stream_msg_ids.items():
			cache.xack(s_name, "telemetry_consumer_group", *m_ids)


def _get_controller_lock_file():
	return os.path.abspath(os.path.join(get_bench_path(), "config", "controller_process"))


def clear_old_jobs():
	try:
		frappe.db.sql("""
			DELETE FROM `tabFS Job`
			WHERE creation < DATE_SUB(NOW(), INTERVAL 30 DAY)
		""")
		frappe.db.sql("""
			DELETE FROM `tabFS Match Condition`
			WHERE job NOT IN (SELECT name FROM `tabFS Job`)
		""")
		frappe.db.commit()
	except Exception:
		frappe.logger("controller").error("Failed to clean up old FS Jobs", exc_info=True)

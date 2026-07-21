from unittest import mock

import frappe
from frappe.tests import IntegrationTestCase


class TestControllerJob(IntegrationTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		frappe.db.delete("Controller Job Type")
		frappe.db.delete("FS Job")

		cls.job_type = frappe.get_doc({
			"doctype": "Controller Job Type",
			"method": "frappe_controller.utils.test_controller.dummy_job",
			"create_log": 1,
			"rate_limit_per_minute": 10
		}).insert()

	@classmethod
	def tearDownClass(cls):
		frappe.db.delete("Controller Job Type")
		frappe.db.delete("FS Job")
		frappe.db.delete("FS Event")
		frappe.db.delete("FS Match Condition")
		frappe.db.commit()
		frappe.cache().delete_keys("fs:*")
		super().tearDownClass()

	def test_ingestion_push(self):
		from frappe.utils.redis_wrapper import RedisWrapper

		controller_events = frappe.get_hooks("controller_events")
		if not controller_events:
			frappe.local.app_modules["controller_events"] = {}
			controller_events = frappe.local.app_modules["controller_events"]
		controller_events["frappe_controller.utils.test_controller.dummy_job"] = {}

		with mock.patch.object(RedisWrapper, 'xadd') as mock_xadd:
			job_promise = frappe.enqueue("frappe_controller.utils.test_controller.dummy_job", queue="low", kwarg1="test")
			job_name = job_promise.job_id

			self.assertTrue(frappe.db.exists("FS Job", job_name))
			frappe.db.commit()
			self.assertTrue(mock_xadd.called)
			args, kwargs = mock_xadd.call_args
			self.assertEqual(args[0], "fs:queue:low")
			self.assertIn("payload", args[1])

	def test_config_sync(self):
		job_type = frappe.get_doc({
			"doctype": "Controller Job Type",
			"method": "frappe_controller.utils.test_controller.dummy_sync",
			"create_log": 1,
			"rate_limit_per_minute": 50,
			"timeout": 300
		}).insert()

		# check redis
		key = "fs:frappe_controller.utils.test_controller.dummy_sync:config"
		raw_limits = frappe.cache().execute_command("HGETALL", key)

		limits = {}
		if isinstance(raw_limits, dict):
			limits = raw_limits
		elif raw_limits:
			for i in range(0, len(raw_limits), 2):
				limits[raw_limits[i]] = raw_limits[i+1]

		val = limits.get(b"rate_limit_per_minute") or limits.get("rate_limit_per_minute")
		if isinstance(val, bytes):
			val = val.decode()
		self.assertEqual(val, "50")

		timeout_val = limits.get(b"timeout") or limits.get("timeout")
		if isinstance(timeout_val, bytes):
			timeout_val = timeout_val.decode()
		self.assertEqual(timeout_val, "300")

	def test_sweeper_rehydration(self):
		import json
		import time

		from frappe_controller.utils.controller import reconcile_orphaned_jobs

		cache = frappe.cache()
		cache.delete("fs:queue:low")
		cache.delete("fs:scheduled:low")
		cache.delete("fs:deferred:low")
		frappe.db.delete("FS Job")

		job_sch = frappe.get_doc({
			"doctype": "FS Job",
			"job_type": self.job_type.name,
			"job_name": "frappe_controller.utils.test_controller.dummy_job",
			"queue": "low",
			"status": "scheduled",
			"arguments": "{}"
		}).insert()

		job_def = frappe.get_doc({
			"doctype": "FS Job",
			"job_type": self.job_type.name,
			"job_name": "frappe_controller.utils.test_controller.dummy_job",
			"queue": "low",
			"status": "deferred",
			"arguments": "{}"
		}).insert()

		frappe.db.commit()

		reconcile_orphaned_jobs()

		sch_items = cache.zrange("fs:scheduled:low", 0, -1)
		self.assertEqual(len(sch_items), 1)
		self.assertIn(job_sch.name, sch_items[0].decode('utf-8'))

		def_items = cache.zrange("fs:deferred:low", 0, -1)
		self.assertEqual(len(def_items), 1)
		self.assertIn(job_def.name, def_items[0].decode('utf-8'))

		frappe.db.delete("FS Job", job_sch.name)
		frappe.db.delete("FS Job", job_def.name)
		frappe.db.commit()

class TestWaitForEvent(IntegrationTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		frappe.db.delete("FS Job")
		frappe.db.delete("Controller Job Type")
		frappe.db.delete("FS Event")
		frappe.db.delete("FS Match Condition")

		frappe.get_doc({
			"doctype": "Controller Job Type",
			"method": "frappe.ping",
			"create_log": 0
		}).insert()
		frappe.db.commit()

	def setUp(self):
		frappe.db.delete("FS Event")
		frappe.db.delete("FS Match Condition")
		frappe.db.delete("FS Job")
		frappe.db.commit()

	def test_wait_for_event_immediate_satisfaction(self):
		from frappe_controller.utils.controller import emit_event, wait_for_event

		# 1. Create a job manually to avoid real worker picking it up
		job_type_name = frappe.db.get_value("Controller Job Type", {"method": "frappe.ping"})
		job = frappe.get_doc({
			"doctype": "FS Job",
			"job_type": job_type_name,
			"job_name": "frappe.ping",
			"queue": "low",
			"status": "queued",
			"arguments": "{}"
		}).insert()
		job_id = job.name
		frappe.flags.current_job_id = job_id
		job.db_set("started_at", frappe.utils.now_datetime())

		# 2. Emit event BEFORE wait
		emit_event("test_event", {"status": "ok"})

		# 3. Call wait_for_event - should return immediately
		result = wait_for_event("test_event", consider_events_since=job.started_at)
		self.assertEqual(result.get("status"), "ok")

		# 4. Verify wait condition created and marked as satisfied
		match_condition = frappe.get_all("FS Match Condition", filters={"job": job_id, "event_key": "test_event"}, fields=["is_satisfied"])
		self.assertEqual(len(match_condition), 1)
		self.assertEqual(match_condition[0].is_satisfied, 1)

	def test_wait_for_event_invalid_context(self):
		from frappe_controller.utils.controller import wait_for_event
		frappe.flags.current_job_id = None
		with self.assertRaisesRegex(Exception, "wait_for_event can only be called within an FS Job context"):
			wait_for_event("some_event")

	def test_wait_for_event_malformed_condition(self):
		from frappe_controller.utils.controller import emit_event, wait_for_event

		job_type_name = frappe.db.get_value("Controller Job Type", {"method": "frappe.ping"})
		job = frappe.get_doc({
			"doctype": "FS Job",
			"job_type": job_type_name,
			"job_name": "frappe.ping",
			"queue": "low",
			"status": "queued",
			"arguments": "{}"
		}).insert()
		frappe.flags.current_job_id = job.name
		job.db_set("started_at", frappe.utils.now_datetime())

		emit_event("bad_cond_event", {"status": "ok"})

		with self.assertRaises(Exception): # should raise SyntaxError/NameError depending on safe_eval
			wait_for_event("bad_cond_event", condition="this is not valid python")

	def test_wait_for_event_toctou_race_prevention(self):
		import frappe_controller.utils.controller as controller_module
		from frappe_controller.utils.controller import emit_event, wait_for_event

		job_type_name = frappe.db.get_value("Controller Job Type", {"method": "frappe.ping"})
		job = frappe.get_doc({
			"doctype": "FS Job",
			"job_type": job_type_name,
			"job_name": "frappe.ping",
			"queue": "low",
			"status": "queued",
			"arguments": "{}"
		}).insert()
		job_id = job.name
		frappe.flags.current_job_id = job_id
		job.db_set("started_at", frappe.utils.now_datetime())

		original_get_all = frappe.get_all

		def patched_get_all(doctype, *args, **kwargs):
			if doctype == "FS Event" and kwargs.get("filters", {}).get("key") == "race_event":
				# Artificially emit the event right before the lookback query
				emit_event("race_event", {"value": "raced!"})
				frappe.db.commit()
			return original_get_all(doctype, *args, **kwargs)

		with mock.patch("frappe.get_all", side_effect=patched_get_all):
			result = wait_for_event("race_event")

		self.assertEqual(result.get("value"), "raced!")
		match_condition = frappe.get_all("FS Match Condition", filters={"job": job_id, "event_key": "race_event"}, fields=["is_satisfied"])
		self.assertEqual(len(match_condition), 1)
		self.assertEqual(match_condition[0].is_satisfied, 1)

	def test_wait_for_event_multiple_events_lookback_order(self):
		import time

		from frappe_controller.utils.controller import emit_event, wait_for_event

		job_type_name = frappe.db.get_value("Controller Job Type", {"method": "frappe.ping"})
		job = frappe.get_doc({
			"doctype": "FS Job",
			"job_type": job_type_name,
			"job_name": "frappe.ping",
			"queue": "low",
			"status": "queued",
			"arguments": "{}"
		}).insert()
		frappe.flags.current_job_id = job.name
		job.db_set("started_at", frappe.utils.now_datetime())

		# Emit first event - wrong status
		emit_event("ordered_event", {"status": "wrong"})
		time.sleep(0.1) # Ensure ordering by creation
		# Emit second event - right status
		emit_event("ordered_event", {"status": "right", "id": 1})
		time.sleep(0.1)
		# Emit third event - also right status, but later
		emit_event("ordered_event", {"status": "right", "id": 2})

		result = wait_for_event("ordered_event", condition="argument.get('status') == 'right'")

		# Should match the first one that met the condition chronologically
		self.assertEqual(result.get("id"), 1)

	def test_wait_for_event_suspension(self):
		from frappe_controller.utils.controller import SuspendJob, wait_for_event

		# 1. Create a job manually
		job_type_name = frappe.db.get_value("Controller Job Type", {"method": "frappe.ping"})
		job = frappe.get_doc({
			"doctype": "FS Job",
			"job_type": job_type_name,
			"job_name": "frappe.ping",
			"queue": "low",
			"status": "queued",
			"arguments": "{}"
		}).insert()
		job_id = job.name
		frappe.flags.current_job_id = job_id
		job.db_set("started_at", frappe.utils.now_datetime())

		# 2. Call wait_for_event - should raise SuspendJob
		with self.assertRaises(SuspendJob) as cm:
			wait_for_event("test_event", consider_events_since=job.started_at)

		self.assertEqual(cm.exception.event_key, "test_event")

		# 3. Verify wait condition created
		self.assertTrue(frappe.db.exists("FS Match Condition", {"job": job_id, "event_key": "test_event"}))

	def test_wait_for_event_with_condition(self):
		from frappe_controller.utils.controller import SuspendJob, emit_event, wait_for_event

		# 1. Create a job manually
		job_type_name = frappe.db.get_value("Controller Job Type", {"method": "frappe.ping"})
		job = frappe.get_doc({
			"doctype": "FS Job",
			"job_type": job_type_name,
			"job_name": "frappe.ping",
			"queue": "low",
			"status": "queued",
			"arguments": "{}"
		}).insert()
		job_id = job.name
		frappe.flags.current_job_id = job_id
		job.db_set("started_at", frappe.utils.now_datetime())

		# 2. Emit event that doesn't match condition
		emit_event("test_event", {"status": "pending"})

		# 3. Call wait_for_event - should still suspend because condition not met
		with self.assertRaises(SuspendJob):
			wait_for_event("test_event", condition="argument.get('status') == 'ok'", consider_events_since=job.started_at)

		# 4. Emit event that matches condition
		emit_event("test_event", {"status": "ok"})

		# 5. Verify lookback works with condition
		result = wait_for_event("test_event", condition="argument.get('status') == 'ok'", consider_events_since=job.started_at)
		self.assertEqual(result.get("status"), "ok")

	def test_orchestrator_event_promotion(self):
		import json

		from frappe_controller.utils.controller import emit_event, process_telemetry_messages

		# 1. Clear Redis related keys
		cache = frappe.cache()
		cache.delete("fs:deferred:low")
		cache.delete("fs:queue:low")
		cache.delete("fs:events")

		# 2. Create a dummy FS Job
		job_type_name = frappe.db.get_value("Controller Job Type", {"method": "frappe.ping"})
		job = frappe.get_doc({
			"doctype": "FS Job",
			"job_type": job_type_name,
			"job_name": "frappe.ping",
			"queue": "low",
			"status": "queued",
			"arguments": "{}"
		}).insert()

		# 3. Create an FS Match Condition
		frappe.get_doc({
			"doctype": "FS Match Condition",
			"job": job.name,
			"event_key": "test_event_key",
			"condition": None,
			"consider_events_since": None,
			"is_satisfied": 0
		}).insert(ignore_permissions=True)

		# 4. Add the job to fs:deferred:low (simulating worker suspending)
		# The worker wraps the job payload in another dictionary with 'payload' key
		job_payload = job.as_dict()
		job_payload['site'] = frappe.local.site
		deferred_msg = {"payload": json.dumps(job_payload, default=str)}
		cache.zadd("fs:deferred:low", {json.dumps(deferred_msg): 9999999999})

		# Verify it's in deferred queue
		self.assertEqual(cache.zcard("fs:deferred:low"), 1)

		# 5. Emit FS Event and generate a telemetry message format
		emit_event("test_event_key", {"status": "success"})
		event_id = frappe.get_all("FS Event", filters={"key": "test_event_key"}, limit=1)[0].name

		# 6. Call process_telemetry_messages
		telemetry_payload = json.dumps({"key": "test_event_key", "event_id": event_id})
		messages = [
			("fs:events", [("123-0", {b"payload": telemetry_payload.encode('utf-8')})])
		]
		process_telemetry_messages(cache, messages)

		# 7. Assertions
		# It should be removed from fs:deferred:low
		self.assertEqual(cache.zcard("fs:deferred:low"), 0)

		# It should be in fs:queue:low
		queue_items = cache.xrange("fs:queue:low")
		self.assertEqual(len(queue_items), 1)

		# The message in fs:queue:low must be correctly formatted
		# meaning its 'payload' key must contain a JSON string with "name": "job_id"
		queued_msg_payload_str = queue_items[0][1].get(b'payload').decode('utf-8')
		queued_msg_payload = json.loads(queued_msg_payload_str)
		self.assertEqual(queued_msg_payload.get("name"), job.name)

	def test_emit_event_redis_format(self):
		import json

		from frappe_controller.utils.controller import emit_event

		# 1. Clear fs:events stream
		cache = frappe.cache()
		cache.delete("fs:events")

		# 2. Emit an event
		emit_event("format_test", {"hello": "world"})

		# 3. Read it back
		items = cache.xrange("fs:events")
		self.assertEqual(len(items), 1)

		msg_id, fields = items[0]

		# Ensure it has a payload or b'payload' envelope
		payload_val = fields.get("payload") or fields.get(b"payload")
		self.assertIsNotNone(payload_val)

		if isinstance(payload_val, bytes):
			payload_val = payload_val.decode('utf-8')

		# Parse JSON and ensure key exists
		parsed = json.loads(payload_val)
		self.assertEqual(parsed.get("key"), "format_test")
		self.assertTrue(parsed.get("event_id"))

	def test_process_telemetry_legacy_byte_format(self):
		import json

		from frappe_controller.utils.controller import emit_event, process_telemetry_messages

		cache = frappe.cache()
		cache.delete("fs:deferred:low")
		cache.delete("fs:queue:low")
		cache.delete("fs:events")

		job_type_name = frappe.db.get_value("Controller Job Type", {"method": "frappe.ping"})
		job = frappe.get_doc({
			"doctype": "FS Job",
			"job_type": job_type_name,
			"job_name": "frappe.ping",
			"queue": "low",
			"status": "queued",
			"arguments": "{}"
		}).insert()

		frappe.get_doc({
			"doctype": "FS Match Condition",
			"job": job.name,
			"event_key": "legacy_byte_event",
			"condition": None,
			"consider_events_since": None,
			"is_satisfied": 0
		}).insert(ignore_permissions=True)

		emit_event("legacy_byte_event", {"status": "success"})
		event_id = frappe.get_all("FS Event", filters={"key": "legacy_byte_event"}, limit=1)[0].name

		# Simulate a message with pure byte keys and values (NO payload envelope)
		legacy_fields = {
			b"key": b"legacy_byte_event",
			b"event_id": event_id.encode('utf-8')
		}

		messages = [
			("fs:events", [("123-0", legacy_fields)])
		]

		process_telemetry_messages(cache, messages)

		# Check if match condition got satisfied
		match_condition = frappe.get_all("FS Match Condition", filters={"job": job.name, "event_key": "legacy_byte_event"}, fields=["is_satisfied"])
		self.assertEqual(len(match_condition), 1)
		self.assertEqual(match_condition[0].is_satisfied, 1)

	def test_orchestrator_event_promotion_race_condition(self):
		import json

		from frappe_controller.utils.controller import emit_event, process_telemetry_messages

		# 1. Clear Redis related keys
		cache = frappe.cache()
		cache.delete("fs:deferred:low")
		cache.delete("fs:queue:low")
		cache.delete("fs:events")

		# 2. Create a dummy FS Job
		job_type_name = frappe.db.get_value("Controller Job Type", {"method": "frappe.ping"})
		job = frappe.get_doc({
			"doctype": "FS Job",
			"job_type": job_type_name,
			"job_name": "frappe.ping",
			"queue": "low",
			"status": "queued",
			"arguments": "{}"
		}).insert()

		# 3. Create an FS Match Condition
		frappe.get_doc({
			"doctype": "FS Match Condition",
			"job": job.name,
			"event_key": "test_event_key_race",
			"condition": None,
			"consider_events_since": None,
			"is_satisfied": 0
		}).insert(ignore_permissions=True)

		# 4. Do NOT add the job to fs:deferred:low yet! (Simulating worker hasn't suspended yet)

		# 5. Emit FS Event and generate a telemetry message format
		emit_event("test_event_key_race", {"status": "success"})
		event_id = frappe.get_all("FS Event", filters={"key": "test_event_key_race"}, limit=1)[0].name

		# 6. Call process_telemetry_messages
		telemetry_payload = json.dumps({"key": "test_event_key_race", "event_id": event_id})
		messages = [
			("fs:events", [("123-0", {b"payload": telemetry_payload.encode('utf-8')})])
		]
		process_telemetry_messages(cache, messages)

		# 7. Assertions for Orchestrator part
		# It should set the promoted flag
		self.assertEqual(cache.execute_command("GET", f"fs:promoted:{job.name}"), b"1")

		# It should NOT be in fs:queue:low yet because it wasn't in deferred
		queue_items = cache.xrange("fs:queue:low")
		self.assertEqual(len(queue_items), 0)

		# 8. Now simulate the worker finally raising SuspendJob and checking the flag
		# (We just assert the flag exists, since the worker code is async and tested separately or via integration)
		# But we can verify the state is exactly what the worker needs to immediately re-queue.
		cache.delete(f"fs:promoted:{job.name}")

def dummy_job(**kwargs):
	pass

def dummy_sync():
	pass

import frappe
from frappe.tests import IntegrationTestCase
from unittest import mock

class TestControllerJob(IntegrationTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		frappe.db.truncate("Controller Job Type")
		frappe.db.truncate("FS Job")

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
		from frappe_controller.utils.background_jobs import enqueue
		from frappe.utils.redis_wrapper import RedisWrapper
		
		with mock.patch.object(RedisWrapper, 'xadd') as mock_xadd:
			job_promise = enqueue("frappe_controller.utils.test_controller.dummy_job", queue="low", kwarg1="test")
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

class TestWaitForEvent(IntegrationTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		frappe.db.truncate("FS Job")
		frappe.db.truncate("Controller Job Type")
		frappe.db.truncate("FS Event")
		frappe.db.truncate("FS Match Condition")
		
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
		from frappe_controller.utils.controller import wait_for_event, emit_event
		
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
		
		# 4. Verify no wait condition created (it was satisfied by lookback)
		self.assertFalse(frappe.db.exists("FS Match Condition", {"job": job_id}))

	def test_wait_for_event_suspension(self):
		from frappe_controller.utils.controller import wait_for_event, SuspendJob
		
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
		from frappe_controller.utils.controller import wait_for_event, emit_event, SuspendJob
		
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

def dummy_job(**kwargs):
	pass

def dummy_sync():
	pass

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
			"method": "frappe_controller.tests.utils.test_controller.dummy_job",
			"create_log": 1,
			"rate_limit_per_minute": 10
		}).insert()

	@classmethod
	def tearDownClass(cls):
		frappe.db.rollback()
		super().tearDownClass()

	def test_ingestion_push(self):
		from frappe_controller.utils.background_jobs import enqueue
		from frappe.utils.redis_wrapper import RedisWrapper
		
		with mock.patch.object(RedisWrapper, 'xadd') as mock_xadd:
			job_name = enqueue("frappe_controller.tests.utils.test_controller.dummy_job", queue="low", kwarg1="test")
			
			self.assertTrue(frappe.db.exists("FS Job", job_name))
			frappe.db.commit()
			self.assertTrue(mock_xadd.called)
			args, kwargs = mock_xadd.call_args
			self.assertEqual(args[0], "fs:queue:low")
			self.assertIn("payload", args[1])

	def test_config_sync(self):
		job_type = frappe.get_doc({
			"doctype": "Controller Job Type",
			"method": "frappe_controller.tests.utils.test_controller.dummy_sync",
			"create_log": 1,
			"rate_limit_per_minute": 50,
			"timeout": 300
		}).insert()
		
		# check redis
		key = "fs:frappe_controller.tests.utils.test_controller.dummy_sync:config"
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

def dummy_job(**kwargs):
	pass

def dummy_sync():
	pass

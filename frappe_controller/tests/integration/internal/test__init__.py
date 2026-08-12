import frappe
from frappe.tests import IntegrationTestCase


class TestFrappeNamespaceExtensionIntegration(IntegrationTestCase):

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		frappe.db.delete("FS Job")
		frappe.db.delete("Controller Job Type")
		frappe.db.delete("FS Event")
		frappe.db.delete("FS Match Condition")

		cls.job_type = frappe.get_doc({
			"doctype": "Controller Job Type",
			"method": "dummy_method",
			"create_log": 0
		}).insert()
		frappe.db.commit()

	@classmethod
	def tearDownClass(cls):
		frappe.db.delete("Controller Job Type")
		frappe.db.delete("FS Job")
		frappe.db.delete("FS Event")
		frappe.db.delete("FS Match Condition")
		frappe.db.commit()
		super().tearDownClass()

	def setUp(self):
		super().setUp()
		frappe.db.delete("FS Job")
		frappe.db.delete("FS Event")
		frappe.db.delete("FS Match Condition")
		frappe.db.commit()

	def test_frappe_global_wait_and_event_patches(self):
		self.assertTrue(hasattr(frappe, "wait_for"))
		self.assertTrue(hasattr(frappe, "wait_for_event"))
		self.assertTrue(hasattr(frappe, "sleep_for"))
		self.assertTrue(hasattr(frappe, "sleep_until"))
		self.assertTrue(hasattr(frappe, "publish_event"))
		self.assertFalse(hasattr(frappe, "emit_event"))
		self.assertTrue(hasattr(frappe, "Job"))
		self.assertTrue(hasattr(frappe, "DeferredJob"))
		self.assertTrue(hasattr(frappe, "JobResult"))
		self.assertFalse(hasattr(frappe, "JobPromise"))
		self.assertFalse(hasattr(frappe, "SuspendJob"))

	def test_frappe_publish_event_creates_record_and_redis(self):
		frappe.cache().delete("fs:events")
		frappe.publish_event(event="ns_test_event", message={"status": "ok"})

		events = frappe.get_all("FS Event", filters={"key": "ns_test_event"}, fields=["name", "argument"])
		self.assertEqual(len(events), 1)

		items = frappe.cache().xrange("fs:events")
		self.assertEqual(len(items), 1)

	def test_frappe_cancel_and_delete_fs_job(self):
		job = frappe.get_doc({
			"doctype": "FS Job",
			"job_type": self.job_type.name,
			"job_name": "dummy_method",
			"queue": "low",
			"status": "queued",
			"arguments": "{}"
		}).insert()
		frappe.db.commit()

		self.assertTrue(frappe.db.exists("FS Job", job.name))

		# Delete job
		deleted = frappe.delete(job.name)
		self.assertTrue(deleted)
		self.assertFalse(frappe.db.exists("FS Job", job.name))

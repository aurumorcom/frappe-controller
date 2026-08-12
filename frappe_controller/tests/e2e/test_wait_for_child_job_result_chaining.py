import json
import frappe
from frappe.tests import IntegrationTestCase
from frappe_controller.utils.controller import JobResult


class TestWaitForChildJobResultChaining(IntegrationTestCase):

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		frappe.db.delete("FS Job")
		frappe.db.delete("Controller Job Type")
		frappe.db.delete("FS Event")

		frappe.get_doc({
			"doctype": "Controller Job Type",
			"method": "frappe_controller.tests.e2e.test_wait_for_child_job_result_chaining.parent_orchestrator",
			"create_log": 0
		}).insert()
		frappe.get_doc({
			"doctype": "Controller Job Type",
			"method": "frappe_controller.tests.e2e.test_wait_for_child_job_result_chaining.child_task_ok",
			"create_log": 0
		}).insert()
		frappe.get_doc({
			"doctype": "Controller Job Type",
			"method": "frappe_controller.tests.e2e.test_wait_for_child_job_result_chaining.child_task_failed",
			"create_log": 0
		}).insert()
		frappe.db.commit()

	def setUp(self):
		super().setUp()
		frappe.db.delete("FS Job")
		frappe.db.delete("FS Event")
		frappe.db.commit()

	def test_e2e_child_job_result_ok_inspection(self):
		from frappe_controller.utils.background_jobs import Job

		child_job = frappe.get_doc({
			"doctype": "FS Job",
			"job_type": frappe.db.get_value("Controller Job Type", {"method": "frappe_controller.tests.e2e.test_wait_for_child_job_result_chaining.child_task_ok"}),
			"job_name": "frappe_controller.tests.e2e.test_wait_for_child_job_result_chaining.child_task_ok",
			"queue": "low",
			"status": "finished",
			"result": json.dumps({"count": 10}),
			"arguments": "{}"
		}).insert()

		job_handle = Job(child_job.name)
		res = job_handle.result()

		self.assertTrue(isinstance(res, JobResult))
		self.assertTrue(res.is_success)
		self.assertFalse(res.is_failure)
		self.assertEqual(res.result, {"count": 10})

	def test_e2e_child_job_result_failed_inspection(self):
		from frappe_controller.utils.background_jobs import get_job_result, Job

		child_job = frappe.get_doc({
			"doctype": "FS Job",
			"job_type": frappe.db.get_value("Controller Job Type", {"method": "frappe_controller.tests.e2e.test_wait_for_child_job_result_chaining.child_task_failed"}),
			"job_name": "frappe_controller.tests.e2e.test_wait_for_child_job_result_chaining.child_task_failed",
			"queue": "low",
			"status": "failed",
			"exc_info": "Database Connection Refused",
			"arguments": "{}"
		}).insert()

		res = get_job_result(child_job.name)
		self.assertTrue(isinstance(res, JobResult))
		self.assertTrue(res.is_failure)
		self.assertFalse(res.is_success)
		self.assertEqual(res.exc_info, "Database Connection Refused")

		job_handle = Job(child_job.name)
		with self.assertRaises(Exception) as cm:
			job_handle.result()
		self.assertIn("failed", str(cm.exception))


def parent_orchestrator():
	pass

def child_task_ok():
	pass

def child_task_failed():
	pass

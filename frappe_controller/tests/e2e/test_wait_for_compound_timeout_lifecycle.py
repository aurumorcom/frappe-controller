import json
from datetime import datetime
import frappe
from frappe.tests import IntegrationTestCase


class TestWaitForCompoundTimeoutLifecycle(IntegrationTestCase):

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		frappe.db.delete("FS Job")
		frappe.db.delete("Controller Job Type")
		frappe.db.delete("FS Event")
		frappe.db.delete("FS Match Condition")

		frappe.get_doc({
			"doctype": "Controller Job Type",
			"method": "frappe_controller.tests.e2e.test_wait_for_compound_timeout_lifecycle.payment_verification_job",
			"create_log": 0
		}).insert()
		frappe.db.commit()

	def setUp(self):
		super().setUp()
		frappe.db.delete("FS Job")
		frappe.db.delete("FS Event")
		frappe.db.delete("FS Match Condition")
		frappe.db.commit()

	def test_e2e_compound_wait_event_arrives_first(self):
		from frappe_controller.utils.controller import DeferredJob, publish_event, wait_for

		job_type_name = frappe.db.get_value("Controller Job Type", {"method": "frappe_controller.tests.e2e.test_wait_for_compound_timeout_lifecycle.payment_verification_job"})
		job = frappe.get_doc({
			"doctype": "FS Job",
			"job_type": job_type_name,
			"job_name": "frappe_controller.tests.e2e.test_wait_for_compound_timeout_lifecycle.payment_verification_job",
			"queue": "low",
			"status": "started",
			"arguments": "{}"
		}).insert()
		frappe.flags.current_job_id = job.name

		# 1. Event already arrived beforehand
		publish_event("payment_webhook:PAY-123", {"status": "success", "txn": "TXN-999"})

		# 2. Call compound wait_for - returns event immediately because of lookback
		res = wait_for(event_key="payment_webhook:PAY-123", seconds=300)
		self.assertEqual(res.get("status"), "success")
		self.assertEqual(res.get("txn"), "TXN-999")

	def test_e2e_compound_wait_already_timed_out(self):
		from frappe_controller.utils.controller import wait_for

		job_type_name = frappe.db.get_value("Controller Job Type", {"method": "frappe_controller.tests.e2e.test_wait_for_compound_timeout_lifecycle.payment_verification_job"})
		job = frappe.get_doc({
			"doctype": "FS Job",
			"job_type": job_type_name,
			"job_name": "frappe_controller.tests.e2e.test_wait_for_compound_timeout_lifecycle.payment_verification_job",
			"queue": "low",
			"status": "started",
			"arguments": "{}"
		}).insert()
		frappe.flags.current_job_id = job.name

		# Call wait_for with a past target timestamp (-10 seconds)
		res = wait_for(event_key="payment_webhook:PAY-TIMEOUT", seconds=-10)
		self.assertTrue(res.get("timed_out"))
		self.assertIsNotNone(res.get("target_timestamp"))


def payment_verification_job():
	pass

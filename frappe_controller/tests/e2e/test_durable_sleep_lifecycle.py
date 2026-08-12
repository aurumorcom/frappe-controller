import json
import frappe
from frappe.tests import IntegrationTestCase


class TestDurableSleepLifecycle(IntegrationTestCase):

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		frappe.db.delete("FS Job")
		frappe.db.delete("Controller Job Type")
		frappe.db.delete("FS Match Condition")

		frappe.get_doc({
			"doctype": "Controller Job Type",
			"method": "frappe_controller.tests.e2e.test_durable_sleep_lifecycle.drip_campaign_job",
			"create_log": 0
		}).insert()
		frappe.db.commit()

	def setUp(self):
		super().setUp()
		frappe.db.delete("FS Job")
		frappe.db.delete("FS Match Condition")
		frappe.db.commit()

	def test_e2e_durable_sleep_for_suspension(self):
		from frappe_controller.utils.controller import SuspendJob

		job_type_name = frappe.db.get_value("Controller Job Type", {"method": "frappe_controller.tests.e2e.test_durable_sleep_lifecycle.drip_campaign_job"})
		job = frappe.get_doc({
			"doctype": "FS Job",
			"job_type": job_type_name,
			"job_name": "frappe_controller.tests.e2e.test_durable_sleep_lifecycle.drip_campaign_job",
			"queue": "low",
			"status": "started",
			"arguments": "{}"
		}).insert()
		frappe.flags.current_job_id = job.name

		# Job calls frappe.sleep_for(seconds=2)
		with self.assertRaises(SuspendJob):
			frappe.sleep_for(seconds=2)

		cond_fields = ["name", "job"]
		if frappe.db.has_column("FS Match Condition", "condition_type"):
			cond_fields.append("condition_type")
		if frappe.db.has_column("FS Match Condition", "target_timestamp"):
			cond_fields.append("target_timestamp")

		cond = frappe.get_all("FS Match Condition", filters={"job": job.name}, fields=cond_fields)
		self.assertEqual(len(cond), 1)
		if "condition_type" in cond[0]:
			self.assertEqual(cond[0].condition_type, "sleep")


def drip_campaign_job():
	pass

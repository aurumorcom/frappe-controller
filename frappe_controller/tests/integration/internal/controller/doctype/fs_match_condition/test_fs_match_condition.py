import frappe
from frappe.tests import IntegrationTestCase


class TestFSMatchConditionIntegration(IntegrationTestCase):

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		frappe.db.delete("FS Match Condition")
		frappe.db.delete("FS Job")
		frappe.db.commit()

	def setUp(self):
		super().setUp()
		frappe.db.delete("FS Match Condition")
		frappe.db.delete("FS Job")
		frappe.db.commit()

	def test_fs_match_condition_schema_expansion(self):
		job_type = frappe.db.get_value("Controller Job Type", {"method": "frappe.ping"}) or frappe.get_doc({
			"doctype": "Controller Job Type",
			"method": "frappe.ping",
			"create_log": 0
		}).insert(ignore_permissions=True).name

		job = frappe.get_doc({
			"doctype": "FS Job",
			"job_type": job_type,
			"job_name": "frappe.ping",
			"queue": "low",
			"status": "queued",
			"arguments": "{}"
		}).insert(ignore_permissions=True)

		doc_dict = {
			"doctype": "FS Match Condition",
			"job": job.name,
			"event_key": "payment_webhook",
			"condition": "argument.get('status') == 'ok'",
			"is_satisfied": 0,
		}
		if frappe.db.has_column("FS Match Condition", "filters"):
			doc_dict["filters"] = '{"amount": [">", 100]}'
		if frappe.db.has_column("FS Match Condition", "condition_type"):
			doc_dict["condition_type"] = "event_or_timeout"
		if frappe.db.has_column("FS Match Condition", "target_timestamp"):
			doc_dict["target_timestamp"] = frappe.utils.now_datetime()
		if frappe.db.has_column("FS Match Condition", "group_id"):
			doc_dict["group_id"] = "GRP-001"
		if frappe.db.has_column("FS Match Condition", "logical_operator"):
			doc_dict["logical_operator"] = "AND"

		doc = frappe.get_doc(doc_dict).insert(ignore_permissions=True)
		frappe.db.commit()

		saved = frappe.get_doc("FS Match Condition", doc.name)
		self.assertEqual(saved.job, job.name)

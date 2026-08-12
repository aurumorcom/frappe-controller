import json
import frappe
from frappe.tests import IntegrationTestCase


class TestWaitForDocTypeLifecycle(IntegrationTestCase):

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		frappe.db.delete("FS Job")
		frappe.db.delete("Controller Job Type")
		frappe.db.delete("FS Event")
		frappe.db.delete("FS Match Condition")

		frappe.get_doc({
			"doctype": "Controller Job Type",
			"method": "frappe_controller.tests.e2e.test_wait_for_doctype_lifecycle.order_fulfillment_job",
			"create_log": 0
		}).insert()
		frappe.db.commit()

	def setUp(self):
		super().setUp()
		frappe.db.delete("FS Job")
		frappe.db.delete("FS Event")
		frappe.db.delete("FS Match Condition")
		frappe.db.commit()

	def test_e2e_doctype_lifecycle_suspension_and_wake(self):
		from frappe_controller.utils.controller import DeferredJob, handle_doc_event, process_telemetry_messages

		cache = frappe.cache()
		cache.delete("fs:deferred:low")
		cache.delete("fs:queue:low")
		cache.delete("fs:events")

		job_type_name = frappe.db.get_value("Controller Job Type", {"method": "frappe_controller.tests.e2e.test_wait_for_doctype_lifecycle.order_fulfillment_job"})
		job = frappe.get_doc({
			"doctype": "FS Job",
			"job_type": job_type_name,
			"job_name": "frappe_controller.tests.e2e.test_wait_for_doctype_lifecycle.order_fulfillment_job",
			"queue": "low",
			"status": "started",
			"arguments": "{}"
		}).insert()
		frappe.flags.current_job_id = job.name

		# 1. Job calls frappe.wait_for
		with self.assertRaises(DeferredJob):
			frappe.wait_for(event_key="on_update", filters={"doctype": "Customer", "customer_name": "Acme Corp"})

		# 2. Verify FS Match Condition created
		match_cond = frappe.get_all("FS Match Condition", filters={"job": job.name}, fields=["name", "event_key"])
		self.assertEqual(len(match_cond), 1)

		# 3. Simulate Worker moving job to fs:deferred:low
		job_payload = job.as_dict()
		job_payload["site"] = frappe.local.site
		cache.zadd("fs:deferred:low", {json.dumps({"payload": json.dumps(job_payload, default=str)}): 9999999999})

		# 4. Trigger doc event for Customer
		doc = frappe._dict({
			"doctype": "Customer",
			"name": "CUST-ACME",
			"customer_name": "Acme Corp",
			"as_dict": lambda: {"doctype": "Customer", "name": "CUST-ACME", "customer_name": "Acme Corp"}
		})
		handle_doc_event(doc, "on_update")

		# 5. Read stream and process telemetry
		events_items = cache.xrange("fs:events")
		self.assertEqual(len(events_items), 1)

		messages = [("fs:events", events_items)]
		process_telemetry_messages(cache, messages)

		# 6. Assert job promoted to fs:queue:low
		queue_items = cache.xrange("fs:queue:low")
		self.assertEqual(len(queue_items), 1)

		# Match condition satisfied
		self.assertEqual(frappe.db.get_value("FS Match Condition", match_cond[0].name, "is_satisfied"), 1)


def order_fulfillment_job():
	pass

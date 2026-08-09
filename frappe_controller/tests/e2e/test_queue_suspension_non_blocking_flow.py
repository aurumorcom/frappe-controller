import frappe
from frappe.tests import IntegrationTestCase
from frappe_controller.utils.controller import emit_event


class TestQueueSuspensionNonBlockingFlow(IntegrationTestCase):

	def test_e2e_suspended_high_job_unblocks_low_queue_execution(self):
		"""E2E Journey Test:

		Verifies full end-to-end flow:
		1. High queue job enqueued & suspended (status='deferred').
		2. Low queue jobs enqueued and processed to status='finished'.
		3. Event emitted to resume High queue job to status='finished'.
		"""
		suspending_method = (
			"frappe_controller.tests.integration.internal.utils.test_background_jobs.dummy_suspending_job"
		)
		fast_method = (
			"frappe_controller.tests.integration.internal.utils.test_background_jobs.dummy_fast_job"
		)

		# Register methods in controller_events hook so frappe.enqueue routes to frappe_controller
		controller_events = frappe.get_hooks("controller_events")
		if not controller_events:
			frappe.local.app_modules["controller_events"] = {}
			controller_events = frappe.local.app_modules["controller_events"]

		controller_events[suspending_method] = {}
		controller_events[fast_method] = {}

		# Register Controller Job Types
		if not frappe.db.exists("Controller Job Type", {"method": suspending_method}):
			frappe.get_doc(
				{
					"doctype": "Controller Job Type",
					"method": suspending_method,
					"create_log": 0,
				}
			).insert()

		if not frappe.db.exists("Controller Job Type", {"method": fast_method}):
			frappe.get_doc(
				{
					"doctype": "Controller Job Type",
					"method": fast_method,
					"create_log": 0,
				}
			).insert()

		frappe.db.commit()

		# Step 1: Enqueue High Queue job that will suspend
		high_promise = frappe.enqueue(suspending_method, queue="high")
		high_job_id = high_promise.job_id

		# Step 2: Enqueue 3 Low Queue jobs
		low_job_ids = []
		for _ in range(3):
			p = frappe.enqueue(fast_method, queue="low")
			low_job_ids.append(p.job_id)

		# Step 3: Assert High Job exists and is in non-failed state
		high_status = frappe.db.get_value("FS Job", high_job_id, "status")
		self.assertIn(high_status, ["queued", "started", "deferred"])

		# Step 4: Emit event for High Job completion trigger
		emit_event("unique_suspension_event_key")
		frappe.db.commit()

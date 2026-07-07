# Copyright (c) 2026, Aurumor and contributors
# License: MIT. See LICENSE

import frappe
from frappe.tests import IntegrationTestCase
from frappe_controller.controller.doctype.controller_job_type.controller_job_type import sync_jobs

class TestControllerJobType(IntegrationTestCase):
	def setUp(self):
		frappe.db.rollback()
		frappe.db.truncate("Controller Job Type")
		frappe.db.truncate("FS Job")

	def test_sync_jobs_from_hooks(self):
		test_hooks = {
			"method.one": {"rate_limit_per_minute": 10},
			"method.two": {"rate_limit_per_hour": 2}
		}
		
		sync_jobs(hooks=test_hooks)

		self.assertTrue(frappe.db.exists("Controller Job Type", {"method": "method.one"}))
		self.assertTrue(frappe.db.exists("Controller Job Type", {"method": "method.two"}))

		job1 = frappe.get_doc("Controller Job Type", {"method": "method.one"})
		self.assertEqual(job1.rate_limit_per_minute, 10)

		# Test cleanup of orphaned jobs
		sync_jobs(hooks={"method.one": {}})
		
		# They are stopped, not deleted
		job2 = frappe.get_doc("Controller Job Type", {"method": "method.two"})
		self.assertEqual(job2.stopped, 1)

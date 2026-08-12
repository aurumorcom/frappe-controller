from datetime import datetime
from unittest import TestCase
from unittest.mock import MagicMock, patch

import frappe
from frappe.tests import UnitTestCase
from frappe_controller.utils.controller import (
	JobResult,
	SuspendJob,
	calculate_target_timestamp,
	evaluate_frappe_filters,
	publish_event,
	sleep_for,
	wait_for,
)


class TestJobResultUnit(UnitTestCase):

	def test_job_result_default_initialization(self):
		res = JobResult(job_id="JOB-100")
		self.assertEqual(res.job_id, "JOB-100")
		self.assertEqual(res.status, "finished")
		self.assertIsNone(res.result)
		self.assertIsNone(res.exc_info)
		self.assertEqual(res.time_taken, 0.0)
		self.assertIsNotNone(res.ended_at)

	def test_job_result_is_success_property(self):
		self.assertTrue(JobResult(status="finished").is_success)
		self.assertFalse(JobResult(status="failed").is_success)
		self.assertFalse(JobResult(status="canceled").is_success)

	def test_job_result_is_failure_property(self):
		self.assertTrue(JobResult(status="failed").is_failure)
		self.assertTrue(JobResult(status="canceled").is_failure)
		self.assertFalse(JobResult(status="finished").is_failure)

	def test_job_result_ok_factory(self):
		res = JobResult.ok(result={"data": 42}, job_id="JOB-101", time_taken=1.5)
		self.assertEqual(res.status, "finished")
		self.assertEqual(res.result, {"data": 42})
		self.assertEqual(res.time_taken, 1.5)
		self.assertTrue(res.is_success)

	def test_job_result_fail_factory_with_string(self):
		res = JobResult.fail(exc_info="SyntaxError: invalid syntax", job_id="JOB-102")
		self.assertEqual(res.status, "failed")
		self.assertIsNone(res.result)
		self.assertEqual(res.exc_info, "SyntaxError: invalid syntax")
		self.assertTrue(res.is_failure)

	def test_job_result_fail_factory_with_exception_object(self):
		exc = ValueError("Invalid argument")
		res = JobResult.fail(exc_info=exc, job_id="JOB-103")
		self.assertEqual(res.status, "failed")
		self.assertIn("ValueError: Invalid argument", str(res.exc_info))

	def test_job_result_dict_attribute_access(self):
		res = JobResult.ok(result="test_data", job_id="JOB-104")
		self.assertEqual(res.status, res["status"])
		self.assertEqual(res.result, res["result"])


class TestCalculateTargetTimestampUnit(UnitTestCase):

	def test_calculate_target_timestamp_from_now(self):
		now = datetime(2026, 1, 1, 12, 0, 0)
		with patch("frappe_controller.utils.controller.now_datetime", return_value=now):
			dt = calculate_target_timestamp(hours=2, minutes=30)
			self.assertEqual(dt, datetime(2026, 1, 1, 14, 30, 0))

	def test_calculate_target_timestamp_from_explicit_date(self):
		dt = calculate_target_timestamp(date="2026-01-01 00:00:00", days=5, weeks=1)
		self.assertEqual(dt, datetime(2026, 1, 13, 0, 0, 0))

	def test_calculate_target_timestamp_leap_year(self):
		dt = calculate_target_timestamp(date="2024-02-28 12:00:00", days=1)
		self.assertEqual(dt, datetime(2024, 2, 29, 12, 0, 0))

	def test_calculate_target_timestamp_negative_offsets(self):
		dt = calculate_target_timestamp(date="2026-06-15 10:00:00", hours=-5)
		self.assertEqual(dt, datetime(2026, 6, 15, 5, 0, 0))


class TestEvaluateFrappeFiltersUnit(UnitTestCase):

	def test_evaluate_frappe_filters_empty(self):
		self.assertTrue(evaluate_frappe_filters({"a": 1}, None))
		self.assertTrue(evaluate_frappe_filters({"a": 1}, {}))

	def test_evaluate_frappe_filters_non_dict_data(self):
		self.assertFalse(evaluate_frappe_filters("invalid", {"a": 1}))
		self.assertFalse(evaluate_frappe_filters(None, {"a": 1}))

	def test_evaluate_frappe_filters_dict_equality_and_operators(self):
		data = {"status": "Submitted", "amount": 150, "customer": "CUST-001"}

		self.assertTrue(evaluate_frappe_filters(data, {"status": "Submitted"}))
		self.assertFalse(evaluate_frappe_filters(data, {"status": "Draft"}))

		self.assertTrue(evaluate_frappe_filters(data, {"amount": [">", 100]}))
		self.assertFalse(evaluate_frappe_filters(data, {"amount": [">", 200]}))

		self.assertTrue(evaluate_frappe_filters(data, {"amount": ["<", 200]}))
		self.assertTrue(evaluate_frappe_filters(data, {"amount": [">=", 150]}))
		self.assertTrue(evaluate_frappe_filters(data, {"amount": ["<=", 150]}))
		self.assertTrue(evaluate_frappe_filters(data, {"status": ["!=", "Draft"]}))

		self.assertTrue(evaluate_frappe_filters(data, {"customer": ["in", ["CUST-001", "CUST-002"]]}))
		self.assertTrue(evaluate_frappe_filters(data, {"customer": ["not in", ["CUST-003"]]}))
		self.assertTrue(evaluate_frappe_filters(data, {"customer": ["like", "%CUST%"]}))

	def test_evaluate_frappe_filters_list_syntax(self):
		data = {"doctype": "Sales Order", "docstatus": 1, "grand_total": 600}
		filters = [
			["Sales Order", "docstatus", "=", 1],
			["Sales Order", "grand_total", ">=", 500],
		]
		self.assertTrue(evaluate_frappe_filters(data, filters))

		failed_filters = [
			["Sales Order", "docstatus", "=", 1],
			["Sales Order", "grand_total", ">", 1000],
		]
		self.assertFalse(evaluate_frappe_filters(data, failed_filters))

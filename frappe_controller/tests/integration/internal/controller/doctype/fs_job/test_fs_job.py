# Copyright (c) 2026, Aurumor and contributors
# License: MIT. See LICENSE

import json
from unittest.mock import MagicMock, patch

import frappe
from frappe.tests import IntegrationTestCase

from frappe_controller.utils.controller import process_telemetry_messages


class TestFSJobControl(IntegrationTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		# Insert a dummy Controller Job Type if not exists
		if not frappe.db.exists("Controller Job Type", "dummy_job"):
			cls.job_type = frappe.get_doc(
				{
					"doctype": "Controller Job Type",
					"method": "dummy_job",
					"stopped": 0,
					"create_log": 1,
					"retries": 0,
				}
			).insert(ignore_permissions=True)
		else:
			cls.job_type = frappe.get_doc("Controller Job Type", "dummy_job")
		frappe.db.commit()

	@classmethod
	def tearDownClass(cls):
		frappe.db.rollback()
		super().tearDownClass()

	def setUp(self):
		super().setUp()
		frappe.db.delete("FS Job")
		frappe.db.delete("FS Match Condition")
		if frappe.db.table_exists("Controller Job Log"):
			frappe.db.delete("Controller Job Log")
		frappe.db.commit()
		frappe.cache().delete_keys("fs:*")

	def tearDown(self):
		frappe.db.rollback()
		super().tearDown()

	def test_global_frappe_cancel_fs_job(self):
		# Create an FS Job in queued status
		job = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "queued",
				"arguments": "{}",
			}
		).insert()
		frappe.db.commit()

		cache = frappe.cache()
		cache.set(f"fs:started:{frappe.local.site}:{job.name}", "1")
		cache.set(f"fs:promoted:{job.name}", "1")

		res = frappe.cancel(job.name)
		self.assertTrue(res)

		job.reload()
		self.assertEqual(job.status, "canceled")
		self.assertIsNone(cache.get(f"fs:started:{frappe.local.site}:{job.name}"))
		self.assertIsNone(cache.get(f"fs:promoted:{job.name}"))

	@patch("frappe.db.exists")
	@patch("frappe.get_doc")
	def test_global_frappe_cancel_rq_job(self, mock_get_doc, mock_db_exists):
		# Setup mock to return True for RQ Job and False for FS Job
		mock_db_exists.side_effect = lambda dt, name: dt == "RQ Job" and name == "mock_rq_id"

		mock_rq_job = MagicMock()
		mock_rq_job.status = "queued"

		# Patch get_doc to return our mock RQ Job
		mock_get_doc.side_effect = lambda dt, name: (
			mock_rq_job if dt == "RQ Job" and name == "mock_rq_id" else None
		)

		res = frappe.cancel("mock_rq_id")
		self.assertTrue(res)
		mock_rq_job.cancel.assert_called_once()

		# Test running RQ Job stop_job delegation
		mock_rq_job.cancel.reset_mock()
		mock_rq_job.status = "started"
		res = frappe.cancel("mock_rq_id")
		self.assertTrue(res)
		mock_rq_job.stop_job.assert_called_once()

	def test_idempotency_cancel(self):
		job = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "finished",
				"arguments": "{}",
			}
		).insert()
		frappe.db.commit()

		res = job.cancel()
		self.assertFalse(res)
		job.reload()
		self.assertEqual(job.status, "finished")

	def test_redis_outage_resilience_during_cancel(self):
		cache = frappe.cache()
		original_delete = cache.delete
		# Mock delete to raise a connection error
		import redis

		cache.delete = MagicMock(side_effect=redis.exceptions.ConnectionError("Outage"))

		try:
			job = frappe.get_doc(
				{
					"doctype": "FS Job",
					"job_type": self.job_type.name,
					"job_name": "dummy_method",
					"queue": "low",
					"status": "started",
					"arguments": "{}",
				}
			).insert()
			frappe.db.commit()

			# Should catch ConnectionError and still set status in DB to canceled
			res = job.cancel()
			self.assertTrue(res)
			job.reload()
			self.assertEqual(job.status, "canceled")
		finally:
			cache.delete = original_delete

	def test_telemetry_receipt_for_already_canceled_job(self):
		job = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "canceled",
				"arguments": "{}",
			}
		).insert()
		frappe.db.commit()

		telemetry_payload = json.dumps(
			{
				"job_id": job.name,
				"status": "finished",
				"site": frappe.local.site,
				"total_tried": 1,
				"result": {"data": "done"},
			}
		)
		messages = [("fs:finished:low", [("123-0", {b"payload": telemetry_payload.encode("utf-8")})])]

		process_telemetry_messages(frappe.cache(), messages)

		# Job should NOT be deleted and status remains canceled
		self.assertTrue(frappe.db.exists("FS Job", job.name))
		job.reload()
		self.assertEqual(job.status, "canceled")
		if frappe.db.table_exists("Controller Job Log"):
			self.assertFalse(frappe.db.exists("Controller Job Log", {"job": job.name}))

	def test_started_and_cancelled_race_condition(self):
		job = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "canceled",
				"arguments": "{}",
			}
		).insert()
		frappe.db.commit()

		telemetry_payload = json.dumps(
			{"job_id": job.name, "status": "started", "site": frappe.local.site, "total_tried": 1}
		)
		messages = [("fs:started:low", [("123-0", {b"payload": telemetry_payload.encode("utf-8")})])]

		process_telemetry_messages(frappe.cache(), messages)

		job.reload()
		self.assertEqual(job.status, "canceled")
		self.assertIsNone(frappe.cache().get(f"fs:started:{frappe.local.site}:{job.name}"))

	def test_promoter_race_condition_under_cancellation(self):
		# Promoter race condition check
		job = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "deferred",
				"arguments": "{}",
			}
		).insert()
		frappe.db.commit()

		cache = frappe.cache()
		cache.set(f"fs:promoted:{job.name}", "1")

		res = job.cancel()
		self.assertTrue(res)
		job.reload()
		self.assertEqual(job.status, "canceled")
		self.assertIsNone(cache.get(f"fs:promoted:{job.name}"))

	def test_cancel_with_null_empty_or_missing_ids(self):
		self.assertFalse(frappe.cancel(None))
		self.assertFalse(frappe.cancel(""))
		self.assertFalse(frappe.cancel("non_existent_id"))

	def test_bulk_cancel_mixed_and_invalid_filters(self):
		job1 = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "queued",
				"arguments": "{}",
			}
		).insert()
		job2 = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "queued",
				"arguments": "{}",
			}
		).insert()
		frappe.db.commit()

		# Passing invalid filters should not crash
		frappe.bulk_cancel(frappe_filter={"non_existent_field": "some_value"})

		job1.reload()
		job2.reload()
		self.assertEqual(job1.status, "queued")
		self.assertEqual(job2.status, "queued")

	def test_bulk_cancel_by_status_and_queue_filter(self):
		job_a = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "queued",
				"arguments": "{}",
			}
		).insert()
		job_b = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "deferred",
				"arguments": "{}",
			}
		).insert()
		job_c = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "high",
				"status": "queued",
				"arguments": "{}",
			}
		).insert()
		frappe.db.commit()

		frappe.bulk_cancel(frappe_filter={"status": "queued", "queue": "low"})

		job_a.reload()
		job_b.reload()
		job_c.reload()

		self.assertEqual(job_a.status, "canceled")
		self.assertEqual(job_b.status, "deferred")
		self.assertEqual(job_c.status, "queued")

	def test_bulk_cancel_with_list_filters(self):
		job1 = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "queued",
				"arguments": "{}",
			}
		).insert()
		job2 = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "deferred",
				"arguments": "{}",
			}
		).insert()
		job3 = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "finished",
				"arguments": "{}",
			}
		).insert()
		frappe.db.commit()

		frappe.bulk_cancel(frappe_filter=[["status", "in", ["queued", "deferred"]]])

		job1.reload()
		job2.reload()
		job3.reload()

		self.assertEqual(job1.status, "canceled")
		self.assertEqual(job2.status, "canceled")
		self.assertEqual(job3.status, "finished")

	def test_fs_job_cascading_match_conditions_cleanup(self):
		job = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "queued",
				"arguments": "{}",
			}
		).insert()

		frappe.get_doc(
			{"doctype": "FS Match Condition", "job": job.name, "event_key": "test_event", "condition": "1"}
		).insert()

		frappe.get_doc(
			{"doctype": "FS Match Condition", "job": job.name, "event_key": "test_event2", "condition": "1"}
		).insert()
		frappe.db.commit()

		self.assertEqual(frappe.db.count("FS Match Condition", {"job": job.name}), 2)

		res = job.cancel()
		self.assertTrue(res)

		self.assertEqual(frappe.db.count("FS Match Condition", {"job": job.name}), 0)

	def test_job_deletion_cascading_cancellation(self):
		job = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "queued",
				"arguments": "{}",
			}
		).insert()

		frappe.get_doc(
			{"doctype": "FS Match Condition", "job": job.name, "event_key": "test_event", "condition": "1"}
		).insert()
		frappe.db.commit()

		cache = frappe.cache()
		cache.set(f"fs:started:{frappe.local.site}:{job.name}", "1")

		# Delete the document
		job.delete()
		frappe.db.commit()

		self.assertFalse(frappe.db.exists("FS Job", job.name))
		self.assertEqual(frappe.db.count("FS Match Condition", {"job": job.name}), 0)
		self.assertIsNone(cache.get(f"fs:started:{frappe.local.site}:{job.name}"))

	def test_delete_finished_job(self):
		job = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "finished",
				"arguments": "{}",
			}
		).insert()
		frappe.db.commit()

		job.delete()
		frappe.db.commit()

		self.assertFalse(frappe.db.exists("FS Job", job.name))

	def test_bulk_delete_cleans_up_active_jobs(self):
		job1 = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "started",
				"arguments": "{}",
			}
		).insert()
		job2 = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "queued",
				"arguments": "{}",
			}
		).insert()
		frappe.db.commit()

		cache = frappe.cache()
		cache.set(f"fs:started:{frappe.local.site}:{job1.name}", "1")
		cache.set(f"fs:started:{frappe.local.site}:{job2.name}", "1")

		frappe.delete_doc("FS Job", job1.name)
		frappe.delete_doc("FS Job", job2.name)
		frappe.db.commit()

		self.assertFalse(frappe.db.exists("FS Job", job1.name))
		self.assertFalse(frappe.db.exists("FS Job", job2.name))
		self.assertIsNone(cache.get(f"fs:started:{frappe.local.site}:{job1.name}"))
		self.assertIsNone(cache.get(f"fs:started:{frappe.local.site}:{job2.name}"))

	def test_cascading_cancellation_of_child_jobs(self):
		parent = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "queued",
				"arguments": "{}",
			}
		).insert()

		child1 = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "queued",
				"parent_job": parent.name,
				"arguments": "{}",
			}
		).insert()

		child2 = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "queued",
				"parent_job": parent.name,
				"arguments": "{}",
			}
		).insert()
		frappe.db.commit()

		res = parent.cancel()
		self.assertTrue(res)

		parent.reload()
		child1.reload()
		child2.reload()

		self.assertEqual(parent.status, "canceled")
		self.assertEqual(child1.status, "canceled")
		self.assertEqual(child2.status, "canceled")

	def test_cascading_deletion_of_child_jobs(self):
		parent = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "queued",
				"arguments": "{}",
			}
		).insert()

		child1 = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "queued",
				"parent_job": parent.name,
				"arguments": "{}",
			}
		).insert()

		child2 = frappe.get_doc(
			{
				"doctype": "FS Job",
				"job_type": self.job_type.name,
				"job_name": "dummy_method",
				"queue": "low",
				"status": "queued",
				"parent_job": parent.name,
				"arguments": "{}",
			}
		).insert()
		frappe.db.commit()

		parent.delete()
		frappe.db.commit()

		self.assertFalse(frappe.db.exists("FS Job", parent.name))
		self.assertFalse(frappe.db.exists("FS Job", child1.name))
		self.assertFalse(frappe.db.exists("FS Job", child2.name))

	def test_cancel_running_job_signals_controller(self):
		cache = frappe.cache()
		original_publish = cache.publish
		cache.publish = MagicMock()

		try:
			job = frappe.get_doc(
				{
					"doctype": "FS Job",
					"job_type": self.job_type.name,
					"job_name": "dummy_method",
					"queue": "low",
					"status": "started",
					"arguments": "{}",
				}
			).insert()
			frappe.db.commit()

			res = job.cancel()
			self.assertTrue(res)

			cache.publish.assert_called_with("fs:cancelled", job.name)
			job.reload()
			self.assertEqual(job.status, "canceled")
		finally:
			cache.publish = original_publish

	def test_delete_job_signals_controller(self):
		cache = frappe.cache()
		original_publish = cache.publish
		cache.publish = MagicMock()

		try:
			job = frappe.get_doc(
				{
					"doctype": "FS Job",
					"job_type": self.job_type.name,
					"job_name": "dummy_method",
					"queue": "low",
					"status": "queued",
					"arguments": "{}",
				}
			).insert()
			frappe.db.commit()

			job.delete()
			frappe.db.commit()

			cache.publish.assert_called_with("fs:deleted", job.name)
			self.assertFalse(frappe.db.exists("FS Job", job.name))
		finally:
			cache.publish = original_publish

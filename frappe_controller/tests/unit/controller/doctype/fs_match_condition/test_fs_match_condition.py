from frappe.tests import UnitTestCase
from frappe_controller.controller.doctype.fs_match_condition.fs_match_condition import FSMatchCondition


class TestFSMatchConditionUnit(UnitTestCase):

	def test_fs_match_condition_doc_class(self):
		doc = FSMatchCondition({"doctype": "FS Match Condition", "job": "JOB-1", "event_key": "evt"})
		self.assertEqual(doc.doctype, "FS Match Condition")
		self.assertEqual(doc.job, "JOB-1")
		self.assertEqual(doc.event_key, "evt")

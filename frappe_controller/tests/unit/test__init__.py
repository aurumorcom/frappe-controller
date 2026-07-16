import frappe
from frappe.tests import UnitTestCase
from unittest.mock import patch, MagicMock

class TestFrappeControllerEnqueuePatch(UnitTestCase):

    def setUp(self):
        super().setUp()
        
        # We want to mock get_hooks, but frappe.get_hooks might be used internally, 
        # so we patch it safely.
        self.patcher_get_hooks = patch("frappe.get_hooks")
        self.mock_get_hooks = self.patcher_get_hooks.start()
        
        self.patcher_controller_enqueue = patch("frappe_controller.utils.background_jobs.enqueue")
        self.mock_controller_enqueue = self.patcher_controller_enqueue.start()
        
        # We also mock _original_enqueue to ensure RQ is not called when it shouldn't be
        self.patcher_original_enqueue = patch("frappe_controller._original_enqueue")
        self.mock_original_enqueue = self.patcher_original_enqueue.start()
        
    def tearDown(self):
        self.patcher_get_hooks.stop()
        self.patcher_controller_enqueue.stop()
        self.patcher_original_enqueue.stop()
        super().tearDown()

    def test_standard_rq_job(self):
        # Scenario 1: Standard RQ Job (No Interception)
        self.mock_get_hooks.return_value = {}
        
        # Call the patched enqueue directly or via frappe.enqueue (which is patched)
        frappe.enqueue("frappe.utils.background_jobs.test_job")
        
        self.mock_controller_enqueue.assert_not_called()
        self.mock_original_enqueue.assert_called_once()
        
        # Check args
        args, kwargs = self.mock_original_enqueue.call_args
        self.assertEqual(args[0], "frappe.utils.background_jobs.test_job")

    def test_explicit_fs_job_routing(self):
        # Scenario 2: Explicit FS Job Routing
        self.mock_get_hooks.return_value = {"my_app.jobs.do_work": {}}
        self.mock_controller_enqueue.return_value = "JobPromise"
        
        result = frappe.enqueue("my_app.jobs.do_work", queue="high")
        
        self.assertEqual(result, "JobPromise")
        self.mock_original_enqueue.assert_not_called()
        self.mock_controller_enqueue.assert_called_once_with(
            method="my_app.jobs.do_work",
            queue="high",
            timeout=None,
            is_async=True
        )

    def test_scheduler_event_interception(self):
        # Scenario 3: Scheduler Event Interception & Strict Precedence
        self.mock_get_hooks.return_value = {"my_app.jobs.do_scheduled_work": {}}
        self.mock_controller_enqueue.return_value = "JobPromise"
        
        result = frappe.enqueue(
            "frappe.core.doctype.scheduled_job_type.scheduled_job_type.run_scheduled_job",
            job_type="my_app.jobs.do_scheduled_work",
            scheduled_job_type="My App Scheduled Work"
        )
        
        self.assertEqual(result, "JobPromise")
        self.mock_original_enqueue.assert_not_called()
        
        self.mock_controller_enqueue.assert_called_once()
        _, kwargs = self.mock_controller_enqueue.call_args
        self.assertEqual(kwargs["method"], "my_app.jobs.do_scheduled_work")
        self.assertNotIn("job_type", kwargs)
        self.assertNotIn("scheduled_job_type", kwargs)

    def test_exception_fallback(self):
        # Scenario 4: Exception Fallback (Graceful Degradation)
        self.mock_get_hooks.return_value = {"my_app.jobs.do_work": {}}
        self.mock_controller_enqueue.side_effect = Exception("DB Error")
        self.mock_original_enqueue.return_value = "RQJob"
        
        result = frappe.enqueue("my_app.jobs.do_work")
        
        self.assertEqual(result, "RQJob")
        self.mock_controller_enqueue.assert_called_once()
        self.mock_original_enqueue.assert_called_once()

    def test_synchronous_execution_preservation(self):
        # Scenario 5: Synchronous Execution Preservation
        self.mock_get_hooks.return_value = {"my_app.jobs.do_work": {}}
        
        # When now=True, our patch detects it and returns _original_enqueue immediately
        frappe.enqueue("my_app.jobs.do_work", now=True)
        
        self.mock_controller_enqueue.assert_not_called()
        self.mock_original_enqueue.assert_called_once()
        
        # Alternatively, if is_async=False and not frappe.in_test, but we are in test...
        # so let's stick to now=True

    def test_callable_signature_handling(self):
        # Scenario 6: Callable Signature Handling
        def my_func():
            pass
            
        method_name = f"{my_func.__module__}.{my_func.__qualname__}"
        self.mock_get_hooks.return_value = {method_name: {}}
        self.mock_controller_enqueue.return_value = "JobPromise"
        
        result = frappe.enqueue(my_func, queue="default")
        
        self.assertEqual(result, "JobPromise")
        self.mock_original_enqueue.assert_not_called()
        self.mock_controller_enqueue.assert_called_once_with(
            method=method_name,
            queue="low",
            timeout=None,
            is_async=True
        )


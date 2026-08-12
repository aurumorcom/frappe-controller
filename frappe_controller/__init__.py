__version__ = "16.4.0"

import inspect

import frappe
from frappe.utils import background_jobs

_original_enqueue = background_jobs.enqueue
_enqueue_sig = inspect.signature(_original_enqueue)


def _patched_enqueue(*args, **kwargs):
	try:
		bound_args = _enqueue_sig.bind(*args, **kwargs)
		bound_args.apply_defaults()
	except Exception:
		# If binding fails, just fallback to original enqueue to handle the error natively
		return _original_enqueue(*args, **kwargs)

	args_dict = bound_args.arguments
	method = args_dict.get("method")
	now = args_dict.get("now")
	is_async = args_dict.get("is_async")

	# Handle legacy async kwarg which frappe.enqueue handles
	job_kwargs = args_dict.get("kwargs", {})
	if "async" in job_kwargs:
		is_async = job_kwargs["async"]

	call_directly = now or (not is_async and not frappe.in_test)
	if call_directly:
		return _original_enqueue(*args, **kwargs)

	if callable(method):
		method_name = f"{method.__module__}.{method.__qualname__}"
	else:
		method_name = method

	actual_method_name = method_name

	if method_name == "frappe.core.doctype.scheduled_job_type.scheduled_job_type.run_scheduled_job":
		actual_method_name = job_kwargs.get("job_type") or method_name

	controller_events = frappe.get_hooks("controller_events") or {}

	if actual_method_name in controller_events:
		try:
			from frappe_controller.utils.background_jobs import enqueue as controller_enqueue

			# If we intercepted a scheduled job, strip the scheduler kwargs
			if actual_method_name != method_name:
				job_kwargs.pop("job_type", None)
				job_kwargs.pop("scheduled_job_type", None)

			queue = args_dict.get("queue")
			# Map standard Frappe queues to FastStream queues to prevent fallback recursion
			if queue not in ("low", "medium", "high"):
				if queue == "short":
					queue = "high"
				elif queue == "long":
					queue = "low"
				else:
					queue = "low"  # default

			timeout = args_dict.get("timeout")

			return controller_enqueue(
				method=actual_method_name, queue=queue, timeout=timeout, is_async=is_async, **job_kwargs
			)
		except ImportError:
			# Graceful degradation if frappe_controller is not available
			pass
		except Exception as e:
			frappe.logger("frappe_controller").error(f"Failed to route job to controller queue: {e}")
			pass

	return _original_enqueue(*args, **kwargs)


# Apply monkey patch
background_jobs.enqueue = _patched_enqueue
frappe.enqueue = _patched_enqueue


def cancel(job_id):
	if not job_id:
		return False
	if frappe.db.exists("FS Job", job_id):
		try:
			doc = frappe.get_doc("FS Job", job_id)
			return doc.cancel()
		except Exception as e:
			frappe.logger("frappe_controller").error(f"Error cancelling FS Job {job_id}: {e}")
			return False
	else:
		try:
			doc = frappe.get_doc("RQ Job", job_id)
			if doc.status == "queued":
				doc.cancel()
			else:
				doc.stop_job()
			return True
		except frappe.DoesNotExistError:
			pass
		except Exception as e:
			frappe.logger("frappe_controller").error(f"Error cancelling RQ Job {job_id}: {e}")
			return False
	return False


def bulk_cancel(frappe_filter=None):
	try:
		fs_jobs = frappe.get_all("FS Job", filters=frappe_filter, fields=["name"])
		for job in fs_jobs:
			cancel(job.name)
	except Exception as e:
		frappe.logger("frappe_controller").warning(
			f"Failed to bulk cancel FS Jobs with filter {frappe_filter}: {e}"
		)

	try:
		rq_jobs = frappe.get_all("RQ Job", filters=frappe_filter)
		for job in rq_jobs:
			cancel(job.name)
	except Exception as e:
		frappe.logger("frappe_controller").warning(
			f"Failed to bulk cancel RQ Jobs with filter {frappe_filter}: {e}"
		)


def delete(job_id):
	if not job_id:
		return False
	if frappe.db.exists("FS Job", job_id):
		try:
			frappe.delete_doc("FS Job", job_id, force=True, ignore_permissions=True)
			return True
		except Exception as e:
			frappe.logger("frappe_controller").error(f"Error deleting FS Job {job_id}: {e}")
			return False
	else:
		try:
			if frappe.db.exists("RQ Job", job_id):
				frappe.delete_doc("RQ Job", job_id, force=True, ignore_permissions=True)
				return True
		except frappe.DoesNotExistError:
			pass
		except Exception as e:
			frappe.logger("frappe_controller").error(f"Error deleting RQ Job {job_id}: {e}")
			return False
	return False


def bulk_delete(frappe_filter=None):
	try:
		fs_jobs = frappe.get_all("FS Job", filters=frappe_filter, fields=["name"])
		for job in fs_jobs:
			delete(job.name)
	except Exception as e:
		frappe.logger("frappe_controller").warning(
			f"Failed to bulk delete FS Jobs with filter {frappe_filter}: {e}"
		)

	try:
		rq_jobs = frappe.get_all("RQ Job", filters=frappe_filter)
		for job in rq_jobs:
			delete(job.name)
	except Exception as e:
		frappe.logger("frappe_controller").warning(
			f"Failed to bulk delete RQ Jobs with filter {frappe_filter}: {e}"
		)


frappe.cancel = cancel
frappe.bulk_cancel = bulk_cancel
frappe.cancel_bulk = bulk_cancel
frappe.delete = delete
frappe.bulk_delete = bulk_delete
frappe.delete_bulk = bulk_delete

from frappe_controller.utils.background_jobs import Job
from frappe_controller.utils.controller import (
	DeferredJob,
	JobResult,
	publish_event,
	sleep_for,
	sleep_until,
	wait_for,
	wait_for_event,
)

frappe.wait_for = wait_for
frappe.wait_for_event = wait_for_event
frappe.sleep_for = sleep_for
frappe.sleep_until = sleep_until
frappe.publish_event = publish_event

frappe.Job = Job
frappe.DeferredJob = DeferredJob
frappe.JobResult = JobResult

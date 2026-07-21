# Copyright (c) 2026, Aurumor and contributors
# License: MIT. See LICENSE

import frappe
from frappe.model.document import Document


class FSJob(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		arguments: DF.Code | None
		ended_at: DF.Datetime | None
		exc_info: DF.Code | None
		job_id: DF.Data | None
		job_name: DF.Data | None
		job_type: DF.Link
		queue: DF.Literal["default", "short", "long"]
		started_at: DF.Datetime | None
		status: DF.Literal["queued", "started", "finished", "failed", "canceled"]
		time_taken: DF.Duration | None
		timeout: DF.Duration | None
		result: DF.Code | None

	# end: auto-generated types
	@frappe.whitelist()
	def replay(self):
		import json

		self.status = "queued"
		self.started_at = None
		self.ended_at = None
		self.exc_info = None
		self.time_taken = 0
		self.save()

		job_payload = self.as_dict()
		job_payload["site"] = frappe.local.site

		queue = self.queue or "low"
		frappe.cache().xadd(f"fs:queue:{queue}", {"payload": json.dumps(job_payload, default=str)})

		return True

	@frappe.whitelist()
	def cancel(self):
		current_status = self.status
		if current_status in ("canceled", "finished", "failed"):
			return False

		self.status = "canceled"

		if current_status == "started":
			frappe.cache().publish("fs:cancelled", self.name)

		frappe.db.delete("FS Match Condition", {"job": self.name})

		cache = frappe.cache()
		try:
			cache.delete(f"fs:started:{frappe.local.site}:{self.name}")
			cache.delete(f"fs:promoted:{self.name}")
		except Exception as e:
			frappe.logger("frappe_controller").error(f"Failed to clear Redis locks for job {self.name}: {e}")

		self.save()

		child_jobs = frappe.get_all("FS Job", filters={"parent_job": self.name}, pluck="name")
		for child in child_jobs:
			frappe.cancel(child)

		return True

	def on_trash(self):
		if self.status in ("queued", "started"):
			self.cancel()

		child_jobs = frappe.get_all("FS Job", filters={"parent_job": self.name}, pluck="name")
		for child in child_jobs:
			frappe.delete_doc("FS Job", child, ignore_permissions=True, force=True)

		try:
			frappe.cache().publish("fs:deleted", self.name)
		except Exception as e:
			frappe.logger("frappe_controller").error(f"Failed to publish fs:deleted for job {self.name}: {e}")

	def delete(self, ignore_permissions=False, force=False, *, delete_permanently=False):
		return super().delete(
			ignore_permissions=ignore_permissions, force=force, delete_permanently=delete_permanently
		)

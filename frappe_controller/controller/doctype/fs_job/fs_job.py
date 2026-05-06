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
		status: DF.Literal["Queued", "Started", "Finished", "Failed", "Canceled"]
		time_taken: DF.Duration | None
		timeout: DF.Duration | None
	# end: auto-generated types
	@frappe.whitelist()
	def replay(self):
		import json
		self.status = "Queued"
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

# Copyright (c) 2021, Frappe Technologies and contributors
# License: MIT. See LICENSE

import json

import frappe
from frappe.model.document import Document
from frappe.utils import now_datetime


class ControllerJobType(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		create_log: DF.Check
		last_execution: DF.Datetime | None
		method: DF.Data
		server_script: DF.Link | None
		stopped: DF.Check
		rate_limit_per_second: DF.Int
		rate_limit_per_minute: DF.Int
		rate_limit_per_hour: DF.Int
		rate_limit_per_day: DF.Int
		retries: DF.Int
	# end: auto-generated types

	def on_update(self):
		import redis

		key = f"fs:{self.method}:config"
		redis_url = frappe.conf.get("redis_cache") or "redis://localhost:13000"
		r = redis.Redis.from_url(redis_url)

		# Clear existing config
		r.delete(key)

		limits = {}
		if self.rate_limit_per_second:
			limits["rate_limit_per_second"] = str(self.rate_limit_per_second)
		if self.rate_limit_per_minute:
			limits["rate_limit_per_minute"] = str(self.rate_limit_per_minute)
		if self.rate_limit_per_hour:
			limits["rate_limit_per_hour"] = str(self.rate_limit_per_hour)
		if self.rate_limit_per_day:
			limits["rate_limit_per_day"] = str(self.rate_limit_per_day)
		if self.timeout:
			limits["timeout"] = str(self.timeout)
		if self.retries:
			limits["retries"] = str(self.retries)

		if limits:
			r.hset(key, mapping=limits)

def sync_jobs(hooks: list | dict = None):
	frappe.reload_doc("controller", "doctype", "controller_job_type")
	frappe.reload_doc("controller", "doctype", "fs_job")

	raw_hooks = hooks if hooks is not None else frappe.get_hooks("controller_events")

	if not raw_hooks:
		return

	# Normalize raw_hooks to a list
	if isinstance(raw_hooks, dict):
		raw_hooks = [raw_hooks]

	defined_methods = []

	for hook_entry in raw_hooks:
		if isinstance(hook_entry, str):
			defined_methods.append(hook_entry)
			insert_single_event(hook_entry)
		elif isinstance(hook_entry, dict):
			# Case 1: {"method": "path", "rate_limit_per_minute": 10}
			if "method" in hook_entry and isinstance(hook_entry["method"], str):
				method = hook_entry["method"]
				defined_methods.append(method)
				insert_single_event(method, hook_entry)
			else:
				# Case 2: {"path": {"limit": 10}} OR {"category": ["m1", "m2"]}
				for key, value in hook_entry.items():
					if isinstance(value, dict):
						# Method is key, value is config
						defined_methods.append(key)
						insert_single_event(key, value)
					elif isinstance(value, list):
						# Check if this is a merged dictionary from Frappe hooks
						if all(isinstance(v, dict) and "method" not in v for v in value):
							# Merge all the dictionaries in the list
							merged_config = {}
							for v in value:
								merged_config.update(v)
							defined_methods.append(key)
							insert_single_event(key, merged_config)
						else:
							# Key is category, value is list of methods
							for method in value:
								if isinstance(method, str):
									defined_methods.append(method)
									insert_single_event(method)
								elif isinstance(method, dict) and "method" in method:
									m_name = method["method"]
									defined_methods.append(m_name)
									insert_single_event(m_name, method)

	# Clear old ones
	for job in frappe.get_all("Controller Job Type", fields=["name", "method", "server_script"]):
		if not job.server_script and job.method not in defined_methods:
			# Instead of deleting (which fails if logs/jobs exist), we just stop them
			frappe.db.set_value("Controller Job Type", job.name, "stopped", 1)

def insert_single_event(method: str, config: dict = None):
	if not method:
		return

	config = config or {}
	job_name = frappe.db.exists("Controller Job Type", {"method": method})

	if job_name:
		doc = frappe.get_doc("Controller Job Type", job_name)
	else:
		doc = frappe.new_doc("Controller Job Type")
		doc.method = method
		doc.stopped = 0
		doc.create_log = 1

	# Helper to unpack frappe.get_hooks list wrapping
	def get_val(key):
		val = config.get(key)
		if isinstance(val, list) and len(val) > 0:
			return val[-1] # Get the last hook value
		return val

	# Safely update numeric fields
	rate_limit_per_second = get_val("rate_limit_per_second")
	if rate_limit_per_second is not None:
		try:
			doc.rate_limit_per_second = int(rate_limit_per_second)
		except (ValueError, TypeError):
			pass

	rate_limit_per_minute = get_val("rate_limit_per_minute")
	if rate_limit_per_minute is not None:
		try:
			doc.rate_limit_per_minute = int(rate_limit_per_minute)
		except (ValueError, TypeError):
			pass

	rate_limit_per_hour = get_val("rate_limit_per_hour")
	if rate_limit_per_hour is not None:
		try:
			doc.rate_limit_per_hour = int(rate_limit_per_hour)
		except (ValueError, TypeError):
			pass

	rate_limit_per_day = get_val("rate_limit_per_day")
	if rate_limit_per_day is not None:
		try:
			doc.rate_limit_per_day = int(rate_limit_per_day)
		except (ValueError, TypeError):
			pass

	timeout = get_val("timeout")
	if timeout is not None:
		try:
			doc.timeout = int(timeout)
		except (ValueError, TypeError):
			pass

	retries = get_val("retries")
	if retries is not None:
		try:
			doc.retries = int(retries)
		except (ValueError, TypeError):
			pass

	doc.save(ignore_permissions=True)

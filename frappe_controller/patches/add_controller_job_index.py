import frappe


def execute():
    try:
        frappe.db.add_index("FS Job", ["status", "job_type", "creation"], "idx_controller_job_status_type_creation")
    except Exception:
        pass

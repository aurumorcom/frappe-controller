import click
import frappe
import frappe.commands.scheduler

@click.command("control")
def start_controller():
	"""Start the custom controller process."""
	from frappe_controller.utils.controller import start_controller as _start_controller
	_start_controller()

commands = [
	start_controller,
]

# Monkey-patch the native frappe `bench worker` command to accept --namespace
frappe.commands.scheduler.start_worker.params.append(
	click.Option(['--namespace'], type=str, help="Start FastStream worker for a specific namespace (e.g. fs)")
)

original_worker_callback = frappe.commands.scheduler.start_worker.callback

def fs_worker_wrapper(**kwargs):
	namespace = kwargs.pop("namespace", None)
	
	if namespace == "fs":
		from frappe_controller.utils.background_jobs import start_worker
		start_worker()
	else:
		return original_worker_callback(**kwargs)

frappe.commands.scheduler.start_worker.callback = fs_worker_wrapper

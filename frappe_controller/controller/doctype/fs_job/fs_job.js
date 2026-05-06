frappe.ui.form.on("FS Job", {
	refresh: function (frm) {
		if (!["Queued", "Started"].includes(frm.doc.status)) {
			frm.add_custom_button(__("Replay"), function () {
				frm.call("replay").then((r) => {
					if (!r.exc) {
						frappe.show_alert({
							message: __("Job re-queued successfully"),
							indicator: "green",
						});
						frm.reload_doc();
					}
				});
			});
		}
	},
});

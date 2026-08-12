## New Features

* **Expanded Filtering Conditions in `fs_match_condition`**: Added new fields (`filters`, `condition_type`, `target_timestamp`, `group_id`, `logical_operator`) to the `fs_match_condition` doctype and made `event_key` optional. Commits: [23f2782](https://github.com/aurumorcom/frappe-controller/commit/23f2782a), [cf7b75e](https://github.com/aurumorcom/frappe-controller/commit/cf7b75eb)
* **Event and Sleep Utility Functions**: Exported `wait_for`, `wait_for_event`, `sleep_for`, `sleep_until`, and `publish_event` functions, introduced the `JobResult` class, added `evaluate_frappe_filters()`, and refactored the underlying event system. Commits: [97d9319](https://github.com/aurumorcom/frappe-controller/commit/97d9319b), [4377dcb](https://github.com/aurumorcom/frappe-controller/commit/4377dcb3)

## Other

* **Controller E2E Integration Tests**: Added comprehensive end-to-end integration tests for the `frappe_controller` module covering lifecycle scenarios, job suspension, and event publishing. Commit: [b43c2c5](https://github.com/aurumorcom/frappe-controller/commit/b43c2c58)
* **Condition and Job Unit Tests**: Added unit and integration tests for the `FSMatchCondition` doctype, `JobResult`, filter evaluation, timestamp calculations, and wait/publish utilities. Commits: [f87b35f](https://github.com/aurumorcom/frappe-controller/commit/f87b35fe), [d78d986](https://github.com/aurumorcom/frappe-controller/commit/d78d9860), [79e96c4](https://github.com/aurumorcom/frappe-controller/commit/79e96c4a)

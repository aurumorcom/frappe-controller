# Changelog v16.1.0

## Breaking Changes

* **Controller Job Log Refactor**
  * Description: Removed Controller Job Log doctype and replaced with direct result storage on FS Job.
  * Commits: [fb6e0d3](https://github.com/aurumorinc/frappe-controller/commit/fb6e0d3e), [e603e2e](https://github.com/aurumorinc/frappe-controller/commit/e603e2e6), [82fce8e](https://github.com/aurumorinc/frappe-controller/commit/82fce8e7)
* **Unified Priority Queue Migration**
  * Description: Replaced per-queue workers with a single worker using asyncio.PriorityQueue.
  * Commits: [85bc111](https://github.com/aurumorinc/frappe-controller/commit/85bc111e), [1b9449a](https://github.com/aurumorinc/frappe-controller/commit/1b9449ad), [581e1cf](https://github.com/aurumorinc/frappe-controller/commit/581e1cf0)
* **Controller Job Log and FS Job Refactor**
  * Description: Removed Controller Job Log doctype, replaced with direct result storage on FS Job, and added result/parent_job fields. Severity: high.
  * Migration: Update any custom queries or scripts referencing the Controller Job Log doctype to query the FS Job doctype directly using the new result and parent_job fields.
* **Unified Priority Queue and Worker Migration**
  * Description: Replaced per-queue workers with a single worker using asyncio.PriorityQueue and modified namespace and queueing behavior. Severity: high.
  * Migration: Update worker deployment configurations to use the new single worker setup instead of per-queue workers.

## Features

* **Job Suspension and Event-Driven Wake-Up**
  * Description: Implemented job suspension, wait_for_event, emit_event, SuspendJob exception, and orchestrator stream listening.
  * Commits: [72d02e6](https://github.com/aurumorinc/frappe-controller/commit/72d02e6c)
* **Job Retry Logic and Rate Limiting**
  * Description: Implemented exponential backoff retries, timeouts, and per-second rate limiting via ZSET.
  * Commits: [2586a9a](https://github.com/aurumorinc/frappe-controller/commit/2586a9ad), [f45b7e8](https://github.com/aurumorinc/frappe-controller/commit/f45b7e8a), [ee0eba3](https://github.com/aurumorinc/frappe-controller/commit/ee0eba37)
* **Job Replay Functionality**
  * Description: Added Replay button and replay method to FS Job form and controller to re-queue tasks.
  * Commits: [43d04aa](https://github.com/aurumorinc/frappe-controller/commit/43d04aac), [12cf53a](https://github.com/aurumorinc/frappe-controller/commit/12cf53a5), [b114778](https://github.com/aurumorinc/frappe-controller/commit/b114778c)
* **Workflow State Replay and Promises**
  * Description: Introduced JobPromise to track and return execution results from controller jobs.
  * Commits: [ad58679](https://github.com/aurumorinc/frappe-controller/commit/ad586796)

## Improvements

* **Refactoring and Style Updates**
  * Description: Applied 7 refactoring and style updates including event parameters, telemetry streams, and code formatting.
  * Commits: [6a0a25c](https://github.com/aurumorinc/frappe-controller/commit/6a0a25c2), [506eedc](https://github.com/aurumorinc/frappe-controller/commit/506eedc0), [c8605e9](https://github.com/aurumorinc/frappe-controller/commit/c8605e93)

## Fixes

* **Multi-Tenant Job Tracking**
  * Description: Scoped job keys and heartbeats to site names and handled queued status lookups.
  * Commits: [e42e371](https://github.com/aurumorinc/frappe-controller/commit/e42e3717), [caeb93e](https://github.com/aurumorinc/frappe-controller/commit/caeb93e3), [08442f2](https://github.com/aurumorinc/frappe-controller/commit/08442f29)
* **Error Handling and DB Recovery**
  * Description: Wrapped database rollback in try-except block and added reconnection logic during telemetry processing.
  * Commits: [73599af](https://github.com/aurumorinc/frappe-controller/commit/73599af3)
* **Additional Bug Fixes**
  * Description: Fixed 4 bug fixes including table existence checks, queue updates, and match conditions.
  * Commits: [3a42bd5](https://github.com/aurumorinc/frappe-controller/commit/3a42bd54), [0820fee](https://github.com/aurumorinc/frappe-controller/commit/0820fee9), [4b8c4ea](https://github.com/aurumorinc/frappe-controller/commit/4b8c4ea1)

## Infrastructure

* **Chore and CI/CD Updates**
  * Description: Applied 5 chore and CI/CD workflow updates including dependencies and release automation.
  * Commits: [e96f394](https://github.com/aurumorinc/frappe-controller/commit/e96f3945), [51054a4](https://github.com/aurumorinc/frappe-controller/commit/51054a41), [93616f0](https://github.com/aurumorinc/frappe-controller/commit/93616f02)

## Docs

* **Documentation Updates**
  * Description: Included 3 documentation updates covering versioning analysis rules and the AI agent guide.
  * Commits: [b59ad80](https://github.com/aurumorinc/frappe-controller/commit/b59ad80b), [df4450d](https://github.com/aurumorinc/frappe-controller/commit/df4450d6), [3edd0e5](https://github.com/aurumorinc/frappe-controller/commit/3edd0e58)

## Other

* **Test Suite Additions**
  * Description: Added 9 test suite additions for integration, unit tests, and worker verification.
  * Commits: [ba8eb21](https://github.com/aurumorinc/frappe-controller/commit/ba8eb21f), [72059b3](https://github.com/aurumorinc/frappe-controller/commit/72059b3a), [1b7e635](https://github.com/aurumorinc/frappe-controller/commit/1b7e6355)

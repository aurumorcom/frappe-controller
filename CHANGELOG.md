## Breaking Changes

- **Rename legacy job classes in the public API**
  - Severity: High
  - Affected: `JobPromise` and `SuspendJob` references
  - Description: Renamed `JobPromise` to `Job`, and `SuspendJob` to `DeferredJob` in the public API to improve naming clarity.
  - Migration Path: Update any references to `JobPromise` to `Job`, and `SuspendJob` to `DeferredJob` in your custom scripts and implementations.

## New Features

- **Expose new Job API classes**
  - Description: Exposed new `Job`, `JobResult`, and `DeferredJob` classes to the public Frappe API and renamed legacy classes for improved clarity.
  - Commits: [cb77366](https://github.com/aurumorcom/frappe-controller/commit/cb77366f), [4e2a772](https://github.com/aurumorcom/frappe-controller/commit/4e2a7729), [ce8c79e](https://github.com/aurumorcom/frappe-controller/commit/ce8c79e8)

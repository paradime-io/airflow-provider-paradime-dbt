# Changelog

All notable changes to this provider are documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.3.0] — 2026-07-03

### Added

- `retry_from_failure` keyword argument on `ParadimeBoltDbtScheduleRunOperator`.
  When True, if the schedule's most recent run failed, the operator resumes it
  from the point of failure (re-running only the failed commands) instead of
  triggering a fresh run. If the latest run did not fail — or there is no
  previous run — a new run is triggered as usual. Defaults to False, so
  existing DAGs are unaffected.
- `ParadimeHook.retry_bolt_schedule_from_failure`, which wraps the
  `retryBoltRunFromFailure` GraphQL mutation and backs the flag above. Mirrors
  the equivalent method in paradime-python-sdk.
- Example DAG `bolt_dbt_schedule_run_retry.py` demonstrating
  `retry_from_failure`.

## [1.2.0] — 2026-06-17

### Added

- `slug` keyword argument on `ParadimeBoltDbtScheduleRunOperator`,
  `ParadimeHook.trigger_bolt_run`, and `ParadimeHook.get_bolt_schedule`.
  This is now the preferred way to identify a Bolt schedule and matches the
  `slug` field exposed by the Paradime backend's public Bolt API.

### Deprecated

- `schedule_name` keyword argument on the operator and hook methods above.
  Continues to work — both kwargs accept a schedule slug — but using
  `schedule_name=` now emits a `DeprecationWarning` pointing at the caller's
  line. Switch to `slug=` when you next touch the DAG.

### Changed

- The provider now sends the new `slug:` field over GraphQL instead of
  `scheduleName:`. The Paradime backend accepts both field names for the
  same slug-typed value, so this is transparent to running schedules.

### Compatibility

- **No behaviour change for existing DAGs**. Calls like
  `ParadimeBoltDbtScheduleRunOperator(schedule_name="x", ...)` resolve the
  same schedule as before, just with a deprecation warning. The XOR
  validation (exactly one of `slug` or `schedule_name` must be provided) is
  deferred to `execute()` so DAG parsing never raises on import.

## [1.1.0] — 2026-04

### Added

- HTTP / HTTPS proxy support for hook connections.

### Changed

- Linting and CI setup refactored; GitHub Actions workflow added for running
  the test suite on PRs.

## [1.0.0] and earlier

Initial Paradime Airflow provider releases covering the core Bolt operators,
hook, and sensor. See git history for details.

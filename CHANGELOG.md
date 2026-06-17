# Changelog

All notable changes to this provider are documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

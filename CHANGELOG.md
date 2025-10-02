# Changelog

## [Unreleased]
### Added
- Optional `skipInaccessibleItems` configuration (and `SKIP_INACCESSIBLE_ITEMS` env override) to skip per-user updates that return HTTP 403, log skip counts, and continue processing without failing the run.
- Run summary logging that reports processed, succeeded, failed, and skipped updates along with per-user skip counts.

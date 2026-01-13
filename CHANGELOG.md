# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-01-13

### Added

- Core `EffectLedger` class with `run()`, `begin()`, `commit()` API
- `MemoryStore` for development and testing
- `PostgresStore` for production use with psycopg3
- Human-in-the-loop approval workflow (`request_approval`, `approve`, `deny`)
- Concurrent safety with Compare-And-Swap (CAS) operations
- Stale effect detection and takeover (opt-in via `stale.after_ms`)
- RFC 8785 JSON canonicalization for stable idempotency keys
- Resource descriptor support for alternative key computation
- Idempotency key subset selection
- OpenTelemetry tracing support (optional, via `effect-ledger[otel]`)
- Structured logging with stdlib logging
- Input validation with size limits (`max_args_size_bytes`)
- Comprehensive test suite (47 tests including concurrency scenarios)

### Security

- All state transitions use CAS to prevent race conditions
- Terminal states (`succeeded`, `failed`, `canceled`, `denied`) are immutable
- Approval states (`requires_approval`, `ready`) protected from overwrites

### Known Limitations

- No fencing tokens for stale takeover (documented trade-off)
- PostgresStore requires manual schema creation (no auto-migration)
- Secret redaction not yet implemented

## [0.0.1] - 2026-01-01

### Added

- Initial project structure
- Basic types and store protocol

[Unreleased]: https://github.com/rune0/effect-ledger-py/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/rune0/effect-ledger-py/compare/v0.0.1...v0.1.0
[0.0.1]: https://github.com/rune0/effect-ledger-py/releases/tag/v0.0.1

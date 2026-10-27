# Changelog

All notable changes to asynctrino, generated from its Conventional Commits.

## [0.1.0] - 2026-10-27

### Features

- **dbapi:** Run SELECT end to end over a sans-IO protocol core
- **dbapi:** Carry session state and expose query metadata
- **auth:** Add Basic/JWT auth, insecure-auth guard and TLS verification
- **errors:** Map failures to the asynctrino hierarchy, retry transient errors, add request_timeout
- **types:** Map every Trino type to Python values
- **dbapi:** Bind parameters via EXECUTE IMMEDIATE and add executemany
- **dbapi:** Cancel abandoned queries and run cursors concurrently
- **dbapi:** Add isolation levels and transactions with commit and rollback
- **sqlalchemy:** Add the asynctrino dialect with URL options mapped onto connect
- **sqlalchemy:** Stream results through server-side cursors
- **dbapi:** Let a connection change its isolation level between transactions
- **sqlalchemy:** Get and set isolation levels through the dialect
- **sqlalchemy:** Replace connections that lost the Coordinator and terminate invalidated ones
- **sqlalchemy:** Validate trino_* keyword arguments against the asynctrino dialect
- **sqlalchemy:** Expose the driver connection and declare Trino's dialect capabilities
- **spooling:** Decode Segments lazily with storage-scoped requests and a segment cursor
- **spooling:** Send heartbeats to the Coordinator while a Segment downloads
- **sqlalchemy:** Compile statements, DDL and types to Trino SQL
- **sqlalchemy:** Extract JSON paths with quoted keys and literal window frames
- **sqlalchemy:** Render trino_* table arguments as CREATE TABLE WITH properties
- **sqlalchemy:** Reflect partition columns as an index and cancel the $partitions query
- **sqlalchemy:** Reflect columns with parsed types and comments
- **sqlalchemy:** Reflect schema, table and view names, view definitions and table comments
- **sqlalchemy:** Render table and column comments inline in CREATE TABLE
- **session:** Keep prepared statements the Coordinator registers for user-written PREPARE and EXECUTE
- **sqlalchemy:** Enable the statement cache, rendering frame offsets per execution

### Bug Fixes

- **dbapi:** Never interpolate tzinfo names or subclass formatting into SQL
- **spooling:** Bound Segment decompression and redact pre-signed URIs
- **spooling:** Reject negative decompressed sizes and redact URL userinfo
- **spooling:** Avoid implicit bool in URL redaction
- **auth:** Refuse a nextUri outside the Coordinator's origin before sending to it
- **dbapi:** Raise ProgrammingError when a closed connection or its cursors are used
- **spooling:** Raise TrinoDataError for undecodable Segment data, not a connection error
- **spooling:** Cancel the query when a Segment download from a segment cursor is cancelled

### Refactor

- **sqlalchemy:** Make the dialect module private as _dialect
- **sqlalchemy:** Name the trino_* keyword argument registration for what it does
- **parameters:** Take the current time from the caller instead of reading the clock in the core
- **core:** Give requests one kind instead of two flags, and responses is_success

### Documentation

- Document installation, usage and the differences from aiotrino and Upstream
- Link the maintainer guide and licence files absolutely for PyPI
- Call the Coordinator by its glossary name in docstrings and messages
- **retry:** Note why retrying the statement POST is safe

### Miscellaneous

- Initial commit
- Scaffold uv, hatchling, ruff, pyrefly, nox and CI toolchain
- **lint:** Allow builtin-shadowing parameter names in local stubs
- Version untagged commits as the next patch's development release
- Add changelog, release notes and package install-check targets
- Publish tagged releases to TestPyPI, PyPI and GitHub Releases
- Pin every action to a commit SHA and keep actions and uv dependencies current with Dependabot
- Take pytest arguments only after a test target so several targets run together
- **ruff:** Explain each per-file ignore and ignore N801/N802 once for the compiler
- Relax runtime dependency ranges, pin tool groups, run nox and prek via uvx
- Write the coverage summary without enforcing the coverage floor
- Keep actions and uv dependencies current with Renovate instead of Dependabot

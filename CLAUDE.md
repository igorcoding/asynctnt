# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

asynctnt is a high-performance async Python connector for Tarantool database, built with Python/Cython/C for asyncio. Requires Python 3.9+ and supports Tarantool 1.10+.

## Virtual Environments

This project uses `uv` for package management. Use `uv pip` instead of plain `pip`.

### Naming Convention

Virtual environments follow the pattern `.venv<PYTHON_VERSION>`:
- `.venv314` - Python 3.14 (main development environment, latest stable)
- `.venv313` - Python 3.13
- `.venv314t` - Python 3.14 free-threaded (no-GIL)
- `.venv311pypy` - PyPy 3.11

### Creating a Virtual Environment

```bash
# List available Python versions
uv python list

# Create a new venv
uv venv --python=3.14 .venv314
uv venv --python=3.13 .venv313
uv venv --python=3.14t .venv314t
```

### Using a Virtual Environment

Either activate the environment first:
```bash
source .venv314/bin/activate
make quicktest
```

Or use binaries directly:
```bash
.venv314/bin/python -c "import asynctnt"
.venv314/bin/pytest tests/test_op_select.py
```

## Build & Development Commands

```bash
# Install for local development
make local

# Install with test and docs dependencies
make build

# Install with Cython debug symbols
make debug

# Run tests (requires running Tarantool instance)
make quicktest          # Single test run
make test               # Full suite: PYTHONASYNCIODEBUG + normal + uvloop

# Run a single test file or test
uv run --active pytest tests/test_op_select.py
uv run --active pytest tests/test_op_select.py::SelectTestCase::test_select_all

# Code quality
make lint               # Check formatting (ruff format --check) + linting (ruff check)
make style              # Auto-format with ruff

# Coverage
make coverage

# Build documentation
make docs
```

## Testing

Tests require a Tarantool instance. The test framework manages instance lifecycle automatically:
- Set `TARANTOOL_DOCKER_VERSION` to run Tarantool in Docker (e.g., `2.11`)
- Set `USE_UVLOOP=1` to run tests with uvloop
- Tests use `unittest.TestCase` with a custom metaclass that wraps async test methods

Test classes inherit from `TarantoolTestCase` which provides:
- Automatic Tarantool instance start/stop
- `self.conn` - connected asynctnt.Connection
- `self.tnt` - Tarantool instance handle
- `ensure_version(min=, max=)` decorator for version-specific tests

## Architecture

### Code Layers

1. **Public API** (`asynctnt/connection.py`, `asynctnt/api.py`)
   - `Connection` class: entry point, manages lifecycle, auto-reconnect
   - `Api` base class: defines all database operations (select, insert, call, eval, execute, etc.)

2. **Protocol Layer** (`asynctnt/iproto/` - Cython)
   - `protocol.pyx`: Core IProto binary protocol implementation
   - `db.pyx`: Request/response handling
   - `response.pyx`: Response parsing, TarantoolTuple creation
   - `schema.pyx`: Schema fetching and caching
   - `requests/`: Individual request type builders (select.pyx, insert.pyx, etc.)
   - `ext/`: Type extension handlers (decimal, uuid, datetime, interval, error)

3. **C Layer** (`asynctnt/iproto/tupleobj/`, `third_party/msgpuck/`)
   - Custom TarantoolTuple object (C extension)
   - msgpuck: MessagePack encoding/decoding library

### Key Classes

- `Connection`: Main connection class with auto-reconnect, schema management
- `TarantoolTuple`: Hybrid tuple/dict result object (access by index or field name)
- `PreparedStatement`: SQL prepared statement wrapper
- `Stream`: MVCC transaction stream

### Exception Hierarchy

```
TarantoolError
├── TarantoolSchemaError
├── TarantoolDatabaseError (has code, message)
└── TarantoolNetworkError
    └── TarantoolNotConnectedError
```

## Key Files

- `setup.py`: Cython compilation config, extension module build
- `asynctnt/__init__.py`: Public API exports
- `asynctnt/instance.py`: Tarantool instance management for tests
- `tests/_testbase.py`: Test infrastructure, TarantoolTestCase base class

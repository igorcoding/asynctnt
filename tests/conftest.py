"""Pytest fixtures and configuration for asynctnt tests."""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import AsyncGenerator, Generator
from typing import Any

import pytest

import asynctnt
from asynctnt.instance import TarantoolSyncDockerInstance, TarantoolSyncInstance

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_APPLUA_PATH = os.path.join(TESTS_DIR, "files", "app.lua")

TESTER_SPACE_ID = 512
TESTER_SPACE_NAME = "tester"

# --- Event Loop Configuration ---


@pytest.fixture(scope="session")
def event_loop_policy() -> Any | None:
    """Use uvloop if USE_UVLOOP is set."""
    if os.environ.get("USE_UVLOOP"):
        import uvloop

        return uvloop.EventLoopPolicy()
    return None


# --- Instance Creation Helper ---


def read_applua(path: str = DEFAULT_APPLUA_PATH) -> str:
    """Read the app.lua file for Tarantool configuration."""
    with open(path, "r") as f:
        return f.read()


def create_tarantool_instance(
    applua_path: str | None = None,
    extra_box_cfg: str = "",
    cleanup: bool = True,
    replication_source: list[str] | str | None = None,
) -> TarantoolSyncInstance | TarantoolSyncDockerInstance:
    """Create a Tarantool instance based on environment configuration."""
    applua: str | None = None
    if applua_path:
        with open(applua_path, "r") as f:
            applua = f.read()

    docker_image = os.getenv("TARANTOOL_DOCKER_IMAGE")
    docker_tag = os.getenv("TARANTOOL_DOCKER_VERSION")

    if docker_tag:
        print(  # noqa: T201
            f"Running tarantool in docker: "
            f"{docker_image or 'tarantool/tarantool'}:{docker_tag}"
        )
        return TarantoolSyncDockerInstance(
            applua=applua,
            docker_image=docker_image,
            docker_tag=docker_tag,
            extra_box_cfg=extra_box_cfg,
            timeout=4 * 60,
            replication_source=replication_source,
        )

    unix_path = os.getenv("TARANTOOL_LISTEN_UNIX_PATH")
    if unix_path:
        return TarantoolSyncInstance(
            host="unix/",
            port=unix_path,
            console_host="127.0.0.1",
            applua=applua,
            extra_box_cfg=extra_box_cfg,
            cleanup=cleanup,
            replication_source=replication_source,
        )

    return TarantoolSyncInstance(
        port=TarantoolSyncInstance.get_random_port(),
        console_port=TarantoolSyncInstance.get_random_port(),
        applua=applua,
        extra_box_cfg=extra_box_cfg,
        cleanup=cleanup,
        replication_source=replication_source,
    )


# --- Binary Version Checking ---

# Global to cache the Tarantool binary version
_tarantool_bin_version: tuple[int, ...] | None = None


def get_tarantool_bin_version() -> tuple[int, ...] | None:
    """Get the Tarantool binary version, starting a temporary instance if needed."""
    global _tarantool_bin_version
    if _tarantool_bin_version is not None:
        return _tarantool_bin_version

    instance = create_tarantool_instance()
    try:
        instance.start()
        _tarantool_bin_version = instance.bin_version
    finally:
        instance.stop()

    return _tarantool_bin_version


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "min_bin_version(version): skip test if Tarantool binary version is below the specified version",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Skip tests based on min_bin_version marker."""
    bin_version = get_tarantool_bin_version()
    if bin_version is None:
        return

    for item in items:
        marker = item.get_closest_marker("min_bin_version")
        if marker is not None:
            min_version = marker.args[0]
            if bin_version < min_version:
                item.add_marker(
                    pytest.mark.skip(
                        reason=f"Requires Tarantool >= {min_version}, got {bin_version}"
                    )
                )


# --- Tarantool Instance Fixtures ---


@pytest.fixture(scope="session")
def tnt() -> Generator[TarantoolSyncInstance, None, None]:
    """Session-scoped Tarantool instance with default config."""
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG", "CRITICAL").upper()),
        stream=sys.stdout,
    )
    instance = create_tarantool_instance(applua_path=DEFAULT_APPLUA_PATH)
    instance.start()
    yield instance
    instance.stop()


# --- Connection Fixtures ---


async def _cleanup_connection(connection: asynctnt.Connection) -> None:
    """Clean up a connection by truncating test data and disconnecting."""
    if connection.is_connected:
        try:
            await connection.call("truncate", timeout=5)
        except Exception:
            pass


@pytest.fixture
async def conn(
    tnt: TarantoolSyncInstance,
) -> AsyncGenerator[asynctnt.Connection, None]:
    """Function-scoped connection with auto-cleanup."""
    connection = asynctnt.Connection(
        host=tnt.host,
        port=tnt.port,
        fetch_schema=True,
        auto_refetch_schema=False,
        reconnect_timeout=1 / 3,
    )
    async with connection:
        yield connection
        await _cleanup_connection(connection)


@pytest.fixture
async def conn_no_schema(
    tnt: TarantoolSyncInstance,
) -> AsyncGenerator[asynctnt.Connection, None]:
    """Connection without schema fetching for schema-related tests."""
    connection = asynctnt.Connection(
        host=tnt.host,
        port=tnt.port,
        fetch_schema=False,
        auto_refetch_schema=False,
        reconnect_timeout=1 / 3,
    )
    async with connection:
        yield connection
        await _cleanup_connection(connection)


@pytest.fixture
def in_docker() -> bool:
    """Check if running Tarantool in docker."""
    return bool(os.getenv("TARANTOOL_DOCKER_VERSION"))


# --- Data Fixtures ---


@pytest.fixture
async def fill_data(conn: asynctnt.Connection) -> list[list]:
    """Insert standard test data (3 rows) and return it."""
    data = []
    for i in range(3):
        t = [i, str(i), 1, 2, "something"]
        data.append(t)
        await conn.insert(TESTER_SPACE_ID, t)
    return data


@pytest.fixture
async def fill_data_dict(conn: asynctnt.Connection) -> list[dict]:
    """Insert standard test data as dicts and return it."""
    data = []
    for i in range(3):
        t = {
            "f1": i,
            "f2": str(i),
            "f3": 1,
            "f4": 2,
            "f5": "something",
        }
        result = await conn.insert(TESTER_SPACE_ID, t)
        data.append(dict(result[0]))
    return data


# --- Version Checking ---


def check_version(
    version: tuple[int, ...],
    *,
    min: tuple[int, ...] | None = None,
    max: tuple[int, ...] | None = None,
    min_included: bool = True,
    max_included: bool = False,
) -> None:
    """
    Check if version meets requirements, skip test if not.

    Args:
        version: The version tuple to check
        min: Minimum required version tuple (e.g., (2, 10))
        max: Maximum required version tuple (e.g., (3, 0))
        min_included: Whether min version is inclusive (default True)
        max_included: Whether max version is inclusive (default False)

    Raises:
        pytest.skip: If version requirements aren't met
    """
    # Check minimum version
    if min is not None:
        if min_included and version < min:
            pytest.skip(f"Requires Tarantool >= {min}, got {version}")
        if not min_included and version <= min:
            pytest.skip(f"Requires Tarantool > {min}, got {version}")

    # Check maximum version
    if max is not None:
        if max_included and version > max:
            pytest.skip(f"Requires Tarantool <= {max}, got {version}")
        if not max_included and version >= max:
            pytest.skip(f"Requires Tarantool < {max}, got {version}")


def ensure_version(
    *,
    min: tuple[int, ...] | None = None,
    max: tuple[int, ...] | None = None,
    min_included: bool = True,
    max_included: bool = False,
    conn: str = "conn",
):
    """
    Decorator to skip a test if Tarantool version requirements aren't met.

    Args:
        min: Minimum required version tuple (e.g., (2, 10))
        max: Maximum required version tuple (e.g., (3, 0))
        min_included: Whether min version is inclusive (default True)
        max_included: Whether max version is inclusive (default False)
        conn: Name of the connection parameter in the test function (default "conn")

    Usage:
        @ensure_version(min=(2, 10))
        async def test_feature(self, conn):
            ...

        @ensure_version(min=(2, 10), conn="conn_mvcc")
        async def test_mvcc_feature(self, conn_mvcc):
            ...
    """
    import functools
    import inspect

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Get the connection from function arguments
            sig = inspect.signature(func)
            params = list(sig.parameters.keys())

            connection = None
            if conn in kwargs:
                connection = kwargs[conn]
            elif conn in params:
                idx = params.index(conn)
                # Account for 'self' in class methods
                if params[0] == "self" and len(args) > idx:
                    connection = args[idx]
                elif params[0] != "self" and len(args) > idx:
                    connection = args[idx]

            if connection is None:
                raise ValueError(
                    f"Could not find connection parameter '{conn}' in test function"
                )

            check_version(
                connection.version,
                min=min,
                max=max,
                min_included=min_included,
                max_included=max_included,
            )

            return await func(*args, **kwargs)

        return wrapper

    return decorator

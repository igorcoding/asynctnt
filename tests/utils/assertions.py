"""Custom assertion helpers for asynctnt tests."""

from __future__ import annotations

from typing import Any

from asynctnt import Response, TarantoolTuple


def assert_response_equal(response: Response, target: list[Any], msg: str = "") -> None:
    """Assert response equals target (comparing as lists)."""
    assert len(response) == len(target), (
        f"Length mismatch: {len(response)} != {len(target)}. {msg}"
    )
    tuples = [
        list(item) if isinstance(item, TarantoolTuple) else item for item in response
    ]
    assert tuples == target, f"Response mismatch: {tuples} != {target}. {msg}"


def assert_response_equal_kv(
    response: Response, target: list[dict[str, Any]], msg: str = ""
) -> None:
    """Assert response equals target (comparing as dicts)."""
    tuples = [
        dict(item) if isinstance(item, TarantoolTuple) else item for item in response
    ]
    assert tuples == target, f"Response mismatch: {tuples} != {target}. {msg}"

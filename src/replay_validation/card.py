"""Pure sequencing rules shared by the Card development staging helper and tests."""

from __future__ import annotations

from collections.abc import Iterable


REPLAY_DATES = (
    "2026-07-05",
    "2026-07-06",
    "2026-07-07",
    "2026-07-08",
    "2026-07-09",
    "2026-07-10",
)


def expected_staged_dates(requested_date: str) -> tuple[str, ...]:
    """Return the exact landing prefix required before staging a date."""

    try:
        position = REPLAY_DATES.index(requested_date)
    except ValueError as error:
        raise ValueError(f"Unsupported Card development date: {requested_date}") from error
    return REPLAY_DATES[:position]


def validate_staged_prefix(requested_date: str, staged_dates: Iterable[str]) -> None:
    """Reject a replay that would repeat, skip, or expose a future snapshot."""

    actual = tuple(sorted(set(staged_dates)))
    expected = expected_staged_dates(requested_date)
    if actual != expected:
        raise ValueError(
            f"Before staging {requested_date}, landing must contain exactly "
            f"{','.join(expected) or '(nothing)'}; found {','.join(actual) or '(nothing)'}"
        )

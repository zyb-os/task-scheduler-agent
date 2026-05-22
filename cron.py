"""
cron.py — Pure-Python cron expression parser and next-run calculator.

Supports the full 5-field cron syntax:

    ┌─── minute        (0–59)
    │  ┌─── hour          (0–23)
    │  │  ┌─── day-of-month (1–31)
    │  │  │  ┌─── month       (1–12 or JAN–DEC)
    │  │  │  │  ┌─── day-of-week  (0–7, 0 and 7 = Sunday, or SUN–SAT)
    │  │  │  │  │
    *  *  *  *  *

Field syntax
────────────
  *        every value
  */n      every n-th value  (e.g. */15 in minutes = 0,15,30,45)
  a-b      range             (e.g. 9-17 in hours)
  a-b/n    range with step   (e.g. 0-30/5 in minutes)
  a,b,c    list              (e.g. 1,15 in day-of-month)

Convenience aliases
───────────────────
  @hourly   →  0 * * * *
  @daily    →  0 0 * * *
  @midnight →  0 0 * * *
  @weekly   →  0 0 * * 0
  @monthly  →  0 0 1 * *
  @yearly   →  0 0 1 1 *
  @annually →  0 0 1 1 *

  every:Xm  →  */X * * * *      (e.g. every:15m  = every 15 minutes)
  every:Xh  →  0 */X * * *      (e.g. every:6h   = every 6 hours)
  every:Xd  →  0 0 */X * *      (e.g. every:2d   = every 2 days)

DOM / DOW semantics
───────────────────
When both day-of-month and day-of-week are restricted (neither is '*'),
a day matches if *either* condition is true (standard Vixie-cron behaviour).
When only one is restricted, that one is the sole constraint.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import FrozenSet


# ── Month / weekday name tables ───────────────────────────────────────────────

_MONTH_NAMES = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "may": 5, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}
_DOW_NAMES = {
    "sun": 0, "mon": 1, "tue": 2, "wed": 3,
    "thu": 4, "fri": 5, "sat": 6,
}

# ── Convenience alias expansion ───────────────────────────────────────────────

_ALIASES: dict[str, str] = {
    "@hourly":   "0 * * * *",
    "@daily":    "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@weekly":   "0 0 * * 0",
    "@monthly":  "0 0 1 * *",
    "@yearly":   "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
}

_INTERVAL_RE = re.compile(r"^every:(\d+)([mhd])$", re.IGNORECASE)


def _expand_alias(expr: str) -> str:
    """Expand @-aliases and every:X shorthand to 5-field cron strings."""
    stripped = expr.strip().lower()

    if stripped in _ALIASES:
        return _ALIASES[stripped]

    m = _INTERVAL_RE.match(stripped)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        if unit == "m":
            if n < 1 or n > 59:
                raise ValueError(f"Interval every:{n}m — minutes must be 1–59")
            return f"*/{n} * * * *"
        if unit == "h":
            if n < 1 or n > 23:
                raise ValueError(f"Interval every:{n}h — hours must be 1–23")
            return f"0 */{n} * * *"
        if unit == "d":
            if n < 1 or n > 30:
                raise ValueError(f"Interval every:{n}d — days must be 1–30")
            return f"0 0 */{n} * *"

    return expr   # return as-is; let the 5-field parser handle it


# ── Field parser ──────────────────────────────────────────────────────────────

def _parse_field(
    expr:    str,
    lo:      int,
    hi:      int,
    names:   dict[str, int] | None = None,
) -> frozenset[int]:
    """
    Parse one cron field into a frozenset of matching integers.

    Substitutes name aliases (e.g. 'jan' → 1) before parsing.
    Normalises day-of-week 7 → 0 (both mean Sunday).
    """
    # Substitute names
    if names:
        for name, val in names.items():
            expr = re.sub(r"(?<![a-z])" + name + r"(?![a-z])", str(val), expr, flags=re.IGNORECASE)

    result: set[int] = set()

    for part in expr.split(","):
        part = part.strip()
        if not part:
            continue

        step = 1
        if "/" in part:
            range_part, step_str = part.split("/", 1)
            step = int(step_str)
            if step < 1:
                raise ValueError(f"Step must be ≥1, got {step}")
        else:
            range_part = part

        if range_part == "*":
            start, end = lo, hi
        elif "-" in range_part:
            parts = range_part.split("-", 1)
            start, end = int(parts[0]), int(parts[1])
        else:
            start = end = int(range_part)

        if not (lo <= start <= hi and lo <= end <= hi):
            raise ValueError(
                f"Value {start}-{end} out of range [{lo}–{hi}] in field {expr!r}"
            )

        result.update(range(start, end + 1, step))

    # Normalise Sunday: 7 → 0
    if 7 in result:
        result.discard(7)
        result.add(0)

    if not result:
        raise ValueError(f"Empty field after parsing: {expr!r}")

    return frozenset(result)


# ── CronExpression ────────────────────────────────────────────────────────────

class CronExpression:
    """
    Parsed cron expression.  Use ``CronExpression.parse(expr)`` to construct.
    """

    __slots__ = (
        "_raw", "_minutes", "_hours", "_doms",
        "_months", "_dows", "_dom_star", "_dow_star",
    )

    def __init__(
        self,
        raw:      str,
        minutes:  frozenset[int],
        hours:    frozenset[int],
        doms:     frozenset[int],
        months:   frozenset[int],
        dows:     frozenset[int],
        dom_star: bool,
        dow_star: bool,
    ) -> None:
        self._raw      = raw
        self._minutes  = minutes
        self._hours    = hours
        self._doms     = doms
        self._months   = months
        self._dows     = dows
        self._dom_star = dom_star   # True when DOM field was originally '*' or '*/1'
        self._dow_star = dow_star   # True when DOW field was originally '*' or '*/1'

    @classmethod
    def parse(cls, raw: str) -> "CronExpression":
        """
        Parse a cron expression string.  Raises ``ValueError`` for invalid input.
        """
        expr = _expand_alias(raw.strip())
        fields = expr.split()
        if len(fields) != 5:
            raise ValueError(
                f"Expected 5 fields (minute hour dom month dow), got {len(fields)}: {raw!r}"
            )

        min_f, hr_f, dom_f, mon_f, dow_f = fields

        dom_star = dom_f in ("*", "*/1")
        dow_star = dow_f in ("*", "*/1")

        return cls(
            raw=raw,
            minutes=_parse_field(min_f, 0, 59),
            hours=_parse_field(hr_f,   0, 23),
            doms=_parse_field(dom_f,   1, 31, names=None),
            months=_parse_field(mon_f, 1, 12, names=_MONTH_NAMES),
            dows=_parse_field(dow_f,   0,  6, names=_DOW_NAMES),
            dom_star=dom_star,
            dow_star=dow_star,
        )

    # ── next_run ──────────────────────────────────────────────────────────

    def next_run(self, after: datetime) -> datetime:
        """
        Return the next datetime (UTC, tz-aware) at which this expression fires,
        strictly *after* the given ``after`` timestamp.

        Raises ``RuntimeError`` if no match is found within 4 years (should
        never happen for valid expressions).
        """
        # Ensure UTC-aware
        if after.tzinfo is None:
            after = after.replace(tzinfo=timezone.utc)
        else:
            after = after.astimezone(timezone.utc)

        # Start from the next whole minute
        dt = after.replace(second=0, microsecond=0) + timedelta(minutes=1)

        # We iterate at most 4 years' worth of minutes
        limit = dt + timedelta(days=4 * 366)

        while dt < limit:
            # ── Month ──────────────────────────────────────────────────────
            if dt.month not in self._months:
                # Jump to 1st of next month at 00:00
                if dt.month == 12:
                    dt = dt.replace(year=dt.year + 1, month=1,
                                    day=1, hour=0, minute=0)
                else:
                    dt = dt.replace(month=dt.month + 1,
                                    day=1, hour=0, minute=0)
                continue

            # ── Day (DOM + DOW) ────────────────────────────────────────────
            # Standard cron: if both are restricted → OR; if one is * → AND
            dom_ok = dt.day in self._doms
            dow_ok = dt.weekday() in self._dows   # Python: Mon=0 … Sun=6

            # Convert Python weekday (Mon=0) to cron weekday (Sun=0)
            cron_dow = (dt.weekday() + 1) % 7     # Mon=1 … Sun=0
            dow_ok   = cron_dow in self._dows

            if self._dom_star and self._dow_star:
                day_match = True
            elif self._dom_star:
                day_match = dow_ok
            elif self._dow_star:
                day_match = dom_ok
            else:
                day_match = dom_ok or dow_ok       # Vixie-cron OR semantics

            if not day_match:
                dt = (dt + timedelta(days=1)).replace(hour=0, minute=0)
                continue

            # ── Hour ───────────────────────────────────────────────────────
            if dt.hour not in self._hours:
                next_hour = next(
                    (h for h in sorted(self._hours) if h > dt.hour), None
                )
                if next_hour is None:
                    # No matching hour today — advance to tomorrow 00:00
                    dt = (dt + timedelta(days=1)).replace(hour=0, minute=0)
                else:
                    dt = dt.replace(hour=next_hour, minute=0)
                continue

            # ── Minute ─────────────────────────────────────────────────────
            if dt.minute not in self._minutes:
                next_min = next(
                    (m for m in sorted(self._minutes) if m > dt.minute), None
                )
                if next_min is None:
                    # No matching minute this hour — advance to next hour :00
                    dt = (dt + timedelta(hours=1)).replace(minute=0)
                else:
                    dt = dt.replace(minute=next_min)
                continue

            # ── All fields match ───────────────────────────────────────────
            return dt

        raise RuntimeError(
            f"next_run: no occurrence found within 4 years for {self._raw!r}"
        )

    # ── Describe ──────────────────────────────────────────────────────────

    def describe(self) -> str:
        """Return a human-readable summary (best-effort, not exhaustive)."""
        raw = self._raw.strip()

        _desc: dict[str, str] = {
            "0 * * * *":   "every hour at :00",
            "0 0 * * *":   "daily at midnight UTC",
            "0 9 * * *":   "daily at 09:00 UTC",
            "0 0 * * 0":   "weekly on Sunday at midnight UTC",
            "0 0 1 * *":   "monthly on the 1st at midnight UTC",
            "0 0 1 1 *":   "yearly on 1 Jan at midnight UTC",
            "* * * * *":   "every minute",
        }
        if raw in _desc:
            return _desc[raw]

        expanded = _expand_alias(raw)
        if expanded in _desc:
            return _desc[expanded]

        return f"cron: {raw}"

    def __repr__(self) -> str:
        return f"CronExpression({self._raw!r})"

    @property
    def raw(self) -> str:
        return self._raw


# ── Public helpers ────────────────────────────────────────────────────────────

def next_cron_run(expr: str, after: datetime) -> datetime:
    """
    Convenience function: parse *expr* and return the next run datetime after *after*.
    """
    return CronExpression.parse(expr).next_run(after)


def validate_cron(expr: str) -> tuple[bool, str]:
    """
    Validate a cron expression string.

    Returns ``(True, description)`` on success or ``(False, error_message)``
    on failure.
    """
    try:
        c = CronExpression.parse(expr)
        return True, c.describe()
    except (ValueError, RuntimeError) as exc:
        return False, str(exc)

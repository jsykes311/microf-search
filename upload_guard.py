"""Guardrails for Daily Sales Dump uploads on the Strategic Partner Report.

An upload does more than add months: it REPLACES the stored production for every month in the file, rebuilds
each quarter from only the months in the file, and stamps a trailing-12-month rollup (built from the file alone)
onto every period it touches. A short file, a partner-filtered file or a file that starts mid-quarter can
therefore overwrite good numbers with partial ones.

`analyze_upload` compares an incoming upload with what is already stored and returns what would change plus
warnings, so the page can show them before anything is written. `snapshot` keeps a copy of the stored data
before each write. Nothing here writes to ActiveCampaign.
"""
from __future__ import annotations

import calendar
import os
import shutil
from datetime import date, datetime
from pathlib import Path

from partner_board import _num, month_label, parse_month, parse_quarter

PERSISTENT_DISK = "/var/data"   # the Render persistent disk (marketing already stores its data here)
DROP_PCT = 0.10          # a replaced month losing more than this share of its apps is flagged
COVERAGE_MIN = 0.90      # share of partner contractors an upload should include


def data_path(name: str, env_override: str, on_render: bool, disk_is_mount: bool, here: str) -> str:
    """Where a data file lives: an explicit setting wins; on Render use the persistent disk when it is mounted
    (anything else in the app folder is wiped on every deploy); otherwise next to the code."""
    if env_override:
        return env_override
    if on_render and disk_is_mount:
        return os.path.join(PERSISTENT_DISK, name)
    return os.path.join(here, name)


def storage_warning(path, on_render: bool):
    """A warning when, on Render, the data file is NOT on the persistent disk (it would reset on each deploy)."""
    if on_render and not str(path).startswith(PERSISTENT_DISK + "/"):
        return _warn("warn", "Report data is not stored on the persistent disk",
                     f"On this server the report data is saved at {path}, which is wiped on every deploy. "
                     f"Point APEX_DATA_FILE at {PERSISTENT_DISK}/apex_data.json (or remove the setting) so it, and its "
                     f"backups, survive deploys.")
    return None


def _rows(period) -> list:
    rows = (period or {}).get("production")
    return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []


def totals(rows) -> dict:
    return {"dealers": len(rows), "apps": sum(_num(r.get("apps")) for r in rows),
            "rpas": sum(_num(r.get("rpas")) for r in rows),
            "revenue": round(sum(float(r.get("revenue") or 0) for r in rows), 2)}


def file_stats(df) -> dict:
    """Facts about a dump DataFrame that the checks need."""
    import pandas as pd
    dates = pd.to_datetime(df["inserted_time"], errors="coerce")
    ok = dates.dropna()
    cols = {str(c).strip().lower() for c in df.columns}
    return {"rows": int(len(df)),
            "date_min": ok.min().date().isoformat() if len(ok) else None,
            "date_max": ok.max().date().isoformat() if len(ok) else None,
            "bad_dates": int(dates.isna().sum()), "has_nia": "nia" in cols,
            "fixes": list(df.attrs.get("fixes", []))}


def _warn(level, title, detail):
    return {"level": level, "title": title, "detail": detail}


def _fmt(t):
    return f"{t['apps']:,} apps, {t['rpas']:,} RPAs"


def analyze_upload(existing, monthly, quarterly, rollup_rows, file, coverage, today=None) -> dict:
    """What an upload would do. monthly/quarterly: {label: [production rows]}."""
    today = today or date.today()
    periods_in = (existing or {}).get("periods") or {}
    d_min = date.fromisoformat(file["date_min"]) if file.get("date_min") else None
    d_max = date.fromisoformat(file["date_max"]) if file.get("date_max") else None
    warnings, periods = [], []

    def describe(label, kind, rows):
        old_rows = _rows(periods_in.get(label))
        new_t, old_t = totals(rows), (totals(old_rows) if old_rows else None)
        return {"label": label, "kind": kind, "action": "replace" if old_rows else "new",
                "incoming": new_t, "existing": old_t}

    month_keys = sorted(k for k in (parse_month(l) for l in monthly) if k)
    for k in month_keys:
        periods.append(describe(month_label(k), "month", monthly[month_label(k)]))
    for label in sorted(quarterly, key=lambda l: parse_quarter(l) or (0, 0)):
        periods.append(describe(label, "quarter", quarterly[label]))
    by_label = {p["label"]: p for p in periods}

    # 1. Only part of the partner contractors included -> other partners' months get wiped
    inc, tag = coverage.get("included", 0), coverage.get("tagged", 0)
    if tag and inc < COVERAGE_MIN * tag:
        f = coverage.get("partner")
        warnings.append(_warn("danger", "Only part of your partner contractors are in this upload",
            f"{inc:,} of {tag:,} strategic-partner contractors are included"
            + (f" (partner filter: {f})" if f else "")
            + ". Every month below will be replaced with just those contractors, so the other partners' "
              "production for these months will be removed."))

    # 2. Short file -> trailing-12 rollup built from too little data and stamped on every period
    n_months = len(month_keys)
    with_rollup = [p["label"] for p in periods if (periods_in.get(p["label"]) or {}).get("rollup")]
    if n_months < 12:
        warnings.append(_warn("danger" if with_rollup else "warn",
            f"This file only covers {n_months} month{'s' if n_months != 1 else ''}",
            f"The trailing-12-month rollup is rebuilt from this file alone and saved on all {len(periods)} periods "
            f"in it, replacing the rollup now stored on {len(with_rollup)} of them. Use a file covering at least "
            f"the last 12 months."))

    # 3. Quarters built from fewer than 3 months
    last_month = max(month_keys) if month_keys else None
    for label in sorted(quarterly, key=lambda l: parse_quarter(l) or (0, 0)):
        q = parse_quarter(label)
        if not q:
            continue
        present = [k for k in month_keys if (k[0], (k[1] - 1) // 3 + 1) == q]
        if len(present) >= 3:
            continue
        in_progress = (last_month is not None and (last_month[0], (last_month[1] - 1) // 3 + 1) == q
                       and min(present, default=None) == (q[0], (q[1] - 1) * 3 + 1))
        if in_progress:
            continue                       # the quarter is simply not over yet
        p = by_label[label]
        if p["existing"] and p["incoming"]["apps"] < p["existing"]["apps"]:
            warnings.append(_warn("danger", f"{label} would be replaced with a partial quarter",
                f"This file has {len(present)} of 3 months of {label}. The stored quarter "
                f"({_fmt(p['existing'])}) would be replaced by {_fmt(p['incoming'])}."))
        else:
            warnings.append(_warn("warn", f"{label} will be saved as a partial quarter",
                f"This file has {len(present)} of 3 months of {label}."))

    # 4. File starts mid-month
    if d_min and d_min.day > 1 and month_keys:
        first = month_label(month_keys[0])
        p = by_label.get(first)
        if p and p["existing"] and p["incoming"]["apps"] < p["existing"]["apps"]:
            warnings.append(_warn("danger", f"{first} starts mid-month",
                f"The file's first application is {d_min:%b} {d_min.day}. The stored {first} ({_fmt(p['existing'])}) "
                f"would be replaced by {_fmt(p['incoming'])}."))
        else:
            warnings.append(_warn("warn", f"{first} starts mid-month",
                f"The file's first application is {d_min:%b} {d_min.day}, so {first} will only hold part of the month."))

    # 5. Replaced months that shrink (the month in progress is expected to differ, so it is skipped)
    drops = []
    for p in periods:
        if p["kind"] != "month" or not p["existing"]:
            continue
        k = parse_month(p["label"])
        if d_max and k == (d_max.year, d_max.month):
            continue
        old, new = p["existing"]["apps"], p["incoming"]["apps"]
        if old >= 20 and new < (1 - DROP_PCT) * old:
            drops.append(f"{p['label']}: {old:,} to {new:,} apps ({(new - old) / old:+.0%})")
    if drops:
        warnings.append(_warn("warn", "Some replaced months would have fewer applications than now",
                              "; ".join(drops[:6]) + (f"; and {len(drops) - 6} more" if len(drops) > 6 else "")))

    # 6-8. Missing NIA, bad dates, stale file
    if not file.get("has_nia"):
        warnings.append(_warn("warn", "No NIA column in this file",
                              "Revenue and NIA counts will show 0 for every month in it."))
    if file.get("bad_dates", 0) > max(5, 0.01 * file.get("rows", 0)):
        warnings.append(_warn("warn", "Some rows have unreadable dates",
                              f"{file['bad_dates']:,} of {file['rows']:,} rows have no usable date and are left out of every month."))
    if d_max and (today - d_max).days > 3:
        warnings.append(_warn("warn", "This file looks out of date",
                              f"The newest application is {d_max:%b} {d_max.day}, {(today - d_max).days} days ago."))

    # Information
    for fix in file.get("fixes", []):
        warnings.append(_warn("info", "File was cleaned up", fix))
    if d_max and d_max.day != calendar.monthrange(d_max.year, d_max.month)[1]:
        warnings.append(_warn("info", f"{month_label((d_max.year, d_max.month))} is still in progress",
                              f"Data runs through {d_max:%b} {d_max.day}; RPAs from the newest weeks usually arrive later."))

    return {"file": file, "periods": periods, "warnings": warnings,
            "counts": {"new": sum(1 for p in periods if p["action"] == "new"),
                       "replace": sum(1 for p in periods if p["action"] == "replace")},
            "requires_ack": any(w["level"] == "danger" for w in warnings)}


def snapshot(path, keep: int = 5):
    """Copy the stored data file next to itself before it is overwritten; keep the newest `keep` copies."""
    src = Path(path)
    if not src.exists():
        return None
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    dst = src.with_name(f"{src.stem}.backup-{stamp}{src.suffix}")
    n = 1
    while dst.exists():                      # two uploads in the same second
        dst = src.with_name(f"{src.stem}.backup-{stamp}-{n}{src.suffix}")
        n += 1
    shutil.copy2(src, dst)
    old = sorted(src.parent.glob(f"{src.stem}.backup-*{src.suffix}"), key=lambda p: p.stat().st_mtime, reverse=True)
    for p in old[keep:]:
        p.unlink(missing_ok=True)
    return str(dst)

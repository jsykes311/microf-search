"""Strategic Partner Leaderboard.

Rolls contractor production (apps and RPAs) up to the strategic partner each contractor belongs to and
ranks the partners. Pure functions over the data the Strategic Partner Report already stores
(apex_data.json: {"periods": {label: {"production": [rows], "period_type": ...}}}); nothing is written.

Rules worth knowing:
  * Ranking is by RPAs, then apps, then name. Ties share a rank (1, 2, 2, 4).
  * A contractor's partner is its CURRENT Strategic Partners tag in ActiveCampaign when known, else the
    partner saved on the production row. Using one tag for every period keeps rank movement honest
    (it reflects performance, not re-tagging).
  * Quarters are built from the monthly rows, because the stored quarterly rows drop the partner.
    The stored quarter is only used when no monthly data exists for it (manual uploads).
  * Year-to-date and trailing-12 use monthly periods only, so a month is never counted twice.
"""
from __future__ import annotations

import re
from datetime import date
from pathlib import Path

from fastapi import APIRouter, Depends, Query
from fastapi.responses import FileResponse

MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August",
          "September", "October", "November", "December"]
_MONTH_NUM = {m: i + 1 for i, m in enumerate(MONTHS)}
UNASSIGNED = "Partner not identified"
PAGE = Path(__file__).parent / "static" / "reports" / "partner-leaderboard.html"


# ----------------------------------------------------------------------------- period helpers
def _num(v) -> int:
    try:
        return int(round(float(str(v).replace(",", "").strip() or 0)))
    except (TypeError, ValueError):
        return 0


def parse_month(label):
    m = re.fullmatch(r"([A-Za-z]+)\s+(\d{4})", (label or "").strip())
    if m and m.group(1).title() in _MONTH_NUM:
        return int(m.group(2)), _MONTH_NUM[m.group(1).title()]
    return None


def parse_quarter(label):
    m = re.fullmatch(r"Q([1-4])\s+(\d{4})", (label or "").strip(), re.I)
    return (int(m.group(2)), int(m.group(1))) if m else None


def month_label(key) -> str:
    return f"{MONTHS[key[1] - 1]} {key[0]}"


def quarter_label(key) -> str:
    return f"Q{key[1]} {key[0]}"


def shift_month(key, n):
    idx = key[0] * 12 + (key[1] - 1) + n
    return idx // 12, idx % 12 + 1


def shift_quarter(key, n):
    idx = key[0] * 4 + (key[1] - 1) + n
    return idx // 4, idx % 4 + 1


def _quarter_months(qkey):
    return [(qkey[0], (qkey[1] - 1) * 3 + i) for i in (1, 2, 3)]


def _periods(data):
    return (data or {}).get("periods") or {}


def monthly_index(data) -> dict:
    """{(year, month): label} for every monthly period that has production rows stored."""
    out = {}
    for label, p in _periods(data).items():
        k = parse_month(label)
        if k and isinstance(p, dict) and isinstance(p.get("production"), list):
            out[k] = label
    return out


def _stored_quarters(data) -> dict:
    out = {}
    for label, p in _periods(data).items():
        k = parse_quarter(label)
        if k and isinstance(p, dict) and isinstance(p.get("production"), list):
            out[k] = label
    return out


def period_options(data, today=None):
    """Dropdown options and the default selection."""
    today = today or date.today()
    months = monthly_index(data)
    quarters = set(_stored_quarters(data))
    for (y, m) in months:
        quarters.add((y, (m - 1) // 3 + 1))
    options = []
    for k in sorted(months, reverse=True):
        options.append({"value": f"m:{month_label(k)}", "label": month_label(k), "group": "Months"})
    for k in sorted(quarters, reverse=True):
        options.append({"value": f"q:{quarter_label(k)}", "label": quarter_label(k), "group": "Quarters"})
    if months:
        anchor = max(months)
        options.append({"value": "ytd", "label": f"Year to date ({anchor[0]})", "group": "Rolling"})
        options.append({"value": "ttm", "label": "Trailing 12 months", "group": "Rolling"})
    current = (today.year, today.month)
    if current in months:
        default = f"m:{month_label(current)}"
    elif months:
        default = f"m:{month_label(max(months))}"
    elif quarters:
        default = f"q:{quarter_label(max(quarters))}"
    else:
        default = ""
    return options, default


def resolve_period(data, spec):
    """Turn a period spec into the months to add up, plus the matching previous period.

    Returns dict(label, month_keys, stored, prior={label, month_keys, stored} | None) or None if unknown.
    """
    months = monthly_index(data)
    stored_q = _stored_quarters(data)

    def quarter(qkey):
        mk = [k for k in _quarter_months(qkey) if k in months]
        return {"label": quarter_label(qkey), "month_keys": mk, "expected": 3,
                "stored": stored_q.get(qkey) if not mk else None}

    def month(k):
        return {"label": month_label(k), "month_keys": [k] if k in months else [], "expected": 1, "stored": None}

    if spec.startswith("m:"):
        k = parse_month(spec[2:])
        if not k:
            return None
        cur, prior = month(k), month(shift_month(k, -1))
    elif spec.startswith("q:"):
        k = parse_quarter(spec[2:])
        if not k:
            return None
        cur, prior = quarter(k), quarter(shift_quarter(k, -1))
    elif spec in ("ytd", "ttm"):
        if not months:
            return None
        anchor = max(months)
        if spec == "ytd":
            def ytd(a):
                return [(a[0], m) for m in range(1, a[1] + 1) if (a[0], m) in months]
            cur = {"label": f"Year to date {anchor[0]} (through {month_label(anchor)})",
                   "month_keys": ytd(anchor), "expected": anchor[1], "stored": None}
            prev_anchor = shift_month(anchor, -1)
            prior = ({"label": f"Year to date {anchor[0]} (through {month_label(prev_anchor)})",
                      "month_keys": ytd(prev_anchor), "stored": None} if anchor[1] > 1 else None)
        else:
            def ttm(a):
                return [k for k in (shift_month(a, -i) for i in range(12)) if k in months]
            cur = {"label": f"Trailing 12 months ending {month_label(anchor)}",
                   "month_keys": ttm(anchor), "expected": 12, "stored": None}
            pa = shift_month(anchor, -1)
            prior = {"label": f"Trailing 12 months ending {month_label(pa)}",
                     "month_keys": ttm(pa), "stored": None}
    else:
        return None
    if prior is not None and not prior["month_keys"] and not prior["stored"]:
        prior = None
    cur["prior"] = prior
    return cur


# ----------------------------------------------------------------------------- aggregation
def collect(data, period, lookup):
    """Add up production per contractor for a resolved period: {key: contractor}."""
    months = monthly_index(data)
    sources = [(_periods(data)[months[k]].get("production") or []) for k in period["month_keys"]]
    if not period["month_keys"] and period.get("stored"):
        sources.append(_periods(data)[period["stored"]].get("production") or [])
    contractors = {}
    for rows in sources:
        for r in rows:
            if not isinstance(r, dict):
                continue
            did = str(r.get("dealer_id") or "").strip()
            name = str(r.get("dealer") or "").strip()
            key = did or name.lower()
            if not key:
                continue
            c = contractors.setdefault(key, {"dealer": name or f"Dealer {did}", "dealer_id": did,
                                              "account_id": str(r.get("account_id") or ""),
                                              "partner": "", "apps": 0, "rpas": 0, "approved": 0})
            c["apps"] += _num(r.get("apps"))
            c["rpas"] += _num(r.get("rpas"))
            c["approved"] += _num(r.get("approved"))
            if not c["account_id"] and r.get("account_id"):
                c["account_id"] = str(r["account_id"])
            partner = (lookup.get(did) or str(r.get("strategic_partner") or "")).strip()
            if partner and not c["partner"]:
                c["partner"] = partner
    return contractors


def _split_partners(value):
    return [p.strip() for p in str(value or "").split(",") if p.strip()]


def group_by_partner(contractors):
    """-> ({partner: {apps, rpas, approved, contractors:[...]}}, [unassigned contractors])."""
    partners, unassigned = {}, []
    for c in contractors.values():
        names = _split_partners(c["partner"])
        if not names:
            unassigned.append(c)
            continue
        for name in names:
            p = partners.setdefault(name, {"apps": 0, "rpas": 0, "approved": 0, "contractors": []})
            p["apps"] += c["apps"]
            p["rpas"] += c["rpas"]
            p["approved"] += c["approved"]
            p["contractors"].append(c)
    return partners, unassigned


def rank_partners(partners) -> dict:
    """Competition ranking by RPAs then apps (ties share a rank)."""
    ordered = sorted(partners.items(), key=lambda kv: (-kv[1]["rpas"], -kv[1]["apps"], kv[0].lower()))
    ranks, prev, pos = {}, None, 0
    for i, (name, p) in enumerate(ordered, 1):
        score = (p["rpas"], p["apps"])
        if score != prev:
            pos, prev = i, score
        ranks[name] = pos
    return ranks


def _contractor_view(c):
    return {"dealer": c["dealer"], "dealer_id": c["dealer_id"], "account_id": c["account_id"],
            "apps": c["apps"], "rpas": c["rpas"], "approved": c["approved"]}


def _sorted_contractors(rows):
    return sorted(rows, key=lambda c: (-c["rpas"], -c["apps"], c["dealer"].lower()))


def build_board(data, spec, lookup=None, partners_tagged=0):
    lookup = lookup or {}
    options, default = period_options(data)
    base = {"periods": {"default": default, "options": options}, "has_data": bool(options)}
    period = resolve_period(data, spec or default) if (spec or default) else None
    if period is None or not (period["month_keys"] or period.get("stored")):
        return {**base, "period": {"value": spec or default, "label": (period or {}).get("label", ""), "months": []},
                "partners": [], "unassigned": None, "prior_label": None,
                "totals": {"partners": 0, "contractors": 0, "active_contractors": 0, "apps": 0, "rpas": 0},
                "coverage": {"partners_with_data": 0, "partners_tagged_in_ac": partners_tagged, "uploaded_at": None}}

    contractors = collect(data, period, lookup)
    partners, unassigned = group_by_partner(contractors)
    ranks = rank_partners(partners)

    prior_ranks, prior_label = {}, None
    if period["prior"]:
        prior_contractors = collect(data, period["prior"], lookup)
        prior_partners, _ = group_by_partner(prior_contractors)
        if prior_partners:
            prior_ranks, prior_label = rank_partners(prior_partners), period["prior"]["label"]

    rows = []
    for name, p in sorted(partners.items(), key=lambda kv: (ranks[kv[0]], kv[0].lower())):
        cs = _sorted_contractors(p["contractors"])
        active = sum(1 for c in cs if c["apps"] or c["rpas"])
        prior_rank = prior_ranks.get(name)
        rows.append({
            "rank": ranks[name], "partner": name, "apps": p["apps"], "rpas": p["rpas"], "approved": p["approved"],
            "contractors": len(cs), "active_contractors": active,
            "prior_rank": prior_rank,
            "rank_change": (prior_rank - ranks[name]) if prior_rank else None,
            "is_new": bool(prior_ranks) and prior_rank is None,
            "contractor_rows": [_contractor_view(c) for c in cs],
        })

    un = None
    if unassigned:
        us = _sorted_contractors(unassigned)
        un = {"contractors": len(us), "apps": sum(c["apps"] for c in us), "rpas": sum(c["rpas"] for c in us),
              "contractor_rows": [_contractor_view(c) for c in us]}

    months = monthly_index(data)
    labels = [months[k] for k in period["month_keys"]] or ([period["stored"]] if period.get("stored") else [])
    uploaded = [(_periods(data)[l].get("production_uploaded_at") or "") for l in labels]
    return {
        **base,
        "period": {"value": spec or default, "label": period["label"], "months": labels,
                   "months_expected": period.get("expected", len(labels)),
                   "partial": bool(period["month_keys"]) and len(period["month_keys"]) < period.get("expected", 0)},
        "prior_label": prior_label,
        "totals": {
            "partners": len(rows),
            "contractors": sum(r["contractors"] for r in rows),
            "active_contractors": sum(r["active_contractors"] for r in rows),
            "apps": sum(r["apps"] for r in rows),
            "rpas": sum(r["rpas"] for r in rows),
        },
        "partners": rows,
        "unassigned": un,
        "coverage": {"partners_with_data": len(rows), "partners_tagged_in_ac": partners_tagged,
                     "uploaded_at": max(uploaded) if any(uploaded) else None},
    }


# ----------------------------------------------------------------------------- wiring
def install_partner_board(app, require_admin, load_data, partner_info):
    """Add the leaderboard page and API.

    require_admin: the app's admin dependency (same sign-in as the other reports).
    load_data():   returns the stored Strategic Partner Report data.
    partner_info(): returns ({dealer_id: partner}, number_of_partners_tagged_in_AC).
    """
    router = APIRouter()

    @router.get("/partner-leaderboard")
    async def page(admin=Depends(require_admin)):
        return FileResponse(PAGE)

    @router.get("/api/partner-board")
    async def api(period: str = Query(default=""), admin=Depends(require_admin)):
        lookup, tagged = partner_info()
        return build_board(load_data(), period.strip(), lookup, tagged)

    app.include_router(router)

"""Clean up Tableau downloads before the Daily Sales Dump importer reads them.

A Tableau "Data" download of a view that shows several measures can come out PIVOTED: two extra columns,
"Measure Names" and "Measure Values", and every application repeated once per measure. Read as is, apps and
RPAs are counted several times over and the dollar measures (NIA, ...) never show up as columns.
`depivot_measures` turns that back into one row per application with each measure as its own column.
Files that are not pivoted are returned untouched.
"""
from __future__ import annotations

import pandas as pd


def depivot_measures(df: "pd.DataFrame"):
    """Return (dataframe, info). info is None when the file was not pivoted, otherwise a dict with
    rows_before, rows_after and measures."""
    by_lower = {str(c).strip().lower(): c for c in df.columns}
    names_c, values_c = by_lower.get("measure names"), by_lower.get("measure values")
    if names_c is None or values_c is None:
        return df, None

    base = [c for c in df.columns if c not in (names_c, values_c)]
    work = df.copy()
    work["_measure"] = work[names_c].astype(str).str.strip()

    # One key per application. Prefer the Application Id; otherwise fingerprint the whole row.
    id_c = next((c for c in base if str(c).strip().lower() == "application id"), None)
    if id_c is not None and work[id_c].notna().all():
        work["_key"] = work[id_c].astype(str)
    else:
        work["_key"] = pd.util.hash_pandas_object(work[base].astype(str), index=False).astype(str)
    # Genuinely identical rows stay separate: the Nth copy of a key pairs with the Nth copy of each measure.
    work["_occ"] = work.groupby(["_key", "_measure"]).cumcount()

    wide = work.pivot(index=["_key", "_occ"], columns="_measure", values=values_c)
    first = work.drop_duplicates(["_key", "_occ"])[base + ["_key", "_occ"]].set_index(["_key", "_occ"])
    wide = wide[[m for m in wide.columns if m not in base]]      # never overwrite a real column
    out = first.join(wide, how="left").reset_index(drop=True)
    out = out[base + list(wide.columns)]
    out.attrs.update(df.attrs)
    return out, {"rows_before": len(df), "rows_after": len(out), "measures": list(wide.columns)}

"""Account consolidation: back up both accounts, move contacts/deals/SLPs/notes, optionally delete the source.

Everything here talks to ActiveCampaign through a small client object (`ac`) with
async get/post/put/delete, so the logic can be tested without the network.
"""
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

SLP_SCHEMA = "d5ccf74f-981f-40ff-8a03-23cd0309808f"
PAGE = 100
BACKUP_NAME_RE = re.compile(r"^account_merge_[0-9]+_to_[0-9]+_[0-9]{8}_[0-9]{6}\.json$")


def backup_dir() -> Path:
    default = "/var/data/account_merge_backups" if os.getenv("RENDER") else "./data/account_merge_backups"
    return Path(os.getenv("ACCOUNT_MERGE_BACKUP_DIR", default))


async def paged(ac, path: str, key: str, params: dict = None) -> list:
    """All rows of a list endpoint, deduped by id. AC paging can repeat rows, so stop on a page with nothing new."""
    seen, rows, offset = set(), [], 0
    while True:
        data = await ac.get(path, {**(params or {}), "limit": PAGE, "offset": offset})
        page = data.get(key, [])
        fresh = [r for r in page if r.get("id") not in seen]
        for r in fresh:
            seen.add(r.get("id"))
            rows.append(r)
        if not fresh or len(page) < PAGE:
            return rows
        offset += PAGE


async def account_links(ac, account_id: str) -> list:
    return await paged(ac, f"accounts/{account_id}/accountContacts", "accountContacts")


async def account_deals(ac, account_id: str) -> list:
    return await paged(ac, "deals", "deals", {"filters[account]": account_id})


async def account_slps(ac, account_id: str) -> list:
    rows = await paged(ac, f"customObjects/records/{SLP_SCHEMA}", "records",
                       {"filters[relationships.account][eq]": account_id})
    return [r for r in rows if str(account_id) in [str(x) for x in (r.get("relationships") or {}).get("account", [])]]


async def account_notes(ac, account_id: str) -> list:
    return await paged(ac, "notes", "notes", {"reltype": "CustomerAccount", "rel_id": account_id})


async def snapshot_account(ac, account_id: str) -> dict:
    """Everything needed to rebuild what a merge touches, as plain JSON."""
    account = (await ac.get(f"accounts/{account_id}")).get("account", {})
    links = await account_links(ac, account_id)
    contacts = []
    for link in links:
        try:
            contacts.append((await ac.get(f"contacts/{link['contact']}")).get("contact", {}))
        except Exception as exc:  # a missing contact must not block the backup
            contacts.append({"id": str(link.get("contact")), "error": str(exc)[:200]})
    try:
        custom = (await ac.get(f"accounts/{account_id}/accountCustomFieldData")).get("customerAccountCustomFieldData", [])
    except Exception:
        custom = []
    return {
        "account": account,
        "custom_fields": custom,
        "account_contacts": links,
        "contacts": contacts,
        "deals": await account_deals(ac, account_id),
        "slps": await account_slps(ac, account_id),
        "notes": await account_notes(ac, account_id),
    }


def write_backup(source_id: str, target_id: str, source: dict, target: dict, plan: dict, directory: Path = None) -> tuple:
    """Write the pre-merge snapshot; returns (file name, parsed payload)."""
    directory = Path(directory or backup_dir())
    directory.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    name = f"account_merge_{source_id}_to_{target_id}_{now:%Y%m%d_%H%M%S}.json"
    payload = {"created": now.isoformat(), "source_id": str(source_id), "target_id": str(target_id),
               "plan": plan, "source": source, "target": target}
    path = directory / name
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=1, default=str))
    tmp.replace(path)
    return name, payload


def read_backup(name: str, directory: Path = None) -> str:
    if not BACKUP_NAME_RE.match(name or ""):
        raise FileNotFoundError(name)
    return (Path(directory or backup_dir()) / name).read_text()


def _step(results: dict, kind: str, ident, ok: bool, detail: str = ""):
    results[kind].append({"id": str(ident), "ok": ok, **({"detail": detail} if detail else {})})
    if not ok:
        results["errors"].append(f"{kind[:-1].capitalize()} {ident}: {detail}")


async def move_contacts(ac, source_id, target_id, contact_ids, source_links, target_links, results):
    by_source = {str(l["contact"]): l for l in source_links}
    on_target = {str(l["contact"]) for l in target_links}
    for cid in map(str, contact_ids):
        try:
            if cid in on_target:
                # Already on the target: drop the source link instead of inserting a duplicate.
                link = by_source.get(cid)
                if link:
                    await ac.delete(f"accountContacts/{link['id']}")
                _step(results, "contacts", cid, True, "already on target")
            elif cid in by_source:
                link = by_source[cid]  # re-point the existing link so the job title survives
                await ac.put(f"accountContacts/{link['id']}", {"accountContact": {
                    "account": str(target_id), "contact": cid, "jobTitle": link.get("jobTitle") or ""}})
                _step(results, "contacts", cid, True)
            else:
                await ac.post("accountContacts", {"accountContact": {"contact": cid, "account": str(target_id)}})
                _step(results, "contacts", cid, True, "was not linked to source")
        except Exception as exc:
            _step(results, "contacts", cid, False, str(exc)[:200])


async def move_deals(ac, target_id, deal_ids, results):
    for did in map(str, deal_ids):
        try:
            await ac.put(f"deals/{did}", {"deal": {"account": str(target_id)}})
            _step(results, "deals", did, True)
        except Exception as exc:
            _step(results, "deals", did, False, str(exc)[:200])


async def move_slps(ac, target_id, slp_ids, source_slps, results):
    by_id = {s["id"]: s for s in source_slps}
    for sid in slp_ids:
        try:
            rec = by_id.get(sid) or (await ac.get(f"customObjects/records/{SLP_SCHEMA}/{sid}")).get("record", {})
            if not rec:
                raise RuntimeError("SLP record not found")
            # POST with the existing id updates the record in place (PUT is not allowed on custom objects).
            await ac.post(f"customObjects/records/{SLP_SCHEMA}", {"record": {
                "id": sid, "fields": rec.get("fields", []), "relationships": {"account": [int(target_id)]}}})
            _step(results, "slps", sid, True)
        except Exception as exc:
            _step(results, "slps", sid, False, str(exc)[:200])


async def move_notes(ac, source_id, target_id) -> dict:
    """Re-create each note on the target, then delete the original. Always re-reads page 1: deleting while paging would skip rows."""
    moved = failed = 0
    stuck = set()
    while True:
        data = await ac.get("notes", {"reltype": "CustomerAccount", "rel_id": source_id, "limit": 25, "offset": 0})
        notes = [n for n in data.get("notes", []) if n["id"] not in stuck]
        if not notes:
            return {"moved": moved, "errors": failed}
        for note in notes:
            try:
                await ac.post("notes", {"note": {"note": note.get("note", ""), "reltype": "CustomerAccount", "relid": target_id}})
                await ac.delete(f"notes/{note['id']}")
                moved += 1
            except Exception:
                failed += 1
                stuck.add(note["id"])


async def leftovers(ac, account_id: str) -> dict:
    return {"contacts": len(await account_links(ac, account_id)),
            "deals": len(await account_deals(ac, account_id)),
            "slps": len(await account_slps(ac, account_id))}


async def merge_accounts(ac, source_id: str, target_id: str, slp_ids=(), contact_ids=(), deal_ids=(),
                         move_notes_too=False, delete_source=False, directory: Path = None) -> dict:
    """Back up both accounts first; stop before changing anything if the backup cannot be written."""
    source_id, target_id = str(source_id), str(target_id)
    if source_id == target_id:
        raise ValueError("Source and target are the same account")
    results = {"slps": [], "contacts": [], "deals": [], "notes": None, "deleted_source": False,
               "backup": None, "errors": []}
    source = await snapshot_account(ac, source_id)
    target = await snapshot_account(ac, target_id)
    plan = {"slp_ids": list(slp_ids), "contact_ids": [str(c) for c in contact_ids], "deal_ids": [str(d) for d in deal_ids],
            "move_notes": move_notes_too, "delete_source": delete_source}
    results["backup"], _ = write_backup(source_id, target_id, source, target, plan, directory)

    await move_contacts(ac, source_id, target_id, contact_ids, source["account_contacts"], target["account_contacts"], results)
    await move_deals(ac, target_id, deal_ids, results)
    await move_slps(ac, target_id, list(slp_ids), source["slps"], results)
    if move_notes_too:
        try:
            results["notes"] = await move_notes(ac, source_id, target_id)
            if results["notes"]["errors"]:
                results["errors"].append(f"Notes: {results['notes']['errors']} failed to move")
        except Exception as exc:
            results["errors"].append(f"Notes: {str(exc)[:200]}")

    if delete_source:
        if results["errors"]:
            results["errors"].append("Source account not deleted because some items failed")
        else:
            left = await leftovers(ac, source_id)
            if any(left.values()):
                results["errors"].append(
                    f"Source account not deleted: still has {left['contacts']} contacts, {left['deals']} deals, {left['slps']} SLPs")
            else:
                status = await ac.delete(f"accounts/{source_id}")
                if status in (200, 204):
                    results["deleted_source"] = True
                else:
                    results["errors"].append(f"Delete source: HTTP {status}")
    results["ok"] = not results["errors"]
    return results

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from typing import List, Dict, Optional, Any
import httpx
import os
import csv
import io
import asyncio
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json

load_dotenv()

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Config
AC_BASE_URL = os.getenv("AC_BASE_URL", "").rstrip("/")
AC_API_KEY = os.getenv("AC_API_KEY", "")
HEADERS = {"Api-Token": AC_API_KEY, "Content-Type": "application/json"}

# Constants
SLP_SCHEMA_ID = "d5ccf74f-981f-40ff-8a03-23cd0309808f"
LICENSE_SCHEMA_ID = "4bc17cb1-31be-4c15-a186-853ea85b1d40"

# ═══════════════════════════════════════════════════════════════════════════
# CACHING SYSTEM - PHASE 1 PERFORMANCE UPGRADE
# ═══════════════════════════════════════════════════════════════════════════

CACHE = {
    "account_custom_fields": {},  # {account_id: {field_id: value}}
    "contact_custom_fields": {},  # {contact_id: {field_id: value}}
    "deal_custom_fields": {},     # {deal_id: {field_id: value}}
    "field_metadata": {},          # {object_type: fields_list}
    "schemas": {},                 # {schema_id: schema_data}
}
CACHE_TIMESTAMPS = {
    "account_custom_fields": {},
    "contact_custom_fields": {},
    "deal_custom_fields": {},
    "field_metadata": {},
    "schemas": {}
}
CACHE_TTL = 300  # 5 minutes

def get_cached(cache_type: str, key: str):
    """Get cached data if not expired."""
    if key in CACHE[cache_type]:
        timestamp = CACHE_TIMESTAMPS[cache_type].get(key, 0)
        if datetime.now().timestamp() - timestamp < CACHE_TTL:
            return CACHE[cache_type][key]
    return None

def set_cached(cache_type: str, key: str, value: any):
    """Set cached data with timestamp."""
    CACHE[cache_type][key] = value
    CACHE_TIMESTAMPS[cache_type][key] = datetime.now().timestamp()

# Rate limiting semaphore
MAX_CONCURRENT_REQUESTS = 20
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# ═══════════════════════════════════════════════════════════════════════════
# END CACHING SYSTEM
# ═══════════════════════════════════════════════════════════════════════════

# Object relationship definitions
RELATIONSHIPS = {
    "slp": {
        "account": {"type": "belongs_to", "field": "account", "fetch": True}
    },
    "contacts": {
        "account": {"type": "belongs_to", "field": "account", "fetch": True}
    },
    "deals": {
        "contact": {"type": "belongs_to", "field": "contact", "fetch": True},
        "account": {"type": "belongs_to", "field": "account", "fetch": True}
    },
    "accounts": {}  # Accounts have no parent relationships
}

def ac_url(path: str) -> str:
    return f"{AC_BASE_URL}/api/3/{path.lstrip('/')}"

async def ac_get(path: str, params: dict = None):
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(ac_url(path), headers=HEADERS, params=params or {})
        r.raise_for_status()
        return r.json()

async def ac_post(path: str, data: dict):
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(ac_url(path), headers=HEADERS, json=data)
        r.raise_for_status()
        return r.json()

async def ac_get_all(path: str, key: str, params: dict) -> list:
    """Paginate through all records."""
    records = []
    offset = 0
    limit = 100
    
    while True:
        p = {**params, "limit": limit, "offset": offset}
        data = await ac_get(path, p)
        page = data.get(key, [])
        
        if not page:
            break
        
        records.extend(page)
        
        meta = data.get("meta", {})
        total = int(meta.get("total", len(records)))
        offset += limit
        
        if offset >= total:
            break
    
    return records


# ═══════════════════════════════════════════════════════════════════════════
# FIELD DISCOVERY
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/objects")
async def list_objects():
    """List all available object types."""
    return {
        "objects": [
            {"id": "slp", "name": "Strategic Lending Partners", "icon": "📊"},
            {"id": "accounts", "name": "Accounts", "icon": "🏢"},
            {"id": "contacts", "name": "Contacts", "icon": "👤"},
            {"id": "deals", "name": "Deals", "icon": "💰"},
            {"id": "license_details", "name": "Contractor License Details", "icon": "📜"}
        ]
    }

@app.get("/api/fields/{object_type}")
async def get_fields(object_type: str):
    """Get all fields for an object including related objects."""
    
    fields = []
    field_types = {}  # Track field types for smart filtering
    
    # ─── Primary Object Fields ───
    
    if object_type == "slp":
        # Get SLP schema
        data = await ac_get(f"customObjects/schemas/{SLP_SCHEMA_ID}")
        schema = data.get("schema", {})
        
        for f in schema.get("fields", []):
            field_id = f.get("id", f.get("slug"))
            field_label = f.get("labels", {}).get("singular", f.get("slug", ""))
            field_type = f.get("type", "text")
            
            fields.append({
                "id": field_id,
                "label": field_label,
                "type": "primary",
                "dataType": field_type
            })
            
            field_types[field_id] = {
                "type": field_type,
                "options": f.get("options", [])
            }
        
        # Add Account fields
        fields.extend(await get_related_fields("account", "Account"))
    
    elif object_type == "license_details":
        # Get License schema
        data = await ac_get(f"customObjects/schemas/{LICENSE_SCHEMA_ID}")
        schema = data.get("schema", {})
        
        for f in schema.get("fields", []):
            field_id = f.get("id", f.get("slug"))
            field_label = f.get("labels", {}).get("singular", f.get("slug", ""))
            field_type = f.get("type", "text")
            
            fields.append({
                "id": field_id,
                "label": field_label,
                "type": "primary",
                "dataType": field_type
            })
            
            field_types[field_id] = {
                "type": field_type,
                "options": f.get("options", [])
            }
        
        # Add Account fields
        fields.extend(await get_related_fields("account", "Account"))
        
    elif object_type == "accounts":
        # Account built-in fields
        sample = await get_sample_account()
        for key, val in sample.items():
            if key not in ["links", "fieldValues"] and not isinstance(val, dict):
                fields.append({
                    "id": key,
                    "label": key,
                    "type": "primary",
                    "dataType": "text"
                })
        
        # Account custom fields
        try:
            data = await ac_get("accountCustomFieldMeta")
            for cf in data.get("accountCustomFieldMeta", []):
                field_id = f"customfield_{cf['id']}"
                field_label = cf.get("fieldLabel", cf.get("fieldName", cf['id']))
                field_type = cf.get("fieldType", "text")  # text, dropdown, date, etc.
                
                field_obj = {
                    "id": field_id,
                    "label": field_label,
                    "type": "primary",
                    "dataType": field_type
                }
                
                # Add options for dropdown fields
                if field_type in ["dropdown", "listbox", "radio"]:
                    options = cf.get("fieldOptions", "")
                    if options:
                        # Options are comma-separated or newline-separated
                        field_obj["options"] = [opt.strip() for opt in options.replace("\n", ",").split(",") if opt.strip()]
                
                fields.append(field_obj)
                field_types[field_id] = field_obj
        except:
            pass
    
    elif object_type == "contacts":
        # Contact built-in fields
        sample = await get_sample_contact()
        for key, val in sample.items():
            if key not in ["links", "fieldValues"] and not isinstance(val, dict):
                fields.append({
                    "id": key,
                    "label": key,
                    "type": "primary",
                    "dataType": "text"
                })
        
        # Add Account fields
        fields.extend(await get_related_fields("account", "Account"))
    
    elif object_type == "deals":
        # Deal built-in fields
        sample = await get_sample_deal()
        for key, val in sample.items():
            if key not in ["links"] and not isinstance(val, dict):
                fields.append({
                    "id": key,
                    "label": key,
                    "type": "primary",
                    "dataType": "text"
                })
        
        # Add Contact and Account fields
        fields.extend(await get_related_fields("contact", "Contact"))
        fields.extend(await get_related_fields("account", "Account"))
    
    return {"fields": fields, "fieldTypes": field_types}


async def get_related_fields(related_type: str, prefix: str) -> list:
    """Get fields from a related object."""
    fields = []
    
    if related_type == "account":
        # Account built-in
        sample = await get_sample_account()
        for key, val in sample.items():
            if key not in ["links", "fieldValues"] and not isinstance(val, dict):
                fields.append({
                    "id": f"account.{key}",
                    "label": f"{prefix}: {key}",
                    "type": "related",
                    "dataType": "text"
                })
        
        # Account custom
        try:
            data = await ac_get("accountCustomFieldMeta")
            for cf in data.get("accountCustomFieldMeta", []):
                field_id = f"account.customfield_{cf['id']}"
                field_label = cf.get("fieldLabel", cf.get("fieldName", cf['id']))
                
                fields.append({
                    "id": field_id,
                    "label": f"{prefix}: {field_label}",
                    "type": "related",
                    "dataType": "text"
                })
        except:
            pass
    
    elif related_type == "contact":
        sample = await get_sample_contact()
        for key, val in sample.items():
            if key not in ["links", "fieldValues"] and not isinstance(val, dict):
                fields.append({
                    "id": f"contact.{key}",
                    "label": f"{prefix}: {key}",
                    "type": "related",
                    "dataType": "text"
                })
    
    return fields


async def get_sample_account():
    try:
        data = await ac_get("accounts", {"limit": 1})
        return data.get("accounts", [{}])[0]
    except:
        return {}

async def get_sample_contact():
    try:
        data = await ac_get("contacts", {"limit": 1})
        return data.get("contacts", [{}])[0]
    except:
        return {}

async def get_sample_deal():
    try:
        data = await ac_get("deals", {"limit": 1})
        return data.get("deals", [{}])[0]
    except:
        return {}


# ═══════════════════════════════════════════════════════════════════════════
# SMART FILTERING - Get unique values for dropdown filters
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/field-values/{object_type}/{field_id}")
async def get_field_values(object_type: str, field_id: str):
    """Get unique values for a field (for dropdown filters)."""
    
    # For demo/performance, limit to first 1000 records
    # In production, you'd cache this or use AC's aggregation if available
    
    values = set()
    
    try:
        if object_type == "slp":
            records = await ac_get_all(f"customObjects/records/{SLP_SCHEMA_ID}", "records", {})
            for r in records[:1000]:
                for field_obj in r.get("fields", []):
                    if field_obj.get("id") == field_id:
                        val = field_obj.get("value")
                        if val:
                            values.add(str(val))
        
        elif object_type == "accounts":
            records = await ac_get_all("accounts", "accounts", {})
            for r in records[:1000]:
                val = r.get(field_id)
                if val:
                    values.add(str(val))
        
        # Sort and return
        return {"values": sorted(list(values))}
    
    except:
        return {"values": []}


# ═══════════════════════════════════════════════════════════════════════════
# REPORT EXECUTION
# ═══════════════════════════════════════════════════════════════════════════

def evaluate_filter(record: dict, filter_def: dict) -> bool:
    """Evaluate a single filter against a record."""
    from datetime import datetime, timedelta, timezone
    
    field = filter_def.get("field")
    filter_type = filter_def.get("type", "text")
    operator = filter_def.get("operator", "equals")
    value = filter_def.get("value")
    values = filter_def.get("values", [])  # For multi-select
    date_range = filter_def.get("dateRange")
    
    if not field:
        return True
    
    # For non-date filters, value is required
    if filter_type != "date" and not value and not values:
        return True
    
    record_value = record.get(field)
    
    # Text filters
    if filter_type == "text":
        if record_value is None:
            return False
        
        record_str = str(record_value).lower()
        
        # Multi-select (OR logic)
        if values:
            return any(str(v).lower() in record_str for v in values)
        
        # Single value
        value_str = str(value).lower()
        
        if operator == "equals":
            return record_str == value_str
        elif operator == "contains":
            return value_str in record_str
        elif operator == "starts_with":
            return record_str.startswith(value_str)
        elif operator == "not_equals":
            return record_str != value_str
    
    # Dropdown filters (multi-select)
    elif filter_type == "dropdown":
        if record_value is None:
            return False
        
        if values:
            return str(record_value) in values
        elif value:
            return str(record_value) == str(value)
    
    # Date filters
    elif filter_type == "date":
        if record_value is None or record_value == "" or record_value == "null":
            return False
        
        try:
            # Parse record date
            if isinstance(record_value, str):
                if not record_value.strip():  # Empty string
                    return False
                if 'T' in record_value:
                    record_date = datetime.fromisoformat(record_value.replace('Z', '+00:00'))
                else:
                    # Try parsing as date only (YYYY-MM-DD)
                    record_date = datetime.strptime(record_value, '%Y-%m-%d')
            else:
                record_date = record_value
            
            
            # Make timezone-aware
            if record_date.tzinfo is None:
                record_date = record_date.replace(tzinfo=timezone.utc)
            
            # Handle date ranges
            if date_range:
                now = datetime.now(timezone.utc)
                
                if date_range == "last_7_days":
                    result = record_date >= (now - timedelta(days=7))
                    return result
                elif date_range == "last_30_days":
                    result = record_date >= (now - timedelta(days=30))
                    return result
                elif date_range == "last_90_days":
                    result = record_date >= (now - timedelta(days=90))
                    return result
                elif date_range == "next_30_days":
                    result = now <= record_date <= (now + timedelta(days=30))
                    return result
                elif date_range == "next_90_days":
                    result = now <= record_date <= (now + timedelta(days=90))
                    return result
                elif date_range == "this_month":
                    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    # Get last day of current month
                    if now.month == 12:
                        end = now.replace(year=now.year+1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
                    else:
                        end = now.replace(month=now.month+1, day=1, hour=0, minute=0, second=0, microsecond=0)
                    
                    result = start <= record_date < end
                    return result
                elif date_range == "last_month":
                    start = (now.replace(day=1) - timedelta(days=1)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    end = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    result = start <= record_date < end
                    return result
                elif date_range == "custom":
                    from_date = filter_def.get("fromDate")
                    to_date = filter_def.get("toDate")
                    
                    
                    if from_date:
                        from_dt = datetime.strptime(from_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                        if record_date < from_dt:
                            return False
                    
                    if to_date:
                        to_dt = datetime.strptime(to_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
                        if record_date > to_dt:
                            return False
                    
                    return True
        
        except Exception as e:
            return False
    
    return True


@app.get("/api/report")
async def generate_report(
    object_type: str = Query(...),
    fields: str = Query(...),
    filters: Optional[str] = Query(None),
    dedup_field: Optional[str] = Query(None)
):
    """Generate a report with cross-object data."""
    
    field_list = fields.split(",") if fields else []
    filter_list = json.loads(filters) if filters else []
    
    print(f"\n{'='*70}")
    print(f"REPORT REQUEST: {object_type}")
    print(f"Fields: {len(field_list)}")
    print(f"Filters: {len(filter_list)}")
    print(f"{'='*70}\n")
    
    records = []
    
    # ─── Fetch Primary Records ───
    
    if object_type == "slp":
        records = await fetch_slp_records()
    elif object_type == "license_details":
        records = await fetch_license_records()
    elif object_type == "accounts":
        # Check if we actually need custom fields
        needs_custom_fields = any(f.startswith('customfield_') for f in field_list)
        needs_custom_fields_for_filter = any(f.get('field', '').startswith('customfield_') for f in filter_list)
        
        if needs_custom_fields or needs_custom_fields_for_filter:
            records = await fetch_account_records()
        else:
            # Fast path - just get basic account info without custom fields
            print(f"Skipping custom fields (not needed for selected columns)")
            records = await fetch_account_records_basic()
    elif object_type == "contacts":
        # Check if we need custom fields
        needs_custom_fields = any(f.startswith('customfield_') for f in field_list)
        needs_custom_fields_for_filter = any(f.get('field', '').startswith('customfield_') for f in filter_list)
        
        if needs_custom_fields or needs_custom_fields_for_filter:
            records = await fetch_contact_records()
        else:
            print(f"Skipping contact custom fields (not needed)")
            records = await fetch_contact_records_basic()
    elif object_type == "deals":
        # Check if we need custom fields  
        needs_custom_fields = any(f.startswith('customfield_') for f in field_list)
        needs_custom_fields_for_filter = any(f.get('field', '').startswith('customfield_') for f in filter_list)
        
        if needs_custom_fields or needs_custom_fields_for_filter:
            records = await fetch_deal_records()
        else:
            print(f"Skipping deal custom fields (not needed)")
            records = await fetch_deal_records_basic()
    else:
        raise HTTPException(status_code=400, detail="Invalid object type")
    
    print(f"Fetched {len(records)} primary records")
    
    # ─── Apply Primary Filters ───
    
    primary_filters = [f for f in filter_list if not any(f.get('field', '').startswith(prefix) for prefix in ['account.', 'contact.', 'deal.'])]
    related_filters = [f for f in filter_list if any(f.get('field', '').startswith(prefix) for prefix in ['account.', 'contact.', 'deal.'])]
    
    if primary_filters:
        print(f"Applying {len(primary_filters)} primary filters...")
        
        # Check a sample record
        if records:
            sample = records[0]
            
            # Show all unique values in customfield_19
            status_values = set()
            for rec in records[:100]:  # Check first 100 records
                val = rec.get('customfield_19')
                if val:
                    status_values.add(val)
        
        records = [r for r in records if all(evaluate_filter(r, f) for f in primary_filters)]
        print(f"After primary filtering: {len(records)} records")
    
    # ─── Fetch Related Data ───
    
    if any(f.startswith('account.') for f in field_list) or related_filters:
        records = await enrich_with_accounts(records, object_type, field_list)
    
    if any(f.startswith('contact.') for f in field_list) or related_filters:
        records = await enrich_with_contacts(records, object_type)
    
    # ─── Apply Related Filters ───
    
    if related_filters:
        print(f"Applying {len(related_filters)} related filters...")
        records = [r for r in records if all(evaluate_filter(r, f) for f in related_filters)]
        print(f"After related filtering: {len(records)} records")
    
    # ─── Apply Deduplication ───
    
    if dedup_field:
        print(f"Deduplicating by {dedup_field}...")
        records = deduplicate_records(records, dedup_field)
        print(f"After deduplication: {len(records)} records")
    
    # ─── Select Fields ───
    
    final_records = []
    for r in records:
        row = {}
        for field_id in field_list:
            row[field_id] = r.get(field_id, "")
        final_records.append(row)
    
    print(f"\nReturning {len(final_records)} records with {len(field_list)} columns\n")
    
    return {"count": len(final_records), "records": final_records}


# ─── Data Fetchers ───

async def fetch_slp_records() -> list:
    """Fetch and normalize SLP records."""
    records_data = await ac_get_all(f"customObjects/records/{SLP_SCHEMA_ID}", "records", {})
    records = []
    
    for r in records_data:
        flat = {"id": r.get("id"), "_relationships": r.get("relationships", {})}
        for field_obj in r.get("fields", []):
            flat[field_obj.get("id")] = field_obj.get("value")
        records.append(flat)
    
    return records

async def fetch_license_records() -> list:
    """Fetch and normalize License records."""
    records_data = await ac_get_all(f"customObjects/records/{LICENSE_SCHEMA_ID}", "records", {})
    records = []
    
    for r in records_data:
        flat = {"id": r.get("id"), "_relationships": r.get("relationships", {})}
        for field_obj in r.get("fields", []):
            flat[field_obj.get("id")] = field_obj.get("value")
        records.append(flat)
    
    return records

async def fetch_account_records() -> list:
    """Fetch and normalize Account records WITH custom fields."""
    accounts_data = await ac_get_all("accounts", "accounts", {})
    records = []
    
    for acc in accounts_data:
        flat = {"id": acc.get("id")}
        for key, val in acc.items():
            if key not in ["links"] and not isinstance(val, dict):
                flat[key] = val
        records.append(flat)
    
    # Fetch custom fields with progress and optimized batching
    print(f"Fetching custom fields for {len(records)} accounts...")
    
    batch_size = 500  # Increased from 100
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        print(f"  Progress: {i}/{len(records)} ({(i/len(records)*100):.1f}%)")
        
        # Fetch in parallel for this batch (with rate limiting via semaphore)
        tasks = [fetch_account_custom_fields(rec) for rec in batch]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    print(f"  ✓ Completed: {len(records)}/{len(records)} (100%)")
    
    return records


async def fetch_account_records_basic() -> list:
    """Fetch Account records WITHOUT custom fields (FAST)."""
    accounts_data = await ac_get_all("accounts", "accounts", {})
    records = []
    
    for acc in accounts_data:
        flat = {"id": acc.get("id")}
        for key, val in acc.items():
            if key not in ["links"] and not isinstance(val, dict):
                flat[key] = val
        records.append(flat)
    
    print(f"Fetched {len(records)} accounts (basic mode - no custom fields)")
    return records


async def fetch_account_custom_fields(record: dict):
    """Fetch custom fields for a single account record with caching and rate limiting."""
    account_id = record['id']
    
    # Check cache first
    cached = get_cached("account_custom_fields", account_id)
    if cached:
        record.update(cached)
        return
    
    async with semaphore:  # Rate limiting
        try:
            data = await ac_get(f"accounts/{account_id}/accountCustomFieldData")
            
            # Store in both record and cache
            custom_fields = {}
            for cf in data.get("customerAccountCustomFieldData", []):
                field_id = cf.get("custom_field_id")
                field_value = (cf.get("custom_field_text_value") or 
                              cf.get("custom_field_date_value") or 
                              cf.get("custom_field_number_value") or 
                              cf.get("custom_field_currency_value"))
                
                if field_id and field_value is not None:
                    key = f"customfield_{field_id}"
                    record[key] = field_value
                    custom_fields[key] = field_value
            
            # Cache the result
            set_cached("account_custom_fields", account_id, custom_fields)
            
        except Exception as e:
            pass  # Silently fail - don't log in production

async def fetch_contact_records() -> list:
    """Fetch and normalize Contact records WITH custom fields."""
    contacts_data = await ac_get_all("contacts", "contacts", {})
    records = []
    
    for c in contacts_data:
        flat = {"id": c.get("id"), "_account_id": c.get("account")}
        for key, val in c.items():
            if key not in ["links", "fieldValues"] and not isinstance(val, dict):
                flat[key] = val
        records.append(flat)
    
    # Fetch custom fields (similar to accounts)
    print(f"Fetching custom fields for {len(records)} contacts...")
    
    batch_size = 500
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        print(f"  Progress: {i}/{len(records)} ({(i/len(records)*100):.1f}%)")
        tasks = [fetch_contact_custom_fields(rec) for rec in batch]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    print(f"  ✓ Completed: {len(records)}/{len(records)} (100%)")
    
    return records


async def fetch_contact_records_basic() -> list:
    """Fetch Contact records WITHOUT custom fields (FAST)."""
    contacts_data = await ac_get_all("contacts", "contacts", {})
    records = []
    
    for c in contacts_data:
        flat = {"id": c.get("id"), "_account_id": c.get("account")}
        for key, val in c.items():
            if key not in ["links", "fieldValues"] and not isinstance(val, dict):
                flat[key] = val
        records.append(flat)
    
    print(f"Fetched {len(records)} contacts (basic mode)")
    return records


async def fetch_contact_custom_fields(record: dict):
    """Fetch custom fields for a single contact with caching."""
    contact_id = record['id']
    
    # Check cache
    cached = get_cached("contact_custom_fields", contact_id)
    if cached:
        record.update(cached)
        return
    
    async with semaphore:
        try:
            data = await ac_get(f"contacts/{contact_id}/fieldValues")
            custom_fields = {}
            
            for fv in data.get("fieldValues", []):
                field_id = fv.get("field")
                field_value = fv.get("value")
                
                if field_id and field_value is not None:
                    key = f"customfield_{field_id}"
                    record[key] = field_value
                    custom_fields[key] = field_value
            
            set_cached("contact_custom_fields", contact_id, custom_fields)
        except:
            pass

async def fetch_deal_records() -> list:
    """Fetch and normalize Deal records WITH custom fields."""
    deals_data = await ac_get_all("deals", "deals", {})
    records = []
    
    for d in deals_data:
        flat = {
            "id": d.get("id"),
            "_contact_id": d.get("contact"),
            "_account_id": d.get("account")
        }
        for key, val in d.items():
            if key not in ["links"] and not isinstance(val, dict):
                flat[key] = val
        records.append(flat)
    
    # Fetch custom fields
    print(f"Fetching custom fields for {len(records)} deals...")
    
    batch_size = 500
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        print(f"  Progress: {i}/{len(records)} ({(i/len(records)*100):.1f}%)")
        tasks = [fetch_deal_custom_fields(rec) for rec in batch]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    print(f"  ✓ Completed: {len(records)}/{len(records)} (100%)")
    
    return records


async def fetch_deal_records_basic() -> list:
    """Fetch Deal records WITHOUT custom fields (FAST)."""
    deals_data = await ac_get_all("deals", "deals", {})
    records = []
    
    for d in deals_data:
        flat = {
            "id": d.get("id"),
            "_contact_id": d.get("contact"),
            "_account_id": d.get("account")
        }
        for key, val in d.items():
            if key not in ["links"] and not isinstance(val, dict):
                flat[key] = val
        records.append(flat)
    
    print(f"Fetched {len(records)} deals (basic mode)")
    return records


async def fetch_deal_custom_fields(record: dict):
    """Fetch custom fields for a single deal with caching."""
    deal_id = record['id']
    
    # Check cache
    cached = get_cached("deal_custom_fields", deal_id)
    if cached:
        record.update(cached)
        return
    
    async with semaphore:
        try:
            data = await ac_get(f"deals/{deal_id}/dealCustomFieldData")
            custom_fields = {}
            
            for cf in data.get("dealCustomFieldData", []):
                field_id = cf.get("customFieldId")
                field_value = cf.get("fieldValue")
                
                if field_id and field_value is not None:
                    key = f"customfield_{field_id}"
                    record[key] = field_value
                    custom_fields[key] = field_value
            
            set_cached("deal_custom_fields", deal_id, custom_fields)
        except:
            pass


# ─── Data Enrichment ───

async def enrich_with_accounts(records: list, source_type: str, field_list: list = []) -> list:
    """Add account data to records. Only fetch custom fields if needed."""
    
    # Extract account IDs
    account_ids = set()
    for rec in records:
        if source_type == "slp" or source_type == "license_details":
            account_rel = rec.get("_relationships", {}).get("account", [])
            if isinstance(account_rel, list) and len(account_rel) > 0:
                account_ids.add(account_rel[0])
        elif source_type in ["contacts", "deals"]:
            acc_id = rec.get("_account_id")
            if acc_id:
                account_ids.add(acc_id)
    
    if not account_ids:
        return records
    
    print(f"Fetching {len(account_ids)} accounts...")
    
    # Check if we need custom fields
    need_custom_fields = any('account.customfield_' in f for f in field_list)
    
    # Fetch accounts (basic info only)
    accounts_map = {}
    for acc_id in account_ids:
        try:
            data = await ac_get(f"accounts/{acc_id}")
            accounts_map[acc_id] = data.get("account", {})
        except:
            accounts_map[acc_id] = {}
    
    # Only fetch custom fields if we need them
    custom_fields_map = {}
    if need_custom_fields:
        print(f"Fetching account custom fields...")
        for acc_id in account_ids:
            try:
                data = await ac_get(f"accounts/{acc_id}/accountCustomFieldData")
                custom_fields_map[acc_id] = {}
                
                # CRITICAL: API returns "customerAccountCustomFieldData" not "accountCustomFieldData"
                for cf in data.get("customerAccountCustomFieldData", []):
                    field_id = cf.get("custom_field_id")  # Note: "custom_field_id" not "customFieldId"
                    
                    # Try multiple value fields
                    field_value = (cf.get("custom_field_text_value") or 
                                  cf.get("custom_field_date_value") or 
                                  cf.get("custom_field_number_value") or 
                                  cf.get("custom_field_currency_value"))
                    
                    if field_id and field_value is not None:
                        custom_fields_map[acc_id][field_id] = field_value
            except:
                custom_fields_map[acc_id] = {}
    else:
        print(f"Skipping custom fields (not needed)")
    
    # Merge into records
    for rec in records:
        if source_type == "slp" or source_type == "license_details":
            account_rel = rec.get("_relationships", {}).get("account", [])
            acc_id = account_rel[0] if isinstance(account_rel, list) and len(account_rel) > 0 else None
        else:
            acc_id = rec.get("_account_id")
        
        if acc_id and acc_id in accounts_map:
            account = accounts_map[acc_id]
            for key, val in account.items():
                if key not in ["links"] and not isinstance(val, dict):
                    rec[f"account.{key}"] = val
            
            if need_custom_fields and acc_id in custom_fields_map:
                for field_id, field_value in custom_fields_map[acc_id].items():
                    rec[f"account.customfield_{field_id}"] = field_value
    
    # Clean up
    for rec in records:
        rec.pop("_relationships", None)
        rec.pop("_account_id", None)
    
    return records


async def enrich_with_contacts(records: list, source_type: str) -> list:
    """Add contact data to records."""
    
    contact_ids = set()
    for rec in records:
        if source_type == "deals":
            contact_id = rec.get("_contact_id")
            if contact_id:
                contact_ids.add(contact_id)
    
    if not contact_ids:
        return records
    
    print(f"Fetching {len(contact_ids)} contacts...")
    
    contacts_map = {}
    for contact_id in contact_ids:
        try:
            data = await ac_get(f"contacts/{contact_id}")
            contacts_map[contact_id] = data.get("contact", {})
        except:
            contacts_map[contact_id] = {}
    
    # Merge
    for rec in records:
        contact_id = rec.get("_contact_id")
        if contact_id and contact_id in contacts_map:
            contact = contacts_map[contact_id]
            for key, val in contact.items():
                if key not in ["links", "fieldValues"] and not isinstance(val, dict):
                    rec[f"contact.{key}"] = val
    
    # Clean up
    for rec in records:
        rec.pop("_contact_id", None)
    
    return records


def deduplicate_records(records: list, dedup_field: str) -> list:
    """Remove duplicate records based on a field."""
    seen = {}
    deduped = []
    
    for rec in records:
        key = rec.get(dedup_field)
        if not key:
            deduped.append(rec)
            continue
        
        if key not in seen:
            seen[key] = rec
            deduped.append(rec)
        else:
            # Keep the one with the most recent activation date
            existing = seen[key]
            existing_date = existing.get('contractor-activated-date', '')
            current_date = rec.get('contractor-activated-date', '')
            
            if current_date > existing_date:
                deduped[deduped.index(existing)] = rec
                seen[key] = rec
    
    return deduped


# ═══════════════════════════════════════════════════════════════════════════
# CSV EXPORT
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/report/csv")
async def export_csv(
    object_type: str = Query(...),
    fields: str = Query(...),
    filters: Optional[str] = Query(None),
    dedup_field: Optional[str] = Query(None)
):
    """Export report as CSV with professional header."""
    
    # Generate report
    result = await generate_report(object_type, fields, filters, dedup_field)
    records = result["records"]
    
    if not records:
        raise HTTPException(status_code=404, detail="No records to export")
    
    # Parse filters for display
    filter_list = json.loads(filters) if filters else []
    
    # Get field labels
    field_labels = {}
    fields_data = await get_fields(object_type)
    for f in fields_data["fields"]:
        field_labels[f["id"]] = f["label"]
    
    # Create CSV with header
    output = io.StringIO()
    from datetime import datetime
    
    # Write report header
    output.write(f"AC Reporter Export\n")
    output.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    output.write(f"\n")
    output.write(f"Object Type: {object_type.upper()}\n")
    output.write(f"Total Records: {len(records)}\n")
    
    if filter_list:
        output.write(f"\nFilters Applied:\n")
        for f in filter_list:
            field_label = field_labels.get(f.get('field'), f.get('field'))
            operator = f.get('operator', 'equals')
            value = f.get('value', '')
            date_range = f.get('dateRange', '')
            if date_range:
                output.write(f"  - {field_label}: {date_range}\n")
            else:
                output.write(f"  - {field_label} {operator} '{value}'\n")
    
    if dedup_field:
        dedup_label = field_labels.get(dedup_field, dedup_field)
        output.write(f"\nDeduplication: {dedup_label}\n")
    
    output.write(f"\n")
    output.write("=" * 80 + "\n")
    output.write(f"\n")
    
    # Write data with friendly column names
    if records:
        # Map field IDs to labels for headers
        fieldnames = list(records[0].keys())
        headers = []
        
        for field_id in fieldnames:
            # Look up the friendly label, fallback to field_id if not found
            header = field_labels.get(field_id, field_id)
            headers.append(header)
        
        
        writer = csv.writer(output)
        writer.writerow(headers)
        
        for record in records:
            writer.writerow([record.get(k, '') for k in fieldnames])
    
    # Return as download
    csv_content = output.getvalue()
    filename = f"report_{object_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return StreamingResponse(
        iter([csv_content]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )



# ═══════════════════════════════════════════════════════════════════════════
# CONTACT LOOKUP - CLEAN ACTIVITY VIEW (NO AUTOMATION NOISE)
# ═══════════════════════════════════════════════════════════════════════════

# Activity types to SHOW (everything else, including automations, is filtered out)
ALLOWED_ACTIVITY_TYPES = {
    "send",          # Email sent
    "open",          # Email opened
    "click",         # Email link clicked
    "bounce",        # Email bounced
    "forward",       # Email forwarded
    "unsubscribe",   # Unsubscribed
    "note",          # Note added on contact
    "task",          # Task (calls logged as tasks)
}

ACTIVITY_LABELS = {
    "send": "Email Sent",
    "open": "Email Opened",
    "click": "Email Clicked",
    "bounce": "Email Bounced",
    "forward": "Email Forwarded",
    "unsubscribe": "Unsubscribed",
    "note": "Note",
    "task": "Task / Call",
}

ACTIVITY_ICONS = {
    "send": "📧",
    "open": "📬",
    "click": "🔗",
    "bounce": "⚠️",
    "forward": "↩️",
    "unsubscribe": "🚫",
    "note": "📝",
    "task": "📞",
}


@app.get("/api/contact-search")
async def contact_search(q: str = Query(..., min_length=2)):
    """Search contacts by name, email, or phone."""
    results = []
    seen_ids = set()

    async with httpx.AsyncClient(timeout=30) as client:
        # Search by email
        try:
            r = await client.get(ac_url("contacts"), headers=HEADERS, params={"search": q, "limit": 20})
            r.raise_for_status()
            for c in r.json().get("contacts", []):
                if c["id"] not in seen_ids:
                    seen_ids.add(c["id"])
                    results.append({
                        "id": c["id"],
                        "firstName": c.get("firstName", ""),
                        "lastName": c.get("lastName", ""),
                        "email": c.get("email", ""),
                        "phone": c.get("phone", ""),
                        "orgName": c.get("orgname", ""),
                    })
        except Exception:
            pass

    return {"contacts": results[:20]}


@app.get("/api/contact-profile/{contact_id}")
async def contact_profile(contact_id: str):
    """Return contact details + filtered activity (no automation noise)."""

    # Fetch contact record and activity log concurrently
    contact_task = ac_get(f"contacts/{contact_id}")
    activity_task = ac_get(f"contacts/{contact_id}/activityLogs", {"limit": 100})
    notes_task = ac_get(f"contacts/{contact_id}/notes", {"limit": 50})

    contact_data, activity_data, notes_data = await asyncio.gather(
        contact_task, activity_task, notes_task, return_exceptions=True
    )

    # ── Contact Details ──
    contact = {}
    if isinstance(contact_data, dict):
        c = contact_data.get("contact", {})
        contact = {
            "id": c.get("id"),
            "firstName": c.get("firstName", ""),
            "lastName": c.get("lastName", ""),
            "email": c.get("email", ""),
            "phone": c.get("phone", ""),
            "orgName": c.get("orgname", ""),
            "created": c.get("cdate", ""),
            "updated": c.get("udate", ""),
        }

    # ── Filtered Activity Feed ──
    activity = []

    # From activityLogs endpoint
    if isinstance(activity_data, dict):
        for log in activity_data.get("contactActivities", []):
            a_type = log.get("type", "").lower()
            if a_type in ALLOWED_ACTIVITY_TYPES:
                activity.append({
                    "type": a_type,
                    "label": ACTIVITY_LABELS.get(a_type, a_type),
                    "icon": ACTIVITY_ICONS.get(a_type, "•"),
                    "description": log.get("subject") or log.get("campaign", {}).get("name", "") if isinstance(log.get("campaign"), dict) else log.get("subject", ""),
                    "timestamp": log.get("tstamp", log.get("cdate", "")),
                })

    # From notes endpoint (notes don't always appear in activityLogs)
    if isinstance(notes_data, dict):
        for note in notes_data.get("notes", []):
            activity.append({
                "type": "note",
                "label": "Note",
                "icon": "📝",
                "description": note.get("note", ""),
                "timestamp": note.get("cdate", ""),
            })

    # Sort by timestamp descending
    def parse_ts(item):
        ts = item.get("timestamp", "")
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            return datetime.min.replace(tzinfo=None)

    activity.sort(key=parse_ts, reverse=True)

    return {"contact": contact, "activity": activity}


# ═══════════════════════════════════════════════════════════════════════════
# FRONTEND
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/mover")
async def serve_mover_ui():
    return FileResponse("static/mover.html")

@app.get("/contacts")
async def serve_contact_lookup():
    return FileResponse("static/contact_lookup.html")

@app.get("/")
async def serve_ui():
    return FileResponse("static/index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

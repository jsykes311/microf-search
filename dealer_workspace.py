"""Authenticated, account-scoped ActiveCampaign workspace actions."""
import asyncio
import hashlib
import json
import re
from datetime import datetime, timedelta
from typing import Literal
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator

CONTACT_FIELDS = ('firstName', 'lastName', 'email', 'phone')

def contact_version(contact):
    return hashlib.sha256(json.dumps({k: contact.get(k) or '' for k in CONTACT_FIELDS}, sort_keys=True).encode()).hexdigest()

class ContactEdit(BaseModel):
    model_config = ConfigDict(extra='forbid', str_strip_whitespace=True)
    firstName: str = Field(max_length=100)
    lastName: str = Field(max_length=100)
    email: str = Field(min_length=3, max_length=254)
    phone: str = Field(max_length=60)
    version: str

    @field_validator('email')
    @classmethod
    def valid_email(cls, value):
        if not re.fullmatch(r'[^\s@]+@[^\s@]+\.[^\s@]+', value):
            raise ValueError('Enter a valid email address')
        return value

class FollowUp(BaseModel):
    model_config = ConfigDict(extra='forbid', str_strip_whitespace=True)
    title: str = Field(min_length=1, max_length=250)
    note: str = Field(default='', max_length=5000)
    contact_id: str = Field(pattern=r'^\d+$')
    assignee: str = Field(pattern=r'^\d+$')
    task_type: str = Field(pattern=r'^\d+$')
    due: datetime

    @field_validator('due')
    @classmethod
    def timezone_required(cls, value):
        if value.tzinfo is None: raise ValueError('Due date must include a timezone')
        return value

class TaskStatus(BaseModel):
    model_config = ConfigDict(extra='forbid')
    status: Literal[0, 1]


def install_workspace(app, get_email, ac_get, ac_post, ac_put, ui_base):
    router = APIRouter()
    slots = asyncio.Semaphore(5)

    def auth(request: Request):
        email = get_email(request)
        if not email: raise HTTPException(401, 'Sign in to use the dealer workspace')
        return email

    def mutation(request: Request, email=Depends(auth)):
        if request.headers.get('X-Workspace-Request') != '1' or request.headers.get('Sec-Fetch-Site') == 'cross-site':
            raise HTTPException(403, 'Invalid workspace request')
        origin = request.headers.get('Origin')
        if origin and origin.rstrip('/') != str(request.base_url).rstrip('/'):
            raise HTTPException(403, 'Invalid request origin')
        return email

    async def call(fn, *args):
        try:
            async with slots: return await fn(*args)
        except HTTPException: raise
        except Exception:
            raise HTTPException(502, 'ActiveCampaign could not complete this request. Refresh to check the latest data before retrying.')

    async def pages(path, key, params=None):
        rows, seen = [], set()
        for offset in range(0, 10000, 100):
            data = await call(ac_get, path, {**(params or {}), 'limit':100, 'offset':offset})
            batch = data.get(key, [])
            new = [r for r in batch if str(r.get('id')) not in seen]
            rows.extend(new); seen.update(str(r.get('id')) for r in new)
            if len(batch) < 100 or not new: return rows
        raise HTTPException(502, 'Too many records to load safely. Open this account in ActiveCampaign.')

    def numeric(value):
        if not re.fullmatch(r'\d+', value): raise HTTPException(422, 'Invalid record ID')
        return value

    async def contacts_for(account_id):
        numeric(account_id)
        rows = await pages(f'accounts/{account_id}/contacts', 'accountContacts')
        return {str(r.get('contact')) for r in rows if r.get('contact')}

    async def check_contact(account_id, contact_id):
        numeric(contact_id)
        if contact_id not in await contacts_for(account_id):
            raise HTTPException(404, 'This contact is not linked to this dealer. Refresh the workspace.')

    async def roster():
        users, types = await asyncio.gather(pages('users', 'users'), pages('dealTasktypes', 'dealTasktypes'))
        return ([{'id':str(u['id']), 'name':(' '.join([u.get('firstName',''),u.get('lastName','')])).strip() or u.get('email',''), 'email':u.get('email','')}
                 for u in users if str(u.get('active','1')).lower() not in ('0','false')],
                [{'id':str(t['id']), 'name':t.get('title') or t.get('name') or 'Task'} for t in types])

    @router.get('/dealer-workspace')
    async def page(email=Depends(auth)):
        return FileResponse('static/dealer-workspace.html', headers={'Cache-Control':'no-store'})

    @router.get('/api/workspace/context')
    async def context(email=Depends(auth)):
        users, types = await roster()
        return {'users':users, 'taskTypes':types, 'email':email}

    @router.get('/api/workspace/{account_id}/contacts')
    async def contacts(account_id: str, email=Depends(auth)):
        ids = await contacts_for(account_id)
        rows = []
        ordered = sorted(ids)
        for start in range(0, len(ordered), 50):
            batch = await pages('contacts', 'contacts', {'ids[]':ordered[start:start+50]})
            rows.extend(c for c in batch if str(c.get('id')) in ids)
        return {'contacts':[{**{k:(c.get(k) or '') for k in CONTACT_FIELDS}, 'id':str(c['id']),
                             'version':contact_version(c), 'url':f"{ui_base}/app/contacts/{c['id']}"} for c in rows]}

    @router.put('/api/workspace/{account_id}/contacts/{contact_id}')
    async def edit_contact(account_id: str, contact_id: str, body: ContactEdit, email=Depends(mutation)):
        await check_contact(account_id, contact_id)
        current = (await call(ac_get, f'contacts/{contact_id}')).get('contact', {})
        if contact_version(current) != body.version:
            raise HTTPException(409, 'This contact changed in ActiveCampaign. Refresh and review the latest details before saving.')
        changes = {k:getattr(body,k) for k in CONTACT_FIELDS if getattr(body,k) != (current.get(k) or '')}
        if changes: await call(ac_put, f'contacts/{contact_id}', {'contact':changes})
        return {'ok':True}

    @router.get('/api/workspace/{account_id}/tasks')
    async def tasks(account_id: str, email=Depends(auth)):
        ids = await contacts_for(account_id)
        rows = await pages('dealTasks', 'dealTasks', {'filters[reltype]':'Subscriber', 'filters[status]':0}) if ids else []
        result = [{k:t.get(k) for k in ('id','title','note','duedate','status','assignee','relid')} for t in rows
                  if str(t.get('relid')) in ids and t.get('reltype') == 'Subscriber' and str(t.get('status')) == '0']
        result.sort(key=lambda t:t.get('duedate') or '9999')
        return {'tasks':result}

    @router.post('/api/workspace/{account_id}/tasks')
    async def create_task(account_id: str, body: FollowUp, email=Depends(mutation)):
        await check_contact(account_id, body.contact_id)
        users, types = await roster()
        if body.assignee not in {u['id'] for u in users} or body.task_type not in {t['id'] for t in types}:
            raise HTTPException(422, 'Choose a current ActiveCampaign owner and task type')
        if body.due <= datetime.now(body.due.tzinfo): raise HTTPException(422, 'Choose a future due date and time')
        result = await call(ac_post, 'dealTasks', {'dealTask':{
            'title':body.title, 'note':body.note, 'ownerType':'contact', 'relid':body.contact_id,
            'assignee':body.assignee, 'dealTasktype':body.task_type, 'status':0,
            'duedate':body.due.isoformat(), 'edate':(body.due+timedelta(minutes=15)).isoformat()}})
        return {'ok':True, 'id':result.get('dealTask',{}).get('id')}

    @router.patch('/api/workspace/{account_id}/tasks/{task_id}')
    async def complete_task(account_id: str, task_id: str, body: TaskStatus, email=Depends(mutation)):
        numeric(task_id)
        task = (await call(ac_get, f'dealTasks/{task_id}')).get('dealTask', {})
        if task.get('reltype') != 'Subscriber': raise HTTPException(404, 'Contact task not found')
        await check_contact(account_id, str(task.get('relid','')))
        await call(ac_put, f'dealTasks/{task_id}', {'dealTask':{'status':body.status}})
        return {'ok':True}

    app.include_router(router)

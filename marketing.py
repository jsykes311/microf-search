"""Marketing project intake, durable files, and transactional email outbox."""
import asyncio
import contextlib
import hashlib
import html
import json
import logging
import os
import sqlite3
import time
import uuid
from datetime import date as Date, datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Literal

import aiosmtplib
from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator
from ac_client import fetch_all_pages

log = logging.getLogger(__name__)
STATES = Literal['New Request', 'Scheduled', 'In Progress', 'Awaiting Feedback', 'Complete']
MANAGER = os.getenv('MARKETING_MANAGER_EMAIL', 'jsykes@microf.com').strip().lower()
ROOT = Path(os.getenv('MARKETING_DATA_DIR', '/var/data/marketing' if os.getenv('RENDER') else './data/marketing'))
BASE = os.getenv('RENDER_EXTERNAL_URL', 'http://127.0.0.1:8001').rstrip('/')
MAX_FILE = 25 * 1024 * 1024
TYPES = {'Email campaign','Social post','Sales material','Presentation','Website update','Training material','Other'}
AUDIENCES = {'Contractors','Customers','Partners','Job candidates','Internal team','Other'}


def now():
    return datetime.now(timezone.utc).isoformat()


def connection():
    ROOT.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(ROOT / 'projects.sqlite3', timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys=ON')
    conn.execute('PRAGMA busy_timeout=15000')
    return conn


def initialize():
    if os.getenv('RENDER') and not Path('/var/data').is_mount():
        raise RuntimeError('Marketing requires the Render persistent disk at /var/data')
    with contextlib.closing(connection()) as db:
        db.execute('PRAGMA journal_mode=WAL')
        db.executescript('''
        CREATE TABLE IF NOT EXISTS projects(id TEXT PRIMARY KEY, payload TEXT NOT NULL, version INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS uploads(id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id), name TEXT NOT NULL, size INTEGER NOT NULL, actor TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS outbox(id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id), recipient TEXT NOT NULL, subject TEXT NOT NULL, body TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0, next_try REAL NOT NULL DEFAULT 0, created TEXT NOT NULL, sent TEXT, error TEXT);
        CREATE TABLE IF NOT EXISTS requests(key TEXT PRIMARY KEY, actor TEXT NOT NULL, fingerprint TEXT NOT NULL, project_id TEXT NOT NULL REFERENCES projects(id));
        ''')
        db.commit()
    (ROOT / 'files').mkdir(exist_ok=True)


class Intake(BaseModel):
    model_config = ConfigDict(extra='forbid', str_strip_whitespace=True)
    title: str = Field(min_length=1, max_length=120)
    type: str
    audience: str
    brief: str = Field(min_length=1, max_length=10000)
    approver: str = Field(min_length=1, max_length=200)
    requestedById: str = Field(min_length=1, max_length=30)
    requested: Date | None = None
    reason: str = Field(default='', max_length=1000)

    @field_validator('title', 'approver')
    @classmethod
    def single_line(cls, value):
        if '\n' in value or '\r' in value:
            raise ValueError('Use one line')
        return value

    @field_validator('type')
    @classmethod
    def valid_type(cls, value):
        if value not in TYPES: raise ValueError('Select a project type')
        return value

    @field_validator('audience')
    @classmethod
    def valid_audience(cls, value):
        if value not in AUDIENCES: raise ValueError('Select an audience')
        return value


class Edit(BaseModel):
    model_config = ConfigDict(extra='forbid')
    status: STATES
    date: Date | None = None
    priority: Literal['Low','Normal','High'] = 'Normal'
    version: int = Field(ge=1)


class Note(BaseModel):
    model_config = ConfigDict(extra='forbid', str_strip_whitespace=True)
    text: str = Field(min_length=1, max_length=10000)


_users = []
_users_time = 0
_users_lock = asyncio.Lock()


async def active_users():
    # AC deletes users on offboarding; /users is the existing-user roster.
    global _users, _users_time
    async with _users_lock:
        if _users and time.monotonic() - _users_time < 60: return _users
        try:
            rows = await fetch_all_pages('users', 'users', sequential=True)
            users = []
            for u in rows:
                email = (u.get('email') or '').strip().lower()
                if not email or '@' not in email or '\n' in email or '\r' in email: continue
                if str(u.get('active', '1')).lower() in ('0','false'): continue
                name = f"{u.get('firstName','')} {u.get('lastName','')}".strip() or email
                users.append({'id':str(u['id']), 'name':name, 'email':email})
            if not users: raise ValueError('Empty user directory')
            _users = sorted(users, key=lambda u: u['name'].casefold())
            _users_time = time.monotonic()
            return _users
        except Exception:
            log.warning('Marketing user directory unavailable')
            raise HTTPException(503, 'ActiveCampaign user list is unavailable. Please try again.')


def get_project(db, pid):
    row = db.execute('SELECT payload,version FROM projects WHERE id=?', (pid,)).fetchone()
    if not row: raise HTTPException(404, 'Project not found')
    p = json.loads(row['payload'])
    p['version'] = row['version']
    return p


def public_project(p, actor):
    return {**p, 'mine': p['createdBy'] == actor or p['requesterEmail'] == actor}


def persist(db, p):
    db.execute('UPDATE projects SET payload=?,version=? WHERE id=?', (json.dumps(p),p['version'],p['id']))


def enqueue(db, p, kind, actor, note='', event_id=None):
    if kind == 'created':
        recipient = MANAGER
        subject = f"New marketing request: {p['title']}"
        body = f"{p['requestedBy']} requested {p['title']} ({p['id']}).\nSubmitted by: {actor}\nRequested deadline: {p['requested'] or 'Not specified'}\n\n{p['brief']}\n\nReview the project to confirm timing and next steps."
    elif kind == 'note':
        recipient = MANAGER
        subject = f"New note: {p['title']}"
        body = f"{actor} added a note to {p['title']} ({p['id']}).\n\n{note}"
    else:
        recipient = p['requesterEmail']
        subject = f"Your marketing project is complete: {p['title']}"
        body = f"Hi {p['requestedBy']},\n\nYour project, {p['title']} ({p['id']}), has been marked complete by Marketing.\n\nOpen the project to see the latest notes and attached files.\n\nThanks,\nJeremy Sykes\nMicrof Marketing"
    body += f"\n\nView project: {BASE}/marketing-projects?project={p['id']}"
    db.execute('INSERT INTO outbox(id,project_id,recipient,subject,body,created) VALUES(?,?,?,?,?,?)', (event_id or str(uuid.uuid4()),p['id'],recipient,subject,body,now()))


def idempotency(db, key, actor, fingerprint):
    if not key or len(key)>100: raise HTTPException(400, 'A valid Idempotency-Key is required')
    previous = db.execute('SELECT * FROM requests WHERE key=?', (key,)).fetchone()
    if previous:
        if previous['actor'] != actor or previous['fingerprint'] != fingerprint:
            raise HTTPException(409, 'This submission key was already used for a different request')
        return previous['project_id']


async def send_message(row):
    user, password = os.getenv('SMTP_USER',''), os.getenv('SMTP_PASS','')
    if not user or not password: raise RuntimeError('Email delivery is not configured')
    msg = EmailMessage()
    msg['From'] = f'Microf Marketing <{user}>'
    msg['To'] = row['recipient']
    msg['Reply-To'] = MANAGER
    msg['Subject'] = row['subject']
    msg['Message-ID'] = f"<{row['id']}@marketing.microf.com>"
    msg.set_content(row['body'])
    url = f"{BASE}/marketing-projects?project={row['project_id']}"
    body = html.escape(row['body'].split('\n\nView project:')[0]).replace('\n', '<br>')
    msg.add_alternative(f'<div style="font-family:Arial,sans-serif;max-width:640px;color:#25352b"><h2>Microf Marketing Projects</h2><p>{body}</p><p><a style="display:inline-block;background:#16984b;color:white;padding:12px 20px;text-decoration:none;border-radius:6px" href="{html.escape(url,quote=True)}">View project</a></p></div>', subtype='html')
    refused, _ = await aiosmtplib.send(msg, hostname=os.getenv('SMTP_HOST','smtp.gmail.com'), port=int(os.getenv('SMTP_PORT','587')), username=user,password=password, use_tls=int(os.getenv('SMTP_PORT','587'))==465, start_tls=int(os.getenv('SMTP_PORT','587'))!=465, timeout=30)

    if refused: raise RuntimeError("Recipient refused by email server")

async def deliver_pending():
    with contextlib.closing(connection()) as db:
        db.execute('BEGIN IMMEDIATE')
        row = db.execute("SELECT * FROM outbox WHERE state IN ('pending','sending') AND next_try<=? ORDER BY created LIMIT 1",(time.time(),)).fetchone()
        if not row: db.rollback(); return False
        db.execute("UPDATE outbox SET state='sending',next_try=? WHERE id=?",(time.time()+300,row['id']))
        db.commit()
    try:
        await send_message(row)
        with contextlib.closing(connection()) as db, db:
            db.execute("UPDATE outbox SET state='sent',sent=?,error=NULL WHERE id=?",(now(),row['id']))
    except Exception as exc:
        attempts = row['attempts']+1
        with contextlib.closing(connection()) as db, db:
            db.execute("UPDATE outbox SET state='pending',attempts=?,next_try=?,error=? WHERE id=?",(attempts,time.time()+min(3600,30*2**min(attempts,7)),type(exc).__name__,row['id']))
        log.warning('Marketing email deferred: %s',type(exc).__name__)
    return True


async def outbox_loop():
    while True:
        try:
            if not await deliver_pending(): await asyncio.sleep(10)
        except asyncio.CancelledError: raise
        except Exception:
            log.exception('Marketing outbox worker error')
            await asyncio.sleep(30)


def install_marketing(app, get_email):
    router = APIRouter()
    def auth(request: Request):
        email = (get_email(request) or '').strip().lower()
        domain = os.getenv('ALLOWED_EMAIL_DOMAIN','microf.com')
        if not email or email.rsplit('@',1)[-1] != domain: raise HTTPException(401,'Sign in with your Microf account')
        return email

    def mutation(request: Request, actor=Depends(auth)):
        if request.headers.get('X-Marketing-Request') != '1': raise HTTPException(403,'Invalid request')
        origin = request.headers.get('Origin')
        if origin and origin.rstrip('/') not in {BASE, str(request.base_url).rstrip('/')}:
            raise HTTPException(403,'Invalid request origin')
        if request.headers.get('Sec-Fetch-Site') == 'cross-site': raise HTTPException(403,'Invalid request origin')
        return actor

    @router.get('/marketing-projects')
    def page(actor=Depends(auth)):
        return FileResponse('static/marketing/index.html', headers={'Cache-Control':'no-store'})

    @router.get('/api/marketing/context')
    async def context(actor=Depends(auth)):
        users = await active_users()
        return {'users':users, 'email':actor, 'name':next((u['name'] for u in users if u['email']==actor),actor),'isManager':actor==MANAGER,'emailConfigured':bool(os.getenv('SMTP_USER') and os.getenv('SMTP_PASS'))}

    @router.get('/api/marketing/projects')
    def listing(actor=Depends(auth)):
        with contextlib.closing(connection()) as db:
            return [public_project(json.loads(r['payload']),actor) for r in db.execute('SELECT payload FROM projects ORDER BY rowid DESC')]

    @router.get('/api/marketing/projects/{pid}')
    def detail(pid: str, actor=Depends(auth)):
        with contextlib.closing(connection()) as db: return public_project(get_project(db,pid),actor)

    @router.post('/api/marketing/projects')
    async def create(payload: str=Form(...), files: list[UploadFile]=File(default=[]), key: str=Header(default='',alias='Idempotency-Key'), actor=Depends(mutation)):
        try: data=Intake.model_validate_json(payload)
        except Exception: raise HTTPException(422,'Please check the required project fields')
        if len(files)>10: raise HTTPException(422,'Up to 10 attachments are allowed')
        users=await active_users()
        requester=next((u for u in users if u['id']==data.requestedById),None)
        if not requester: raise HTTPException(422,'Select a current ActiveCampaign user')
        fingerprint=hashlib.sha256(('create:'+data.model_dump_json()).encode()).hexdigest()
        with contextlib.closing(connection()) as db:
            previous=idempotency(db,key,actor,fingerprint)
            if previous: return public_project(get_project(db,previous),actor)
        saved=[]
        try:
            for upload in files:
                uid=str(uuid.uuid4()); path=ROOT/'files'/uid; size=0
                name=Path((upload.filename or 'attachment').replace('\\','/')).name[:200]
                if not name or any(ord(c)<32 for c in name): raise HTTPException(422,'Invalid filename')
                saved.append({'id':uid,'name':name,'size':0})
                with path.open('xb') as out:
                    while chunk:=await upload.read(1024*1024):
                        size+=len(chunk)
                        if size>MAX_FILE: raise HTTPException(413,'Each file must be 25 MB or smaller')
                        out.write(chunk)
                saved[-1]['size']=size
            with contextlib.closing(connection()) as db:
                db.execute('BEGIN IMMEDIATE')
                previous=idempotency(db,key,actor,fingerprint)
                if previous:
                    for f in saved: (ROOT/'files'/f['id']).unlink(missing_ok=True)
                    return public_project(get_project(db,previous),actor)
                pid='MP-'+uuid.uuid4().hex[:10].upper()
                p={**data.model_dump(mode='json'),'id':pid,'requestedBy':requester['name'],'requesterEmail':requester['email'],'createdBy':actor,'status':'New Request','date':'','priority':'Normal','next':'Needs brief review','version':1,'createdAt':now(),'updates':[{'text':'Request received. Timing will be confirmed after review.','date':now(),'actor':actor}], 'files':[{**f,'url':'/api/marketing/files/'+f['id']} for f in saved]}
                db.execute('INSERT INTO projects VALUES(?,?,?)',(pid,json.dumps(p),1))
                for f in saved: db.execute('INSERT INTO uploads VALUES(?,?,?,?,?)',(f['id'],pid,f['name'],f['size'],actor))
                enqueue(db,p,'created',actor)
                db.execute('INSERT INTO requests VALUES(?,?,?,?)',(key,actor,fingerprint,pid))
                db.commit()
                return public_project(p,actor)
        except Exception:
            for f in saved: (ROOT/'files'/f['id']).unlink(missing_ok=True)
            raise

    @router.patch('/api/marketing/projects/{pid}')
    def edit(pid: str, body: Edit, actor=Depends(mutation)):
        if actor!=MANAGER: raise HTTPException(403,'Only the marketing manager can change project status')
        with contextlib.closing(connection()) as db:
            db.execute('BEGIN IMMEDIATE'); p=get_project(db,pid)
            if p['version']!=body.version: raise HTTPException(409,'This project changed. Close and reopen it before saving.')
            previous=p['status']
            p.update(status=body.status,date=body.date.isoformat() if body.date else '',priority=body.priority,version=p['version']+1)
            p['next']={'Complete':'Delivered','Awaiting Feedback':'Waiting on approval','Scheduled':'Scheduled for production','New Request':'Needs brief review','In Progress':'Work underway'}[body.status]
            p['updates'].insert(0,{'text':f"Project updated: {p['status']}. Target: {p['date'] or 'Not confirmed'}.",'date':now(),'actor':actor})
            persist(db,p)
            if previous!='Complete' and body.status=='Complete': enqueue(db,p,'complete',actor,event_id=f"{pid}-complete-v{p['version']}")
            db.commit(); return public_project(p,actor)

    @router.post('/api/marketing/projects/{pid}/notes')
    def note(pid: str, body: Note, key: str=Header(default='',alias='Idempotency-Key'), actor=Depends(mutation)):
        fingerprint=hashlib.sha256(('note:'+pid+':'+body.text).encode()).hexdigest()
        with contextlib.closing(connection()) as db:
            db.execute('BEGIN IMMEDIATE'); previous=idempotency(db,key,actor,fingerprint)
            if previous: return public_project(get_project(db,previous),actor)
            p=get_project(db,pid)
            p['version']+=1
            p['updates'].insert(0,{'text':body.text,'date':now(),'actor':actor})
            persist(db,p); enqueue(db,p,'note',actor,note=body.text)
            db.execute('INSERT INTO requests VALUES(?,?,?,?)',(key,actor,fingerprint,pid))
            db.commit(); return public_project(p,actor)

    @router.post('/api/marketing/projects/{pid}/files')
    async def attach(pid: str, files: list[UploadFile]=File(...),actor=Depends(mutation)):
        if not files or len(files)>10: raise HTTPException(422,'Choose 1–10 files')
        with contextlib.closing(connection()) as db:
            p=get_project(db,pid)
            if actor not in {MANAGER,p['createdBy'],p['requesterEmail']}: raise HTTPException(403,'Only the requester or manager can add files')
        saved=[]
        try:
            for f in files:
                uid=str(uuid.uuid4());size=0;name=Path((f.filename or 'attachment').replace('\\','/')).name[:200]
                if not name or any(ord(c)<32 for c in name): raise HTTPException(422,'Invalid filename')
                saved.append({'id':uid,'name':name,'size':0,'url':'/api/marketing/files/'+uid})
                with (ROOT/'files'/uid).open('xb') as out:
                    while chunk:=await f.read(1024*1024):
                        size+=len(chunk)
                        if size>MAX_FILE: raise HTTPException(413,'Each file must be 25 MB or smaller')
                        out.write(chunk)
                saved[-1]['size']=size
            with contextlib.closing(connection()) as db:
                db.execute('BEGIN IMMEDIATE'); p=get_project(db,pid)
                p['files'].extend(saved);p['version']+=1
                p['updates'].insert(0,{'text':'Files added: '+', '.join(f['name'] for f in saved),'date':now(),'actor':actor})
                for f in saved: db.execute('INSERT INTO uploads VALUES(?,?,?,?,?)',(f['id'],pid,f['name'],f['size'],actor))
                persist(db,p);db.commit();return public_project(p,actor)
        except Exception:
            for f in saved:(ROOT/'files'/f['id']).unlink(missing_ok=True)
            raise

    @router.get('/api/marketing/files/{uid}')
    def download(uid: str,actor=Depends(auth)):
        with contextlib.closing(connection()) as db: f=db.execute('SELECT * FROM uploads WHERE id=?',(uid,)).fetchone()
        if not f or not (ROOT/'files'/f['id']).is_file(): raise HTTPException(404,'File not found')
        return FileResponse(ROOT/'files'/f['id'],filename=f['name'],media_type='application/octet-stream',headers={'X-Content-Type-Options':'nosniff','Cache-Control':'private, no-store'})

    @router.get('/api/marketing/notifications')
    def notifications(actor=Depends(auth)):
        if actor!=MANAGER: raise HTTPException(403,'Manager only')
        with contextlib.closing(connection()) as db:
            return [dict(r) for r in db.execute('SELECT id,project_id,recipient,subject,body,state,attempts,created,sent,error FROM outbox ORDER BY created DESC LIMIT 100')]

    app.include_router(router)
    worker=None
    @app.on_event('startup')
    async def start():
        nonlocal worker
        initialize(); worker=asyncio.create_task(outbox_loop())
    @app.on_event('shutdown')
    async def stop():
        if worker:
            worker.cancel()
            with contextlib.suppress(asyncio.CancelledError): await worker

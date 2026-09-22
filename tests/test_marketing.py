import asyncio
import contextlib
import json
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
import marketing as m

@pytest.fixture
def client(tmp_path,monkeypatch):
    monkeypatch.setattr(m,'ROOT',tmp_path)
    async def users(): return [{'id':'1','name':'Jeremy Sykes','email':m.MANAGER},{'id':'2','name':'Requester','email':'requester@microf.com'}]
    monkeypatch.setattr(m,'active_users',users)
    m.initialize()
    app=FastAPI();m.install_marketing(app,lambda r:r.headers.get('test-user'))
    return TestClient(app)

def headers(actor='requester@microf.com',key='create-1'):
    return {'test-user':actor,'X-Marketing-Request':'1','Idempotency-Key':key}

def create(client,key='create-1'):
    payload={'title':'Campaign','brief':'Useful brief','type':'Email campaign','audience':'Contractors','approver':'Jeremy','requestedById':'2'}
    return client.post('/api/marketing/projects',headers=headers(key=key),data={'payload':json.dumps(payload)},files={'files':('brief.txt',b'brief','text/plain')})

def test_flow_persistence_notifications(client):
    response=create(client);assert response.status_code==200,response.text
    p=response.json();pid=p['id'];assert p['requesterEmail']=='requester@microf.com'
    assert create(client).json()['id']==pid
    url='/api/marketing/projects/'+pid
    assert client.get(p['files'][0]['url'],headers=headers()).content==b'brief'
    assert client.get(p['files'][0]['url']).status_code==401
    patch={'status':'Complete','date':None,'priority':'Normal','version':1}
    assert client.patch(url,headers=headers(),json=patch).status_code==403
    note=client.post(url+'/notes',headers=headers(key='note1'),json={'text':'Ready'});assert note.status_code==200
    assert client.post(url+'/notes',headers=headers(key='note1'),json={'text':'Ready'}).json()['version']==2
    assert client.patch(url,headers=headers(m.MANAGER),json=patch).status_code==409
    patch['version']=2
    done=client.patch(url,headers=headers(m.MANAGER),json=patch);assert done.status_code==200
    patch['version']=3
    assert client.patch(url,headers=headers(m.MANAGER),json=patch).status_code==200
    m.initialize()
    assert client.get(url,headers=headers()).json()['status']=='Complete'
    rows=client.get('/api/marketing/notifications',headers=headers(m.MANAGER)).json()
    assert len(rows)==3
    assert sorted(r['recipient'] for r in rows)==sorted([m.MANAGER,m.MANAGER,'requester@microf.com'])
    assert len(list((m.ROOT/'files').iterdir()))==1

def test_permissions_validation(client):
    assert client.get('/api/marketing/projects').status_code==401
    assert client.get('/api/marketing/context',headers=headers('outside@example.com')).status_code==401
    assert client.get('/api/marketing/notifications',headers=headers()).status_code==403
    assert client.post('/api/marketing/projects',headers={'test-user':m.MANAGER},data={'payload':'{}'}).status_code==403
    p=create(client).json();url='/api/marketing/projects/'+p['id']
    assert client.post(url+'/notes',headers={**headers(),'Origin':'https://evil.example'},json={'text':'Hello'}).status_code==403
    assert client.post(url+'/notes',headers=headers(key='blank'),json={'text':'  '}).status_code==422
    assert client.post(url+'/files',headers=headers('other@microf.com'),files={'files':('x.txt',b'x')}).status_code==403
    assert client.post(url+'/files',headers=headers(m.MANAGER),files={'files':('../../x.txt',b'x')}).json()['files'][-1]['name']=='x.txt'

def test_outbox_retry(client,monkeypatch):
    create(client)
    async def fail(row):raise ConnectionError('Offline')
    monkeypatch.setattr(m,'send_message',fail)
    assert asyncio.run(m.deliver_pending())
    with contextlib.closing(m.connection()) as db,db:
        row=db.execute('SELECT * FROM outbox').fetchone();assert row['state']=='pending';assert row['attempts']==1
        db.execute('UPDATE outbox SET next_try=0')
    sent=[]
    async def send(row):sent.append(row['id'])
    monkeypatch.setattr(m,'send_message',send)
    asyncio.run(m.deliver_pending());assert len(sent)==1
    assert not asyncio.run(m.deliver_pending())

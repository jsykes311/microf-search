"""Workspace integration tests use fake AC data; no live records are modified."""
import copy
import sys
import unittest
from pathlib import Path
from datetime import datetime, timedelta, timezone
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fastapi import FastAPI
from fastapi.testclient import TestClient
from dealer_workspace import install_workspace

class FakeAC:
    def __init__(self):
        self.contacts={'11':{'id':'11','firstName':'Jamie','lastName':'Rivera','email':'jamie@example.com','phone':'555-0100'},'12':{'id':'12','firstName':'Morgan','lastName':'Lee','email':'morgan@example.com','phone':'555-0190'}}
        self.tasks={}; self.writes=[];self.fail=False
    async def get(self,path,params=None):
        if self.fail:raise RuntimeError('upstream secret failure')
        params=params or {}
        if params.get('offset',0):return {key:[] for key in ['accountContacts','contacts','users','dealTasktypes','dealTasks']}
        if path.startswith('accounts/') and path.endswith('/contacts'):
            return {'accountContacts':[{'id':'1','account':'7','contact':'11'},{'id':'2','account':'7','contact':'12'}] if path=='accounts/7/contacts' else []}
        if path=='contacts':return {'contacts':[copy.deepcopy(self.contacts[i]) for i in params['ids[]']]}
        if path.startswith('contacts/'):return {'contact':copy.deepcopy(self.contacts[path.split('/')[1]])}
        if path=='users':return {'users':[{'id':'2','firstName':'Alex','lastName':'Taylor','email':'alex@microf.com'}]}
        if path=='dealTasktypes':return {'dealTasktypes':[{'id':'1','title':'Call'}]}
        if path=='dealTasks':return {'dealTasks':list(self.tasks.values())}
        if path.startswith('dealTasks/'):return {'dealTask':copy.deepcopy(self.tasks[path.split('/')[1]])}
        raise RuntimeError(path)
    async def post(self,path,body):
        self.writes.append((path,copy.deepcopy(body)));t=copy.deepcopy(body['dealTask']);t.update(id='5',reltype='Subscriber');self.tasks['5']=t;return {'dealTask':t}
    async def put(self,path,body):
        self.writes.append((path,copy.deepcopy(body)))
        if path.startswith('contacts/'):self.contacts[path.split('/')[1]].update(body['contact']);return {'contact':self.contacts[path.split('/')[1]]}
        self.tasks[path.split('/')[1]].update(body['dealTask']);return {'dealTask':self.tasks[path.split('/')[1]]}

def fixture():
    upstream=FakeAC();app=FastAPI();install_workspace(app,lambda r:r.headers.get('test-user'),upstream.get,upstream.post,upstream.put,'https://example.activehosted.com');return app,upstream

class WorkspaceTests(unittest.TestCase):
    def setUp(self):
        app,self.ac=fixture();self.client=TestClient(app);self.headers={'test-user':'alex@microf.com','X-Workspace-Request':'1'}
    def contact(self):return self.client.get('/api/workspace/7/contacts',headers=self.headers).json()['contacts'][0]
    def task(self):return {'title':'Call about training','contact_id':'11','assignee':'2','task_type':'1','due':(datetime.now(timezone.utc)+timedelta(days=2)).isoformat(),'note':'Discuss next steps'}
    def test_auth_and_csrf(self):
        self.assertEqual(self.client.get('/api/workspace/7/contacts').status_code,401)
        self.assertEqual(self.client.post('/api/workspace/7/tasks',headers={'test-user':'alex'},json=self.task()).status_code,403)
        self.assertEqual(self.client.post('/api/workspace/7/tasks',headers={**self.headers,'Origin':'https://evil.example'},json=self.task()).status_code,403)
    def test_contact_partial_write_and_conflict(self):
        c=self.contact();payload={k:c[k] for k in ['firstName','lastName','email','phone','version']};payload['phone']='555-0222'
        self.assertEqual(self.client.put('/api/workspace/7/contacts/11',headers=self.headers,json=payload).status_code,200)
        self.assertEqual(self.ac.writes[0][1],{'contact':{'phone':'555-0222'}})
        self.assertEqual(self.client.put('/api/workspace/7/contacts/11',headers=self.headers,json=payload).status_code,409)
        self.assertEqual(len(self.ac.writes),1)
    def test_contact_membership_and_validation(self):
        c=self.contact();payload={k:c[k] for k in ['firstName','lastName','email','phone','version']}
        self.assertEqual(self.client.put('/api/workspace/8/contacts/11',headers=self.headers,json=payload).status_code,404)
        payload['email']='bad';self.assertEqual(self.client.put('/api/workspace/7/contacts/11',headers=self.headers,json=payload).status_code,422)
        self.assertFalse(self.ac.writes)
    def test_task_creation_scope_and_completion(self):
        res=self.client.post('/api/workspace/7/tasks',headers=self.headers,json=self.task());self.assertEqual(res.status_code,200,res.text)
        saved=self.ac.writes[0][1]['dealTask'];self.assertEqual(saved['ownerType'],'contact');self.assertEqual(saved['relid'],'11');self.assertEqual(saved['assignee'],'2');self.assertEqual(saved['status'],0)
        self.assertEqual(len(self.client.get('/api/workspace/7/tasks',headers=self.headers).json()['tasks']),1)
        self.assertEqual(self.client.patch('/api/workspace/8/tasks/5',headers=self.headers,json={'status':1}).status_code,404)
        self.assertEqual(self.client.patch('/api/workspace/7/tasks/5',headers=self.headers,json={'status':1}).status_code,200)
        self.assertEqual(self.client.get('/api/workspace/7/tasks',headers=self.headers).json()['tasks'],[])
    def test_task_invalid_owner_type_date(self):
        for key,value in [('assignee','99'),('task_type','99'),('contact_id','99'),('due','2020-01-01T12:00:00Z'),('due','2030-01-01T12:00:00')]:
            payload=self.task();payload[key]=value;self.assertIn(self.client.post('/api/workspace/7/tasks',headers=self.headers,json=payload).status_code,[404,422])
        self.assertFalse(self.ac.writes)
    def test_tasks_filter_foreign_and_completed(self):
        self.ac.tasks={'1':{'id':'1','relid':'99','reltype':'Subscriber','status':0},'2':{'id':'2','relid':'11','reltype':'Deal','status':0},'3':{'id':'3','relid':'11','reltype':'Subscriber','status':1}}
        self.assertEqual(self.client.get('/api/workspace/7/tasks',headers=self.headers).json()['tasks'],[])
    def test_upstream_error_is_not_empty_success(self):
        self.ac.fail=True;r=self.client.get('/api/workspace/7/contacts',headers=self.headers);self.assertEqual(r.status_code,502);self.assertNotIn('secret',r.text)

if __name__=='__main__':unittest.main()

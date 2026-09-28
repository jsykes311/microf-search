'use strict';
const $ = id => document.getElementById(id);
const escapeHtml = value => String(value ?? '').replace(/[&<>"']/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const h = escapeHtml;
let dealer, contacts = [], tasks = [], context, action, contactDraft, sequence = 0, searchSequence = 0, busy = false;
const nameOf = c => [c.firstName,c.lastName].filter(Boolean).join(' ') || c.email || 'Contact';
const dateLabel = v => { if (!v) return 'No date'; const d = new Date(/^\d{4}-\d{2}-\d{2}$/.test(v) ? v+'T12:00:00' : v); return isNaN(d) ? v : d.toLocaleDateString(undefined,{month:'short',day:'numeric',year:'numeric'}); };
const dateTime = v => { const d=new Date(v); return isNaN(d) ? v : d.toLocaleString(undefined,{dateStyle:'medium',timeStyle:'short'}); };
async function api(path, options={}) {
  const response=await fetch(path,{...options,headers:{'Content-Type':'application/json','X-Workspace-Request':'1',...options.headers}});
  const data=await response.json().catch(()=>({}));
  if(!response.ok) { if(response.status===401) throw Error('Your session expired. Sign in again, then refresh this workspace.'); const detail=data.detail; throw Error(Array.isArray(detail)?detail.map(e=>e.msg).join('; '):(detail||`Request failed (${response.status})`)); }
  return data;
}
const panel = (id,title,subtitle,button='') => `<section class="panel"><div class="panel-head"><div><h3>${title}</h3><p>${subtitle}</p></div>${button}</div><div class="panel-body" id="${id}"><p class="empty">Loading…</p></div></section>`;
const option = (v,label,selected=false)=>`<option value="${h(v)}" ${selected?'selected':''}>${h(label)}</option>`;
function toast(text){$('toast').textContent=text;$('toast').hidden=false;setTimeout(()=>$('toast').hidden=true,5000);}
function fail(id,e){$(id).innerHTML=`<p class="notice">${h(e.message)}</p>`;}

$('dealer-search').addEventListener('submit',async e=>{
  e.preventDefault(); const q=$('query').value.trim(); if(!q)return;
  const seq=++searchSequence;$('search-results').textContent='Searching dealers…';
  try {
    const data=await api('/api/global-search?q='+encodeURIComponent(q)); if(seq!==searchSequence)return;
    const matches=new Map((data.accounts||[]).map(a=>[String(a.id),a]));
    for(const c of data.contacts||[])if(c.account_id&&!matches.has(String(c.account_id)))matches.set(String(c.account_id),{id:c.account_id,name:c.name+' — linked account'});
    $('search-results').innerHTML=[...matches.values()].slice(0,30).map(a=>`<a class="search-result" href="/dealer-workspace?account=${encodeURIComponent(a.id)}"><div><strong>${h(a.name)}</strong><small>${a.dealer_id?'Dealer ID '+h(a.dealer_id):'Open dealer workspace'}</small></div><span aria-hidden="true">→</span></a>`).join('')||'<p>No matching dealer found. Try a dealer name, ID, or contact email.</p>';
  }catch(e){fail('search-results',e);}
});

async function openDealer(id){
  const seq=++sequence;contacts=[];tasks=[];dealer=null;
  $('welcome').hidden=true;$('workspace').hidden=true;$('workspace-status').textContent='Opening dealer workspace…';
  try{
    const d=await api(`/api/accounts/${encodeURIComponent(id)}/detail?include_contacts=false`);if(seq!==sequence)return;
    dealer=d;document.body.classList.add('workspace-open');const a=d.account,f=a.fields||{};document.title=a.name+' · Dealer Workspace';
    $('workspace-status').innerHTML=(d.warnings||[]).length?`<p class="notice">Some information could not load: ${h(d.warnings.join(', '))}. Refresh to try again.</p>`:'';
    $('workspace').innerHTML=`<section class="dealer-head"><div><p class="eyebrow">DEALER WORKSPACE</p><h2>${h(a.name)}</h2><div class="dealer-meta"><span>Dealer ID ${h(f['Parent Dealer ID']||'—')}</span><span>${h(f['Sales Region']||'Region not listed')}</span><span id="dealer-owner">${h(f['Assigned BDR']||'')}</span></div></div><div class="actions"><button data-action="note" class="primary">＋ Add note</button><button data-action="task">Add follow-up</button><button data-action="refresh" class="small">Refresh</button><a class="text-link" href="${h(a.ac_url)}" target="_blank" rel="noopener">ActiveCampaign ↗</a></div></section>
    <div class="stats"><div class="stat"><strong>${d.warnings?.includes('Programs')?'—':d.slps.length}</strong><span>Programs</span></div><div class="stat"><strong id="contact-count">…</strong><span>Contacts</span></div><div class="stat"><strong id="task-count">…</strong><span>Open follow-ups</span></div><div class="stat"><strong id="last-activity" style="font-size:17px;line-height:40px">…</strong><span>Latest logged activity</span></div></div>
    <div class="workspace-grid"><div class="stack">${panel('programs','Programs & status','Where this dealer stands')}${panel('contact-list','People to know','Contact details saved in ActiveCampaign')}${panel('activity','Recent activity','Account notes and training', '<button class="small" data-action="note">Add note</button>')}</div><div class="stack">${panel('followups','Next steps','Open contact tasks in ActiveCampaign','<button class="small" data-action="task">＋ Follow-up</button>')}${panel('details','Dealer details','Account information')}${panel('training','Training','Sessions logged for this dealer','<button class="small" data-action="training">Log training</button>')}</div></div>`;
    $('workspace').hidden=false;
    $('programs').innerHTML=d.warnings?.includes('Programs')?'<p class="notice">Programs could not load. Refresh to try again.</p>':d.slps.map(s=>`<div class="row"><div class="row-top"><strong>${h(s.channel||s.platform||s.name||'Program')}</strong><span class="pill">${h(s['slp-status-detail']||s.status||'Status not listed')}</span></div><p>${s['contractor-activated-date']?'Activated '+h(dateLabel(s['contractor-activated-date'])):'Activation date not listed'}</p>${s['oracle-producer-ids']?`<small>Producer ID ${h(s['oracle-producer-ids'])}</small>`:''}</div>`).join('')||'<p class="empty">No programs linked to this dealer.</p>';
    const fields=['DBA Name','Account Status','Dealer Status','Phone Number','Primary Contact Email','Doing Business in States','Account Type','Assigned BDR'];
    $('details').innerHTML=`<dl class="details">${fields.filter(k=>f[k]).map(k=>`<div><dt>${h(k)}</dt><dd>${h(f[k])}</dd></div>`).join('')||'<p class="empty">No additional details listed.</p>'}</dl><a class="text-link" href="/#account/${encodeURIComponent(id)}">Full account view →</a>`;
    await Promise.allSettled([loadContacts(id,seq),loadTasks(id,seq),loadActivity(id,seq),loadContext(seq)]);
    if(seq===sequence)decorateTasks();
  }catch(e){$('workspace-status').innerHTML=`<p class="notice">${h(e.message)} <a href="/dealer-workspace?account=${encodeURIComponent(id)}">Try again</a></p>`;}
}
async function loadContext(seq){
  try{context=await api('/api/workspace/context');if(seq!==sequence)return; const owner=context.users.find(u=>u.id===String(dealer.account.owner));if(owner)$('dealer-owner').textContent='Owner: '+owner.name;}catch(e){context=null;}
}
async function loadContacts(id,seq=sequence){
  try{const data=await api(`/api/workspace/${id}/contacts`);if(seq!==sequence)return;contacts=data.contacts;$('contact-count').textContent=contacts.length;
    $('contact-list').innerHTML=contacts.map(c=>`<div class="row"><div class="row-top"><div class="person"><span class="avatar" aria-hidden="true">${h(nameOf(c).charAt(0).toUpperCase())}</span><div><strong>${h(nameOf(c))}</strong><p>${h(c.email)}</p>${c.phone?`<p>${h(c.phone)}</p>`:''}</div></div><button class="small" data-action="contact" data-id="${h(c.id)}" aria-label="Edit ${h(nameOf(c))}">Edit</button></div><div class="contact-actions">${c.email?`<a href="mailto:${h(encodeURIComponent(c.email))}">Email</a>`:''}${c.phone?`<a href="tel:${h(c.phone.replace(/[^+\d]/g,''))}">Call</a>`:''}<a href="${h(c.url)}" target="_blank" rel="noopener">ActiveCampaign ↗</a></div></div>`).join('')||'<p class="empty">No contacts linked yet. Add a contact in ActiveCampaign to start a follow-up.</p>';
  }catch(e){$('contact-count').textContent='—';fail('contact-list',e);}
}
async function loadTasks(id,seq=sequence){
  try{const data=await api(`/api/workspace/${id}/tasks`);if(seq!==sequence)return;tasks=data.tasks;$('task-count').textContent=tasks.length;
    $('followups').innerHTML=tasks.map(t=>{const overdue=new Date(t.duedate)<new Date();return `<div class="row"><div class="row-top"><strong>${h(t.title||'Follow-up')}</strong><span class="pill ${overdue?'overdue':''}">${overdue?'Overdue':'Upcoming'}</span></div><p>${h(dateTime(t.duedate))}</p>${t.note?`<p>${h(t.note)}</p>`:''}<div class="row-top" style="margin-top:10px"><small>Contact #${h(t.relid)} · Owner #${h(t.assignee)}</small><button class="small" data-action="complete" data-id="${h(t.id)}">Mark complete</button></div></div>`;}).join('')||'<p class="empty">No open follow-ups for this dealer’s contacts.<br>Add the next conversation when you’re ready.</p>';
    decorateTasks();
  }catch(e){$('task-count').textContent='—';fail('followups',e);}
}
function decorateTasks(){if(!context||!contacts.length)return;document.querySelectorAll('#followups .row').forEach((el,i)=>{const t=tasks[i];if(!t)return;el.querySelector('small').textContent=(nameOf(contacts.find(c=>c.id===String(t.relid))||{}))+' · '+(context.users.find(u=>u.id===String(t.assignee))?.name||'Assigned user');});}
async function loadActivity(id,seq=sequence){
  const results=await Promise.allSettled([api(`/api/accounts/${id}/notes`),api(`/api/accounts/${id}/training`)]);if(seq!==sequence)return;
  const notes=results[0].status==='fulfilled'?results[0].value.notes:[];
  const training=results[1].status==='fulfilled'?results[1].value.training:[];
  const events=[...notes.map(n=>({date:n.activity_date,title:n.subject||n.activity_type||'Note',body:n.body,by:n.performed_by,type:n.activity_type})),...training.map(t=>({date:t.date_of_training,title:t.training_agenda||'Training',body:t.training_notes,by:t.trained_by,type:t.training_type}))].sort((a,b)=>String(b.date).localeCompare(String(a.date)));
  $('last-activity').textContent=events.length?dateLabel(events[0].date):results.some(r=>r.status==='rejected')?'Unavailable':'No activity';
  $('activity').innerHTML=(results.some(r=>r.status==='rejected')?'<p class="notice">Some activity could not load. Refresh to try again.</p>':'')+(events.slice(0,20).map(e=>`<div class="row"><div class="row-top"><strong>${h(e.title)}</strong><small>${h(dateLabel(e.date))}</small></div><p>${h(e.body)}</p><small>${h(e.type)}${e.by?' · '+h(e.by):''}</small></div>`).join('')||'<p class="empty">No activity logged yet.</p>');
  $('training').innerHTML=results[1].status==='rejected'?'<p class="notice">Training could not load. Refresh to try again.</p>':training.slice(0,5).map(t=>`<div class="row"><strong>${h(t.training_agenda||'Training')}</strong><p>${h(t.training_type)} · ${h(dateLabel(t.date_of_training))}</p><small>${h(t.trained_by||'')}</small></div>`).join('')||'<p class="empty">No training sessions logged.</p>';
}
const input=(name,label,value='',type='text',required=true)=>`<label for="field-${name}">${label}</label><input id="field-${name}" name="${name}" type="${type}" value="${h(value)}" ${required?'required':''}>`;
const select=(name,label,options)=>`<label for="field-${name}">${label}</label><select id="field-${name}" name="${name}" required>${options}</select>`;
const textarea=(name,label,required=false)=>`<label for="field-${name}">${label}</label><textarea id="field-${name}" name="${name}" rows="3" ${required?'required':''} maxlength="5000"></textarea>`;
async function openAction(type,id){
  if(busy||!dealer)return;action={type,id,account:dealer.account.id};contactDraft=null;
  $('dialog-dealer').textContent=dealer.account.name;$('action-status').textContent='';$('save-action').textContent='Save to ActiveCampaign';$('save-action').disabled=false;
  let fields='';
  if(type==='contact'){
    const c=contacts.find(c=>c.id===id);if(!c)return;
    $('dialog-title').textContent='Edit contact';fields=`<div class="form-grid"><div>${input('firstName','First name',c.firstName,'text',false)}</div><div>${input('lastName','Last name',c.lastName,'text',false)}</div></div>${input('email','Email',c.email,'email')}${input('phone','Phone',c.phone,'tel',false)}<p class="muted" style="margin-top:16px;font-size:12px">Changes update this contact in ActiveCampaign.</p>`;$('save-action').textContent='Review changes';
  }else if(type==='note'){
    $('dialog-title').textContent='Log a conversation';fields=select('activity_type','Activity type',['Internal Note','Call','Email','Text'].map(v=>option(v,v)).join(''))+input('subject','Subject')+textarea('note_body','What happened?',true);
  }else if(type==='training'){
    $('dialog-title').textContent='Log training';fields=select('training_type','Format',['Webinar','Video Link','In Person'].map(v=>option(v,v)).join(''))+select('training_agenda','Agenda',['Enrollment Training','Refresh Training'].map(v=>option(v,v)).join(''))+input('trained_by','Trainer')+input('date_of_training','Training date',new Date(Date.now()-new Date().getTimezoneOffset()*60000).toISOString().slice(0,10),'date')+textarea('training_notes','Notes');
  }else if(type==='task'){
    if(!contacts.length){toast('Link a contact in ActiveCampaign before adding a follow-up.');return;}
    if(!context){try{context=await api('/api/workspace/context');}catch(e){toast(e.message);return;}}
    $('dialog-title').textContent='Plan a follow-up';fields=input('title','Next step')+select('contact_id','Contact',contacts.map(c=>option(c.id,nameOf(c))).join(''))+`<div class="form-grid"><div>${select('assignee','Assigned to',option('','Choose an owner')+context.users.map(u=>option(u.id,u.name,u.email.toLowerCase()===context.email.toLowerCase())).join(''))}</div><div>${select('task_type','Task type',option('','Choose a type')+context.taskTypes.map(t=>option(t.id,t.name)).join(''))}</div></div>`+input('due','Due date & time (your local time)','','datetime-local')+textarea('note','Details')+'<p class="muted" style="margin-top:16px;font-size:12px">Creates a task on the selected contact in ActiveCampaign.</p>';
  }else if(type==='complete'){
    const t=tasks.find(t=>String(t.id)===id);$('dialog-title').textContent='Complete follow-up';fields=`<p>Mark <strong>${h(t?.title||'this follow-up')}</strong> complete in ActiveCampaign?</p>`;$('save-action').textContent='Mark complete';
  }
  $('dialog-fields').innerHTML=fields;$('action-dialog').showModal();
}
function closeDialog(){if(!busy)$('action-dialog').close();}
$('close-dialog').onclick=closeDialog;$('cancel-dialog').onclick=closeDialog;
$('action-dialog').addEventListener('cancel',e=>{if(busy)e.preventDefault();});
$('workspace').addEventListener('click',e=>{const b=e.target.closest('[data-action]');if(!b)return;if(b.dataset.action==='refresh'){openDealer(dealer.account.id);return;}openAction(b.dataset.action,b.dataset.id);});
$('action-form').addEventListener('submit',async e=>{
  e.preventDefault();if(busy||!action)return;const values=Object.fromEntries(new FormData(e.target));const {type,id,account}=action;
  if(type==='contact'&&!contactDraft){
    const c=contacts.find(c=>c.id===id);const changes=Object.keys(values).filter(k=>values[k]!==c[k]);if(!changes.length){closeDialog();return;}
    contactDraft={...values,version:c.version};$('dialog-fields').innerHTML='<p>Review the changes before saving.</p>'+changes.map(k=>`<div class="row"><strong>${h({firstName:'First name',lastName:'Last name',email:'Email',phone:'Phone'}[k])}</strong><p>${h(c[k]||'(empty)')} → ${h(values[k]||'(empty)')}</p></div>`).join('');$('save-action').textContent='Save to ActiveCampaign';return;
  }
  busy=true;$('save-action').disabled=true;$('action-status').textContent='Saving…';
  let saved=false;
  try{
    if(type==='contact')await api(`/api/workspace/${account}/contacts/${id}`,{method:'PUT',body:JSON.stringify(contactDraft)});
    if(type==='note')await api(`/api/accounts/${account}/notes`,{method:'POST',body:JSON.stringify(values)});
    if(type==='training')await api(`/api/accounts/${account}/training`,{method:'POST',body:JSON.stringify({...values,name:dealer.account.name,dealer_id:String(dealer.account.fields['Parent Dealer ID']||'')})});
    if(type==='task')await api(`/api/workspace/${account}/tasks`,{method:'POST',body:JSON.stringify({...values,due:new Date(values.due).toISOString()})});
    if(type==='complete')await api(`/api/workspace/${account}/tasks/${id}`,{method:'PATCH',body:JSON.stringify({status:1})});
    saved=true;$('action-dialog').close();toast('Saved to ActiveCampaign.');
    if(type==='contact')await loadContacts(account);
    if(type==='task'||type==='complete')await loadTasks(account);
    if(type==='note'||type==='training')await loadActivity(account);
    decorateTasks();
  }catch(e){$('action-status').textContent=e.message;}
  finally{busy=false;$('save-action').disabled=false;if(saved)action=null;}
});
const accountId=new URLSearchParams(location.search).get('account');if(accountId&&/^\d+$/.test(accountId))openDealer(accountId);

import React, { useState, useRef, useEffect } from "react";
import {
  Megaphone,
  Buildings,
  ChartBar,
  ClipboardText,
  GearSix,
  MagnifyingGlass,
  Plus,
  ListBullets,
  Kanban,
  Info,
  CaretRight,
  ArrowRight,
  ArrowLeft,
  X,
  CalendarBlank,
  Users,
  UploadSimple,
  Paperclip,
  CheckCircle,
  ArrowCounterClockwise,
  Funnel,
  ChatCircleText,
  Bell,
  Envelope,
} from "@phosphor-icons/react";
import "./styles.css";
const states = [
  "New Request",
  "Scheduled",
  "In Progress",
  "Awaiting Feedback",
  "Complete",
];
const stateClass = (s) => s.toLowerCase().replaceAll(" ", "-");
async function api(path, options = {}) {
  const response = await fetch("/api/marketing" + path, {
    ...options,
    headers: { "X-Marketing-Request": "1", ...options.headers },
  });
  if (!response.ok) {
    let body;
    try {
      body = await response.json();
    } catch {}
    throw new Error(
      typeof body?.detail === "string"
        ? body.detail
        : response.status === 401
          ? "Your session expired. Sign in again."
          : "Unable to save. Please try again.",
    );
  }
  return response.json();
}
function Badge({ status }) {
  return <span className={"badge " + stateClass(status)}>{status}</span>;
}
function dateText(d) {
  return d
    ? new Date(d + "T12:00:00").toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
        year: "numeric",
      })
    : "Not confirmed";
}
export function App() {
  const [projects, setProjects] = useState([]),
    [view, setView] = useState("list"),
    [scope, setScope] = useState("all"),
    [screen, setScreen] = useState("projects"),
    [query, setQuery] = useState(""),
    [filter, setFilter] = useState("All statuses"),
    [selected, setSelected] = useState(null),
    [toast, setToast] = useState(""),
    [sort, setSort] = useState("default");
  const [context, setContext] = useState(null),
    [error, setError] = useState(""),
    [emails, setEmails] = useState([]),
    [showEmails, setShowEmails] = useState(false);
  const role = context?.isManager ? "manager" : "requester";
  async function refresh() {
    try {
      const [c, p] = await Promise.all([api("/context"), api("/projects")]);
      setContext(c);
      setProjects(p);
      setError("");
    } catch (e) {
      setError(e.message);
    }
  }
  useEffect(() => {
    refresh();
    const timer = setInterval(refresh, 30000);
    const id = new URLSearchParams(location.search).get("project");
    if (id) setSelected(id);
    return () => clearInterval(timer);
  }, []);
  async function showNotifications() {
    try {
      setEmails(await api("/notifications"));
      setShowEmails(true);
    } catch (e) {
      setError(e.message);
    }
  }
  const dialog = useRef(null);
  const project = projects.find((p) => p.id === selected);
  useEffect(() => {
    if (selected) dialog.current?.showModal();
    else dialog.current?.close();
  }, [selected]);
  useEffect(() => {
    if (toast) {
      const t = setTimeout(() => setToast(""), 5000);
      return () => clearTimeout(t);
    }
  }, [toast]);
  const notify = (t) => setToast(t);
  function replaceProject(p) {
    setProjects((ps) =>
      ps.some((v) => v.id === p.id)
        ? ps.map((v) => (v.id === p.id ? p : v))
        : [p, ...ps],
    );
  }
  async function update(id, patch) {
    const p = await api("/projects/" + id, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(patch),
    });
    replaceProject(p);
    return p;
  }
  async function addNote(id, text, key) {
    const p = await api("/projects/" + id + "/notes", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Idempotency-Key": key },
      body: JSON.stringify({ text }),
    });
    replaceProject(p);
    return p;
  }
  async function addFiles(id, files) {
    const data = new FormData();
    [...files].forEach((f) => data.append("files", f));
    replaceProject(
      await api("/projects/" + id + "/files", { method: "POST", body: data }),
    );
  }
  const visible = projects
    .filter(
      (p) =>
        (scope === "all" || p.mine) &&
        (filter === "All statuses" || p.status === filter) &&
        `${p.title} ${p.requestedBy} ${p.type}`
          .toLowerCase()
          .includes(query.toLowerCase()),
    )
    .sort((a, b) =>
      sort === "date"
        ? (a.date || "9999").localeCompare(b.date || "9999")
        : sort === "priority"
          ? (a.priority === "High" ? -1 : 0) - (b.priority === "High" ? -1 : 0)
          : 0,
    );
  const openRequest = () => {
    setScreen("request");
    setSelected(null);
  };
  async function submitted(data, files, key) {
    const body = new FormData();
    body.append(
      "payload",
      JSON.stringify({ ...data, requested: data.requested || null }),
    );
    files.forEach((f) => body.append("files", f));
    const p = await api("/projects", {
      method: "POST",
      headers: { "Idempotency-Key": key },
      body,
    });
    replaceProject(p);
    setScreen("projects");
    setScope("mine");
    setQuery("");
    setFilter("All statuses");
    notify("Request created. Jeremy’s notification is queued.");
  }
  return (
    <div className="app">
      <aside className="sidebar">
        <div className="site-name">Microf-Search</div>
        <div className="brand">
          <ChartBar weight="duotone" size={30} />
          <div>
            <strong>AC Reports</strong>
            <small>REPORTING PORTAL</small>
          </div>
        </div>
        <div className="nav-label">VIEWS</div>
        <a className="context-nav" href="/">
          <Buildings size={19} />
          Back to Apps
        </a>
        <button
          className="nav-active"
          onClick={() => {
            setScreen("projects");
            setScope("all");
          }}
        >
          <Megaphone size={20} weight="fill" />
          Marketing Projects
        </button>
        <div className="sidebar-bottom">
          <span className="avatar">
            {context?.name
              ?.split(" ")
              .map((n) => n[0])
              .slice(0, 2)
              .join("")}
          </span>
          <div>
            <strong>{context?.name || "Loading…"}</strong>
            <small>
              {context?.isManager ? "Marketing manager" : "Team member"}
            </small>
          </div>
        </div>
      </aside>
      <div className="workspace">
        <header className="topbar">
          <span className="crumb">
            AC Reports <CaretRight size={12} />
            <strong>Marketing Projects</strong>
          </span>
          <div className="topbar-actions">
            {context?.isManager && (
              <button
                className="email-preview-button"
                onClick={showNotifications}
              >
                <Bell size={18} />
                <span>Email notifications</span>
              </button>
            )}
          </div>
        </header>
        <main>
          <div className="heading-row">
            <div>
              <div className="eyebrow">MARKETING WORKSPACE</div>
              <h1>Microf Marketing Projects</h1>
              <p className="subtitle">
                One place to request work and see what’s moving.
              </p>
            </div>
            {screen === "projects" && (
              <button className="primary request-button" onClick={openRequest}>
                <Plus size={20} />
                Request a Project
              </button>
            )}
          </div>
          {error && (
            <div className="preview-note" role="alert">
              <Info size={15} />
              <span>{error}</span>
              <button onClick={refresh}>Retry</button>
              <a href="/">Sign in</a>
            </div>
          )}
          {!context && <p role="status">Loading your workspace…</p>}
          {screen === "request" ? (
            <>
              <button
                className="back-link"
                onClick={() => setScreen("projects")}
              >
                <ArrowLeft size={17} />
                Back to projects
              </button>
              <div className="request-layout">
                <RequestForm
                  users={context?.users || []}
                  email={context?.email}
                  onSubmit={submitted}
                  onCancel={() => setScreen("projects")}
                />
                <aside className="motion-panel">
                  <h2>What’s in motion</h2>
                  <button
                    className="text-button"
                    onClick={() => {
                      setScreen("projects");
                      setScope("all");
                    }}
                  >
                    View all projects <ArrowRight />
                  </button>
                  {projects
                    .filter((p) => p.status !== "Complete")
                    .slice(0, 4)
                    .map((p) => (
                      <button
                        className="motion-row"
                        key={p.id}
                        onClick={() => setSelected(p.id)}
                      >
                        <strong>{p.title}</strong>
                        <Badge status={p.status} />
                        <span>
                          {p.date
                            ? "Target: " + dateText(p.date)
                            : "Date not confirmed"}
                          <CaretRight />
                        </span>
                      </button>
                    ))}
                  <div className="motion-footer">
                    Already submitted something?
                    <button
                      className="text-button"
                      onClick={() => {
                        setScreen("projects");
                        setScope("mine");
                      }}
                    >
                      My Requests <ArrowRight />
                    </button>
                  </div>
                </aside>
              </div>
            </>
          ) : (
            <>
              <div className="tabs">
                <button
                  className={scope === "all" ? "active" : ""}
                  onClick={() => setScope("all")}
                >
                  All Projects <span>{projects.length}</span>
                </button>
                <button
                  className={scope === "mine" ? "active" : ""}
                  onClick={() => setScope("mine")}
                >
                  My Requests{" "}
                  <span>{projects.filter((p) => p.mine).length}</span>
                </button>
              </div>
              <div className="summary">
                <span>
                  <strong>
                    {projects.filter((p) => p.status !== "Complete").length}
                  </strong>{" "}
                  active
                </span>
                <span>
                  <strong>
                    {
                      projects.filter((p) => p.status === "Awaiting Feedback")
                        .length
                    }
                  </strong>{" "}
                  awaiting feedback
                </span>
                <span>
                  <strong>
                    {projects.filter((p) => p.status === "Complete").length}
                  </strong>{" "}
                  completed
                </span>
              </div>
              <div className="toolbar">
                <label className="search">
                  <MagnifyingGlass size={20} />
                  <input
                    aria-label="Search projects"
                    placeholder="Search projects…"
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                  />
                  {query && (
                    <button
                      aria-label="Clear search"
                      onClick={() => setQuery("")}
                    >
                      <X size={16} />
                    </button>
                  )}
                </label>
                <label className="filter">
                  <Funnel size={17} />
                  <select
                    aria-label="Filter by status"
                    value={filter}
                    onChange={(e) => setFilter(e.target.value)}
                  >
                    {["All statuses", ...states].map((s) => (
                      <option key={s}>{s}</option>
                    ))}
                  </select>
                </label>
                <div className="view-switch" aria-label="Project view">
                  <button
                    aria-pressed={view === "list"}
                    className={view === "list" ? "selected" : ""}
                    onClick={() => setView("list")}
                  >
                    <ListBullets size={18} />
                    List
                  </button>
                  <button
                    aria-pressed={view === "board"}
                    className={view === "board" ? "selected" : ""}
                    onClick={() => setView("board")}
                  >
                    <Kanban size={18} />
                    Board
                  </button>
                </div>
              </div>
              {!visible.length ? (
                <div className="empty">
                  <MagnifyingGlass size={35} />
                  <h2>
                    {projects.length
                      ? "No projects found"
                      : "Your marketing workspace is ready"}
                  </h2>
                  <p>
                    {projects.length
                      ? "Try a different search or status."
                      : "Submit your first project to get started."}
                  </p>
                  <button
                    className="secondary"
                    onClick={() => {
                      setQuery("");
                      setFilter("All statuses");
                    }}
                  >
                    Clear filters
                  </button>
                </div>
              ) : view === "list" ? (
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>Project</th>
                        <th>Requested by</th>
                        <th>Status</th>
                        <th>
                          <button
                            onClick={() =>
                              setSort(sort === "date" ? "default" : "date")
                            }
                          >
                            Target date {sort === "date" && <span>↑</span>}
                          </button>
                        </th>
                        <th>
                          <button
                            onClick={() =>
                              setSort(
                                sort === "priority" ? "default" : "priority",
                              )
                            }
                          >
                            Priority {sort === "priority" && <span>↑</span>}
                          </button>
                        </th>
                        <th aria-label="Open project" />
                      </tr>
                    </thead>
                    <tbody>
                      {visible.map((p) => (
                        <tr key={p.id}>
                          <td>
                            <button
                              className="project-title"
                              onClick={() => setSelected(p.id)}
                            >
                              {p.title}
                            </button>
                            <small>
                              {p.id} <span>·</span> {p.type}
                            </small>
                          </td>
                          <td>{p.requestedBy}</td>
                          <td>
                            <Badge status={p.status} />
                          </td>
                          <td className={!p.date ? "unconfirmed" : ""}>
                            {dateText(p.date)}
                          </td>
                          <td>
                            <span
                              className={"priority " + p.priority.toLowerCase()}
                            >
                              {p.priority}
                            </span>
                          </td>
                          <td>
                            <button
                              className="icon-button"
                              aria-label={"Open " + p.title}
                              onClick={() => setSelected(p.id)}
                            >
                              <CaretRight size={18} />
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="board">
                  {states
                    .filter((s) => filter === "All statuses" || s === filter)
                    .map((s) => (
                      <section
                        className={"board-column " + stateClass(s)}
                        key={s}
                      >
                        <header>
                          <h2>{s}</h2>
                          <span>
                            {visible.filter((p) => p.status === s).length}
                          </span>
                        </header>
                        <div className="cards">
                          {visible
                            .filter((p) => p.status === s)
                            .map((p) => (
                              <button
                                className="project-card"
                                key={p.id}
                                onClick={() => setSelected(p.id)}
                              >
                                <strong>{p.title}</strong>
                                <span className="type-label">{p.type}</span>
                                <span className="card-meta">
                                  <Users size={16} />
                                  {p.requestedBy}
                                </span>
                                <span className="card-meta">
                                  <CalendarBlank size={16} />
                                  {dateText(p.date)}
                                </span>
                                <span className="card-next">
                                  {p.next}
                                  <ArrowRight size={17} />
                                </span>
                              </button>
                            ))}
                          {!visible.some((p) => p.status === s) && (
                            <p className="column-empty">No projects here</p>
                          )}
                        </div>
                      </section>
                    ))}
                </div>
              )}
              <footer className="page-footer">
                <span>
                  <Info size={17} />
                  Requested deadlines are confirmed after review.
                </span>
              </footer>
            </>
          )}
        </main>
      </div>
      <dialog
        aria-label="Project details"
        ref={dialog}
        onCancel={() => setSelected(null)}
        onClick={(e) => {
          if (e.target === dialog.current) setSelected(null);
        }}
      >
        <button
          className="dialog-close icon-button"
          aria-label="Close project"
          onClick={() => setSelected(null)}
        >
          <X size={23} />
        </button>
        {project && (
          <ProjectDetails
            key={project.id}
            p={project}
            role={role}
            onChange={(patch) => update(project.id, patch)}
            onNote={(text, key) => addNote(project.id, text, key)}
            onFiles={(files) => addFiles(project.id, files)}
            canAttach={role === "manager" || project.mine}
            notify={notify}
          />
        )}
      </dialog>
      {showEmails && (
        <EmailPreviews
          emails={emails}
          onClose={() => setShowEmails(false)}
          onOpen={(id) => {
            setShowEmails(false);
            setSelected(id);
          }}
        />
      )}
      {toast && (
        <div className="toast" role="status">
          <CheckCircle size={23} weight="fill" />
          <span>{toast}</span>
          <button
            aria-label="Dismiss notification"
            onClick={() => setToast("")}
          >
            <X size={18} />
          </button>
        </div>
      )}
    </div>
  );
}
function RequestForm({ onSubmit, onCancel, users, email }) {
  const submissionKey = useRef(crypto.randomUUID()),
    submissionSignature = useRef(null);
  const [saving, setSaving] = useState(false),
    [error, setError] = useState("");
  const [files, setFiles] = useState([]),
    [fileError, setFileError] = useState("");
  function attach(incoming) {
    const list = [...incoming];
    if (list.some((f) => f.size > 25 * 1024 * 1024)) {
      setFileError("Please choose files smaller than 25 MB.");
      return;
    }
    setFiles((prev) => [...prev, ...list].slice(0, 10));
    setFileError("");
  }
  async function submit(e) {
    e.preventDefault();
    const data = Object.fromEntries(new FormData(e.currentTarget));
    const signature = JSON.stringify([
      data,
      files.map((f) => [f.name, f.size, f.lastModified]),
    ]);
    if (submissionSignature.current !== signature) {
      submissionKey.current = crypto.randomUUID();
      submissionSignature.current = signature;
    }
    setSaving(true);
    setError("");
    try {
      await onSubmit(data, files, submissionKey.current);
    } catch (e) {
      setError(e.message);
    } finally {
      setSaving(false);
    }
  }
  return (
    <form className="request-form" onSubmit={submit}>
      <h2>Request a project</h2>
      <p className="muted">A clear brief helps us get started.</p>
      <label>
        Project title <span className="required">*</span>
        <input
          name="title"
          required
          maxLength={120}
          pattern=".*\S.*"
          placeholder="e.g. Fall contractor email campaign"
        />
      </label>
      <div className="form-grid">
        <label>
          Project type <span className="required">*</span>
          <select name="type" required defaultValue="">
            <option value="" disabled>
              Select type
            </option>
            {[
              "Email campaign",
              "Social post",
              "Sales material",
              "Presentation",
              "Website update",
              "Training material",
              "Other",
            ].map((s) => (
              <option key={s}>{s}</option>
            ))}
          </select>
        </label>
        <label>
          Audience <span className="required">*</span>
          <select name="audience" required defaultValue="">
            <option value="" disabled>
              Select audience
            </option>
            {[
              "Contractors",
              "Customers",
              "Partners",
              "Job candidates",
              "Internal team",
              "Other",
            ].map((s) => (
              <option key={s}>{s}</option>
            ))}
          </select>
        </label>
      </div>
      <label>
        What do you need? <span className="required">*</span>
        <textarea
          name="brief"
          required
          maxLength={4000}
          rows={4}
          placeholder="Describe the goal, key message, and deliverables."
        />
      </label>
      <div className="form-grid">
        <label>
          Requested deadline
          <input type="date" name="requested" />
        </label>
        <label>
          Approver <span className="required">*</span>
          <input name="approver" required placeholder="Name or email" />
        </label>
      </div>
      <div className="form-grid">
        <label>
          Requested by <span className="required">*</span>
          <select name="requestedById" required defaultValue="">
            <option value="" disabled>
              Select a user
            </option>
            {users.map((u) => (
              <option key={u.id} value={u.id}>
                {u.name}
              </option>
            ))}
          </select>
          <small className="field-hint">
            Current ActiveCampaign users. Completion updates go to the selected
            person.
          </small>
        </label>
        <label>
          What’s driving this date?
          <input name="reason" placeholder="Event, launch, or other timing" />
        </label>
      </div>
      <div className="upload-label">Attach files or examples</div>
      <label
        className="upload"
        onDragOver={(e) => e.preventDefault()}
        onDrop={(e) => {
          e.preventDefault();
          attach(e.dataTransfer.files);
        }}
      >
        <UploadSimple size={30} />
        <span>
          <strong>Click to upload</strong> or drag and drop
          <small>Up to 10 files · 25 MB each</small>
        </span>
        <input
          aria-label="Attach files"
          type="file"
          multiple
          onChange={(e) => attach(e.target.files)}
        />
      </label>
      {fileError && (
        <p role="alert" className="error">
          {fileError}
        </p>
      )}
      {files.map((f, i) => (
        <div className="file-row" key={i}>
          <Paperclip />
          {f.name}
          <button
            type="button"
            className="icon-button"
            aria-label={"Remove " + f.name}
            onClick={() => setFiles(files.filter((_, j) => j !== i))}
          >
            <X />
          </button>
        </div>
      ))}
      <p className="form-note">
        Jeremy will review your request and confirm timing.
      </p>
      {error && (
        <p role="alert" className="error">
          {error}
        </p>
      )}
      <div className="form-actions">
        <button
          className="primary"
          type="submit"
          disabled={saving || !users.length}
        >
          {saving ? "Submitting…" : "Submit Request"}
          <ArrowRight size={18} />
        </button>
        <button type="button" className="text-button muted" onClick={onCancel}>
          Cancel
        </button>
      </div>
    </form>
  );
}
function ProjectDetails({
  p,
  role,
  onChange,
  onNote,
  onFiles,
  canAttach,
  notify,
}) {
  const [status, setStatus] = useState(p.status),
    [date, setDate] = useState(p.date),
    [priority, setPriority] = useState(p.priority),
    [note, setNote] = useState("");
  const [saving, setSaving] = useState(false),
    [error, setError] = useState("");
  const noteKey = useRef(crypto.randomUUID()),
    editVersion = useRef(p.version);
  async function perform(fn) {
    setSaving(true);
    setError("");
    try {
      await fn();
    } catch (e) {
      setError(e.message);
    } finally {
      setSaving(false);
    }
  }
  async function save(e) {
    e.preventDefault();
    const target = new FormData(e.currentTarget).get("targetDate");
    await perform(async () => {
      const updated = await onChange({
        status,
        date: target || null,
        priority,
        version: editVersion.current,
      });
      editVersion.current = updated.version;
      notify(
        status === "Complete" && p.status !== "Complete"
          ? "Project completed. The requester’s notification is queued."
          : "Project updated.",
      );
    });
  }
  async function addNote(e) {
    e.preventDefault();
    if (!note.trim()) return;
    await perform(async () => {
      const updated = await onNote(note.trim(), noteKey.current);
      if (updated.version === editVersion.current + 1)
        editVersion.current = updated.version;
      setNote("");
      noteKey.current = crypto.randomUUID();
      notify("Note added. Jeremy’s notification is queued.");
    });
  }
  return (
    <div className="details">
      <div className="eyebrow">
        {p.id} · {p.type}
      </div>
      <h2>{p.title}</h2>
      {error && (
        <p role="alert" className="error">
          {error}
        </p>
      )}
      <Badge status={p.status} />
      <div className="detail-meta">
        <div>
          <small>Requested by</small>
          <strong>{p.requestedBy}</strong>
        </div>
        <div>
          <small>Audience</small>
          <strong>{p.audience}</strong>
        </div>
        <div>
          <small>Requested deadline</small>
          <strong>{dateText(p.requested)}</strong>
        </div>
        <div>
          <small>Approver</small>
          <strong>{p.approver}</strong>
        </div>
      </div>
      <section>
        <h3>Project brief</h3>
        <p>{p.brief}</p>
        {p.reason && (
          <p className="muted">
            <strong>Timing:</strong> {p.reason}
          </p>
        )}
      </section>
      <section>
        <h3>Files & deliverables</h3>
        {p.files.length ? (
          p.files.map((f, i) => (
            <a className="file-row" href={f.url} download={f.name} key={i}>
              <Paperclip />
              {f.name}
            </a>
          ))
        ) : (
          <p className="muted">No files attached yet.</p>
        )}
        {canAttach && (
          <label className="upload">
            Add files or deliverables
            <input
              aria-label="Add deliverables"
              type="file"
              multiple
              disabled={saving}
              onChange={(e) => {
                const files = [...e.target.files];
                if (files.length) perform(() => onFiles(files));
                e.target.value = "";
              }}
            />
          </label>
        )}
      </section>
      {role === "manager" ? (
        <form className="management" onSubmit={save}>
          <h3>Manage project</h3>
          <div className="manage-grid">
            <label>
              Status
              <select
                value={status}
                onChange={(e) => setStatus(e.target.value)}
              >
                {states.map((s) => (
                  <option key={s}>{s}</option>
                ))}
              </select>
            </label>
            <label>
              Confirmed target date
              <input type="date" name="targetDate" defaultValue={date} />
            </label>
            <label>
              Priority
              <select
                value={priority}
                onChange={(e) => setPriority(e.target.value)}
              >
                <option>Normal</option>
                <option>High</option>
                <option>Low</option>
              </select>
            </label>
          </div>
          <p className="notification-hint">
            <Envelope size={15} />
            Marking this project complete notifies {p.requestedBy}.
          </p>
          <button className="primary small" type="submit" disabled={saving}>
            Save changes
          </button>
        </form>
      ) : (
        <div className="requester-summary">
          <Info size={19} />
          <p>
            Target date: <strong>{dateText(p.date)}</strong>. Marketing manages
            project priorities and confirmed dates.
          </p>
        </div>
      )}
      <section>
        <h3>Updates & feedback</h3>
        <p className="notification-hint">
          <Bell size={15} />
          New notes notify Jeremy.
        </p>
        <form className="note-form" onSubmit={addNote}>
          <textarea
            aria-label="Project update"
            placeholder={
              role === "manager"
                ? "Share a progress update…"
                : "Add feedback or a clarification…"
            }
            rows={2}
            value={note}
            onChange={(e) => {
              setNote(e.target.value);
              noteKey.current = crypto.randomUUID();
            }}
          />
          <button className="secondary" disabled={saving || !note.trim()}>
            Post update
            <ChatCircleText size={18} />
          </button>
        </form>
        <div className="updates">
          {p.updates.map((u, i) => (
            <div className="update" key={i}>
              <span className="update-icon">
                <ChatCircleText size={16} />
              </span>
              <div>
                <p>{u.text}</p>
                <small>
                  {new Date(u.date).toLocaleString()} · {u.actor}
                </small>
              </div>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}

function EmailPreviews({ emails, onClose, onOpen }) {
  const ref = useRef(null);
  useEffect(() => {
    ref.current?.showModal();
  }, []);
  return (
    <dialog
      className="email-dialog"
      aria-label="Email notifications"
      ref={ref}
      onCancel={onClose}
      onClick={(e) => {
        if (e.target === ref.current) onClose();
      }}
    >
      <button
        className="dialog-close icon-button"
        aria-label="Close notifications"
        onClick={onClose}
      >
        <X size={23} />
      </button>
      <div className="email-header">
        <Envelope size={27} />
        <h2>Email notifications</h2>
        <p>
          Delivery status for the latest 100 notifications. Pending messages
          retry automatically.
        </p>
      </div>
      <div className="notification-rules">
        <strong>Who gets notified</strong>
        <p>
          New project <ArrowRight size={13} /> Jeremy
        </p>
        <p>
          New project note <ArrowRight size={13} /> Jeremy
        </p>
        <p>
          Project completed <ArrowRight size={13} /> Requester
        </p>
      </div>
      {!emails.length ? (
        <div className="email-empty">
          <Bell size={30} />
          <h3>No notifications yet</h3>
          <p>
            Create a project, add a note, or mark a project complete to see its
            delivery status here.
          </p>
        </div>
      ) : (
        <div className="email-list">
          {emails.map((email) => (
            <details className="email-item" key={email.id}>
              <summary>
                <span>
                  <small>To: {email.recipient}</small>
                  <strong>{email.subject}</strong>
                </span>
                <CaretRight size={18} />
              </summary>
              <div className="email-message">
                <span className="preview-stamp">
                  {email.state === "sent"
                    ? "SENT"
                    : email.state === "sending"
                      ? "SENDING"
                      : "QUEUED"}
                  {email.attempts > 0 && email.state !== "sent"
                    ? " · Retry scheduled"
                    : ""}
                </span>
                <p>{email.body}</p>
                <button
                  className="primary small"
                  onClick={() => onOpen(email.project_id)}
                >
                  View project
                  <ArrowRight size={16} />
                </button>
              </div>
            </details>
          ))}
        </div>
      )}
    </dialog>
  );
}

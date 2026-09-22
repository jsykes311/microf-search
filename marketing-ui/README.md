# Marketing Projects

Authenticated workspace at `/marketing-projects`, linked from Apps. Requesters come from the current ActiveCampaign user roster. The default manager is `jsykes@microf.com`; override with `MARKETING_MANAGER_EMAIL`.

The backend uses the existing Microsoft session, AC credentials, and SMTP configuration. On Render, projects, uploads, and the transactional email outbox are stored at `/var/data/marketing` on the existing persistent disk. Keep one service instance. Email retries survive restarts; SMTP delivery is at least once, so a crash immediately after SMTP acceptance can cause a duplicate. Sent indicates SMTP acceptance, not confirmed inbox delivery.

Build the frontend with `npm ci && npm run build` from this directory. Commit the generated `static/marketing` bundle because the existing Render build installs Python dependencies only. Source and production bundle must be updated together.

Run backend checks from the repository root with `python -m pytest tests/test_marketing.py` after installing requirements and pytest. Tests use temporary storage, a fake directory, and a fake sender; they do not contact live services.

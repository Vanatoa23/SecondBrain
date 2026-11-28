# Scheduled Maintenance Templates

Templates for automating Second Brain's recurring local maintenance:

- **Nightly curator** (`scripts/curator.py`) — rescans staleness, contradictions, isolated nodes, and merges `flagged_issues.json` into `review_queue.json`.
- **Nightly wiki export** (`scripts/export_wiki.py`) — updates the local `wiki/` Markdown portability view.
- **Weekly logical backup** (`scripts/backup.sh`) — creates a compressed `pg_dump -Fc` snapshot under `./backups/`.

## Linux / macOS (`cron`)

1. Copy and edit `secondbrain.cron.example` with your absolute repo path, Python venv path, and `DATABASE_URL` (or source your `.env`).
2. Install into your user crontab:
   ```bash
   crontab -e
   ```
3. Check logs under `logs/` (already gitignored).

## Windows (Task Scheduler)

Preview what tasks will be created (dry-run, no changes):

```powershell
powershell -ExecutionPolicy Bypass -File scripts\schedule\register_windows_tasks.ps1 -WhatIf
```

Register the nightly curator task (03:00 daily) using your repo root and Python executable:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\schedule\register_windows_tasks.ps1 `
  -RepoRoot "C:\path\to\SecondBrain" `
  -PythonExe "C:\path\to\SecondBrain\.venv\Scripts\python.exe"
```

Note: Keep `DATABASE_URL` in your user environment variables (never hardcode database passwords inside scheduled task arguments).

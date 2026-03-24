# DocAtlas Charter Run Workflow

This is the standard operational workflow for a new application Charter run.

## Inputs
- Local source folder: `C:\Users\faisal.islam\Documents\DocAtlas_Files\<SourceFolder>`
- Server repo: `~/DocAtlas`
- Server input: `/mnt/nas/faisal/DocAtlas/input/<app_slug>`
- Server output: `/mnt/nas/faisal/DocAtlas/output/<app_slug>/charter/<run_tag>`
- Local review output: `C:\Users\faisal.islam\Documents\DocAtlas_Review\charter\<app_slug>\<run_tag>`

## Standard Steps
1. Confirm taxonomy/config changes are already in `applications.json`, `category_path_map.json`, and deterministic routing.
2. Confirm the server repo is on the required commit:
   - `cd ~/DocAtlas && git rev-parse --short HEAD`
3. Create or verify the canonical server folders:
   - `/mnt/nas/faisal/DocAtlas/input/<app_slug>`
   - `/mnt/nas/faisal/DocAtlas/output/<app_slug>/charter/<run_tag>`
4. Upload the local source contents into the server input folder.
   - Preserve the source structure exactly.
   - If the source is already packaged as `.zip`, keep the zip files intact unless there is a specific reason to unpack them.
5. Start the run in detached `tmux`:
   - session: `docatlas_<app_slug>_<mmdd>`
   - command:

```bash
tmux new-session -d -s docatlas_<app_slug>_<mmdd> "bash -ic 'cd ~/DocAtlas && source .venv/bin/activate && python docatlas.py --input /mnt/nas/faisal/DocAtlas/input/<app_slug> --output /mnt/nas/faisal/DocAtlas/output/<app_slug>/charter/<run_tag> --app \"<App Name>\" --config ./applications.json --category-path-map ./category_path_map.json --charter-mode --workers 1; code=$?; echo __EXIT__:$code; exec bash'"
```

6. Monitor as needed:
   - `tmux attach -t docatlas_<app_slug>_<mmdd>`
   - Telegram bot commands:
     - `status`
     - `tail`
     - `summary`
     - `errors`
     - `disk`
7. After completion, verify the run folder contains:
   - `summary_report.txt`
   - `*_docatlas_summaries.xlsx`
   - `*_docatlas_import.xlsx`
   - `unsupported_files_report.txt`
   - `unsupported_cleanup.xlsx`
   - `docatlas.log`
   - `last_run_stats.json`
8. Download the full run folder to the Windows review tree.
9. Mark the pulled run as the current handoff anchor:
   - `CURRENT_FINAL_RUN.txt` in `...\charter\<app_slug>`
   - `FINAL_RUN_FOR_NOW.txt` inside the run folder

## Recovery
- If upload fails partway through, verify remote file sizes and resume only the missing files.
- If the local PC is shut down after the `tmux` run starts, the server run continues.
- If context is lost, recover from:
  - this runbook
  - `summary_report.txt`
  - `last_run_stats.json`
  - the Telegram bot commands

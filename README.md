# DocAtlas
â€” DocAtlas â€”

DocAtlas is a CLI + GUI tool to extract content from PDFs/DOC/DOCX/PPT/PPTX/XLS/XLSX, summarize, categorize, tag, detect duplicates, and organize files into category folders.

## Quick Start (Windows)
1. Install Python (3.10+).
2. Ensure Python is on PATH.
3. Install dependencies:
```bash
pip install -r requirements.txt
```
4. (Optional) Install OCR dependencies (see below).
5. Set Azure/OpenAI environment variables (see below).
6. Run:
```bash
python docatlas.py
```

## Build Portable EXE (Windows)
This builds a single-file executable so other PCs can run the tool without installing Python.

```bash
cd C:\Users\faisal.islam\Desktop\Codex\docatlas
.\build.ps1
```

Output:
```
dist\docatlas.exe
```

Notes:
- The EXE still needs OCR tools installed on the target machine if you want OCR.
- You still need environment variables for the API keys on the target machine.

## What It Produces
- `{application}__docatlas_summaries.xlsx` (falls back to `uncategorized__docatlas_summaries.xlsx`)
  - `Documents` sheet (compact, one row per document):
    - `Category`, `FilePath`, `FileName`, `DuplicateOf`, `DupScore`,
      `NearDuplicateOf`, `NearDupScore`, `LongSummary`, `ShortSummary`,
      `ReviewFlag`, `ExtractionStatus`, `DuplicateClusterID`,
      `NearDuplicateClusterID`, `ReviewGroupID`, `DuplicateRelationType`
  - `Duplicates` sheet:
    - unified exact+near review members, sorted by category/review-group/relation/score
  - `Articles` sheet (conditional):
    - written only when article generation is enabled (`--articles` in CLI or GUI toggle)
    - in append mode, an already-existing `Articles` sheet is preserved unchanged even if article generation is off
- `{application}__docatlas_import.xlsx` (falls back to `uncategorized__docatlas_import.xlsx`)
  - `import` sheet:
    - `Id`, `Path`, `Title`, `Content`, `Summary`, `Tags`, `Attachments`, `AutoPublish`, `ArticleType`
- `{application}__docatlas_full_text.jsonl.gz`
  - written by default on every run
  - one JSON object per document, gzip-compressed
  - legacy Excel structure documented historically in `full_text_legacy_structure.txt`
- `summary_report.txt`
  - counts by category, duplicates, extraction status
- `unsupported_files_report.txt`
  - unsupported datatype counts and detailed skipped-file inventory with relative/logical paths
  - includes top noisy source folders to guide preprocessing before reruns
- `unsupported_cleanup.xlsx`
  - `cleanup_queue` sheet for human review of unsupported files before source cleanup
  - `cleanup_legend` sheet with decision meanings and color coding
- In each `<category>_Duplicate` folder:
  - `duplicate_groups_overview.xlsx` (group-review tracker with `Group ID`, `Relation`, `FileName`, `Exact_sc`, `Near_sc`, `Assigned to`, `Action`)

## Requirements
Install dependencies:

```bash
pip install -r requirements.txt
```

## Install Python (Windows)
1. Download Python from https://www.python.org/downloads/windows/ (recommended) or Microsoft Store.
2. In the installer, check **Add Python to PATH**.
3. Verify:
```bash
python --version
pip --version
```

## Linux Server Setup (OCR + Scale)
Install system OCR tools (Ubuntu/Debian):
```bash
sudo apt-get update
sudo apt-get install -y tesseract-ocr ghostscript qpdf poppler-utils
```

Create a virtualenv and install Python deps:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run server entrypoint (parallel workers):
```bash
python docatlas_server.py --input "/data/input" --output "/data/output" --app "qPCR" --workers 4
```

## Server/CLI Entry Point
Use `docatlas_server.py` for headless servers:
```bash
python docatlas_server.py --input "/data/input" --output "/data/output" --app "qPCR" --workers 4
```

## Azure OpenAI Configuration
Set environment variables before running:

```bash
# Required
setx AZURE_OPENAI_API_KEY "<your-key>"

# Optional (defaults shown)
setx AZURE_OPENAI_API_VERSION "2025-03-01-preview"
setx AZURE_OPENAI_API_KEY_HEADER "api-key"
setx AZURE_CHAT_BASE_URL "https://api.geneai.thermofisher.com/dev/gpt5"
setx AZURE_EMBEDDINGS_BASE_URL "https://api.geneai.thermofisher.com/dev/embeddingsv2"
setx AZURE_CHAT_DEPLOYMENT "gpt-5.2"
setx AZURE_EMBEDDINGS_DEPLOYMENT "text-embedding-3-small"
setx AZURE_CHAT_PATH "/openai/deployments/{deployment}/chat/completions"
setx AZURE_EMBEDDINGS_PATH "/openai/deployments/{deployment}/embeddings"
setx AZURE_INCLUDE_MODEL_IN_BODY "1"
setx DOCATLAS_API_DELAY "0.5"
setx DOCATLAS_API_MAX_RETRIES "10"
setx DOCATLAS_API_RETRY_BASE "2.0"
setx DOCATLAS_API_RETRY_MAX "60"
setx DOCATLAS_API_TIMEOUT "150"

# Optional: separate keys for chat/embeddings (if your gateway uses different keys)
setx AZURE_CHAT_API_KEY "<your-chat-key>"
setx AZURE_EMBEDDINGS_API_KEY "<your-embeddings-key>"
```

If your API gateway expects a different path, override `AZURE_CHAT_PATH` and `AZURE_EMBEDDINGS_PATH`.

`DOCATLAS_API_DELAY` adds a small delay (seconds) before each LLM/embeddings call to reduce
transient Windows socket exhaustion errors (e.g., WinError 10048). Default is 0.5 seconds.

For transient DNS/network instability, DocAtlas now retries chat/embeddings requests with exponential
backoff and jitter. You can tune with:
- `DOCATLAS_API_MAX_RETRIES` (default `10`)
- `DOCATLAS_API_RETRY_BASE` seconds (default `2.0`)
- `DOCATLAS_API_RETRY_MAX` seconds cap (default `60`)
- `DOCATLAS_API_TIMEOUT` seconds per request (default `150`)

If the chat endpoint returns a `content_filter` error for a document/article, DocAtlas now applies
a local deterministic extractive fallback summary instead of leaving that summary empty. Documents
that used this fallback get `summary_fallback_content_filter` in `ReviewFlag`.

Final document category selection is deterministic and rule-based from extracted text
(keyword/phrase scoring with stable tie-break), to reduce run-to-run category drift.

In the GUI flow, if keys are missing, DocAtlas will prompt separately for the **LLM key** and the **embeddings key**.

## OCR Dependencies (Windows)
OCR is optional. The tool runs without it, but scanned/locked PDFs may yield little or no text. In that case, `extraction_status` will indicate OCR was unavailable and summaries may be weak.

### Tesseract
Download: https://github.com/tesseract-ocr/tesseract  
Add to PATH: `C:\Program Files\Tesseract-OCR\`  
Test:
```bash
tesseract --version
```

### Poppler (for pdftoppm)
Download: https://github.com/oschwartz10612/poppler-windows  
Add to PATH: `C:\poppler\Library\bin` (or your install path)  
Test:
```bash
pdftoppm -h
```

### Ghostscript
Download: https://www.ghostscript.com/download/gsdnld.html  
Add to PATH: `C:\Program Files\gs\gs10.xx\bin`  
Test:
```bash
gswin64c -v
```

### qpdf
Download: https://github.com/qpdf/qpdf/releases  
Add to PATH: `C:\qpdf\bin`  
Test:
```bash
qpdf --version
```

## Run (GUI)

```bash
python docatlas.py
```

The GUI will:
1. Ask for input folder
2. Ask for output folder (Cancel = use input folder)
3. Ask for application (dropdown) and categories (one per line)
4. Ask for API key if `AZURE_OPENAI_API_KEY` is not set
5. Ask whether to use OCRmyPDF for PDFs
6. Optional: click `Edit Apps` to edit the application/category config
7. Optional: click `Test OCR` to check dependencies before running
8. Shows a progress window with ETA during processing

## Run (CLI)

```bash
python docatlas.py --input "C:\path\to\docs" --output "C:\path\to\out" --categories "Finance;HR;Legal"
```

Or use an application from config:

```bash
python docatlas.py --input "C:\path\to\docs" --output "C:\path\to\out" --app "Sequencing"
```

The default config is `applications.json` in the same folder. You can override it:

```bash
python docatlas.py --config "C:\path\to\applications.json" --input "C:\path\to\docs" --output "C:\path\to\out" --app "qPCR"
```

### Create App Folder Structure From Config
To pre-create a stable DocAtlas NAS layout from `applications.json`:

```bash
python build_app_folder_structure.py --base /mnt/nas/faisal/DocAtlas --config ./applications.json
```

This creates:
- `/mnt/nas/faisal/DocAtlas/input/<app_slug>`
- `/mnt/nas/faisal/DocAtlas/output/<app_slug>/charter`
- `/mnt/nas/faisal/DocAtlas/output/<app_slug>/atlas`
- `/mnt/nas/faisal/DocAtlas/archive/zips`
- `/mnt/nas/faisal/DocAtlas/archive/old_runs`

### Options
- `--dry-run`: do not call APIs or move files (hash-based duplicates only)
- `--no-resume`: disable resume cache
- `--no-ocrmypdf`: disable OCRmyPDF and use Tesseract fallback
- `--workers N`: run CLI processing in parallel with N workers (default: `1`; GUI path remains single-worker)
- `--articles`: enable PDF article generation (disabled by default)
- `--no-articles`: deprecated compatibility alias (default is already no-article mode)
- `--category-path-map`: path to `category_path_map.json` for import `Path` mapping
- `--include-full-text-output`: deprecated compatibility flag; full-text JSONL.GZ archive is now always written
- `--embeddings-source summary|full_text|none`: choose embeddings input (default: `full_text`)
- `--overwrite-excel`: overwrite Excel outputs instead of appending (default is append)
- `--limit N`: process only the first N files (useful for time estimation)
- `--no-move`: do not move files (useful for estimation runs)
- `--charter-mode`: preview-only mode (no file moves)
- `--config`: path to applications config JSON
- `--app`: application name from config (use instead of `--categories`)
- `--edit-config`: open the applications config editor

Config validation (automatic on every run):
- `applications.json` and `category_path_map.json` are validated before processing.
- Run fails fast if an application/category is missing on either side, duplicated, or mapped to an empty path.
- `--test-embeddings`: test embeddings endpoint/key and exit
- `--test-chat`: test chat endpoint/key and exit

If you run with `--dry-run`, the API key is not required.

Parallel example:

```bash
python docatlas.py --input "C:\path\to\docs" --output "C:\path\to\out" --app "Sequencing" --workers 4
```

## Notes
- Exact duplicates are detected from document byte hashes.
- Near-duplicates are detected from embeddings within the same category, with a stricter weak-edge guard based on file/path structure.
- Low/no-text files are routed to the `Unreadable` category.
- `Unreadable` documents are excluded from the import workbook by default.
- Excel outputs are appended by default; use `--overwrite-excel` to rebuild from scratch.
- If `--limit` is used, DocAtlas logs a rough total-time estimate.
- Token usage estimates are added to the summary report.
- In the GUI, you can toggle append vs overwrite in the "Embeddings Source" step.
- Embeddings can be computed from the **full text** (default, stricter), **long summary** (lower cost), or **disabled** (hash-only duplicates).
- Unified duplicate review groups (exact + near) are moved to `<category>_Duplicate/<ReviewGroupID>/`.
- Grouping is same-category only to reduce cross-topic false positives.
- Each `<category>_Duplicate` folder also gets `duplicate_groups_overview.xlsx` for manual assignment/review.
- Import output is written as a standalone workbook: `<app>__docatlas_import.xlsx`.
- PDF article splitting is optional and disabled by default.
- When enabled, splitting uses a conservative strong-heading heuristic.
- In no-article mode, new/overwrite runs do not create an `Articles` sheet.
- In append mode, an existing `Articles` sheet is preserved unchanged.
- Resume cache stored as `resume.json` in the output folder.
- OCR fallback for PDFs: if extracted text is too short, OCRmyPDF runs non-forced first, then forced pass only if still low-text, then Tesseract fallback.
- If OCR is enabled, embedded images inside `.docx` and `.pptx` are also OCR-processed.
- Embeddings are skipped for very short texts to reduce cost (configurable in code).
- `.doc` files are supported by auto-conversion to `.docx` via LibreOffice (`soffice`) if installed.
- `.xls` files are supported by auto-conversion to `.xlsx` via LibreOffice (`soffice`) if installed.
- `.zip` archives in the input tree are auto-unpacked into a temporary staging folder; supported files inside them are processed normally.
- Workbook/report `FilePath` values are stored relative to the selected input root, not as machine-specific absolute paths.
- Files discovered inside archives keep a logical relative path like `archive.zip!/inner/file.pdf` in reports and Excel outputs.
- Unsupported files from normal folders and archive contents are skipped for extraction but recorded in `unsupported_files_report.txt`.
- When unsupported files exist, DocAtlas also writes `unsupported_cleanup.xlsx` as a review queue with decisions like `Keep`, `Delete at Source`, `Ignore`, and `Needs Follow-up`.
- Full extracted document text is archived by default in `<app>__docatlas_full_text.jsonl.gz`.
- Tags are deduplicated and capped to a reasonable size.
- `summary_report.txt` includes file type breakdown, category percentages, OCR usage count, duplicate group stats, and document length stats.
- `summary_report.txt` also includes compact unsupported-file counts by datatype, source kind, and the top unsupported source folders.
- Errors are captured per file and reported in `summary_report.txt` without stopping the run.
- Very large documents use a deterministic local summary fallback instead of attempting oversized chat prompts; these docs get `summary_truncated_large_doc` in `ReviewFlag`.
- `extraction_status` column values:
  - `ok`: text extracted normally
  - `ocrmypdf_used`: OCRmyPDF used successfully
  - `ocrmypdf_used_forced`: OCRmyPDF forced pass used successfully after non-forced pass was insufficient
  - `ocrmypdf_failed_then_ocr_used`: OCRmyPDF failed/no text, Tesseract OCR used
  - `ocr_used`: Tesseract OCR used (OCRmyPDF unavailable)
  - `no_text`: no text after extraction/OCR
  - `no_text_ocr_unavailable`: OCR libraries not available
  - `no_text_ocr_failed`: OCR attempted but failed
  - `no_text_ocrmypdf_unavailable`: OCRmyPDF not installed
  - `no_text_ocrmypdf_failed`: OCRmyPDF failed
- `no_text_ocrmypdf`: OCRmyPDF produced no text

### OCR Dependencies (Optional)
OCR uses OCRmyPDF (default), Tesseract, and Poppler.
- Install Tesseract (Windows): https://github.com/tesseract-ocr/tesseract
- Install Poppler (Windows): https://github.com/oschwartz10612/poppler-windows
- Install Ghostscript (Windows): https://www.ghostscript.com/download/gsdnld.html
- Install qpdf (Windows): https://github.com/qpdf/qpdf/releases

OCRmyPDF docs: https://ocrmypdf.readthedocs.io/

If these are not installed, the tool will still run and mark `extraction_status` as `no_text_ocr_unavailable` or `no_text_ocrmypdf_unavailable` when needed.

The tool will warn at startup if OCR dependencies are missing.

### Full Text Archive
Full extracted document text is written by default as `<app>__docatlas_full_text.jsonl.gz`.
- One JSON object per document
- Gzip-compressed for lower storage use
- Decompress with standard tools such as `gzip -cd` or `gunzip`
- `--include-full-text-output` is now a deprecated compatibility flag and does not change behavior
- Legacy Excel structure is documented in `full_text_legacy_structure.txt`


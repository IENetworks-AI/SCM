# Cleanup & Prepare for Deploy — Draft Report

This is an automated, non-destructive analysis and proposal generated on branch `ci/cleanup-prepare-deploy`.

Generated artifacts (local):
- `cleanup_report/tree.txt` — repository tree
- `cleanup_report/all_files.tsv` — all files with sizes
- `cleanup_report/large_files.tsv` — files > 10MB
- `cleanup_report/potential_secrets.tsv` — potential secret matches (file paths + line numbers)

Top large files found:
- `.git/objects/pack/pack-*.pack` (~234 MB) — Git packfile
- `airflow/output/index.json` (~36 MB)
- `airflow/output/approved1.json` (~20 MB)
- `airflow/output/*.json` several ~13-15 MB

Suggested non-destructive actions (implemented in branch):
- Added `scripts/archive_artifacts.ps1` — archives files >= 10MB into `artifacts/archived/<ts>` and zips them.
- Added `scripts/cleanup_non_destructive.ps1` — archives caches and build artifacts.
- Updated `.gitignore` to ignore caches, artifacts, and heavy output folders.

Next steps (manual review required):
1. Review `cleanup_report/large_files.tsv` and confirm which large outputs are safe to archive.
2. Run the archiving script: `./scripts/archive_artifacts.ps1` (PowerShell) to copy large artifacts to `artifacts/archived/<ts>`.
3. After verification, create a single PR that removes or moves approved files; include rollback instructions.

Files containing potential secrets (paths only) are listed in `cleanup_report/potential_secrets.tsv`. DO NOT store secrets in the repo. For each flagged file, replace with a template and move secrets to a secure store.

See `cleanup_report/tree.txt` for a full tree and `cleanup_report/all_files.tsv` for a complete file-size listing.

-- End of draft

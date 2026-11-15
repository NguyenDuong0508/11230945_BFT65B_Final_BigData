# BFT65B — Part 1: Acquisition and Storage

Student ID: 11230945  
Repository: https://github.com/NguyenDuong0508/11230945_BFT65B_Final_BigData

This repository component acquires and verifies the FDIC data required by the final examination. It compares CSV and Parquet storage on identical Financials observations. The report, data-quality treatments, labels, model splits, features and models belong to later stages and are not included here.

## Excellent-level objectives (15 points)

| Assessment requirement | Implementation | Evidence |
|---|---|---|
| Complete, verified scope | All 170 quarters, 1984Q1–2026Q2; unfiltered Failures and Institutions; selected columns | Per-quarter API counts, independent expected-quarter list, schema/key checks, SHA-256, `audit.json` |
| Reproducible acquisition | Fixed scope, explicit field list, dictionary snapshots, bounded pagination, retries, atomic writes, verified restart | `acquisition_manifest.json`, `selected_fields.json`, executed notebook |
| Efficient storage design | Typed Arrow schema; Zstandard Parquet; year directories and quarterly restart checkpoints | Schema and rationale in notebook; size and file counts in benchmark |
| Measured comparison with CSV | CSV, Snappy Parquet, Zstandard Parquet and compact year-partitioned Zstandard Parquet | Exact integer/string comparisons and floating tolerance checks, repeated timings, process peak RSS, recorded environment |

The rubric does not guarantee a score for using a particular format. Conclusions must follow the measured results. A failed completeness check must be investigated before Part 1 is treated as complete.

## Files to commit

- `README.md`, `requirements.txt`, `.gitignore`.
- `notebooks/01_acquisition_storage.ipynb`, with execution outputs.
- `src/__init__.py`, `src/acquisition.py`, `src/storage.py`.
- `tests/test_part1.py`: regression checks for failure detection and storage equivalence.
- `evidence/*.json`: aggregate audit/benchmark results, source metadata, definitions and verification records. These are evidence, not bank-level datasets.

Do not commit downloaded bank records, benchmark CSV/Parquet files, virtual environments, the distribution ZIP, or the course PDF. The ignore file is an additional guard; GitHub web uploads must also be checked manually. File extensions alone do not determine whether an artifact is source data: inspect the content.

## Step-by-step: GitHub and Google Colab

1. Extract the delivered ZIP on your computer. Open the repository URL above. Choose **Add file → Upload files** (or **uploading an existing file** for an empty repository). Upload the extracted contents at the repository root, preserving `notebooks/`, `src/`, `tests/` and `evidence/`. Do not upload a ZIP or add another enclosing directory. Ensure `.gitignore` is included; if your file picker hides it, use **Add file → Create new file**, name it `.gitignore`, and paste its supplied contents. Commit to `main` with a descriptive message.
2. Confirm the repository is **Public** and that the files can be viewed in a private/incognito browser window. The examination requires access from the deadline until grades are released.
3. Open https://colab.research.google.com/github/NguyenDuong0508/11230945_BFT65B_Final_BigData/blob/main/notebooks/01_acquisition_storage.ipynb . If asked to trust/run the notebook, review its contents first. Select **Runtime → Change runtime type → Python 3, CPU**. GPU is unnecessary.
4. Run **Section 1**. It clones the repository into `/content/11230945_BFT65B_Final_BigData` and installs pinned dependencies. Use a fresh runtime. If Colab requests a restart after installation, restart the session and rerun Section 1 before proceeding. Do not execute older imported library objects after changing versions.
5. Run **Section 2** and authorize the Google Drive mount. Source data and checkpoints are stored in `MyDrive/BFT65B/part1_data`; this survives a Colab runtime disconnect. Google Drive authentication is managed by Colab; no token belongs in the notebook or repository.
6. Run **Sections 3–4** to inspect the official field definitions and execute the offline integrity tests. Review the field rationale and confirm the scope is 170 quarters. This is the point to understand the schema before acquisition.
7. Run **Section 5**, the separate four-quarter 2023 smoke download. It downloads complete Failures and Institutions as well. A smoke success validates connectivity and mechanics; it is not completion of the examination scope. Its directory is separate from the full snapshot.
8. Run **Section 6**, the complete download. Four workers prefetch independent quarters; a single writer commits each verified file and its manifest. Each worker uses its own HTTP session. Transient errors are retried. Leave the cell running. The duration depends on API throughput and Drive writes; the course's one-hour estimate is not a guarantee. If interrupted, reconnect, rerun setup/mount, and rerun Section 6. Existing files are hashed and validated before skipping. Do not start concurrent notebook sessions writing to the same data directory.
9. Run **Section 7**, the independent audit. Required: `passed=True`, `full_course_scope=True`, 170 verified quarters, no missing/invalid checkpoints, and no unexpected Parquet files. Inspect counts for all three tables. Never edit an audit result to make it pass. The source estimates of 1.68 million/4,100 rows are descriptive, not expected exact counts.
10. Run **Section 8** to copy only the verified Financials snapshot to `/content` for local-disk experiments. Reserve several GB of free runtime disk and use a runtime with enough RAM to materialize the selected panel (normally a standard Colab CPU runtime is sufficient). All layouts contain the same selected fields and observations. This copy and the benchmark layouts are temporary; rerun this section after a runtime reset.
11. Run **Sections 9–10**. The benchmark verifies all outputs before timing, warms each combination once, then measures three randomized repetitions in fresh processes. Review the size, time and RSS tables. A year-filter query uses 2009; full-scan and column-projection queries use the full panel. The measurements describe this machine and warm-cache protocol. Explain tradeoffs using the observed numbers rather than assuming every Parquet operation will be faster.
12. Run **Section 11** to save metadata and checks to `evidence/` and download an evidence-only ZIP. Extract it locally and upload its `evidence/` folder to GitHub, replacing earlier results. Source records are excluded. Save the *executed notebook* through **File → Save a copy in GitHub**, select this repository, branch `main`, path `notebooks/01_acquisition_storage.ipynb`, and enable inclusion of outputs. Alternatively, download the executed `.ipynb` and upload it to that exact path. The notebook file in the cloned runtime directory does not automatically acquire the active Colab session's outputs.
13. Open the saved notebook on GitHub. Confirm outputs show the full scope, successful audit, matching benchmark results, and measured tables. Confirm metadata was refreshed from the same run. Make the final commit before the examination deadline. Do not include later report or modelling claims in Part 1 evidence.

## Re-run outside Colab

Use Linux and Python 3.11 or 3.12. From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python -m src.acquisition --root data/full
```

Open the notebook and run its cells. Outside Colab it uses the current repository or its parent, stores source files under `data/`, and runs benchmarks under `benchmark_work/`. Optional environment variables `BFT_REPO_ROOT`, `BFT_DATA_BASE` and `BFT_BENCH_ROOT` change these paths. They never alter the full examination date scope. The notebook's seed is 42; acquisition is deterministic with respect to the recorded source responses, not future API revisions.

## Storage decisions and measurement boundaries

- `CERT` is an integer identifier; `REPDTE` is an integer `YYYYMMDD` acquisition key. Parse it as a calendar date in Part 2. Financial amounts/ratios use Float64 to retain nulls and source precision; their economic units remain unchanged.
- Failures retains the API-returned record identifier as string `SOURCE_ID`. Its uniqueness certifies acquisition even when `CERT` is missing. Missing bank identifiers are preserved and counted, not imputed or deleted. Institutions uses CERT as its source key.
- Names, categorical codes and auxiliary-table dates are retained as strings to preserve source representations. Schema validity is not a substitute for semantic data-quality checks.
- `financials/year=YYYY/qMM.parquet` uses 43 year directories and 170 restart files. The `qMM` suffix denotes the quarter-end month (03, 06, 09, 12). Files contain only the selected source columns; `year` is a Hive directory key. The benchmark also tests compaction to one file per year, including the partial year 2026.
- Acquisition uses Zstandard level 3 and 65,536-row groups. A quarter has fewer rows than that limit, so it normally occupies one row group. The benchmark uses the same row-group target for its larger files. Small checkpoint files favor recovery, while compaction can reduce file-open overhead. Part 1 retains the recoverable source layout and measures the alternative instead of deleting provenance.
- CSV is uncompressed, quotes strings and retains nulls. PyArrow writes and reads both CSV and Parquet; CSV uses an explicit schema and column selection, avoiding an artificially disadvantaged type-inference baseline. Column projection still requires CSV parsing; Parquet can avoid unnecessary column reads. The year-partitioned query adds a year predicate and the same date predicate used by every layout.
- Timings exclude layout preparation, interpreter startup and result validation. They include materialized reading and filtering. Cache is not flushed. Process RSS is sampled externally every 2 ms throughout the worker lifetime; its maximum includes interpreter/import overhead and may miss short peaks. RAM and read-time claims must use these definitions. This experiment does not replace the pandas–Polars benchmark or Spark demonstration required in Part 3.
- Resume verifies the stored snapshot; it does not refresh completed quarters when FDIC revises its API. To obtain a fresh snapshot, use a new data root. Within each paginated download, total count and API index metadata must remain stable. Across quarters, the manifest records the actual index used; a cross-quarter transaction or historical-vintage database is not claimed.

## Handoff to Parts 2–5

Use the full audited Financials files, complete auxiliary tables, archived definitions and manifest. Financials has one source row per `CERT, REPDTE`; acquisition performs no imputation, winsorization, outcome construction, feature fitting, or active-bank filtering. Part 2 must investigate missing values, units, reporting conventions, dates, exit types and joins before constructing a clean panel.

The official Failures dictionary describes failed **and assisted** institutions. The verified snapshot includes missing CERT values; these records remain in the source layer under SOURCE_ID and require explicit treatment in Part 2. Preserve `RESTYPE` and inspect its observed categories before applying the examination's failure definition. The number of rows in this endpoint is not automatically the number of eligible failure events. `COST` is an outcome-related estimated loss; `ACTIVE`, `ENDEFYMD` and `CHANGEC1` can describe information learned after a historical prediction date. They are retained for outcome/exit analysis, not approved as historical predictors.

NETINC and ratios require their reporting/annualization definitions to be checked before quarterly feature construction. Dictionaries sometimes contain short or missing descriptions; retaining an official title is not proof of a complete economic definition. All future modelling observations must stop at 2025Q2 under the examination rules, although Part 1 acquires Financials through 2026Q2. Download time and reporting period are recorded separately; publication lags and revised history remain point-in-time limitations for later stages.

## Sources and academic integrity

1. BFT65B, *Final Examination — Individual Research Report: A Big-Data Pipeline for Bank Failure Early Warning*, supplied course PDF, pp. 1–3; course announcement supplied by the student. Scope, assessment and submission rules.
2. BFT65B, `fdic_download_starter.py`, supplied course starter. The API field examples, pagination, per-quarter checkpoints, retry and completeness ideas informed `src/acquisition.py`; the implementation adds strict schemas, dictionary validation, hashing, atomic replacement, bounded prefetch and independent scope audits.
3. FDIC BankFind Suite API: https://api.fdic.gov/banks/docs/ ; Financials dictionary: https://api.fdic.gov/banks/docs/risview_properties.yaml ; Failures: https://api.fdic.gov/banks/docs/failure_properties.yaml ; Institutions: https://api.fdic.gov/banks/docs/institution_properties.yaml . Downloaded definitions and hashes are recorded with each snapshot.
4. Apache Arrow documentation: https://arrow.apache.org/docs/python/parquet.html and https://arrow.apache.org/docs/python/dataset.html ; pandas testing API: https://pandas.pydata.org/docs/reference/api/pandas.testing.assert_frame_equal.html . Relevant library interfaces are exercised in code and regression tests; documentation is not a source of measured performance claims.
5. OpenAI ChatGPT/Codex assisted with requirement interpretation, implementation, notebook documentation and automated verification. The supplied execution evidence was generated in a managed Linux environment, not in the student's Colab account. The student must understand the code, inspect results, rerun it in Colab, and disclose AI assistance in the final report as required by the examination.

This README documents Part 1 only. It is not the final English research report.

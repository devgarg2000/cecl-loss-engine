# Concepts Q&A: Dev's learning notes

Plain-words answers to the "how does this work?" questions that come up while building this project.
Read it top to bottom once, then come back and search for a question.

**How this file works**
- Questions are grouped by topic. Each has a short answer in simple words.
- Whenever Dev asks a concept question in chat, Claude adds it here (rule 12 in `.claude/rules/working-style.md`).
- Entries tagged **(asked)** came from Dev's own questions. Entries tagged **(anticipated)** are questions Claude expects Dev to have next.
- Facts about this project were measured on this machine; the date after each entry says when it was added.

## Contents
1. [Docker and containers](#1-docker-and-containers)
2. [Spark](#2-spark)
3. [Data lake and file formats](#3-data-lake-and-file-formats)
4. [Iceberg](#4-iceberg)
5. [This project's pipeline](#5-this-projects-pipeline)
6. [CECL and the business problem](#6-cecl-and-the-business-problem)

---

## 1. Docker and containers

### Q: How does Docker help us run Spark, MinIO and the other tools? (asked)
Docker gives every tool its own small sealed computer (a **container**) and joins them with a private network.
You never install Java, Spark, MinIO or Postgres on Windows. Each one starts from a ready-made **image** (a recipe with the tool already set up).
`docker-compose.yml` is the building plan that says which containers exist and how they connect.
Think of an apartment building: each tool has its own apartment, and they call each other over an internal phone system. *Added 2026-09-20.*

### Q: What is the difference between an image and a container? (anticipated)
An **image** is the recipe (a frozen, ready-to-run copy of a tool). A **container** is a running apartment made from that recipe.
You can delete a container and start a new one from the same image any time. *Added 2026-09-20.*

### Q: Why is MinIO at `localhost:9001` in my browser but `minio:9000` in the jobs? (asked)
There are two address books:
- **From Windows** (browser, DBeaver): use `localhost:PORT`. It goes through a **published port**, a door from the street into one apartment.
- **From inside a container** (Spark, Airflow): use the container's **name** (`minio:9000`, `spark-master:7077`). The private network translates the name.

`localhost` inside a container means "this same container", not Windows. Same MinIO, two different doors. *Added 2026-09-20.*

### Q: What is a volume? (anticipated)
A **volume** is a storage room shared with or kept beside a container.
- **Named volumes** (`minio_data`, `postgres_data`, `thrift_meta`) keep data even if the container is deleted and rebuilt.
- **Folder mounts** (`./jobs`, `./data/raw`) share a Windows folder into a container. That is how Spark can read your raw files.
- A trailing `:ro` means read-only: the container can look but not change. *Added 2026-09-20.*

### Q: What does `docker exec -u root cecl-spark-master ...` mean and why do we need `-u root`? (anticipated)
`docker exec` runs a command **inside** a running container. `cecl-spark-master` is which container. `-u root` runs it as the administrator user.
The Spark image's normal user has no name and no home folder, so Hadoop (the file layer under Spark) fails with "invalid null input: name". Running as `root` avoids that. *Added 2026-09-20.*

---

## 2. Spark

### Q: What are the master, worker, executor and driver? (anticipated)
Think of a kitchen.
- **Master:** the manager who takes orders and assigns cooks.
- **Worker:** the cook's station (our worker has 6 cores and 4 GB of memory).
- **Executor:** the cook doing the actual work on the worker, for one program.
- **Driver:** the program's own coordinator. It plans the work and sends pieces to the executors. In our commands the driver runs in the `spark-master` container. *Added 2026-09-20.*

### Q: What is the Thrift server, and why does "spark-thrift" show up in the Spark page? (asked)
It is a **waiter**. DBeaver cannot talk to Spark directly. It sends SQL to the Thrift server on port 10000, which hands the work to Spark and returns the table.
It appears in the Spark page's **Running Applications** because it is itself a Spark program that stays on and **holds 2 cores and 1 GB** even when idle, so your queries start fast. *Added 2026-09-20.*

### Q: What is the difference between a job and a service? (asked)
- A **job** does a piece of work and then **finishes** (example: the bronze ingest).
- A **service** **stays on** and waits for requests (example: the Thrift server, MinIO, Airflow).

Spark also uses the word "job" for a much smaller step inside a program. When we say "job" we mean a whole batch script. *Added 2026-09-20.*

### Q: Why did we stop the Thrift server for heavy work? (asked)
Because it permanently reserves 2 of the 6 cores. Stopping it gives a big job all 6 cores and 4 GB. The cost is that DBeaver is disconnected until we start it again. Your saved views survive because they live in a Docker volume. *Added 2026-09-20.*

### Q: Why did an interactive `spark-sql` session once block everything? (anticipated)
By default a Spark program grabs **every free core** and keeps them until it exits. A leftover session held all 6 cores, so the Thrift server waited with "has not accepted any resources". Fix: cap interactive tools with `--total-executor-cores`, and look at **Running Applications** at http://localhost:8081 when something hangs. *Added 2026-09-20.*

---

## 3. Data lake and file formats

### Q: What are bronze, silver and gold? (asked)
The **medallion** layout: each layer is cleaner than the one before.
| Layer | Holds | In this project |
|---|---|---|
| **Bronze** | The raw data, unchanged | Freddie Mac files as Parquet, all columns as text |
| **Silver** | Cleaned and typed tables | `loans` and `loan_performance` (next step) |
| **Gold** | Business-ready results | Simulation output and the reserve per scenario |

If a number looks wrong, you can trace it back through the layers. *Added 2026-09-20.*

### Q: What is MinIO, and what does `s3a://` mean? (anticipated)
**MinIO** is storage that speaks the same language as Amazon S3, so it works like a cloud data lake on your laptop. Data is kept as objects in buckets (ours is `cecl-lake`).
`s3a://` is the address style Spark uses to reach S3-style storage. The "a" is Hadoop's S3 connector, which makes object storage look like folders. *Added 2026-09-20.*

### Q: What is Parquet and why not CSV? (anticipated)
Parquet is a **columnar** file format: it stores each column's values together, compressed, with the column types.
That makes queries that need only a few columns much faster, and the files much smaller than text. A query can also skip whole chunks using the min/max values stored in the file. *Added 2026-09-20.*

### Q: What is partitioning, and why `source_quarter`? (anticipated)
A **partition** is a folder for one slice of the data, like `bronze/perf/source_quarter=2023Q1/`.
A query with `WHERE source_quarter = '2023Q1'` reads only that folder, which is far faster than scanning 644 million rows. It also lets us replace one quarter without touching the others. *Added 2026-09-20.*

### Q: Why are all bronze columns text? (asked)
Bronze keeps values **exactly as delivered**. Converting types can fail or change values (for example a placeholder `999`). We convert in silver, where the rules are written down. *Added 2026-09-20.*

### Q: What is a view, and why did DBeaver show `col_25` errors after the rename? (asked)
A **view** is a saved query, not a copy of data. When we created `bronze.orig` as `SELECT *`, Spark saved the column list it saw at that time. After the Parquet columns were renamed, the saved list no longer matched, so we recreated the view. *Added 2026-09-20.*

---

## 4. Iceberg

### Q: What is Iceberg? (asked)
**Apache Iceberg is a table format.** It is not a database, not a storage system and not a file format.
Your data is still stored as Parquet files in MinIO. Iceberg adds a **layer of bookkeeping files** that says exactly which data files make up the table right now, what its columns are, and what changed over time.
Think of a library: the Parquet files are the pages, and Iceberg is the **index and the change log** that turns a pile of pages into one reliable book. Because of that bookkeeping you get:
- **All-or-nothing writes:** a change either fully appears or not at all, so nobody sees a half-written table.
- **Snapshots (history):** every write creates a snapshot. You can read the table as it was earlier, or roll back.
- **Safe schema changes:** you can rename or add columns without rewriting data or breaking things (columns are tracked by ID, not by name).
- **Replace one slice:** overwrite one partition (one quarter) atomically.
- **Faster reads:** it keeps file-level statistics so engines can skip files.

*Added 2026-09-20.*

### Q: Do I still have Parquet files if I use Iceberg? (anticipated)
Yes. The data is still Parquet under `cecl-lake/silver/...`. Iceberg only adds a `metadata/` folder beside it. Any tool that understands Iceberg (Spark, Trino, DuckDB and others) can read the same table. *Added 2026-09-20.*

### Q: What is an Iceberg catalog? (anticipated)
The **catalog** is the phone book that maps a table name (`lake.silver.loans`) to where its metadata lives. Ours is called `lake` and is a **Hadoop catalog** stored in MinIO, which needs no extra service. *Added 2026-09-20.*

### Q: What is a snapshot, and what is time travel? (anticipated)
Each write to an Iceberg table creates a **snapshot**: a saved version of the table. In our smoke test, one insert made an `append` snapshot and an overwrite made an `overwrite` snapshot.
**Time travel** means querying an older snapshot. It is what will let step 7 (recompute only what changed) compare "before" and "after". *Added 2026-09-20.*

### Q: Why Iceberg for silver and gold but not for bronze? (asked)
Bronze is raw, append-and-replace data that we can always rebuild from the source, so plain Parquet plus our manifest is enough. Silver and gold are tables that other steps depend on, so we want atomic quarter replacement, safe schema changes and history. *Added 2026-09-20.*

### Q: How is Iceberg different from Hudi (and Delta Lake)? (asked)
All three are **open table formats**: the same idea (bookkeeping on top of Parquet files that gives safe writes, history and schema changes), built for different jobs.

| | **Iceberg** | **Hudi** |
|---|---|---|
| Came from | Netflix | Uber |
| Built first for | **Huge analytic tables** that many different tools read: reliable metadata, safe schema changes, easy history | **Fast updates of individual records** and "give me only what changed since X" for streaming feeds |
| A row changes... | Supported, but the table thinks in **files and snapshots** | The table thinks in **records with a key**: an index finds the record and updates it (an "upsert") |
| Its speciality | Hidden partitioning, partition changes, tracking columns by ID, planning queries from manifests without listing folders | Built-in incremental queries and automatic table upkeep (compaction, cleaning) |
| Engines that read it | Very many (Spark, Flink, Trino, Snowflake, DuckDB, ...) | Strongest in Spark and Flink shops |
| Typical user | Data warehouses on a lake, batch analytics | Change-data-capture and near-real-time pipelines |

**Simple picture:** Iceberg is a careful librarian who keeps a perfect index of a huge library that many different readers use. Hudi is a busy order desk that quickly updates single orders and can hand the next team "everything that changed since 10 am".

**Delta Lake** (from Databricks) is the third cousin: very close to Iceberg in purpose, with its strongest support in the Databricks and Spark world.

**Which fits us:** our work is batch and quarterly. We replace whole slices (a quarter of loans) and run big rollups over them. We do not update single loan records all day or stream changes. That is Iceberg's home ground. Hudi's best features (record-key upserts, change feeds) would mostly sit unused.

*The comparison of the formats is general industry knowledge, not something measured on this machine. Which fits us is our own reasoning. Added 2026-09-20.*

### Q: Why do we even need Iceberg? (asked)
Honest answer: **we don't strictly need it.** Bronze proves plain Parquet plus our manifest works. Iceberg starts paying off for **silver and gold**, where other steps depend on the tables:

1. **Replacing a slice safely.** With plain Parquet, overwriting a quarter means delete-then-write. A crash in between leaves half a quarter, and readers can see it. An Iceberg commit is one atomic step: the new version appears completely or not at all.
2. **Renaming columns without breakage.** This week our DBeaver views failed after the column rename because they had saved the old column list. Iceberg tracks columns by ID, so a rename does not break the table.
3. **History.** Every write is a snapshot. You can compare "before" and "after" and roll back a bad run. Step 7 (recompute only what changed) needs exactly that.
4. **No folder listing.** The Thrift log showed "Listing leaf files and directories for 37 paths" for our plain-Parquet view. Iceberg reads a small manifest instead of listing folders, which matters as the table grows.
5. **It is the industry-standard lakehouse table format,** which is useful for your portfolio.

**What it costs:** one extra jar, a catalog to configure, and occasional upkeep (expiring old snapshots, merging small files). Our Hadoop catalog is also safe only with one writer at a time. For a single-user local project the real gains are 1 to 3 plus learning value. *Added 2026-09-20.*

### Q: What is an upsert? (anticipated)
"Update if it exists, insert if it does not," decided by a key such as `loan_seq`. It is the core of Hudi's design. Our batch pipeline mostly replaces whole quarters, so we rarely need it. *Added 2026-09-20.*

### Q: What are copy-on-write and merge-on-read? (anticipated)
Two ways a table can handle an update.
- **Copy-on-write:** rewrite the whole data file with the change. Writes are slower, reads are fast and simple.
- **Merge-on-read:** write the change into a small side file and merge it in when reading. Writes are fast, reads do a little extra work.

Both Hudi and Iceberg offer versions of both. We do not need either yet, because our writes replace whole partitions. *Added 2026-09-20.*

### Q: What is compaction, and the "small files problem"? (anticipated)
Many tiny files make reads slow, because each file has fixed overhead. **Compaction** merges them into fewer, bigger files. Hudi can do this automatically. In Iceberg you run a maintenance step yourself. We write big batches, and we have not measured a small-file problem yet. *Added 2026-09-20.*

### Q: What is the weak spot of our Hadoop catalog? (anticipated)
Iceberg commits a change by **renaming a file**, and S3-style storage cannot rename atomically. With **one writer at a time** (our case) it is fine. With several writers at once you could get a conflict. The upgrade path is Iceberg's JDBC catalog on the existing Postgres. *Added 2026-09-20.*

---

## 5. This project's pipeline

### Q: What does "resumable" mean, and how does the ingest do it? (asked)
Running the job twice must be safe, and a crash must not force a restart. The ingest keeps a **manifest** (a ledger in the lake). After a quarter is fully written and its row count matches the source, one row is written for it. The next run skips quarters that have a matching row and redoes anything missing, changed or failed. The manifest is written **last**, so a crash mid-quarter leaves no entry and the quarter is simply redone. *Added 2026-09-20.*

### Q: What does the alignment check prove? (asked)
It takes random rows from bronze, rebuilds each into the original text line, and searches the source file for that exact line. If every line is found, no value moved to a wrong column. Matching row counts alone cannot prove this. *Added 2026-09-20.*

### Q: Why did we audit the raw files before reloading? (asked)
Because checking only the first lines of a few files once missed a real 35th column that appears in the recent months of most quarters. The audit now reads **every line of every file** for structure and a sample of every column for content. Lesson: check data at scale before you build on it. *Added 2026-09-20.*

### Q: What is `undocumented_amount`? (asked)
The 35th column in the performance files. Freddie's guide and layout file do not mention it. It is a two-decimal amount, only ever `0.00`. It appears on the final row of ended loans, and on every row from report month 202510. It is kept in bronze under an honest name and left out of silver until Freddie documents it. Full evidence is in `docs/data_dictionary.md`. *Added 2026-09-20.*

### Q: What does silver actually do? (asked)
Silver turns the raw text copy into a **typed, cleaned table** without changing what the data says and **without dropping a row** (one bronze row = one silver row).
- **Types:** text becomes integers, dates and decimals (`'202305'` becomes `2023-05-01`, `'794'` becomes `794`).
- **"Not available" becomes null:** credit score `9999`, LTV/CLTV/DTI `999`, units `99`, and so on. Left as numbers they would silently ruin averages.
- **One derived column:** `delinq_months` (a number), null for the codes `RA` and `XX`.
- **Left out on purpose:** `undocumented_amount` and `mi_cancellation_indicator_orig`.
- **Does not do:** simulation, forecasting, filtering, guessing missing values.

Values that cannot be converted become null and are counted in `lake.silver.quality_report` (so far 0 in the 2023Q1 test).
Example, loan `F23Q10000001`: `credit_score '794'` becomes `794`; `first_pay_date '202305'` becomes `2023-05-01`; `orig_upb '104000'` becomes `104000.00`. *Added 2026-09-20.*

### Q: What is the silver manifest, and what does `read_silver_manifest` do? (asked)
The **manifest** (`lake.silver.build_manifest`) is the silver job's ledger: one row per table and quarter, saying it was built from a given bronze load with a given rule version, with its row counts and status.
`read_silver_manifest` reads it into memory. The build job compares it with the bronze manifest to decide, for each quarter, whether to **skip** (already built, nothing changed) or **rebuild** (new, failed, rules changed or bronze changed). It is written last, so a crash mid-quarter leaves no entry and the quarter is redone. *Added 2026-09-20.*

### Q: Which Python file is the main job for each step, and what are the other files for? (asked)
Each step has **one main job** that you run. Other files are **helpers** that the main job imports.

| Step | Main job (you run it) | Helpers it uses |
|---|---|---|
| Raw text to bronze | `jobs/ingest_bronze.py` | `columns.py` (names), `lake.py` (connection) |
| Bronze to silver | `jobs/build_silver.py` | `silver_schema.py` (types and rules), `columns.py`, `lake.py`, and the manifest reader in `ingest_bronze.py` |
| Proof after a build | `jobs/check_bronze.py`, `jobs/verify_silver.py` | same helpers |
| Audits (read-only) | `jobs/profile_raw.py`, `jobs/precheck_silver.py` | same helpers |

Rule of thumb: a file you start with `spark-submit ... /opt/spark-jobs/<name>.py` is a job. A file only imported by others (`columns.py`, `lake.py`, `silver_schema.py`) is a helper. *Added 2026-09-20.*

---

## 6. CECL and the business problem

### Q: What is CECL in plain words? (anticipated)
**CECL** (Current Expected Credit Loss) is an accounting rule. A bank must estimate **today** how much money it expects to lose over the **whole remaining life** of its loans and set that amount aside as a reserve. The estimate depends on forecasts of the economy, so banks compute it under several scenarios (baseline, adverse, severely adverse). *Added 2026-09-20.*

### Q: Why is this a scale problem and not just a big file? (anticipated)
Each loan must be simulated **month by month** for up to 360 remaining months, once **per scenario**. Cells to compute = loans x remaining months x scenarios. With 15.0 million loans and 3 scenarios that reaches billions of cells, and it must be recomputed every quarter when the forecasts change. Adding scenarios multiplies the work, which is the engineering challenge. *Added 2026-09-20.*

### Q: What is a "hazard" and what do prepay, default and pay mean? (anticipated)
Each month a loan can go one of three ways: it **pays** as normal, it **prepays** (paid off early) or it **defaults**. The **hazard** is the probability of each outcome that month. It depends on the loan's traits (credit score, loan-to-value, rate) and on the economic scenario. Step 4 builds this for every loan. *Added 2026-09-20.*

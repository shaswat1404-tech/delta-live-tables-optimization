# Delta Live Tables Optimisation
### POS Data Pipeline Rebuild | Fashion Retail Industry

> A complete rebuild of a production POS data pipeline — migrating from regular Databricks notebooks to Delta Live Tables, eliminating a cross-join bottleneck, implementing exactly-once file processing, and scaling throughput from 20 files per 5 minutes to 4,500–5,000 files per 15 minutes.

---

## Overview

This documents the end-to-end redesign of a production POS transaction pipeline for a fashion retail brand. The pipeline processes JSON files — one file per POS transaction — uploaded to Azure Blob Storage Gen2 from retail stores via MuleSoft. These files flow through a Databricks DLT pipeline (Bronze → Silver → Gold) and surface in Power BI reports.

The original pipeline worked adequately at low store counts. As the number of active stores grew significantly, two compounding problems caused it to break down completely — cluster overloading, driver crashes, and DLT stuck in a single phase for hours at a time. This document covers the diagnosis, redesign, and outcome.

**Solo ownership:** All analysis, redesign, implementation, validation, and documentation was done independently.

---

## Source System & Ingestion Pattern

**POS System:** Predict Spring (now acquired by Salesforce)
**Upload mechanism:** Store teams upload JSON files to Azure Blob Storage Gen2 via MuleSoft

One JSON file = one POS transaction. Files are not delivered via API — only one API connection is available on the source side, which the client team uses for another system (XML format). The Blob Storage upload is the integration point for this pipeline.

**MuleSoft upload behaviour — important detail:**
MuleSoft uploads files gradually — a file first appears as 0 bytes, then fills to its full size over a short period. Picking up a 0-byte or partially written file causes silent data loss. To handle this, only files with a last-modified timestamp of **at least 2 minutes ago** are picked up — ensuring the file is fully written before processing begins.

---

## The Problem

### Root Cause 1 — Processing Capacity vs Store Growth

The original pipeline processed **20 files every 5 minutes**. This was sufficient when the number of active stores was low. As the store count grew significantly, file arrival rate exceeded processing capacity — creating a backlog that compounded over time.

### Root Cause 2 — Cross Join in JSON Explosion

The JSON structure requires multiple explode operations to flatten nested arrays. The original pipeline performed **all explodes across every file**, regardless of whether the transaction was a sale or a return — and the field that identified the transaction type only became accessible **after all explodes were complete**.

The consequence: a cross join across all intermediate exploded rows, producing **n⁴ total rows** for n actual rows. At low file volumes this was invisible. As file sizes and store counts grew, the intermediate dataset became enormous:

- Cluster memory exhausted
- Driver node overloaded
- DLT pipeline stuck in a single phase for hours
- Retries did not resolve the issue — they added to it, consuming more resources on an already overloaded cluster

### Compounding Factor — No Documentation

The original pipeline had no documentation. Understanding the JSON schema, the explosion logic, and the intended data model required reverse-engineering the files and the existing code from scratch before any redesign could begin.

---

## The Solution

### Step 1 — Understand the JSON Schema

Without existing documentation, the JSON files were analysed directly to build a complete field-level mapping. This became the foundation document for all subsequent work — ensuring the redesign was based on verified understanding, not assumptions.

### Step 2 — Identify the Transaction Type Field Early

A field was identified within the JSON that:
- Is accessible **without performing any explode operations**
- **Accurately and reliably identifies** whether a transaction is a sale or a return

This was the key insight that unlocked the entire optimisation. The field existed in the original JSON all along — it had not been identified or documented previously.

The logic was confirmed with the client before implementation.

### Step 3 — Implement Exactly-Once Processing

With the transaction type identified upfront, each file is now routed before any explosion occurs:

```
JSON file arrives
      │
      ▼
Read transaction type field (no explode required)
      │
      ├── Sale   → explodes for sales fields only   → Sales fact table
      │
      └── Return → explodes for return fields only  → Returns fact table
```

**Before:** All explodes on every file regardless of type → cross join → n⁴ rows
**After:** Type-specific explodes per file, on the relevant fields only → no cross join → n rows

Each file is processed exactly once, into exactly the right table.

### Step 4 — Migrate from Notebooks to Delta Live Tables

The redesigned logic was implemented in Delta Live Tables rather than regular Databricks notebooks:

| Concern | Notebooks (before) | DLT (after) |
|---|---|---|
| Dependency management | Manual, fragile | Declarative, automatic |
| Error recovery | Manual restart | Checkpointed, auto-retry |
| Data quality | Ad-hoc | Structured, logged |
| Incremental processing | Custom logic | Managed by DLT |
| Auditability | Limited | Full event log |

### Step 5 — Data Quality & Automated Failure Notification

Rather than letting failed files silently drop or halt the pipeline, failures are captured and surfaced immediately:

- Processing failures (file name, processing datetime, which fact table failed, reason for failure) are captured in pipeline logs
- A dedicated post-processing notebook reads the logs after each DLT run
- If failures are found, the notebook sends a structured payload to a **Logic App HTTP trigger**
- The Logic App builds an HTML table from the payload and sends an email to the relevant team with full failure details
- The team can then rectify the source file and re-upload

This gives the operations team actionable, file-level failure information without requiring access to Databricks logs.

---

## Results

| Metric | Before | After |
|---|---|---|
| Throughput | 20 files / 5 min | 4,500–5,000 files / 15 min |
| Cross join | n⁴ rows | Eliminated |
| Transaction type routing | Post-explosion | Pre-explosion (0 explodes to identify) |
| Explodes per file | All explodes, all types | Type-specific explodes only |
| Cluster/driver overloading | Frequent | Resolved |
| DLT stuck in phase | Hours | Resolved |
| Failure email notification | ✓ (carried forward) | ✓ |
| Documentation | None | Full schema mapping + logic documented |

**Current lag:** 15–40 minutes (DLT run frequency is once per 15 minutes; lag includes queue time)

---

## Current State & Known Limitation

The pipeline is in **production**, running on a **job cluster** (spins up on schedule, terminates after run). Approximately 5–6 minutes of each run cycle is consumed by cluster startup time.

A 15-minute run interval is maintained intentionally — it provides a buffer for runs that take longer than expected due to factors outside the pipeline's control (API latency, file volume spikes, etc.), preventing overlapping runs.

**Current end-to-end lag: 15–40 minutes** (from file generation at source to data available in Databricks Gold layer). This is a scheduling and cluster startup constraint, not a processing bottleneck.

## Auto Loader & File Tracking

The DLT pipeline uses **Auto Loader** for file ingestion — Auto Loader maintains a checkpoint of all processed files, ensuring each file is processed exactly once regardless of how many times the pipeline runs.

In addition, after processing, each file is moved to a processed folder with a structured path:

```
.../processed/YYYY/MM/DD/filename.json
```

This provides a secondary audit trail at the storage layer — independent of the Bronze table — allowing the team to verify on which day a given file was processed directly from Blob Storage if needed.

All Bronze tables are **append-only streaming tables**, processing only new files on each run — consistent with the Auto Loader checkpoint approach.

## Future Scope

Further optimisation is planned targeting:
- Migration to **serverless compute** — eliminates the 5–6 minute cluster startup time
- Custom error notification using **DLT decorators** (replacing the post-processing notebook pattern, where feasible)
- Additional code optimisations
- **Target:** reduce Data Engineering side lag from current levels to 5–10 minutes

---

## Pipeline Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    SOURCE                                │
│  Predict Spring POS (Salesforce)                         │
│  Store teams → MuleSoft → Azure Blob Storage Gen2        │
│  1 JSON file per transaction                             │
│  Files picked up only after 2-min last-modified delay    │
│  (handles MuleSoft gradual upload behaviour)             │
└─────────────────────────┬────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────┐
│               DELTA LIVE TABLES PIPELINE                 │
│          (Azure Databricks — Job Cluster)                │
│                                                          │
│  BRONZE  ── Auto Loader ingestion (exactly-once)         │
│             Last-modified filter applied                 │
│             Transaction type identified (no explode)     │
│             Append-only streaming tables                 │
│             Processed files moved to:                    │
│             .../processed/YYYY/MM/DD/filename.json       │
│     │                                                    │
│  SILVER  ── Type-routed explosion                        │
│             Sales path → sales fields only               │
│             Returns path → return fields only            │
│             No cross join                                │
│     │                                                    │
│  GOLD    ── Aggregated metrics, reporting-ready          │
└─────────────────────────┬────────────────────────────────┘
                          │
          ┌───────────────┴────────────────┐
          ▼                                ▼
   Power BI Reports          Post-processing notebook
                             (failure log check)
                                    │
                             Logic App (HTTP trigger)
                                    │
                             HTML email → operations team
                             (file name, datetime,
                              failed fact, reason)
```

---

## Tech Stack

`Azure Databricks` `Delta Live Tables` `Delta Lake` `PySpark` `Python` `Azure Blob Storage Gen2` `Logic Apps` `Power BI` `MuleSoft` `Predict Spring POS`

# AeroMart Semantic Vector Pipeline System Documentation

## 1. System Purpose

The verified repository implements a Spark-based semantic vector pipeline for review and product metadata. It ingests Amazon-style product and review records, joins them into a Silver layer, aggregates them into Gold datasets, chunks review text, embeds chunks with a sentence-transformer model, and writes final vector results to Parquet. The repository supports this as a data-engineering and retrieval foundation rather than as a complete production search application.

This is consistent with the project framing in the repository: the README describes a data pipeline intended to support an AI search initiative on-premise (`README.md:1-3`), while the code path itself is implemented in `ingestion.py`, `transformation.py`, and `load_gold.py`.

Important point: this document describes the current implementation only. It does not describe the repository as having a complete vector database, search API, or operational production stack.

## 2. Current Architecture Overview

The current repository implements a three-stage design that is visible in code:

1. Bronze ingestion stage
   - Reads raw JSONL review and metadata files from local Bronze paths.
   - Code: `ingestion.py:12-26`

2. Silver transformation stage
   - Selects and cleans fields, deduplicates metadata by `metadata_parent_asin`, filters null review text, performs an inner join on `parent_asin`, and writes a cleaned Silver dataset.
   - Code: `ingestion.py:16-47`

3. Gold and vectorization stage
   - Reads Silver data, creates product and user aggregate tables, generates review chunks, and then embeds those chunks using a sentence-transformer model.
   - Code: `transformation.py:15-101`, `load_gold.py:15-33`

Current implementation summary:
- Bronze data is read from raw JSONL files, not created or managed by a separate pipeline stage in this repository.
- Silver output is written as Parquet in `./data/silver/complete_reviews`.
- Gold tables are written to `./data/gold` as Parquet.
- Final vector storage is written to `./data/gold/final_vector_storage` as Parquet.
- No vector database or ANN index is implemented in the verified repository.

## 3. End-to-End Data Flow

The repository’s verified flow is:

1. Read Bronze metadata: `./data/bronze/metadata/*.jsonl.gz`
2. Read Bronze reviews: `./data/bronze/review/*.jsonl.gz`
3. Select and clean Silver fields in `ingestion.py`
4. Deduplicate product metadata by `metadata_parent_asin`
5. Filter reviews where `text IS NULL`
6. Inner-join reviews to metadata on `parent_asin` = `metadata_parent_asin`
7. Write Silver output to `data/silver/complete_reviews`
8. Read Silver in `transformation.py`
9. Create `dim_products`, `dim_users`, and `fact_vectors`
10. Chunk review text using the active chunker
11. Write Gold Parquet folders
12. Read chunks in `load_gold.py`
13. Generate embeddings with `SentenceTransformer('all-MiniLM-L6-v2')`
14. Write final vector Parquet output to `data/gold/final_vector_storage`

This is a direct code-based flow; the repository does not include an orchestration layer, DAG definition, or deployment file proving a production scheduler or workflow engine.

## 4. Bronze Layer

### Current implementation

The Bronze layer is the raw input layer represented by local JSONL files. The repository explicitly reads these files:

- Metadata path: `./data/bronze/metadata/*.jsonl.gz` (`ingestion.py:12-17`)
- Review path: `./data/bronze/review/*.jsonl.gz` (`ingestion.py:24-26`)

The Bronze data is not created by a separate transformation in this repository. The code reads it and immediately selects/cleans fields for Silver.

### Verified Bronze field behavior

From `ingestion.py:16-34`:
- Metadata is read and reduced to:
  - `metadata_parent_asin` (from `parent_asin`)
  - `title`
  - `main_category`
  - `price`
- Price is processed with `coalesce(col('price'), lit(0.0))` and aliased as `price`.
- Review data is read and reduced to:
  - `parent_asin`
  - `user_id`
  - `rating`
  - `text`
  - `timestamp`

The code then calls `.drop_duplicates(["metadata_parent_asin"])` on the metadata dataset (`ingestion.py:17-22`) and filters review rows where `text IS NOT NULL` (`ingestion.py:28-34`).

### Known risk / quality concern

The current code changes metadata price semantics by converting missing prices to `0.0` in Bronze-to-Silver preparation. This is currently existing behavior, not a fixed condition. The quality design treats this as a known concern to be measured or preserved before transformation: `docs/DATA_QUALITY_CHECKS.md` and `docs/AUDIT_REPORT.md` document the associated risk.

## 5. Silver Layer

### Current implementation

The Silver layer is materialized as `data/silver/complete_reviews` in `ingestion.py:37-47`.

The logic currently does the following:
- Selects cleaned metadata and review columns
- Deduplicates metadata by `metadata_parent_asin`
- Filters reviews where `text` is not null
- Performs an inner join between `review_clean` and `meta_clean` on `parent_asin` = `metadata_parent_asin`
- Drops `metadata_parent_asin` after the join
- Writes the result as a single Parquet file via `coalesce(1)`

Code references:
- `ingestion.py:16-22` — metadata cleaning and deduplication
- `ingestion.py:28-34` — review cleaning and null-text filter
- `ingestion.py:37-45` — join logic and Silver write
- `ingestion.py:46-47` — final output count logging

### Verified Silver schema

The final Silver dataset includes the fields selected in the join output:
- `parent_asin`
- `user_id`
- `rating`
- `text`
- `timestamp`
- `title`
- `main_category`
- `price`

This is consistent with `ingestion.py:28-34` and `ingestion.py:37-45`.

### Current behavioral notes

- The join is an inner join; the code does not record how many reviews are lost by the join.
- The code does not currently validate review retention after the join.
- `price` is not preserved as a distinct unknown-value state after the coalesce step; the repository currently writes `0.0` for missing metadata price values.

## 6. Gold Layer

### Current implementation

The Gold layer is created in `transformation.py:74-101`.

It currently writes three datasets to `./data/gold`:
- `dim_products`
- `dim_users`
- `fact_vectors`

Code references:
- `transformation.py:74-81` — `dim_products`
- `transformation.py:82-88` — `dim_users`
- `transformation.py:89-101` — `fact_vectors`

### Gold datasets

#### dim_products
- Source: Silver data
- Derived columns:
  - `product_id` = `parent_asin`
  - `product_name` = `title`
  - `main_category`
  - `price`
- Implementation: `transformation.py:74-81`
- Specific behavior: `.distinct()` is used on the product dimension; the repository does not show a uniqueness assertion beyond that.

#### dim_users
- Source: Silver data
- Derived columns:
  - `user_id`
  - `review_count`
  - `avg_rating_given`
- Implementation: `transformation.py:82-88`
- Specific behavior: grouped by `user_id` and aggregated with `count(*)` and `avg(rating)`.

#### fact_vectors
- Source: Silver text review data
- Derived columns:
  - `product_id` = `parent_asin`
  - `user_id`
  - `rating`
  - `review_chunk`
- Implementation: `transformation.py:89-101`
- Specific behavior: `review_chunk` is produced by the chunk UDF and exploded using `explode(chunk_udf(col("text")))`.
- Important distinction: `fact_vectors` contains chunks; it does not yet contain embeddings. Embeddings are written later in `load_gold.py`.

## 7. Embedding / Final Vector Storage

### Current implementation

The final vector storage stage is implemented in `load_gold.py:15-33`.

Process:
- Reads `data/gold/fact_vectors` (`load_gold.py:15-16`)
- Loads the sentence-transformer model `all-MiniLM-L6-v2` (`load_gold.py:18-19`)
- Applies `get_embedding` to each `review_chunk` with a Python UDF (`load_gold.py:21-29`)
- Writes the result to `data/gold/final_vector_storage` as Parquet (`load_gold.py:30-33`)

### Verified embedding behavior
- The current embedding contract is 384 dimensions.
- The code does not show a runtime validation that every row has a 384-length vector.
- The repository does not demonstrate local model packaging, mirroring, or guaranteed offline availability.
- Final vector storage is Parquet, not a vector database or search index.

### Important boundary

The repository does not contain any verified implementation of:
- a vector database
- an ANN index
- a retrieval API
- a semantic-search endpoint
- a production orchestration layer

This is consistent with the repository evidence and the audit findings. The system boundary is the data pipeline itself, not the application layer.

## 8. Dataset and Schema Reference

### Source fields and current verified schema

The repository evidence supports the following verified dataset structures.

#### Bronze metadata input
- `parent_asin`
- `title`
- `main_category`
- `price`

Source: `ingestion.py:16-22`

#### Bronze review input
- `parent_asin`
- `user_id`
- `rating`
- `text`
- `timestamp`

Source: `ingestion.py:28-34`

#### Silver dataset (`data/silver/complete_reviews`)
- `parent_asin`
- `user_id`
- `rating`
- `text`
- `timestamp`
- `title`
- `main_category`
- `price`

Source: `ingestion.py:37-47`

#### Gold `dim_products`
- `product_id`
- `product_name`
- `main_category`
- `price`

Source: `transformation.py:74-81`

#### Gold `dim_users`
- `user_id`
- `review_count`
- `avg_rating_given`

Source: `transformation.py:82-88`

#### Gold `fact_vectors`
- `product_id`
- `user_id`
- `rating`
- `review_chunk`

Source: `transformation.py:89-101`

#### Final vector storage (`data/gold/final_vector_storage`)
- `product_id`
- `user_id`
- `rating`
- `review_chunk`
- `embedding`

Source: `load_gold.py:27-33`

### Recommended but currently absent fields

The repository does not currently provide stable lineage identifiers. The quality design explicitly identifies these as future required fields:
- `review_id`
- `chunk_id`

These are recommended for production, but they are not part of the current verified implementation. This is a known gap rather than a current schema feature.

## 9. Transformation and Chunking Behavior

### Current chunking implementation

The active chunker in `transformation.py:24-72` is a fixed-size chunking function that uses:
- `chunk_size = 60`
- `overlap = 10`
- `step = chunk_size - overlap`

This means the active implementation is fixed-window chunking, not a truly recursive chunking algorithm, even though the function is named `recursive_chunker`.

Observed behavior:
- If input is missing or empty, the function returns an empty list.
- If input length is less than 10 characters, the function returns an empty list.
- Exceptions are caught and converted to an empty list.
- Text is split by whitespace into words and chunked in windows of 60 words with 10-word overlap.

Source: `transformation.py:24-72`

### Important distinction

The repository does not provide evidence justifying the 60/10 design as the correct production chunking strategy. The function name and implementation do not match, and the repository does not demonstrate a measured retrieval-quality rationale for the chosen chunk size and overlap.

### Fact-vector generation

The far Gold table is created as:

```python
fact_vectors = silver_df.repartition(20).withColumn("review_chunk", explode(chunk_udf(col("text"))))
```

This is implemented in `transformation.py:89-101`.

Critical evidence: `fact_vectors` contains review chunks before embedding. The embedding is generated later by `load_gold.py` and stored separately.

## 10. Data Quality Control Design

The repository includes a data-quality design specification in `docs/DATA_QUALITY_CHECKS.md`. This is a proposed production-control design and is explicitly not implemented in the current pipeline.

The approved design contains 10 core checks and 2 monitoring checks:
- DQC-01 through DQC-10 cover completeness, price semantics, join retention, duplicates, uniqueness, chunk validity, lineage, embedding presence, and embedding dimensionality.
- DQC-11 and DQC-12 are monitoring checks for text quality and chunk-growth trends.

These are explicitly described as proposed production controls, not as active pipeline enforcement.

Key points from the design that are relevant to the current system:
- Structurally required Silver fields are distinct from business-completeness fields (`docs/DATA_QUALITY_CHECKS.md`)
- Missing-price semantics cannot be recovered after `coalesce(price, 0.0)` in the Silver layer
- `review_id` and `chunk_id` are future required controls, not current data fields
- `F.size(F.col("embedding")) != 384` is the recommended Spark-native embedding-dimension validation pattern
- Unsupported thresholds remain `TBD — requires baseline/SLA`

This specification is valuable as a production design baseline, but it does not imply that the current pipeline implements these checks.

## 11. On-Premise Runtime and Dependencies

The repository evidence shows the following runtime dependencies and assumptions:

- The code uses Spark local mode via `SparkSession.builder...master('local[*]')` in `ingestion.py`, `transformation.py`, and `load_gold.py`.
- This indicates the current implementation is designed around a local Spark runtime rather than a distributed orchestrated cluster deployment.
- The dependency manifest lists:
  - `pandas`
  - `pyarrow`
  - `PyYAML`
  - `sentence-transformers`

Source: `requirements.txt:1-4`

The repository does not show:
- a Kubernetes or job scheduler definition
- a Docker or deployment manifest
- a production model artifact packaging step
- a guaranteed offline on-prem model source

This is not a claim that those components do not exist elsewhere in the organization; it is simply that they are not evidenced in the repository as implemented.

## 12. Operational Characteristics

From the code, the system currently behaves as follows:

- It is script-driven and local-file-based.
- It uses Spark in local mode.
- It writes Parquet datasets to local directories under `data/`.
- It performs row-wise embedding generation in a Python UDF.
- It logs counts with `print()` statements rather than a monitored operational framework.
- It does not include explicit data-quality gating, alerting, retries, or pipeline orchestration.

The repository therefore supports a “current implementation” that is workable as a local data pipeline prototype, but not yet a full production operating system.

## 13. Known Production Gaps

The repository evidence supports the following known production gaps:

1. No vector database / retrieval layer implemented
   - Verified by the absence of any vector store or retrieval application code in the repository and by the final output being Parquet-only.

2. No proven offline model packaging or mirroring strategy
   - The model is referenced as `all-MiniLM-L6-v2`, but the repository does not show a packaged local artifact or guaranteed offline availability.

3. No stable review/chunk lineage
   - There are no `review_id` or `chunk_id` fields in the current datasets.
   - This gap is explicitly called out in `docs/AUDIT_REPORT.md` and `docs/DATA_QUALITY_CHECKS.md`.

4. Missing quality gates across Bronze/Silver/Gold/vector boundaries
   - This is a direct repository observation based on the absence of assertion checks.

5. Row-wise embedding architecture
   - The UDF-based embedding step is a known scalability concern.

6. Silent chunking failure path
   - Empty-list returns on short or invalid input are implemented in the chunker, which can silently reduce the chunk corpus.

7. Unmeasured join retention risk
   - The inner join is performed without a measured record-loss check.

8. Missing operational observability and testing evidence
   - The repository does not provide a verified pipeline test suite, health checks, or data-quality telemetry in the files reviewed.

## 14. Current System Boundary

The verified repo boundary is:

- Input: Bronze JSONL data on disk.
- Processing: Spark scripts that read, select, clean, aggregate, chunk, and embed text.
- Storage: Parquet files in `data/silver` and `data/gold`.
- Output: final vector Parquet output in `data/gold/final_vector_storage`.

The current system boundary does not include:
- vector database indexing
- retrieval service
- search UI or API
- orchestrator or workflow scheduler
- production deployment and observability stack

This is the system as it currently exists in the repository, and it is not broader than the verified evidence.

## 15. Production Evolution Path

The following items are future work and should be treated as recommendations rather than current implementation features.

### Recommended next steps

1. Vector database / index integration
   - Add a retrieval layer that indexes embeddings and supports similarity search.
   - This is not yet in the repository.

2. Stable review and chunk lineage
   - Introduce `review_id` and `chunk_id` identifiers and enforce them through Gold/vector quality gates.

3. Quality-gate implementation
   - Implement the DQC checks from `docs/DATA_QUALITY_CHECKS.md` as production controls after establishing operational baselines and SLAs.

4. Offline model artifact management
   - Package or mirror the embedding model locally so runtime does not depend on undocumented external access.

5. Orchestration and observability
   - Add a scheduler, run metadata, alerts, and operational health checks.

6. Testing and validation
   - Add automated tests for join behavior, chunk generation, embedding dimensions, and file output integrity.

7. Scalable embedding execution
   - Replace the row-wise UDF with a more scalable batch-based embedding strategy.

These recommendations are explicitly future work. They do not alter the current system boundary or imply that they are already implemented.

## Evidence Reference

This documentation is grounded in the repository evidence below.

- `README.md:1-3` — project framing as an AI search initiative on-premise.
- `requirements.txt:1-4` — verified runtime dependencies.
- `ingestion.py:12-17` — Bronze metadata read and metadata cleaning.
- `ingestion.py:17-22` — `coalesce(price, 0.0)` and deduplication of metadata by `metadata_parent_asin`.
- `ingestion.py:28-34` — review field selection and null-text filter.
- `ingestion.py:37-45` — inner join and Silver Parquet write.
- `transformation.py:24-72` — active chunking implementation and observed behavior.
- `transformation.py:74-101` — Gold dataset creation (`dim_products`, `dim_users`, `fact_vectors`).
- `load_gold.py:15-33` — model load, UDF embedding generation, and final Parquet output.
- `docs/AUDIT_REPORT.md` — verified production-readiness findings and architecture gaps.
- `docs/DATA_QUALITY_CHECKS.md` — proposed production quality controls and explicit statement that they are not implemented.

## Final Statement

The verified AeroMart semantic-vector pipeline in this repository is a script-driven Spark workflow that reads Bronze data, cleans and joins into a Silver dataset, writes Gold dimension/fact outputs, chunks text, embeds chunks, and saves vector Parquet files. It is a valid implementation of the current repository architecture, but it remains an evidence-supported prototype with clear production gaps: no stable lineage, no vector database, no proven offline model packaging, no production quality gates, and an embedding implementation that is row-wise and not yet validated for scalable production execution.

Unknown / Not established by repository: any external orchestration, deployment architecture, SLA, production monitoring stack, or guaranteed offline model availability beyond what the repository itself demonstrates.

# AeroMart Semantic Search Pipeline Audit

## 1. Executive Summary

This repository implements a three-stage Spark pipeline that ingests Amazon review and product metadata JSONL files, cleans and joins them into a Silver layer, derives product/user/chunk tables in Gold, and writes vector embeddings to Parquet in a final vector-storage folder. The code path is recognizably organized as a data engineering pipeline, but it is not yet production-ready for AeroMart’s on-premise semantic search use case.

The most serious risks are concentrated in the embedding stage and in the lack of quality gates across the pipeline. The repository does not validate vector length, does not measure or guard against joined-record loss, and relies on a row-wise Python UDF that calls the embedding model per record. In a production environment, those issues would materially reduce search quality, slow the pipeline, and make data quality failures difficult to detect before they reach downstream users.

The audit also identifies a set of architectural gaps that are not strictly code defects but cause real production-readiness risk: the project does not include a vector database or search layer; the model is not confirmed to be available in an offline on-prem environment; and the pipeline is only runnable via hand-ordered scripts with no explicit orchestration or dependencies.

This audit is evidence-based and intentionally limited to repository-supported findings. It does not rely on assumptions about external infrastructure or undocumented runtime environments.

## 2. Scope and Audit Method

This audit covers the repository’s core pipeline scripts:

- `ingestion.py`
- `transformation.py`
- `load_gold.py`

It also references supporting repository metadata where directly relevant to the code path and environment constraints:

- `CASE_STUDY.md`
- `requirements.txt`
- `.gitignore`
- `README.md`

The review method was a direct evidence check against the actual code in the repository. Each finding below includes:

- Finding ID
- Category
- exact source file and line numbers
- observed code behavior
- technical/business impact
- recommended remediation

Where the repository does not provide proof, the report explicitly labels that as an unverified risk or architecture gap rather than a proven defect.

## 3. Verified Pipeline Context

The repository code establishes the following pipeline context:

1. Bronze ingestion: raw review and metadata files are read from `./data/bronze/metadata/*.jsonl.gz` and `./data/bronze/review/*.jsonl.gz` in `ingestion.py:12-26`.
2. Silver processing: metadata and review records are cleaned and joined by `parent_asin` into a Silver dataset, then written to `./data/silver/complete_reviews` in `ingestion.py:16-47`.
3. Gold transformations: `transformation.py:74-101` creates `dim_products`, `dim_users`, and `fact_vectors` as Parquet datasets in the `data/gold` directory.
4. Final vector storage: `load_gold.py:15-33` reads `data/gold/fact_vectors`, applies the `all-MiniLM-L6-v2` model, and writes `data/gold/final_vector_storage` as Parquet.
5. Chunking and vectorization logic: `transformation.py:45-72` defines a `recursive_chunker` UDF that splits review text into chunks; `load_gold.py:21-29` encodes each chunk into a dense vector using `SentenceTransformer`.

This code path is consistent with the repository summary in `CASE_STUDY.md:66-70`, which describes the pipeline as Bronze -> Silver -> Gold -> final vector storage.

## 4. P0 — Critical Production Risks

### P0-01 — Row-wise embedding architecture
- Category: Performance/Scalability; Production Architecture
- Source: `load_gold.py:18-29`
- Observed code behavior:
  - The script creates the model once on the driver: `model = SentenceTransformer('all-MiniLM-L6-v2')` (`load_gold.py:18-19`).
  - It then defines a Python UDF: `@udf(returnType=ArrayType(FloatType()))` and calls `model.encode(text).tolist()` for each row inside the UDF (`load_gold.py:21-25`).
  - The vectorized column is created as `final_vectors_df = chunks_df.withColumn("embedding", get_embedding(col("review_chunk")))` (`load_gold.py:27-29`).
- Technical/business impact:
  - This is a row-at-a-time Python UDF pattern, which forces each chunk to cross the JVM/Python boundary and invokes the model per row. This architecture will not scale efficiently as review volume grows and will increase compute time and memory pressure substantially.
  - For AeroMart, this means one of the core semantic search stages is structurally too slow and operationally fragile for large production datasets.
- Recommended remediation:
  - Replace the row-wise Python UDF with a vectorized or pandas UDF that processes batches in Python or use a Spark-native model-serving approach that keeps the model on executors and avoids per-row serialization overhead.
  - Validate throughput and cost on a production-sized sample before accepting the design.

### P0-02 — Silent chunking failure and data loss path
- Category: Reliability/Operability; Correctness
- Source: `transformation.py:45-69`
- Observed code behavior:
  - The `recursive_chunker` function catches all exceptions and returns an empty list: `except Exception: return []` (`transformation.py:68-69`).
  - The function also discards short text with `if len(text) < 10: return []` (`transformation.py:47-49`).
- Technical/business impact:
  - Bad text, encoding issues, unexpected schema changes, or malformed values can silently disappear from Gold without a pipeline alert. This is a serious reliability problem because the pipeline appears to continue successfully while dropping data.
  - For AeroMart, this means semantic search coverage can shrink silently and operators may not detect it until downstream business impact is visible.
- Recommended remediation:
  - Catch narrow exceptions, log the root cause, and either quarantine the bad row or fail the job explicitly.
  - Replace silent empty returns with a validation path that records dropped-text counts and reasons.

### P0-03 — Missing Silver/Gold/vector quality gates
- Category: Data Quality; Production Controls
- Source: `ingestion.py:37-47`, `transformation.py:74-104`, `load_gold.py:27-33`
- Observed code behavior:
  - The pipeline writes Silver and Gold outputs with no explicit null-rate checks, row-loss checks, join validation, vector-length validation, or schema assertions before persisting results.
  - `ingestion.py` stores the joined product-review data without logging dropped reviews caused by the inner join (`ingestion.py:38-45`).
  - `transformation.py` writes `dim_products`, `dim_users`, and `fact_vectors` and prints counts without any validation step (`transformation.py:74-102`).
  - `load_gold.py` writes final vectors without checking that each row has a valid 384-dimensional vector (`load_gold.py:27-33`).
- Technical/business impact:
  - Bad data can move downstream without detection, and the dataset may be silently wrong while appearing “successful” in logs.
  - In a product search use case, incorrect vector cardinality or a failing join can degrade retrieval quality and reduce customer trust.
- Recommended remediation:
  - Implement explicit data-quality gates before every write: join-loss metrics, null rates, short-review counts, duplicate counts, and vector-dimension assertions.
  - Treat any gate failure as a fail-fast event, not a warning-only condition.

### P0-04 — On-premise model availability is not guaranteed by the repository
- Category: On-Premise Readiness; Architecture
- Source: `load_gold.py:18-25`
- Observed code behavior:
  - The code instantiates the model with `SentenceTransformer('all-MiniLM-L6-v2')` (`load_gold.py:18-19`).
  - The repository does not show any local model artifact, private mirror, or offline package path for the model.
- Technical/business impact:
  - AeroMart requires on-prem deployment without cloud compute. The repository does not demonstrate that `all-MiniLM-L6-v2` is available on the target server without external network access.
  - This is a deployment risk, not just a code-style risk, because the pipeline may fail at runtime in isolated infrastructure.
- Recommended remediation:
  - Package or pre-stage the model artifact in an internal artifact repository or local cache, and verify that the pipeline runs fully offline during deployment testing.
  - Document the exact model provenance and required network policy.

## 5. P1 — High-Priority Risks

### P1-01 — `coalesce(1)` creates a single-partition output and limits scalability
- Category: Performance/Scalability
- Source: `ingestion.py:45-45`, `transformation.py:80-90`, `transformation.py:100-101`
- Observed code behavior:
  - All writes use `coalesce(1).write.mode("overwrite")`, including Silver and Gold outputs (`ingestion.py:45`, `transformation.py:80-101`).
- Technical/business impact:
  - This forces each dataset into a single partition before writing, which reduces parallel read/write throughput and can result in very large files as the dataset grows.
  - For a production semantic-search pipeline, this will increase I/O bottlenecks and slow the entire supply chain from data prep to vector generation.
- Recommended remediation:
  - Choose a sensible partition count based on data volume and executor resources; avoid one-file outputs unless the dataset is intentionally tiny.

### P1-02 — Missing vector database or retrieval layer is a production architecture gap
- Category: Architectural Gap; On-Premise Readiness
- Source: `load_gold.py:27-33`, `CASE_STUDY.md:66-70`
- Observed code behavior:
  - The pipeline ends with a Parquet output file: `final_vectors_df.write.mode("overwrite").parquet("data/gold/final_vector_storage")` (`load_gold.py:27-33`).
  - There is no vector database, index, or retrieval service in the repo.
- Technical/business impact:
  - The repository creates vectors but does not provide a production search path. Without a vector index and query layer, the embeddings are not yet operationally useful for semantic search.
  - This is an architecture gap rather than a single code defect, but it is a material production-readiness issue.
- Recommended remediation:
  - Add a concrete on-prem vector store or index layer, define similarity metric and index configuration, and implement a retrieval service that consumes the generated vectors.

### P1-03 — Missing stable review/chunk lineage prevents traceability
- Category: Data Quality; Traceability
- Source: `transformation.py:92-100`, `load_gold.py:27-29`
- Observed code behavior:
  - `fact_vectors` keeps `product_id`, `user_id`, `rating`, and `review_chunk`, but no source review identifier or chunk identifier (`transformation.py:95-100`).
  - The embedding step appends only an `embedding` vector, not a stable lineage key (`load_gold.py:27-29`).
- Technical/business impact:
  - A search result cannot be traced back to the precise source review or chunk that produced it. This makes debugging, model evaluation, and customer-impact triage harder than it should be.
- Recommended remediation:
  - Persist a stable `review_id` and `chunk_id` alongside each vector, plus a lineage hash or source-row identifier, so each vector can be traced back to its source review.

### P1-04 — Embedding-dimension validation is absent
- Category: Correctness; Data Quality
- Source: `load_gold.py:21-29`
- Observed code behavior:
  - `return model.encode(text).tolist()` (`load_gold.py:25`) writes the embedding output with no length validation.
  - The repository states that the model is `all-MiniLM-L6-v2` in `CASE_STUDY.md:68-70`, but the code does not enforce the expected embedding dimension of 384.
- Technical/business impact:
  - Even a small schema drift in the embedding model or library version could produce vectors of inconsistent length, and the pipeline would not fail fast.
  - This can corrupt downstream indexing and similarity calculations.
- Recommended remediation:
  - Assert or check that each `embedding` has length 384 and log or fail on mismatch. Store the model version and dimension in metadata alongside the vectors.

### P1-05 — Inner-join record loss is not measured or validated
- Category: Data Quality; Operational Monitoring
- Source: `ingestion.py:37-45`
- Observed code behavior:
  - The Silver step performs an inner join on `review_clean.parent_asin == meta_clean.metadata_parent_asin` (`ingestion.py:39-43`).
  - The code does not log the number of reviews that are dropped because they do not have matching metadata, nor does it compare join counts to the original review volume.
- Technical/business impact:
  - This is not a defect in the use of an inner join itself; the issue is that record loss is silently unobserved. In any dataset with sparse metadata, the pipeline can reduce the usable review set without any alert.
  - For AeroMart, this creates blind spots in customer sentiment coverage and search indexing quality.
- Recommended remediation:
  - Measure left rows, right rows, join results, and dropped-row counts before writing Silver. Make the loss ratio part of a formal data-quality gate.

### P1-06 — NULL price converted to zero changes the meaning of missing value
- Category: Data Quality
- Source: `ingestion.py:17-22`
- Observed code behavior:
  - Metadata price is transformed with `coalesce(col('price'), lit(0.0)).alias("price")` (`ingestion.py:17-22`).
- Technical/business impact:
  - A missing price becomes zero, which is semantically different from “price unknown.” This can distort product filtering and analytics, and it can falsely appear as a valid zero-cost item in downstream search or catalog logic.
  - Business impact is not merely analytical; it can affect product discovery and product-quality rules that assume missing values are missing rather than zero.
- Recommended remediation:
  - Preserve nulls and add an explicit `price_missing` flag if downstream logic requires a valid numeric field; otherwise, handle unknown price separately from zero.

## 6. P2 — Engineering Improvements

### P2-01 — Repeated Spark actions recompute the DataFrames multiple times
- Category: Performance/Scalability
- Source: `ingestion.py:22-23`, `ingestion.py:34-35`, `ingestion.py:47-47`, `transformation.py:80-90`, `transformation.py:101-102`, `load_gold.py:29-33`
- Observed code behavior:
  - `.count()` is called in multiple print statements and again after writes.
  - Each `.count()` is an action that can trigger a fresh DataFrame computation unless previously cached or persisted.
- Technical/business impact:
  - On larger data, repeated actions can materially increase runtime and cluster overhead.
  - It also makes the pipeline harder to reason about at scale because each log line may trigger a new scan.
- Recommended remediation:
  - Cache or persist DataFrames that are reused, and use a controlled validation checkpoint pattern instead of repeated ad hoc counts.

### P2-02 — Python chunking UDF increases serialization and execution overhead
- Category: Performance/Scalability
- Source: `transformation.py:45-72`, `transformation.py:94-100`
- Observed code behavior:
  - `chunk_udf = udf(recursive_chunker, ArrayType(StringType()))` and `withColumn("review_chunk", explode(chunk_udf(col("text"))))` (`transformation.py:72-100`).
- Technical/business impact:
  - Review text is moved from the JVM to Python for every row and then returned to Spark, which adds serialization overhead and reduces throughput.
  - This is a structural performance problem, not merely a tuning issue.
- Recommended remediation:
  - Evaluate a Spark-native chunking approach or a more batch-oriented UDF design; benchmark the cost on realistic data before adopting this pattern.

### P2-03 — Metadata deduplication is non-deterministic and may choose an arbitrary row
- Category: Data Quality
- Source: `ingestion.py:17-23`
- Observed code behavior:
  - `meta_clean = meta_df.select(...).drop_duplicates(["metadata_parent_asin"])` (`ingestion.py:17-23`) does not specify a deterministic ordering or selection rule.
- Technical/business impact:
  - If a product appears more than once in the metadata source with conflicting values, Spark may keep an arbitrary row based on the internal ordering. This can introduce inconsistent product metadata into Silver.
- Recommended remediation:
  - Add a deterministic sort or ranking rule before deduplicating, such as “keep the most complete row” or “keep the latest effective time.”

### P2-04 — Pipeline orchestration and error handling are absent
- Category: Reliability/Operability
- Source: `ingestion.py:12-47`, `transformation.py:13-104`, `load_gold.py:15-33`
- Observed code behavior:
  - Each script runs with a fixed path and no dependency checks, no workflow orchestrator, and no failure gate. If an earlier stage fails or data is absent, later scripts can proceed with stale or missing input.
- Technical/business impact:
  - This pattern is operationally fragile. It relies entirely on a developer invoking the scripts in the correct order with no automation.
- Recommended remediation:
  - Add a single orchestration entry point that validates prerequisites, checks existence and freshness of upstream outputs, and fails fast when dependencies are not met.

### P2-05 — Logging and testing are not present in a production-ready form
- Category: Reliability/Operability; Engineering Quality
- Source: `ingestion.py:12-47`, `transformation.py:74-104`, `load_gold.py:27-33`
- Observed code behavior:
  - The repository uses `print` statements instead of structured logging and does not define tests for the pipeline logic (`README.md:1-2`, `CASE_STUDY.md:206-216`).
- Technical/business impact:
  - Operators have no structured way to track failures, record counts, or alert on anomalies.
  - Existing issues can remain hidden until the downstream business team notices degraded search results.
- Recommended remediation:
  - Introduce structured logging, explicit validation metrics, and a minimal automated test suite covering join behavior, chunking, and vector output checks.

### P2-06 — Path and runtime assumptions are brittle in a team environment
- Category: Reliability/Operability
- Source: `ingestion.py:9-15`, `transformation.py:13-18`, `transformation.py:81-101`, `load_gold.py:15-32`
- Observed code behavior:
  - The scripts use either `./data/...` or `data/...` paths and assume a current working directory that contains the expected input directories and output folders.
- Technical/business impact:
  - Running the scripts from a different working directory can lead to incorrect reads or writes and can cause operational confusion in automation or when a teammate runs them in a different environment.
- Recommended remediation:
  - Use a project-root-relative path resolution strategy tied to the script location rather than the shell working directory.

## 7. P3 — Maintainability and Documentation

### P3-01 — Function naming and chunking design are misaligned
- Category: Maintainability; Documentation
- Source: `transformation.py:45-72`
- Observed code behavior:
  - The function is named `recursive_chunker`, but the active implementation uses fixed windows: `chunk_size = 60`, `overlap = 10`, and step = `chunk_size - overlap` (`transformation.py:51-66`).
  - The repository provides no evidence that the 60/10 configuration was selected based on retrieval performance or domain testing.
- Technical/business impact:
  - The code misleads developers about the actual chunking strategy, and the design rationale is undocumented. This makes future tuning and troubleshooting difficult and risks incorrect assumptions in the retrieval layer.
- Recommended remediation:
  - Rename the function to reflect the actual implementation (for example, fixed-window chunker) or implement a true recursive chunker consistent with the function name, and document the design rationale for 60/10.

### P3-02 — Dead code, unused imports, and comment clutter reduce code clarity
- Category: Maintainability
- Source: `transformation.py:1-3`, `transformation.py:20-43`, `load_gold.py:11-33`
- Observed code behavior:
  - `coalesce` is imported but unused in `transformation.py:1-3`.
  - A large commented-out alternate chunker remains in the file (`transformation.py:20-43`).
  - The code contains typos in comments and numbering inconsistencies, such as `# Spark Cinfig` and `# 3. Generate the Vectors` in `load_gold.py:11-33`.
- Technical/business impact:
  - These issues reduce maintainability and make the code harder to review, trust, and evolve over time.
- Recommended remediation:
  - Remove dead code, remove unused imports, and clean up comments and naming conventions before the repo is used for production engineering.

### P3-03 — README, docstrings, and data dictionary gaps limit operational understanding
- Category: Documentation; Maintainability
- Source: `README.md:1-2`, `ingestion.py:12-47`, `transformation.py:45-104`, `load_gold.py:15-33`
- Observed code behavior:
  - `README.md` is effectively a placeholder and does not describe setup, architecture, inputs, outputs, or operational steps (`README.md:1-2`).
  - The repository includes no docstrings for the scripts or UDFs.
  - There is no data dictionary covering the actual columns produced at each stage.
- Technical/business impact:
  - New engineers and operators cannot onboard reliably or understand the data contract without reverse engineering the code.
  - This increases operational risk and slows debugging and change review.
- Recommended remediation:
  - Add a concise operational README, UDF and stage docstrings, and a schema/data-dictionary document that matches the actual output columns of each stage.

## 8. Unverified Risks / Repository Limitations

The repository does not provide enough evidence to validate the following items conclusively. They are therefore documented as unverified risks rather than confirmed defects:

1. Review text quality issues (markup, encoding, or formatting problems) beyond what is visible in the code; the repository does not include a real sample or text-normalization layer that proves this.
2. Exact impact of missing `verified_purchase`, `helpful_vote`, and review title fields on search quality; the code intentionally drops them, but the repository does not provide a retrieval evaluation showing whether those fields would improve semantics.
3. PII risk from customer review text; the code stores the text as-is without detection or redaction, but the repository contains no privacy policy or PII detection implementation to prove the risk in production.
4. Exact runtime behavior of the model in an offline environment; the code does not show whether `all-MiniLM-L6-v2` is pre-staged locally, mirrored internally, or available without internet access.
5. Final retrieval behavior and similarity metric; the repository writes vectors to Parquet but does not define the downstream vector database or the similarity metric used in searching.
6. The actual effect of the fixed 60/10 chunk design on search recall and precision; this is a design choice without repository evidence supporting its performance characteristics.
7. Test coverage; the repo does not include a test suite. This is a repository gap rather than a finding against any single code path.

## 9. Recommended Remediation Sequence

1. Stabilize the embedding pipeline architecture.
   - Replace the row-wise Python UDF with a batch-oriented embedding approach that minimizes JVM/Python serialization overhead.
   - Add vector-length validation and explicit error handling for failed embedding rows.

2. Add data-quality gates before each write.
   - Validate joined-record loss, null rates, duplicate counts, and chunk output quality before writing Silver and Gold outputs.
   - Ensure the pipeline fails fast when data quality drops below thresholds.

3. Make the pipeline operationally reliable.
   - Resolve path assumptions to the project root.
   - Add structured logging and a single orchestrated entry point.
   - Ensure Spark sessions are closed after execution and that dependencies are explicitly validated.

4. Establish lineage and traceability.
   - Add stable review and chunk IDs that travel through to the final vector storage.
   - Store source metadata that makes each vector traceable back to the original review and product.

5. Close the architectural gap to production search.
   - Add an on-prem vector index and retrieval layer, and define the similarity metric and index configuration explicitly.
   - Ensure the model artifact is vendored locally or mirrored internally.

6. Improve maintainability and documentation.
   - Remove dead code and unused imports.
   - Align naming with behavior (`recursive_chunker` vs. fixed-window chunking).
   - Add a proper README, schema document, and docstrings before broader adoption.

## 10. Production-Readiness Conclusion

The current repository is a serviceable prototype for a local Spark-based semantic search experiment, but it is not yet production-ready for an AeroMart-grade on-prem enterprise workflow. The strongest technical risks are concentrated in the embedding path, the lack of data-quality gates, and the absence of a validated retrieval layer.

The most important production blockers are:

- row-wise embedding execution that scales poorly and is inconsistent with a high-volume data pipeline,
- silent data loss in the chunking and join stages,
- missing validation of vector shape and source data quality,
- lack of guaranteed offline model availability,
- absence of a real vector database and retrieval system,
- manual, non-orchestrated execution order with no fail-fast guardrails.

The repository does demonstrate the intended Bronze-to-Silver-to-Gold-to-vector pipeline flow, but it does not yet demonstrate operational control, data trust, or on-prem search readiness at enterprise scale. The next phase should be focused not on adding new features, but on enforcing data quality, stabilizing the model and embedding pipeline, and defining the retrieval architecture required to make the vectors actually usable in production.

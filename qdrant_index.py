"""Index the generated AeroMart review embeddings in Qdrant."""

import math
import os
from functools import partial
from itertools import chain
from uuid import NAMESPACE_URL, uuid5


VECTOR_SIZE = 384
DEFAULT_COLLECTION = "aeromart_reviews"
DEFAULT_BATCH_SIZE = 128


def stable_point_id(chunk_id: str) -> str:
    """Return a stable Qdrant UUID for a pipeline chunk identifier."""
    if not chunk_id:
        raise ValueError("chunk_id must be a non-empty string")
    return str(uuid5(NAMESPACE_URL, f"aeromart-review-chunk:{chunk_id}"))


def _as_point(row, point_struct):
    """Validate an embedding row and convert it to a Qdrant point."""
    vector = row.embedding
    if vector is None or len(vector) != VECTOR_SIZE:
        raise ValueError(
            f"chunk {row.chunk_id!r} must have an embedding of {VECTOR_SIZE} values"
        )

    values = [float(value) for value in vector]
    if not all(math.isfinite(value) for value in values):
        raise ValueError(f"chunk {row.chunk_id!r} contains a non-finite embedding value")

    payload = {
        "chunk_id": row.chunk_id,
        "review_id": row.review_id,
        "product_id": row.product_id,
        "user_id": row.user_id,
        "rating": row.rating,
        "timestamp": row.timestamp,
        "review_chunk": row.review_chunk,
    }
    return point_struct(
        id=stable_point_id(row.chunk_id),
        vector=values,
        payload=payload,
    )


def _upsert_partition(rows, url, api_key, collection_name, batch_size):
    """Upsert one Spark partition in bounded batches, closing its client afterward."""
    from qdrant_client import QdrantClient, models

    iterator = iter(rows)
    first = next(iterator, None)
    if first is None:
        return

    client = QdrantClient(url=url, api_key=api_key)
    try:
        batch = []
        for row in chain((first,), iterator):
            batch.append(_as_point(row, models.PointStruct))
            if len(batch) >= batch_size:
                client.upsert(
                    collection_name=collection_name,
                    points=batch,
                    wait=True,
                )
                batch = []
        if batch:
            client.upsert(
                collection_name=collection_name,
                points=batch,
                wait=True,
            )
    finally:
        client.close()


def _ensure_collection(client, collection_name):
    """Create the collection or reject an incompatible existing vector schema."""
    from qdrant_client import models

    collection_names = {
        collection.name for collection in client.get_collections().collections
    }
    if collection_name not in collection_names:
        client.create_collection(
            collection_name=collection_name,
            vectors_config=models.VectorParams(
                size=VECTOR_SIZE,
                distance=models.Distance.COSINE,
            ),
        )
        return

    info = client.get_collection(collection_name=collection_name)
    vectors = info.config.params.vectors
    if isinstance(vectors, dict):
        raise ValueError(
            f"Collection {collection_name!r} uses named vectors; "
            "this pipeline expects one unnamed vector"
        )
    if vectors.size != VECTOR_SIZE or vectors.distance != models.Distance.COSINE:
        raise ValueError(
            f"Collection {collection_name!r} must use one {VECTOR_SIZE}-dimension "
            "Cosine vector"
        )


def index_vectors(
    spark,
    input_path,
    url,
    collection_name=DEFAULT_COLLECTION,
    batch_size=DEFAULT_BATCH_SIZE,
    api_key=None,
):
    """Validate the embedding schema and upsert partitions into Qdrant."""
    if batch_size < 1:
        raise ValueError("batch_size must be greater than zero")

    from qdrant_client import QdrantClient

    client = QdrantClient(url=url, api_key=api_key)
    try:
        _ensure_collection(client, collection_name)
    finally:
        client.close()

    embeddings_df = spark.read.parquet(input_path)
    required_columns = {
        "chunk_id",
        "review_id",
        "product_id",
        "user_id",
        "rating",
        "timestamp",
        "review_chunk",
        "embedding",
    }
    missing_columns = required_columns.difference(embeddings_df.columns)
    if missing_columns:
        raise ValueError(
            "Embedding Parquet is missing required columns: "
            + ", ".join(sorted(missing_columns))
        )

    write_partition = partial(
        _upsert_partition,
        url=url,
        api_key=api_key,
        collection_name=collection_name,
        batch_size=batch_size,
    )
    embeddings_df.foreachPartition(write_partition)


def main():
    """Read configuration and run the Spark-to-Qdrant indexing job."""
    from pyspark.sql import SparkSession

    url = os.environ.get("QDRANT_URL")
    if not url:
        raise RuntimeError("Set QDRANT_URL to the on-prem Qdrant server URL")

    input_path = os.environ.get(
        "VECTOR_INPUT_PATH",
        "data/gold/final_vector_storage",
    )
    collection_name = os.environ.get("QDRANT_COLLECTION", DEFAULT_COLLECTION)
    batch_size = int(os.environ.get("QDRANT_BATCH_SIZE", DEFAULT_BATCH_SIZE))
    api_key = os.environ.get("QDRANT_API_KEY")

    spark = (
        SparkSession.builder.appName("AeroMartQdrantIndex")
        .master("local[*]")
        .getOrCreate()
    )
    try:
        index_vectors(
            spark=spark,
            input_path=input_path,
            url=url,
            collection_name=collection_name,
            batch_size=batch_size,
            api_key=api_key,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

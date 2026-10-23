"""Search the AeroMart review vectors stored in Qdrant."""

import json
import math
import os


VECTOR_SIZE = 384
DEFAULT_COLLECTION = "aeromart_reviews"
MODEL_NAME = "all-MiniLM-L6-v2"


def search_reviews(query, limit=10):
    """Embed a text query and return the nearest Qdrant review chunks."""
    if not query or not query.strip():
        raise ValueError("query must contain non-whitespace text")
    if limit < 1:
        raise ValueError("limit must be greater than zero")

    from qdrant_client import QdrantClient
    from sentence_transformers import SentenceTransformer

    url = os.environ.get("QDRANT_URL")
    if not url:
        raise RuntimeError("Set QDRANT_URL to the on-prem Qdrant server URL")

    collection_name = os.environ.get("QDRANT_COLLECTION", DEFAULT_COLLECTION)
    api_key = os.environ.get("QDRANT_API_KEY")
    model = SentenceTransformer(MODEL_NAME)
    vector = [float(value) for value in model.encode(query.strip()).tolist()]
    if len(vector) != VECTOR_SIZE or not all(math.isfinite(value) for value in vector):
        raise ValueError(f"Query embedding must contain {VECTOR_SIZE} finite values")

    client = QdrantClient(url=url, api_key=api_key)
    try:
        response = client.query_points(
            collection_name=collection_name,
            query=vector,
            limit=limit,
            with_payload=True,
        )
    finally:
        client.close()

    return [
        {
            "score": point.score,
            "payload": point.payload,
        }
        for point in response.points
    ]


def main():
    """Run a query supplied through the command line and print JSON results."""
    import argparse

    parser = argparse.ArgumentParser(description="Search indexed AeroMart reviews")
    parser.add_argument("query", help="Natural-language search query")
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()
    results = search_reviews(args.query, args.limit)
    print(json.dumps(results, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()

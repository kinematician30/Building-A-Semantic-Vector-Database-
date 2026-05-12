from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count, avg, udf, coalesce
from pyspark.sql.types import ArrayType, StringType
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# Spark Config
spark = SparkSession.builder.appName("VectorEngine").config("spark.sql.shuffle.partitions", "20").config("spark.sql.caseSensitive", "true").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").master('local[*]').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

SILVER_LAYER = "./data/silver/complete_reviews"
GOLD_LAYER = "./data/gold"
os.makedirs(GOLD_LAYER, exist_ok=True)

# Load the silver layer into the pipeline
silver_df = spark.read.parquet(SILVER_LAYER)

# def recursive_chunker(text): 
#     try:
#         if not text or len(str(text)) < 5: return []
            
#         text = str(text) # to ensure 
#         max_chars = 400
#         overlap = 50
#         separators = ["\n\n", "\n", ". ", " ", "", ","] # from largest to smallest order of preference in cutting
        
#         chunks = []
#         start = 0
#         while start < len(text):
#             end = min(start + max_chars, len(text))
#             if end < len(text):
#                 for sep in separators:
#                     last_sep = text.rfind(sep, start, end)
#                     if last_sep != -1 and last_sep > start:
#                         end = last_sep + len(sep)
#                         break
#             chunks.append(text[start:end].strip())
#             start = end - overlap if end < len(text) else end
#         return [c for c in chunks if len(c) > 10]
#     except Exception:
#         return [] # In case of any unexpected error, return empty list to avoid crashing the pipeline

def recursive_chunker(text):
    try:
        if not text: return []
        text = str(text).strip()
        if len(text) < 10: return []
        
        words = text.split() 
        chunks = []
        
        chunk_size = 60 
        overlap = 10    
        
        step = chunk_size - overlap
        if step <= 0: step = chunk_size
        
        for i in range(0, len(words), step):
            chunk_string = " ".join(words[i : i + chunk_size])
            
            if len(chunk_string) > 10:
                chunks.append(chunk_string)
                
        return chunks
        
    except Exception:
        return []
    
# Finally register the recursive chunker fucntion as a udf in pyspark
chunk_udf = udf(recursive_chunker, ArrayType(StringType()))

#  GOLD TABLE 1: dim_products 
print("Extracting Product Metadata...")
dim_products = silver_df.select(
    col("parent_asin").alias("product_id"), 
    col("title").alias("product_name"), "main_category", "price"
).distinct()
print(f"Added {dim_products.count()} unique products to dim_products table.")
dim_products.coalesce(1).write.mode("overwrite").parquet("./data/gold/dim_products")

#  GOLD TABLE 2: dim_users 
print("Extracting User Personalization Data...")
dim_users = silver_df.groupBy("user_id").agg(
    count("*").alias("review_count"),
    avg("rating").alias("avg_rating_given")
)
print(f"Added {dim_users.count()} unique users to dim_users table.")
dim_users.coalesce(1).write.mode("overwrite").parquet("data/gold/dim_users")

#  GOLD TABLE 3: fact_review_vectors 
print("Applying Recursive Splitter for Vector Embeddings...")
fact_vectors = silver_df.repartition(20).withColumn("review_chunk", explode(chunk_udf(col("text")))) \
    .select(
        col("parent_asin").alias("product_id"),
        "user_id",
        "rating",
        "review_chunk"
    )
fact_vectors.coalesce(1).write.mode("overwrite").parquet("data/gold/fact_vectors")
print(f"Added {fact_vectors.count()} unique users to dim_users table.")

print("Gold Layer Finished: 3 Tables ready for Database Loading.")
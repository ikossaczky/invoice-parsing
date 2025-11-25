"""
Spark Streaming Demo - Monitor folder for text files and write to SQLite database
"""
import os
import sys
import json

from dotenv import load_dotenv

load_dotenv()

os.environ['HADOOP_USER_NAME'] = os.getenv('USER', 'spark')
os.environ['HADOOP_HOME'] = '/tmp/hadoop'

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

import numpy as np
import faiss
from sentence_transformers import SentenceTransformer

DATA_LANDING_FOLDER = "/home/igor/Git/invoice-parsing/data_landing"
DATABASE_PATH = "/home/igor/Git/invoice-parsing/databases/streaming_demo.sqlite"
CHECKPOINT_PATH = "/home/igor/Git/invoice-parsing/data_landing/_spark_checkpoint"
VECTORSTORE_DIR = "/home/igor/Git/invoice-parsing/vectorstores"
FAISS_INDEX_PATH = os.path.join(VECTORSTORE_DIR, "faiss_demo_index.bin")
FAISS_METADATA_PATH = os.path.join(VECTORSTORE_DIR, "faiss_demo_metadata.json")

_embedding_model = None


def get_embedding_model():
    global _embedding_model
    if _embedding_model is None:
        _embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
    return _embedding_model


def update_faiss_vectorstore(pandas_df):
    if pandas_df.empty:
        return

    os.makedirs(VECTORSTORE_DIR, exist_ok=True)

    texts = pandas_df["content"].astype(str).tolist()
    file_names = pandas_df["file_name"].astype(str).tolist()

    model = get_embedding_model()
    embeddings = model.encode(texts, convert_to_numpy=True)
    embeddings = embeddings.astype("float32")

    if os.path.exists(FAISS_INDEX_PATH):
        index = faiss.read_index(FAISS_INDEX_PATH)
        if os.path.exists(FAISS_METADATA_PATH):
            with open(FAISS_METADATA_PATH, "r", encoding="utf-8") as f:
                metadata = json.load(f)
        else:
            metadata = {"files": []}
    else:
        dimension = embeddings.shape[1]
        index = faiss.IndexFlatL2(dimension)
        metadata = {"files": []}

    index.add(embeddings)

    for name in file_names:
        metadata["files"].append({"file_name": name})

    faiss.write_index(index, FAISS_INDEX_PATH)
    with open(FAISS_METADATA_PATH, "w", encoding="utf-8") as f:
        json.dump(metadata, f)


def create_spark_session():
    """Create and configure Spark session"""
    spark = SparkSession.builder \
        .appName("TextFileStreamingDemo") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_to_sqlite(batch_df, batch_id):
    """
    Write each batch to SQLite database
    
    Args:
        batch_df: DataFrame containing the batch data
        batch_id: Unique identifier for the batch
    """
    if batch_df.count() > 0:
        # Aggregate by file so that each file becomes exactly one row
        aggregated_df = batch_df.groupBy("file_name").agg(
            F.collect_list("content").alias("contents"),
            F.min("processed_time").alias("processed_time"),
        ).withColumn(
            "content", F.array_join(F.col("contents"), "\n")
        ).select("content", "file_name", "processed_time")

        # Convert to Pandas and write to SQLite
        import sqlite3
        
        # Create connection
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Create table if not exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS text_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content TEXT,
                file_name TEXT,
                processed_time TIMESTAMP
            )
        ''')
        
        pandas_df = aggregated_df.toPandas()
        
        pandas_df.to_sql('text_files', conn, if_exists='append', index=False)
        
        conn.commit()
        conn.close()

        update_faiss_vectorstore(pandas_df)

        print(f"Batch {batch_id}: Wrote {aggregated_df.count()} files to database and vectorstore")


def main():
    """Main streaming application"""
    # Ensure directories exist
    os.makedirs(DATA_LANDING_FOLDER, exist_ok=True)
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
    os.makedirs(CHECKPOINT_PATH, exist_ok=True)
    
    # Create Spark session
    spark = create_spark_session()
    
    print(f"Starting Spark Streaming...")
    print(f"Monitoring folder: {DATA_LANDING_FOLDER}")
    print(f"Database location: {DATABASE_PATH}")
    print(f"Checkpoint location: {CHECKPOINT_PATH}")
    print("\nWaiting for text files...")
    
    # Define schema for the streaming data
    schema = StructType([
        StructField("value", StringType(), True)
    ])
    
    # Read streaming data from text files
    streaming_df = spark.readStream \
        .format("text") \
        .option("maxFilesPerTrigger", 1) \
        .schema(schema) \
        .load(DATA_LANDING_FOLDER)
    
    # Add metadata columns
    processed_df = streaming_df \
        .withColumn("file_name", F.input_file_name()) \
        .withColumn("processed_time", F.current_timestamp()) \
        .select(
            F.col("value").alias("content"),
            F.col("file_name"),
            F.col("processed_time")
        )
    
    # Write stream to SQLite using foreachBatch
    query = processed_df.writeStream \
        .foreachBatch(write_to_sqlite) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime='5 seconds') \
        .start()
    
    print("\nStreaming started successfully!")
    print("Press Ctrl+C to stop...\n")
    
    # Wait for termination
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\nStopping streaming...")
        query.stop()
        spark.stop()
        print("Streaming stopped successfully!")


if __name__ == "__main__":
    main()

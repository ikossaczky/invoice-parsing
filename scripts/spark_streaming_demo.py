"""
Spark Streaming Demo - Monitor folder for text files and write to SQLite database
"""
import os
import sys
from dotenv import load_dotenv

load_dotenv()

# Set environment variables before importing PySpark
# Set Hadoop username to avoid authentication issues
os.environ['HADOOP_USER_NAME'] = os.getenv('USER', 'spark')
# Disable Hadoop native library warnings
os.environ['HADOOP_HOME'] = '/tmp/hadoop'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Configuration
OBSERVED_FOLDER = "/home/igor/Git/invoice-parsing/observed_data"
DATABASE_PATH = "/home/igor/Git/invoice-parsing/databases/streaming_demo.sqlite"
CHECKPOINT_PATH = "/home/igor/Git/invoice-parsing/observed_data/_spark_checkpoint"


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
        
        # Convert Spark DataFrame to Pandas
        pandas_df = batch_df.toPandas()
        
        # Write to SQLite
        pandas_df.to_sql('text_files', conn, if_exists='append', index=False)
        
        conn.commit()
        conn.close()
        
        print(f"Batch {batch_id}: Wrote {batch_df.count()} records to database")


def main():
    """Main streaming application"""
    # Ensure directories exist
    os.makedirs(OBSERVED_FOLDER, exist_ok=True)
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
    os.makedirs(CHECKPOINT_PATH, exist_ok=True)
    
    # Create Spark session
    spark = create_spark_session()
    
    print(f"Starting Spark Streaming...")
    print(f"Monitoring folder: {OBSERVED_FOLDER}")
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
        .load(OBSERVED_FOLDER)
    
    # Add metadata columns
    processed_df = streaming_df \
        .withColumn("file_name", input_file_name()) \
        .withColumn("processed_time", current_timestamp()) \
        .select(
            col("value").alias("content"),
            col("file_name"),
            col("processed_time")
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

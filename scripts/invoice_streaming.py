"""Invoice Parsing Streaming Pipeline.

Monitors folder for invoice files, extracts text, processes items, and stores results.
"""
import os
from datetime import datetime
from typing import List, Tuple

from dotenv import load_dotenv

load_dotenv()

# Set Hadoop configuration for Spark
os.environ['HADOOP_USER_NAME'] = os.getenv('USER', 'spark')
os.environ['HADOOP_HOME'] = '/tmp/hadoop'

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from modules.pdf_extractor import extract_text_from_file
from modules.llm_client import LLMClient
from modules.vectorstore_manager import VectorstoreManager
from modules.database_manager import DatabaseManager

# Configuration constants
DATA_LANDING_FOLDER = "/home/igor/Git/invoice-parsing/data_landing"
DATABASE_PATH = "/home/igor/Git/invoice-parsing/databases/invoice_database.db"
CHECKPOINT_PATH = "/home/igor/Git/invoice-parsing/data_landing/_invoice_checkpoint"
VECTORSTORE_DIR = "/home/igor/Git/invoice-parsing/vectorstores"
FAISS_INDEX_PATH = os.path.join(VECTORSTORE_DIR, "invoice_vectorstore_index.bin")
FAISS_METADATA_PATH = os.path.join(VECTORSTORE_DIR, "invoice_vectorstore_metadata.json")

SIMILARITY_THRESHOLD = 0.9
MONITORED_FILE_EXTENSIONS = ['pdf', 'jpg', 'jpeg', 'png']

# Global instances (initialized once per executor)
_llm_client = None
_vectorstore_manager = None
_database_manager = None


def get_llm_client():
    """Get or create LLM client instance"""
    global _llm_client
    if _llm_client is None:
        _llm_client = LLMClient(model="gpt-5-mini", embedding_model="text-embedding-3-small")
    return _llm_client


def get_vectorstore_manager():
    """Get or create vectorstore manager instance"""
    global _vectorstore_manager
    if _vectorstore_manager is None:
        _vectorstore_manager = VectorstoreManager(FAISS_INDEX_PATH, FAISS_METADATA_PATH)
    return _vectorstore_manager


def get_database_manager():
    """Get or create database manager instance"""
    global _database_manager
    if _database_manager is None:
        _database_manager = DatabaseManager(DATABASE_PATH)
    return _database_manager


def is_monitored_file(file_path: str) -> bool:
    """Check if file extension is in monitored list"""
    ext = os.path.splitext(file_path)[1].lower().lstrip('.')
    return ext in MONITORED_FILE_EXTENSIONS


def process_invoice_batch(batch_df, batch_id):
    """
    Process each batch of invoice files
    
    Args:
        batch_df: DataFrame containing batch data with file paths
        batch_id: Unique identifier for the batch
    """
    if batch_df.count() == 0:
        return
    
    print(f"\n{'='*60}")
    print(f"Processing Batch {batch_id}")
    print(f"{'='*60}")
    
    # Collect file paths from batch
    files_data = batch_df.collect()
    
    # Initialize managers
    llm_client = get_llm_client()
    vectorstore_manager = get_vectorstore_manager()
    database_manager = get_database_manager()
    
    for row in files_data:
        raw_path = row['file_path']
        # Spark binaryFile source uses URI-style paths like file:/...
        # Normalize to a regular filesystem path for local access
        if raw_path.startswith("file:/"):
            # Handle both file:/home/... and file:///home/...
            if raw_path.startswith("file://"):
                file_path = raw_path[len("file://"):]
            else:
                file_path = raw_path[len("file:"):]
        else:
            file_path = raw_path

        ingestion_time = datetime.now()
        
        # Filter by file extension
        if not is_monitored_file(file_path):
            print(f"\nSkipping non-monitored file: {file_path}")
            continue
        
        print(f"\n{'─'*60}")
        print(f"Processing: {os.path.basename(file_path)}")
        print(f"{'─'*60}")
        
        # Step 1: Extract text from invoice
        invoice_text, extraction_method = extract_text_from_file(file_path)
        
        if not invoice_text:
            print(f"  ❌ Failed to extract text, skipping file")
            continue
        
        # Step 2: Extract raw items using LLM
        print(f"  🤖 Extracting items using LLM...")
        raw_items = llm_client.extract_raw_items(invoice_text)
        
        if not raw_items:
            print(f"  ❌ No items found in invoice")
            continue
        
        print(f"  ✅ Found {len(raw_items)} items:")
        for item in raw_items:
            print(f"    • {item}")
        
        # Step 3: Get embeddings for raw items
        print(f"  🤖 Computing embeddings for {len(raw_items)} items...")
        embeddings = llm_client.get_embeddings(raw_items)
        
        if not embeddings or len(embeddings) != len(raw_items):
            print(f"  💥 Failed to get embeddings, skipping file")
            continue
        
        # Step 4: Search vectorstore for similar items
        print(f"  🧩 Searching vectorstore (threshold: {SIMILARITY_THRESHOLD})...")
        known_items, unknown_indices = vectorstore_manager.search_similar_items(
            embeddings, 
            SIMILARITY_THRESHOLD
        )
        
        # Separate known and unknown items
        known_raw_items = []
        known_processed_items = []
        unknown_raw_items = []
        unknown_embeddings = []
        
        for i, raw_item in enumerate(raw_items):
            if i in unknown_indices:
                unknown_raw_items.append(raw_item)
                unknown_embeddings.append(embeddings[i])
        
        # Extract known items
        for known_item in known_items:
            known_raw_items.append(known_item['raw_item_name'])
            known_processed_items.append(known_item['processed_item_name'])
        
        print(f"  🧩 Found {len(known_raw_items)} known items in vectorstore:")
        for raw, processed in zip(known_raw_items, known_processed_items):
            print(f"    • {raw} → {processed}")
        
        print(f"  ⚠️ Found {len(unknown_raw_items)} unknown items:")
        for item in unknown_raw_items:
            print(f"    • {item}")
        
        # Step 5: Process unknown items with LLM
        unknown_processed_items = []
        if unknown_raw_items:
            print(f"  🤖 Processing {len(unknown_raw_items)} unknown items with LLM...")
            processed_mapping = llm_client.process_unknown_items(unknown_raw_items)
            
            # Extract processed items in same order as unknown_raw_items
            for raw_item in unknown_raw_items:
                processed_item = processed_mapping.get(raw_item, raw_item)
                unknown_processed_items.append(processed_item)
            
            print(f"  🤖 LLM processing complete:")
            for raw, processed in zip(unknown_raw_items, unknown_processed_items):
                print(f"    • {raw} → {processed}")
            
            # Step 6: Update vectorstore with unknown items
            vectorstore_manager.add_items(
                unknown_raw_items,
                unknown_processed_items,
                unknown_embeddings
            )
        
        # Step 7: Combine all items for database insertion
        all_raw_items = known_raw_items + unknown_raw_items
        all_processed_items = known_processed_items + unknown_processed_items
        
        # Step 8: Save to database
        print(f"  🛢️ Writing {len(all_raw_items)} items to database...")
        
        # Create extraction methods list: VECTORSTORE for known items, LLM for unknown items
        extraction_methods = ['VECTORSTORE'] * len(known_raw_items) + ['LLM'] * len(unknown_raw_items)
        
        rows_inserted = database_manager.insert_items(
            file_name=os.path.basename(file_path),
            ingestion_time=ingestion_time,
            raw_items=all_raw_items,
            processed_items=all_processed_items,
            extraction_methods=extraction_methods
        )
        
        print(f"  🛢️ Wrote {rows_inserted} rows to database")
    
    print(f"\n{'='*60}")
    print(f"Batch {batch_id} Complete")
    print(f"{'='*60}\n")


def create_spark_session():
    """Create and configure Spark session"""
    spark = SparkSession.builder \
        .appName("InvoiceParsingPipeline") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    """Main streaming application"""
    # Ensure directories exist
    os.makedirs(DATA_LANDING_FOLDER, exist_ok=True)
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
    os.makedirs(VECTORSTORE_DIR, exist_ok=True)
    os.makedirs(CHECKPOINT_PATH, exist_ok=True)
    
    # Create Spark session
    spark = create_spark_session()
    
    print(f"\n{'='*60}")
    print(f"Invoice Parsing Streaming Pipeline")
    print(f"{'='*60}")
    print(f"Monitored folder: {DATA_LANDING_FOLDER}")
    print(f"Database: {DATABASE_PATH}")
    print(f"Vectorstore: {VECTORSTORE_DIR}")
    print(f"Checkpoint: {CHECKPOINT_PATH}")
    print(f"File extensions: {', '.join(MONITORED_FILE_EXTENSIONS)}")
    print(f"Similarity threshold: {SIMILARITY_THRESHOLD}")
    print(f"\nWaiting for invoice files...")
    print(f"{'='*60}\n")
    
    # Read streaming data from binary files (handles PDFs and images)
    # Let Spark use the default binaryFile schema
    streaming_df = spark.readStream \
        .format("binaryFile") \
        .option("maxFilesPerTrigger", 1) \
        .load(DATA_LANDING_FOLDER)
    
    # Extract file path
    processed_df = streaming_df \
        .select(F.col("path").alias("file_path"))
    
    # Write stream using foreachBatch
    query = processed_df.writeStream \
        .foreachBatch(process_invoice_batch) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime='5 seconds') \
        .start()
    
    print("Streaming started successfully!")
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

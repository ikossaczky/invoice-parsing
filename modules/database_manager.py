"""
SQLite database manager for invoice data
"""
import sqlite3
from typing import List
from datetime import datetime


class DatabaseManager:
    """Manages SQLite database for invoice items"""
    
    def __init__(self, db_path: str):
        """
        Initialize database manager
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self._create_table()
    
    def _create_table(self):
        """Create invoice_items table if it doesn't exist"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS invoice_items (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                FILE_NAME TEXT NOT NULL,
                INGESTION_TIME TIMESTAMP NOT NULL,
                RAW_ITEM_DESCRIPTION TEXT NOT NULL,
                PROCESSED_ITEM_DESCRIPTION TEXT NOT NULL,
                EXTRACTION_METHOD TEXT NOT NULL
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def insert_items(
        self, 
        file_name: str, 
        ingestion_time: datetime,
        raw_items: List[str], 
        processed_items: List[str],
        extraction_methods: List[str]
    ) -> int:
        """
        Insert invoice items into database
        
        Args:
            file_name: Name of the invoice file
            ingestion_time: Timestamp of ingestion
            raw_items: List of raw item descriptions
            processed_items: List of processed item descriptions
            extraction_methods: List of extraction methods (VECTORSTORE or LLM)
            
        Returns:
            Number of rows inserted
        """
        if not raw_items or len(raw_items) != len(processed_items) or len(raw_items) != len(extraction_methods):
            print("  → Warning: Inconsistent lengths for raw_items, processed_items, and extraction_methods")
            return 0
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        rows = [
            (file_name, ingestion_time, raw_item, processed_item, extraction_method)
            for raw_item, processed_item, extraction_method in zip(raw_items, processed_items, extraction_methods)
        ]
        
        cursor.executemany(
            '''
            INSERT INTO invoice_items 
            (FILE_NAME, INGESTION_TIME, RAW_ITEM_DESCRIPTION, PROCESSED_ITEM_DESCRIPTION, EXTRACTION_METHOD)
            VALUES (?, ?, ?, ?, ?)
            ''',
            rows
        )
        
        conn.commit()
        row_count = cursor.rowcount
        conn.close()
        
        return row_count

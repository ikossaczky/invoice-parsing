"""
FAISS vectorstore manager for invoice item similarity search
"""
import os
import json
from typing import List, Tuple, Dict
import numpy as np
import faiss


class VectorstoreManager:
    """Manages FAISS vectorstore for invoice items"""
    
    def __init__(self, index_path: str, metadata_path: str):
        """
        Initialize vectorstore manager
        
        Args:
            index_path: Path to FAISS index file
            metadata_path: Path to metadata JSON file
        """
        self.index_path = index_path
        self.metadata_path = metadata_path
        self.index = None
        self.metadata = {"items": []}
        
        # Load existing index and metadata if they exist
        self._load()
    
    def _load(self):
        """Load existing FAISS index and metadata"""
        if os.path.exists(self.index_path):
            self.index = faiss.read_index(self.index_path)
            print(f"  🧩 Loaded existing FAISS index with {self.index.ntotal} items")
        
        if os.path.exists(self.metadata_path):
            with open(self.metadata_path, 'r', encoding='utf-8') as f:
                self.metadata = json.load(f)
    
    def _save(self):
        """Save FAISS index and metadata to disk"""
        os.makedirs(os.path.dirname(self.index_path), exist_ok=True)
        
        if self.index is not None:
            faiss.write_index(self.index, self.index_path)
        
        with open(self.metadata_path, 'w', encoding='utf-8') as f:
            json.dump(self.metadata, f, indent=2)
    
    def search_similar_items(
        self, 
        query_embeddings: List[List[float]], 
        threshold: float
    ) -> Tuple[List[Dict[str, str]], List[int]]:
        """
        Search for similar items in vectorstore
        
        Args:
            query_embeddings: List of embedding vectors to search
            threshold: Similarity threshold (0-1), items with similarity > threshold are considered known
            
        Returns:
            Tuple of (indices_and_similar_items, indices_without_similar_items)
            - indices_and_similar_items: List of dicts with raw_item_name and processed_item_name
            - indices_without_similar_items: Indices of items that are unknown (below threshold)
        """
        if self.index is None or self.index.ntotal == 0:
            # No items in vectorstore, all items are unknown
            print(f"  🧩 Vectorstore is empty, all {len(query_embeddings)} items are unknown")
            return [], list(range(len(query_embeddings)))
        
        # Convert to numpy array
        query_vectors = np.array(query_embeddings, dtype='float32')
        
        # Search for nearest neighbors
        # FAISS returns L2 distances, we need to convert to similarity
        k = 1  # Get the single most similar item
        distances, indices = self.index.search(query_vectors, k)
        
        indices_and_similar_items = []
        indices_without_similar_items = []
        
        for i, (distance, idx) in enumerate(zip(distances, indices)):
            # Convert L2 distance to cosine similarity approximation
            # For normalized vectors: similarity ≈ 1 - (distance² / 2)
            # For non-normalized: use distance directly with threshold
            similarity = 1.0 / (1.0 + float(distance[0]))

            print(f"  💎 Item {i}: similarity {similarity}, index {idx[0]}")
            
            if similarity >= threshold and idx[0] >= 0:
                # Item is known
                metadata_item = self.metadata["items"][idx[0]]
                indices_and_similar_items.append({
                    "similar_raw_item_name": metadata_item["raw_item_name"],
                    "similar_processed_item_name": metadata_item["processed_item_name"],
                    "index": i
                })
            else:
                # Item is unknown
                indices_without_similar_items.append(i)
        
        return indices_and_similar_items, indices_without_similar_items
    
    def add_items(
        self, 
        raw_items: List[str], 
        processed_items: List[str], 
        embeddings: List[List[float]]
    ):
        """
        Add new items to vectorstore
        
        Args:
            raw_items: List of raw item names
            processed_items: List of processed item names
            embeddings: List of embedding vectors
        """
        if not raw_items or len(raw_items) != len(processed_items) != len(embeddings):
            print("  → Warning: Inconsistent lengths for raw_items, processed_items, and embeddings")
            return
        
        # Convert to numpy array
        vectors = np.array(embeddings, dtype='float32')
        
        # Create index if it doesn't exist
        if self.index is None:
            dimension = vectors.shape[1]
            self.index = faiss.IndexFlatL2(dimension)
            print(f"  🧩 Created new FAISS index with dimension {dimension}")
        
        # Add vectors to index
        self.index.add(vectors)
        
        # Add metadata
        for raw_item, processed_item in zip(raw_items, processed_items):
            self.metadata["items"].append({
                "raw_item_name": raw_item,
                "processed_item_name": processed_item
            })
        
        # Save to disk
        self._save()
        
        print(f"  🧩 Added {len(raw_items)} items to vectorstore (total: {self.index.ntotal})")

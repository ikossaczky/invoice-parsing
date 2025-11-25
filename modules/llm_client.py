"""
OpenAI LLM client for invoice item extraction and processing
"""
import os
from typing import List, Dict
from openai import OpenAI
from pydantic import BaseModel


class InvoiceItems(BaseModel):
    """Pydantic model for extracting raw items from invoice text"""
    items: List[str]


class ItemMapping(BaseModel):
    """Single raw to processed item mapping"""
    raw_item: str
    processed_item: str


class ProcessedItems(BaseModel):
    """Pydantic model for mapping raw items to processed items"""
    mappings: List[ItemMapping]


class LLMClient:
    """OpenAI client for invoice processing"""
    
    def __init__(self, model: str = "gpt-5-mini", embedding_model: str = "text-embedding-3-small"):
        """
        Initialize LLM client
        
        Args:
            model: OpenAI chat model to use
            embedding_model: OpenAI embedding model to use
        """
        self.client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        self.model = model
        self.embedding_model = embedding_model
    
    def extract_raw_items(self, invoice_text: str) -> List[str]:
        """
        Extract raw item names from invoice text using structured output
        
        Args:
            invoice_text: Raw text extracted from invoice
            
        Returns:
            List of raw item names/descriptions
        """
        try:
            completion = self.client.beta.chat.completions.parse(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert at extracting item names and descriptions from invoices. Extract all items mentioned in the invoice as a list of strings."
                    },
                    {
                        "role": "user",
                        "content": f"Extract all item names/descriptions from this invoice:\n\n{invoice_text}"
                    }
                ],
                response_format=InvoiceItems
            )
            
            result = completion.choices[0].message.parsed
            return result.items if result else []
        
        except Exception as e:
            print(f"Error extracting items with LLM: {str(e)}")
            return []
    
    def process_unknown_items(self, raw_items: List[str]) -> Dict[str, str]:
        """
        Process unknown raw items into standardized processed items
        
        Args:
            raw_items: List of raw item names that need processing
            
        Returns:
            Dictionary mapping raw_item -> processed_item
        """
        try:
            items_text = "\n".join([f"- {item}" for item in raw_items])
            
            completion = self.client.beta.chat.completions.parse(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert at standardizing product names. Given raw item descriptions, convert them to clean, simplified and short product names. Return a list of mappings from raw items to their processed versions."
                    },
                    {
                        "role": "user",
                        "content": f"Standardize these item names:\n{items_text}"
                    }
                ],
                response_format=ProcessedItems
            )
            
            result = completion.choices[0].message.parsed
            if result and result.mappings:
                # Convert list of mappings to dict
                return {mapping.raw_item: mapping.processed_item for mapping in result.mappings}
            return {}
        
        except Exception as e:
            print(f"Error processing items with LLM: {str(e)}")
            return {}
    
    def get_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Get embeddings for a list of texts
        
        Args:
            texts: List of text strings to embed
            
        Returns:
            List of embedding vectors
        """
        try:
            response = self.client.embeddings.create(
                model=self.embedding_model,
                input=texts
            )
            return [item.embedding for item in response.data]
        
        except Exception as e:
            print(f"Error getting embeddings: {str(e)}")
            return []

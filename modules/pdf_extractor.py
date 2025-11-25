"""
PDF and Image text extraction module
Supports PyPDF2 for PDFs and Tesseract OCR for images and failed PDF extractions
"""
import os
from typing import Optional
from PIL import Image
import pytesseract
import PyPDF2


def extract_text_from_pdf(file_path: str) -> Optional[str]:
    """
    Extract text from PDF using PyPDF2
    
    Args:
        file_path: Path to PDF file
        
    Returns:
        Extracted text or None if extraction failed
    """
    try:
        with open(file_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            text = ""
            for page in pdf_reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
            
            # Return text only if we extracted something meaningful
            if text.strip():
                return text.strip()
            return None
    except Exception as e:
        print(f"  💥📝 PyPDF2 extraction failed for {file_path}: {str(e)}")
        return None


def extract_text_from_image(file_path: str) -> Optional[str]:
    """
    Extract text from image using Tesseract OCR
    
    Args:
        file_path: Path to image file
        
    Returns:
        Extracted text or None if extraction failed
    """
    try:
        image = Image.open(file_path)
        text = pytesseract.image_to_string(image)
        
        if text.strip():
            return text.strip()
        return None
    except Exception as e:
        print(f"  💥👁️ Tesseract extraction failed for {file_path}: {str(e)}")
        return None


def extract_text_from_file(file_path: str) -> tuple[str, str]:
    """
    Extract text from invoice file (PDF or image)
    Tries PyPDF2 first for PDFs, falls back to Tesseract if needed
    
    Args:
        file_path: Path to invoice file
        
    Returns:
        Tuple of (extracted_text, extraction_method)
        extraction_method is either 'pypdf2', 'tesseract', or 'failed'
    """
    file_ext = os.path.splitext(file_path)[1].lower()
    
    # For PDFs, try PyPDF2 first, then fall back to Tesseract
    if file_ext == '.pdf':
        print(f"  📝 Extracting text from PDF...")
        text = extract_text_from_pdf(file_path)
        if text:
            print(f"  📝 Successfully extracted text using PyPDF2")
            return text, 'pypdf2'
        
        # PyPDF2 failed, try Tesseract
        print(f"  💥📝 PyPDF2 failed, trying Tesseract OCR...")
        text = extract_text_from_image(file_path)
        if text:
            print(f"  👁️ Successfully extracted text using Tesseract")
            return text, 'tesseract'
    
    # For images, use Tesseract directly
    elif file_ext in ['.jpg', '.jpeg', '.png']:
        print(f"  👁️ Extracting text from image...")
        text = extract_text_from_image(file_path)
        if text:
            print(f"  👁️ Successfully extracted text using Tesseract")
            return text, 'tesseract'
    
    print(f"  💥 Text extraction failed for {file_path}")
    return "", 'failed'

# Invoice Parsing Streaming Pipeline

Real-time invoice processing system using Apache Spark, OpenAI LLMs, and FAISS vectorstore.

## Features

- **Real-time monitoring**: Automatically detects new invoice files (PDF, JPG, PNG)
- **Text extraction**: Uses PyPDF2 for PDFs, falls back to Tesseract OCR if needed
- **AI-powered extraction**: OpenAI GPT-5-mini extracts items from invoices
- **Smart caching**: FAISS vectorstore remembers previously processed items
- **Automatic standardization**: Unknown items are standardized via LLM
- **Persistent storage**: SQLite database stores all invoice data

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Install Tesseract OCR

**Ubuntu/Debian:**
```bash
sudo apt-get install tesseract-ocr
```

**macOS:**
```bash
brew install tesseract
```

**Windows:**
Download from: https://github.com/UB-Mannheim/tesseract/wiki

### 3. Configure Environment

Create a `.env` file in the project root:

```env
OPENAI_API_KEY=your_openai_api_key_here
USER=your_username
```

## Usage

### Start the Streaming Pipeline

```bash
python scripts/invoice_streaming.py
```

The pipeline will:
1. Monitor `data_landing/` folder for new invoice files
2. Extract text from invoices (PDF or images)
3. Extract item names using LLM
4. Check vectorstore for known items
5. Process unknown items with LLM
6. Update vectorstore and database

### Add Invoice Files

Simply copy invoice files to the `data_landing/` folder:

```bash
cp my_invoice.pdf data_landing/
```

Supported formats: `.pdf`, `.jpg`, `.jpeg`, `.png`

## Configuration

Edit constants in `scripts/invoice_streaming.py`:

- `SIMILARITY_THRESHOLD`: Threshold for vectorstore matching (default: 0.95)
- `MONITORED_FILE_EXTENSIONS`: File types to process (default: pdf, jpg, jpeg, png)
- `DATA_LANDING_FOLDER`: Folder to monitor (data_landing)
- `DATABASE_PATH`: SQLite database location
- `VECTORSTORE_DIR`: FAISS vectorstore location

## Project Structure

```
invoice-parsing/
├── modules/
│   ├── pdf_extractor.py        # Text extraction (PyPDF2 + Tesseract)
│   ├── llm_client.py           # OpenAI API client
│   ├── vectorstore_manager.py  # FAISS vectorstore operations
│   └── database_manager.py     # SQLite database operations
├── scripts/
│   └── invoice_streaming.py    # Main streaming pipeline
├── data_landing/               # Landing zone for invoice files
├── databases/                  # SQLite database storage
│   └── invoice_database.db
├── vectorstores/               # FAISS vectorstore files
│   ├── invoice_vectorstore_index.bin
│   └── invoice_vectorstore_metadata.json
└── requirements.txt
```

## Database Schema

**Table: invoice_items**

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key (auto-increment) |
| file_name | TEXT | Invoice filename |
| ingestion_time | TIMESTAMP | Processing timestamp |
| raw_item_description | TEXT | Original item text from invoice |
| processed_item_description | TEXT | Standardized item name |

## Vectorstore Structure

FAISS index stores embeddings of raw item names with metadata:
- `raw_item_name`: Original item description
- `processed_item_name`: Standardized name

## Logging

The pipeline logs all operations to stdout:
- Text extraction method used (PyPDF2 or Tesseract)
- Number of items found
- Items matched in vectorstore (known items)
- Items requiring LLM processing (unknown items)
- Vectorstore updates
- Database insertions

## Troubleshooting

**Issue: Tesseract not found**
- Ensure Tesseract is installed and in PATH
- Set `TESSERACT_CMD` environment variable if needed

**Issue: OpenAI API errors**
- Verify `OPENAI_API_KEY` in `.env` file
- Check API quota and billing

**Issue: Spark errors**
- Ensure Java is installed (Spark requirement)
- Check checkpoint directory permissions

## Notes

- First run will create empty vectorstore
- Subsequent runs benefit from cached items
- Each invoice can have multiple items (multiple DB rows)
- Checkpoint data allows recovery after interruption

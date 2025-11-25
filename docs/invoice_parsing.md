# Invoice parsing demo:

## Implementation logic:

This demo shows how to use Spark to process invoice data and extract relevant information from it.
It includes the following steps:

- Spark streaming monitors folder data_landing for new invoice files.
- New invoice files comes - following steps are done, using foreachBatch
   - Invoice text is extracted from the file using PyPDF2
   - If PyPDF2 fail, we use Tesseract to extract text from the file
   - Openai LLM gpt-5-mini is called to extract items from the invoice text - it uses Pydantic structured format to return list of strings -> this list of strings is called raw_items
   - FAISS is used to get most similar item from faiss vectorstore (if the vectorstore already exists)
   - collect all embeddings where similarity > threshold intoo list known_raw_items
   - collect all embeddings where similarity < threshold intoo list unknown_raw_items
   - with known_raw_items: 
      - collect known_processed_items from the most similar items from vectorstore
   - with unknown_raw_items: 
      - use LLM to return strucutred dict {raw_item: str, processed_item: str}
      - extract list unknown_processed_items
      - update vectorstore: add unknown item names, their embeddings and unknown_processed_items to vectorstore

   - Save unknown_raw_items with corresponding unknown_processed_items as well as known_raw_items with corresponding known_processed_items to database. Provide also filename and ingestion timestamp and extraction_method (VECTORSTORE or LLM)

## Models used:
- as LLM for prompting we use Openai `gpt-5-mini`
- as LLM for embeddings we use Openai `text-embedding-3-small`
- as vectorstore we use FAISS
- credentials for these models are in .env file, use load_dotenv() to load them into environment variables

## Vectorstore structure
- FAISS vectorstore is stored in folder vectorstores
- name: 
   - invoice_vectorstore_index.bin
   - invoice_vectorstore_metadata.json
- We compute embeddings from raw items
- as metadatadata we store:
   - raw_item_name: str
   - processed_item_name: str

## Database structure
- SQLLite in folder databases
- name: 
   - invoice_database.db
- We store the final invoice data in a database:
   - FILE_NAME: str
   - INGESTION_TIME: timestamp
   - PROCESSED_ITEM_DESCRIPTION: str
   - RAW_ITEM_DESCRIPTION: str
   - EXTRACTION_METHOD: str (VECTORSTORE or LLM)

- one row is one item, but given FILE_NAME and INGESTION_TIME, obviously correspinds to several rows

## Code structure
- Make the code structure modular.
- Main functionality should be stored in folder modules
- the script which will run the streaming/invoice processing pipeline will be in folder scripts

## Custom instructions
- Ask if anything is unclear
- Web search if unsure about implementation of something
- update requirements.txt
- **IMPORTANT: see how things are done in scripts/spark_streaming_demo.py : that works, and what can be done it that way should be done in that way as far as it makes sense.**
- do not be overly verbose with the code - be oncise, to the point, but comment the code well.
- log to standard input  what is happening:
   - if we use pypdf2 or teseract for parsing
   - how many items we found
   - how many items we compare with vectorstore
   - how many and which items we "found" in vectorstore, and just take the processed_item_description from there
   - how many and which items we did not find in vectorstore, and need to ask LLM for processed_item_description
   - how many items we added to vectorstore
   - how many rows we wrote to database




        




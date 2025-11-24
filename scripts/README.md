# Spark Streaming Demo

This demo monitors a folder for text files and writes their contents to a SQLite database.

## Installation

```bash
pip install -r ../requirements.txt
```

## Usage

1. **Start the streaming application:**
   ```bash
   python spark_streaming_demo.py
   ```

2. **Add text files to the monitored folder:**
   ```bash
   echo "Hello, World!" > ../observed_data/test1.txt
   echo "Another message" > ../observed_data/test2.txt
   ```

3. **Stop the application:**
   Press `Ctrl+C` to gracefully stop the streaming application.

## How It Works

- **Monitored Folder:** `/home/igor/Git/invoice-parsing/observed_data`
- **Database:** `/home/igor/Git/invoice-parsing/databases/streaming_demo.sqlite`
- **Checkpoint:** `/home/igor/Git/invoice-parsing/observed_data/_spark_checkpoint`

The application:
1. Monitors the `observed_data` folder for new text files
2. Reads each line from the text files
3. Stores the content along with the filename and timestamp in SQLite
4. Processes files every 5 seconds

## Database Schema

The SQLite database contains a `text_files` table with the following columns:
- `id` (INTEGER PRIMARY KEY AUTOINCREMENT)
- `content` (TEXT) - The line content from the text file
- `file_name` (TEXT) - The full path of the source file
- `processed_time` (TIMESTAMP) - When the record was processed

## Querying the Database

```bash
sqlite3 ../databases/streaming_demo.sqlite "SELECT * FROM text_files;"
```

Or use Python:
```python
import sqlite3
conn = sqlite3.connect('../databases/streaming_demo.sqlite')
cursor = conn.cursor()
cursor.execute("SELECT * FROM text_files")
for row in cursor.fetchall():
    print(row)
conn.close()
```

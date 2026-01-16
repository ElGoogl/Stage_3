#!/bin/bash

# Example script to manually trigger indexing of a book
# Usage: ./index_book.sh <book_id> <file_path>

if [ $# -ne 2 ]; then
    echo "Usage: $0 <book_id> <file_path>"
    echo "Example: $0 1342 data_repository/datalake_v1/1342.json"
    exit 1
fi

BOOK_ID=$1
FILE_PATH=$2

if [ ! -f "$FILE_PATH" ]; then
    echo "Error: File not found: $FILE_PATH"
    exit 1
fi

# Create indexing event JSON
EVENT_JSON=$(cat <<EOF
{
  "bookId": "$BOOK_ID",
  "filePath": "/app/$FILE_PATH",
  "status": "READY"
}
EOF
)

echo "Publishing indexing event for book $BOOK_ID..."

# Send to ActiveMQ via REST API
curl -X POST \
  -u admin:admin \
  -H "Content-Type: application/json" \
  -d "$EVENT_JSON" \
  "http://localhost:8161/api/message/books.ingested?type=queue"

echo ""
echo "Event published successfully!"

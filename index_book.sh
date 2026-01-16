#!/bin/bash

# Example script to manually trigger indexing of a book
# Usage: ./index_book.sh <lake_path>

if [ $# -ne 1 ]; then
    echo "Usage: $0 <lake_path>"
    echo "Example: $0 datalake_node1/1342.json"
    echo "         or: $0 20260112/23/1346.json"
    exit 1
fi

LAKE_PATH=$1

# Create indexing request JSON
REQUEST_JSON=$(cat <<EOF
{
  "lakePath": "$LAKE_PATH"
}
EOF
)

echo "Sending indexing request for: $LAKE_PATH..."

# Send to indexing service REST API
RESPONSE=$(curl -X POST \
  -H "Content-Type: application/json" \
  -d "$REQUEST_JSON" \
  -w "\nHTTP_CODE:%{http_code}" \
  "http://localhost:7002/index" 2>/dev/null)

HTTP_CODE=$(echo "$RESPONSE" | grep "HTTP_CODE" | cut -d':' -f2)
BODY=$(echo "$RESPONSE" | grep -v "HTTP_CODE")

echo ""
if [ "$HTTP_CODE" == "200" ]; then
    echo "✓ Success! Book indexed."
elif [ "$HTTP_CODE" == "409" ]; then
    echo "⚠ Already indexed (conflict)."
else
    echo "✗ Error (HTTP $HTTP_CODE)"
fi
echo "Response: $BODY"

#!/bin/bash

# Prevent system sleep during upload process
# Usage: ./run_upload.sh <repo-name>

if [ $# -eq 0 ]; then
    echo "Usage: $0 <repo-name>"
    echo "Example: $0 abigailhaddad/fedscope"
    exit 1
fi

REPO_NAME=$1

echo "Starting upload process with caffeinate to prevent sleep..."
echo "Repository: $REPO_NAME"
echo "This will keep your computer awake during the entire upload process."
echo ""

# Use caffeinate to prevent system sleep
# -d: prevent display sleep
# -i: prevent system idle sleep
# -s: prevent system sleep
caffeinate -dis python export_and_upload_one_by_one.py "$REPO_NAME"

echo ""
echo "Upload process completed!"
#!/usr/bin/env python3
"""
Upload dataset_card.md as README.md to Hugging Face
"""

from huggingface_hub import HfApi
import os
import sys

def upload_readme(repo_name):
    """Upload dataset_card.md as README.md to Hugging Face."""
    
    # Check if dataset_card.md exists
    dataset_card_path = "dataset_card.md"
    if not os.path.exists(dataset_card_path):
        print(f"Error: Dataset card not found: {dataset_card_path}")
        return
    
    try:
        # Upload as README.md
        api = HfApi()
        api.upload_file(
            path_or_fileobj=dataset_card_path,
            path_in_repo="README.md",
            repo_id=repo_name,
            repo_type="dataset",
            commit_message="Update dataset card (README.md)"
        )
        
        print(f"✅ Successfully uploaded dataset card as README.md to {repo_name}")
        
    except Exception as e:
        print(f"❌ Failed to upload dataset card: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python upload_readme.py <repo_name>")
        print("Example: python upload_readme.py abigailhaddad/fedscope")
        sys.exit(1)
    
    repo_name = sys.argv[1]
    upload_readme(repo_name)
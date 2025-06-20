#!/usr/bin/env python3
"""
Upload dataset_card.md as README.md to Hugging Face
"""

from huggingface_hub import HfApi
import os
import sys

def upload_readme(repo_name):
    """Upload README.md to Hugging Face."""
    
    # Check if README.md exists
    readme_path = "README.md"
    if not os.path.exists(readme_path):
        print(f"Error: README file not found: {readme_path}")
        return
    
    try:
        # Upload as README.md
        api = HfApi()
        api.upload_file(
            path_or_fileobj=readme_path,
            path_in_repo="README.md",
            repo_id=repo_name,
            repo_type="dataset",
            commit_message="Update README.md"
        )
        
        print(f"✅ Successfully uploaded README.md to {repo_name}")
        
    except Exception as e:
        print(f"❌ Failed to upload README: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python upload_readme.py <repo_name>")
        print("Example: python upload_readme.py abigailhaddad/fedscope")
        sys.exit(1)
    
    repo_name = sys.argv[1]
    upload_readme(repo_name)
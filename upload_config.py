#!/usr/bin/env python3
"""
Upload .huggingface.yml configuration to the dataset repository
"""

from huggingface_hub import HfApi
import os
import sys

def upload_config(repo_name):
    """Upload .huggingface.yml to Hugging Face."""
    
    config_path = ".huggingface.yml"
    if not os.path.exists(config_path):
        print(f"Error: Configuration file not found: {config_path}")
        return
    
    try:
        api = HfApi()
        api.upload_file(
            path_or_fileobj=config_path,
            path_in_repo=".huggingface.yml",
            repo_id=repo_name,
            repo_type="dataset",
            commit_message="Add dataset configuration to specify column types"
        )
        
        print(f"✅ Successfully uploaded .huggingface.yml to {repo_name}")
        print("This should help with the dataset preview parsing issues.")
        
    except Exception as e:
        print(f"❌ Failed to upload configuration: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python upload_config.py <repo_name>")
        print("Example: python upload_config.py abigailhaddad/fedscope")
        sys.exit(1)
    
    repo_name = sys.argv[1]
    upload_config(repo_name)
#!/usr/bin/env python3
"""
Collect all PDF documentation files from extracted data directories.
"""

import os
import shutil
import glob
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def collect_pdfs():
    """Copy all PDF files to documentation_pdfs directory."""
    source_dir = "fedscope_data/extracted"
    target_dir = "documentation_pdfs"
    
    # Find all PDFs
    pdf_pattern = os.path.join(source_dir, "*", "*.pdf")
    pdf_files = glob.glob(pdf_pattern)
    
    logger.info(f"Found {len(pdf_files)} PDF files")
    
    # Copy each PDF with a standardized name
    for pdf_path in sorted(pdf_files):
        # Extract quarter and year from path
        parent_dir = os.path.basename(os.path.dirname(pdf_path))
        parts = parent_dir.split('_')
        if len(parts) >= 4:
            quarter = parts[2]
            year = parts[3]
            new_name = f"FedScope_Employment_{quarter}_{year}_Documentation.pdf"
            
            target_path = os.path.join(target_dir, new_name)
            
            logger.info(f"Copying {os.path.basename(pdf_path)} → {new_name}")
            shutil.copy2(pdf_path, target_path)
    
    # Check final size
    total_size = sum(os.path.getsize(os.path.join(target_dir, f)) 
                    for f in os.listdir(target_dir) if f.endswith('.pdf'))
    
    logger.info(f"\n✅ Copied {len(pdf_files)} PDF files")
    logger.info(f"Total size: {total_size / (1024*1024):.1f} MB")
    logger.info(f"PDFs saved to: {target_dir}/")

if __name__ == "__main__":
    collect_pdfs()
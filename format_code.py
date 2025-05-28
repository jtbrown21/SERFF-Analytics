#!/usr/bin/env python3
"""Code formatter for AI agents"""
import subprocess
import sys
import os

def format_python_files():
    """Format all Python files with black"""
    print("Formatting Python files...")
    
    # Find all Python files
    py_files = []
    for root, dirs, files in os.walk("src"):
        for file in files:
            if file.endswith(".py"):
                py_files.append(os.path.join(root, file))
    
    if py_files:
        result = subprocess.run([
            sys.executable, "-m", "black", "--line-length", "100"
        ] + py_files, capture_output=True, text=True)
        print(f"Formatted {len(py_files)} files")
        return result.returncode == 0
    return True

if __name__ == "__main__":
    success = format_python_files()
    sys.exit(0 if success else 1)

#!/usr/bin/env python3
"""Code formatter for AI agents"""
import subprocess
import sys
from pathlib import Path

def format_python_files():
    """Format all Python files with black"""
    print("Formatting Python files...")
    
    # Find all Python files under relevant directories
    base_dirs = ["serff_analytics", "src", "scripts"]
    py_files = []
    for base in base_dirs:
        path = Path(base)
        if path.is_dir():
            py_files.extend(str(p) for p in path.rglob("*.py"))
    
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

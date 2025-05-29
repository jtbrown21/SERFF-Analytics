#!/bin/bash
# Simple setup script for development environment
set -e

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi

source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

echo "Environment ready."

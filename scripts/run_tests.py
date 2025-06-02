#!/usr/bin/env python3
"""Test runner for AI agents to validate changes"""
import subprocess
import sys

def run_tests():
    """Run all tests and return results"""
    print("Running tests...")
    result = subprocess.run([
        sys.executable, "-m", "pytest", "-v", "--tb=short"
    ], capture_output=True, text=True)
    
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    
    return result.returncode == 0

if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)

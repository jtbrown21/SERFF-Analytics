#!/usr/bin/env python3
"""Embed CSS file contents into the agent_report_v2 template."""
import sys
import re

PY_FILE = 'src/agent_report_v2.py'
CSS_FILE = 'dev/style.css'

if len(sys.argv) > 1:
    CSS_FILE = sys.argv[1]
if len(sys.argv) > 2:
    PY_FILE = sys.argv[2]

with open(CSS_FILE) as f:
    css = f.read().strip()

with open(PY_FILE) as f:
    data = f.read()

pattern = re.compile(r'(<style>)(.*?)(</style>)', re.DOTALL)

def repl(match):
    start, _, end = match.groups()
    # Indent CSS with 8 spaces to match original formatting
    indented_css = '\n'.join('        ' + line.rstrip() for line in css.splitlines())
    return f"{start}\n{indented_css}\n        {end}"

new_data, count = pattern.subn(repl, data, count=1)

if count == 0:
    sys.exit('Could not find <style> block in template')

with open(PY_FILE, 'w') as f:
    f.write(new_data)

print(f'Embedded CSS from {CSS_FILE} into {PY_FILE}')

#!/usr/bin/env python3
"""Create a complete flattened package including anonymisation module."""

import re

# Read the three anonymisation files
with open('odw/core/anonymisation/base.py', 'r') as f:
    base_content = f.read()

with open('odw/core/anonymisation/config.py', 'r') as f:
    config_content = f.read()

with open('odw/core/anonymisation/engine.py', 'r') as f:
    engine_content = f.read()

# Remove relative imports from engine.py (lines 14-18)
# Replace the import block with comments
engine_content = re.sub(
    r'from \.base import \(\s+BaseStrategy,\s+default_strategies,\s+\)',
    '# REMOVED relative import: from .base import (BaseStrategy, default_strategies)',
    engine_content
)
engine_content = re.sub(
    r'from \.config import AnonymisationConfig',
    '# REMOVED relative import: from .config import AnonymisationConfig',
    engine_content
)

# Combine in correct order: base -> config -> engine
combined = "### ANONYMISATION MODULE START ###\n\n"
combined += "### Module: odw.core.anonymisation.base ###\n\n"
combined += base_content + "\n\n"
combined += "### Module: odw.core.anonymisation.config ###\n\n"
combined += config_content + "\n\n"
combined += "### Module: odw.core.anonymisation.engine ###\n\n"
combined += engine_content + "\n\n"
combined += "### ANONYMISATION MODULE END ###\n\n"

# Read the existing flattened output
with open('odw/flatten_package_into_notebook__output.py', 'r') as f:
    existing_content = f.read()

# Combine and write
final_content = combined + existing_content

with open('odw/flatten_package_with_anonymisation.py', 'w') as f:
    f.write(final_content)

print("✅ Created odw/flatten_package_with_anonymisation.py")
print(f"   Total size: {len(final_content)} characters")

#!/usr/bin/env python3
"""
Check and validate environment variables for Purview integration tests.

This script helps detect which environment variables are configured and
validates their values for running integration tests.
"""

import os
import sys
from typing import Dict, List, Tuple


def check_env_vars() -> Tuple[Dict[str, str], List[str], List[str]]:
    """
    Check environment variables for Purview integration tests.
    
    Returns:
        Tuple of (configured_vars, missing_required, missing_optional)
    """
    # Required variables
    required = {
        "ODW_TENANT_ID": "Azure AD tenant ID",
        "ODW_CLIENT_ID": "Service principal client ID",
    }
    
    # Optional variables with defaults or alternative auth methods
    optional = {
        "ODW_CLIENT_SECRET": "Service principal secret (or 'AZURE_IDENTITY' for DefaultAzureCredential)",
        "ODW_PURVIEW_NAME": "Purview account name (default: pins-pview)",
        "ODW_STORAGE_ACCOUNT_DFS_HOST": "Storage account DFS hostname",
        "PURVIEW_TEST_ASSET_QUALIFIED_NAME": "Custom test asset qualified name",
        "PURVIEW_TEST_ASSET_TYPE": "Asset type name (default: azure_datalake_gen2_resource_set)",
    }
    
    configured = {}
    missing_required = []
    missing_optional = []
    
    # Check required variables
    for var, desc in required.items():
        value = os.getenv(var)
        if value:
            configured[var] = value
        else:
            missing_required.append(f"{var}: {desc}")
    
    # Check optional variables
    for var, desc in optional.items():
        value = os.getenv(var)
        if value:
            configured[var] = value
        else:
            missing_optional.append(f"{var}: {desc}")
    
    return configured, missing_required, missing_optional


def print_status():
    """Print environment variable status with color coding."""
    configured, missing_required, missing_optional = check_env_vars()
    
    print("=" * 70)
    print("Purview Integration Test Environment Check")
    print("=" * 70)
    print()
    
    # Configured variables
    if configured:
        print("✅ CONFIGURED VARIABLES:")
        print("-" * 70)
        for var, value in sorted(configured.items()):
            # Mask secrets
            if "SECRET" in var or "PASSWORD" in var:
                display_value = "***" + value[-4:] if len(value) > 4 else "***"
            else:
                display_value = value
            print(f"  {var:40} = {display_value}")
        print()
    
    # Missing required variables
    if missing_required:
        print("❌ MISSING REQUIRED VARIABLES:")
        print("-" * 70)
        for item in missing_required:
            print(f"  {item}")
        print()
    
    # Missing optional variables
    if missing_optional:
        print("⚠️  MISSING OPTIONAL VARIABLES (using defaults or skipping tests):")
        print("-" * 70)
        for item in missing_optional:
            print(f"  {item}")
        print()
    
    # Status summary
    print("=" * 70)
    if not missing_required:
        print("✅ STATUS: Ready to run integration tests!")
        print()
        print("Run tests with:")
        print("  pytest odw/test/integration_test/anonymisation/ -v")
        return 0
    else:
        print("❌ STATUS: Missing required environment variables")
        print()
        print("To set variables in fish shell:")
        print()
        for item in missing_required:
            var_name = item.split(":")[0]
            print(f"  set -gx {var_name} \"your-value-here\"")
        print()
        print("Or export them (works in current session only):")
        print()
        for item in missing_required:
            var_name = item.split(":")[0]
            print(f"  export {var_name}=\"your-value-here\"")
        print()
        return 1


def print_fish_setup():
    """Print fish shell commands to set up environment."""
    print("\n# Fish shell setup commands:")
    print("# Copy and paste these into your terminal, replacing with actual values")
    print()
    print("# Required")
    print('set -gx ODW_TENANT_ID "5878df98-6f88-48ab-9322-998ce557088d"')
    print('set -gx ODW_CLIENT_ID "5750ab9b-597c-4b0d-b0f0-f4ef94e91fc0"')
    print()
    print("# Optional (recommended)")
    print('set -gx ODW_CLIENT_SECRET "AZURE_IDENTITY"  # Or your actual secret')
    print('set -gx ODW_PURVIEW_NAME "pins-pview"')
    print('set -gx ODW_STORAGE_ACCOUNT_DFS_HOST "pinsstodwdevuks9h80mb.dfs.core.windows.net"')
    print()
    print("# To persist across sessions, add to ~/.config/fish/config.fish")


if __name__ == "__main__":
    exit_code = print_status()
    
    if exit_code != 0 and "--fish-setup" in sys.argv:
        print_fish_setup()
    
    sys.exit(exit_code)

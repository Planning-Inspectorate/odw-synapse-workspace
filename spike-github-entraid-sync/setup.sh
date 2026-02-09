#!/bin/bash

# GitHub Teams + Entra ID Sync - Spike Test Setup Script

set -e

echo "=========================================="
echo "GitHub + Entra ID Sync Spike Test Setup"
echo "=========================================="
echo ""

# Check prerequisites
echo "Checking prerequisites..."
echo ""

# Check Azure CLI
if ! command -v az &> /dev/null; then
    echo "❌ Azure CLI not found. Please install: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli"
    exit 1
fi
echo "✅ Azure CLI found"

# Check Terraform
if ! command -v terraform &> /dev/null; then
    echo "❌ Terraform not found. Please install: https://www.terraform.io/downloads"
    exit 1
fi
echo "✅ Terraform found ($(terraform version | head -n1))"

# Check Azure login
if ! az account show &> /dev/null; then
    echo "❌ Not logged into Azure. Please run: az login"
    exit 1
fi
echo "✅ Logged into Azure as: $(az account show --query user.name -o tsv)"

# Check GitHub token
if [ -z "$GITHUB_TOKEN" ]; then
    echo "❌ GITHUB_TOKEN environment variable not set"
    echo ""
    echo "Please create a GitHub Personal Access Token with these scopes:"
    echo "  - admin:org (full control of organizations)"
    echo "  - repo (full control of private repositories)"
    echo ""
    echo "Create token at: https://github.com/settings/tokens"
    echo ""
    echo "Then set it as an environment variable:"
    echo "  export GITHUB_TOKEN='your_token_here'"
    echo ""
    exit 1
fi
echo "✅ GITHUB_TOKEN is set"

echo ""
echo "All prerequisites met!"
echo ""

# Navigate to spike directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "Initializing Terraform..."
terraform init

echo ""
echo "=========================================="
echo "Setup complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Review the plan: terraform plan"
echo "  2. Apply changes:    terraform apply"
echo "  3. Verify in Azure Portal and GitHub"
echo "  4. When done:        terraform destroy"
echo ""
echo "See README.md for detailed instructions."
echo ""

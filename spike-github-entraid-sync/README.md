# GitHub Teams + Entra ID Sync - Spike Test

This spike tests the approach of syncing GitHub teams with Azure Entra ID security groups for managing repository access.

## Overview

This spike demonstrates:
- Creating an Entra ID security group (`ODW-GitHub-Admins-Spike`)
- Creating a corresponding GitHub team (`odw-admins-spike`)
- Adding your account (`stef-solirius`) to both
- Granting admin access to the `odw-synapse-workspace` repository via the team

## Prerequisites

1. **Azure CLI** - Already authenticated with sufficient permissions to create Entra ID groups
   ```fish
   az login
   az account show
   ```

2. **GitHub Personal Access Token** - With the following scopes:
   - `admin:org` (full control of organizations)
   - `repo` (full control of private repositories)
   
   Create token at: https://github.com/settings/tokens
   
   Export as environment variable:
   ```fish
   set -x GITHUB_TOKEN "your_token_here"
   ```

3. **Terraform** - Version >= 1.0
   ```fish
   terraform version
   ```

## Running the Spike

1. **Navigate to spike directory**
   ```fish
   cd spike-github-entraid-sync
   ```

2. **Initialize Terraform**
   ```fish
   terraform init
   ```

3. **Review the plan**
   ```fish
   terraform plan
   ```
   
   This will show:
   - Entra ID group to be created
   - GitHub team to be created
   - Team membership for stef-solirius
   - Repository access grant

4. **Apply the configuration**
   ```fish
   terraform apply
   ```
   
   Type `yes` when prompted.

5. **Verify the resources**
   
   **In Azure Portal:**
   - Navigate to Entra ID > Groups
   - Search for "ODW-GitHub-Admins-Spike"
   - Verify you're listed as a member
   
   **In GitHub:**
   - Go to https://github.com/orgs/Planning-Inspectorate/teams/odw-admins-spike
   - Verify the team exists
   - Check that you're a member
   - Verify the team has admin access to odw-synapse-workspace

## Testing Access

1. **Test repository access**
   - Try to access repository settings for `odw-synapse-workspace`
   - You should have admin permissions via the team membership

2. **Test CODEOWNERS integration** (future)
   - Once this approach is approved, the CODEOWNERS file can be updated to reference the team:
     ```
     /.github/CODEOWNERS    @Planning-Inspectorate/odw-admins-spike
     ```

## Automatic Sync Setup (Future Step)

For true automatic provisioning/deprovisioning, you'll need:

1. **GitHub Enterprise Cloud** with SAML SSO configured
2. **Entra ID Application** for GitHub SSO
3. **Group-based assignment** in the Enterprise Application

With this setup:
- Users added to the Entra ID group automatically get added to the GitHub team
- Users removed from the Entra ID group automatically get removed from the GitHub team
- No manual Terraform updates needed for user management

## Current Limitations in Spike

- User membership is managed via Terraform (not automatic sync)
- This is intentional for the spike to keep it simple
- Full automation requires GitHub Enterprise + SAML SSO setup

## Cleanup

When done testing:

```fish
terraform destroy
```

This will remove:
- GitHub team and team memberships
- Entra ID group
- Repository access grants

## Next Steps

If this spike is successful:

1. Set up GitHub Enterprise SAML SSO with Entra ID
2. Create the production Entra ID groups (ODW-Admins, ODW-Developers, etc.)
3. Configure automatic group sync
4. Update CODEOWNERS to reference teams instead of individual users
5. Document the process for adding/removing users

## Notes

- The spike creates resources with `-Spike` suffix to avoid conflicts
- Your account (`stef-solirius`) is used as the test subject
- All resources are tagged/named clearly for easy identification

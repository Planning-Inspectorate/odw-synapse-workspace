# Quick Start Guide

This guide will walk you through running the spike test in under 10 minutes.

## Prerequisites Checklist

- [ ] Azure CLI installed and logged in
- [ ] Terraform installed (v1.0+)
- [ ] GitHub Personal Access Token with `admin:org` and `repo` scopes
- [ ] Member of Planning-Inspectorate GitHub organization

## Step-by-Step Instructions

### 1. Create GitHub Personal Access Token

1. Go to https://github.com/settings/tokens/new
2. Name: `ODW-Spike-Test`
3. Expiration: 7 days (enough for spike test)
4. Select scopes:
   - `admin:org` - Full control of orgs and teams
   - `repo` - Full control of private repositories
5. Click "Generate token"
6. Copy the token (you won't see it again!)

### 2. Set Environment Variable

Fish shell:
```fish
set -x GITHUB_TOKEN "ghp_your_token_here"
```

Bash/Zsh:
```bash
export GITHUB_TOKEN="ghp_your_token_here"
```

### 3. Run Setup Script

```fish
cd spike-github-entraid-sync
./setup.sh
```

This will:
- Check all prerequisites
- Verify Azure login
- Verify GitHub token
- Initialize Terraform

### 4. Review What Will Be Created

```fish
terraform plan
```

Expected output:
```
Plan: 5 to add, 0 to change, 0 to destroy.

Changes:
  + azuread_group.odw_github_admins_spike
  + github_team.odw_admins_spike
  + github_team_membership.spike_test_member
  + github_team_repository.odw_synapse_workspace_spike
```

### 5. Apply Changes

```fish
terraform apply
```

Type `yes` when prompted.

Wait 30-60 seconds for resources to be created.

### 6. Verify in Azure Portal

1. Go to https://portal.azure.com
2. Navigate to: Entra ID > Groups
3. Search for: `ODW-GitHub-Admins-Spike`
4. Verify you're listed as a member

### 7. Verify in GitHub

1. Go to: https://github.com/orgs/Planning-Inspectorate/teams/odw-admins-spike
2. Verify:
   - Team exists
   - You're a member
   - Team has access to `odw-synapse-workspace`

### 8. Test Access

1. Go to: https://github.com/Planning-Inspectorate/odw-synapse-workspace/settings
2. Verify you can access repository settings
3. Check: Settings > Collaborators and teams
4. You should see `odw-admins-spike` with Admin access

### 9. Test CODEOWNERS (Optional)

1. Create a test branch
2. Update `.github/CODEOWNERS`:
   ```
   # Test line for spike
   /spike-github-entraid-sync/**  @Planning-Inspectorate/odw-admins-spike
   ```
3. Create a PR
4. Verify the team is requested for review

### 10. Cleanup

When finished testing:

```fish
terraform destroy
```

Type `yes` to confirm.

This removes:
- GitHub team
- Team memberships
- Repository access grants
- Entra ID group

## Troubleshooting

### "Error: token does not have required scopes"
- Your GitHub token needs `admin:org` and `repo` scopes
- Create a new token with correct scopes

### "Error: insufficient privileges to manage groups"
- You need permissions to create Entra ID groups
- Contact your Azure admin to grant permissions
- Alternatively, ask an admin to run the spike test

### "Error: user not found"
- The email in `variables.tf` might be wrong
- Update it to match your Entra ID email

### Team not showing in GitHub
- Wait a few minutes after `terraform apply`
- Refresh the GitHub teams page
- Check the Terraform output for any errors

### Access not working
- Log out and log back into GitHub
- Clear browser cache
- Verify team membership in GitHub UI

## What This Proves

After completing this spike, you will have validated:

✅ Entra ID groups can be created programmatically  
✅ GitHub teams can be created via Terraform  
✅ Team membership can be managed  
✅ Repository access can be granted via teams  
✅ CODEOWNERS can reference teams  
✅ The approach is technically viable  

## Next Steps After Spike

If successful:
1. Present findings to the team
2. Plan GitHub Enterprise SAML SSO setup
3. Design production group structure
4. Create migration plan from individual access to team-based
5. Update documentation and runbooks

## Questions?

See the full README.md and COMPARISON.md for more details.

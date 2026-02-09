# Expected Terraform Outputs

This document shows what you should expect to see when running the spike test.

## After `terraform plan`

```hcl
Terraform used the selected providers to generate the following execution plan.
Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # azuread_group.odw_github_admins_spike will be created
  + resource "azuread_group" "odw_github_admins_spike" {
      + display_name       = "ODW-GitHub-Admins-Spike"
      + description        = "Spike test: Repository administrators with full access for ODW repositories"
      + id                 = (known after apply)
      + members            = [
          + "<your-object-id>",
        ]
      + object_id          = (known after apply)
      + security_enabled   = true
    }

  # github_team.odw_admins_spike will be created
  + resource "github_team" "odw_admins_spike" {
      + description = "Spike test: Repository administrators with full access for ODW repositories"
      + id          = (known after apply)
      + name        = "odw-admins-spike"
      + node_id     = (known after apply)
      + privacy     = "closed"
      + slug        = (known after apply)
    }

  # github_team_membership.spike_test_member will be created
  + resource "github_team_membership" "spike_test_member" {
      + id       = (known after apply)
      + role     = "member"
      + team_id  = (known after apply)
      + username = "stef-solirius"
    }

  # github_team_repository.odw_synapse_workspace_spike will be created
  + resource "github_team_repository" "odw_synapse_workspace_spike" {
      + id         = (known after apply)
      + permission = "admin"
      + repository = "odw-synapse-workspace"
      + team_id    = (known after apply)
    }

Plan: 4 to add, 0 to change, 0 to destroy.

Changes to Outputs:
  + entraid_group_id   = (known after apply)
  + entraid_group_name = "ODW-GitHub-Admins-Spike"
  + github_team_id     = (known after apply)
  + github_team_name   = "odw-admins-spike"
```

## After `terraform apply`

```
Apply complete! Resources: 4 added, 0 changed, 0 destroyed.

Outputs:

entraid_group_id = "12345678-1234-1234-1234-123456789abc"
entraid_group_name = "ODW-GitHub-Admins-Spike"
github_team_id = "7654321"
github_team_name = "odw-admins-spike"
```

## What Each Resource Does

### 1. `azuread_group.odw_github_admins_spike`
**Purpose**: Creates the Entra ID security group  
**What it contains**:
- Display name: `ODW-GitHub-Admins-Spike`
- Your user account as a member
- Security enabled for access control

**Where to verify**: Azure Portal > Entra ID > Groups

### 2. `github_team.odw_admins_spike`
**Purpose**: Creates the GitHub team  
**What it contains**:
- Team name: `odw-admins-spike`
- Privacy: `closed` (only org members can see it)
- Description linking it to the spike test

**Where to verify**: GitHub > Organization > Teams

### 3. `github_team_membership.spike_test_member`
**Purpose**: Adds you to the GitHub team  
**What it does**:
- Links your GitHub username (`stef-solirius`) to the team
- Role: `member` (could also be `maintainer`)

**Where to verify**: GitHub > Teams > odw-admins-spike > Members

### 4. `github_team_repository.odw_synapse_workspace_spike`
**Purpose**: Grants team access to repository  
**What it does**:
- Links the team to `odw-synapse-workspace` repository
- Permission level: `admin` (full control)

**Where to verify**: GitHub > Repository > Settings > Collaborators and teams

## Common Errors and Solutions

### Error: "Insufficient privileges"

```
Error: creating Group: graphrbac.GroupsClient#Create: Failure responding to request: 
StatusCode=403 -- Original Error: autorest/azure: Service returned an error. 
Status=403 Code="Forbidden" Message="Insufficient privileges to complete the operation."
```

**Solution**: Your Azure account needs permissions to create Entra ID groups. Contact your Azure admin.

### Error: "Token does not have required scopes"

```
Error: GET https://api.github.com/orgs/Planning-Inspectorate/teams: 404 Not Found []
```

**Solution**: 
1. Ensure your `GITHUB_TOKEN` has `admin:org` scope
2. Verify you're a member of the `Planning-Inspectorate` organization
3. Create a new token with correct permissions

### Error: "User not found"

```
Error: reading User Principal "stefania.deligia@planninginspectorate.gov.uk": 
UserObjectIDNotFound: Could not find user with principal name 
```

**Solution**: Update the email in `variables.tf` to match your exact Entra ID UPN

### Error: "Team already exists"

```
Error: POST https://api.github.com/orgs/Planning-Inspectorate/teams: 422 
Validation Failed [{Resource:Team Field:name Code:already_exists}]
```

**Solution**: 
1. The team name is already taken
2. Either delete the existing team manually
3. Or change the team name in `main.tf`

## After `terraform destroy`

```
Destroy complete! Resources: 4 destroyed.
```

All resources will be removed:
- ✅ GitHub team deleted
- ✅ Team memberships removed
- ✅ Repository access revoked
- ✅ Entra ID group deleted

Your personal access and permissions remain unchanged.

## Verification Checklist

After running `terraform apply`, verify each of these:

- [ ] Terraform shows 4 resources created
- [ ] Azure Portal shows the Entra ID group with you as a member
- [ ] GitHub shows the team exists
- [ ] GitHub shows you as a team member
- [ ] GitHub shows team has admin access to repository
- [ ] You can access repository settings in GitHub
- [ ] CODEOWNERS can reference the team (optional test)

If all checked, the spike test is successful!

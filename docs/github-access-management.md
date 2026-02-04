# GitHub Access Management - ODW Team

## Overview
This document outlines the centralized access management approach for ODW repositories using GitHub teams integrated with Azure Entra ID groups.

## Current State
**Access Method**: Individual users listed in CODEOWNERS and repository collaborators
**Problem**: Manual updates required when team members join/leave, error-prone, difficult to scale

## Proposed Solution
**Access Method**: GitHub teams synced with Azure Entra ID security groups
**Benefits**: 
- Single source of truth in Entra ID
- Automatic provisioning/deprovisioning
- Consistent with PINS identity management
- Reduced manual effort

## Team Structure

### ODW-Admins
**Purpose**: Repository administrators with full access
**Permissions**: Admin role on all ODW repositories
**Responsibilities**: 
- Manage repository settings
- Configure workflows and branch protections
- Approve critical changes

**Proposed Members** (from current CODEOWNERS):
- @beejjacobs
- @m-juckes-pins
- @stef-solirius
- @raamvar

**Entra ID Group**: `ODW-GitHub-Admins` (to be created)
**Proposed Members from Entra ID**: Users from `pins-odw-prod-administrators` or manually selected

### ODW-Workflow-Approvers
**Purpose**: Senior engineers who can approve changes to GitHub Actions workflows
**Permissions**: Write role on all ODW repositories with CODEOWNERS protection for .github/workflows
**Responsibilities**:
- Develop and maintain data pipelines
- Write Synapse notebooks and scripts
- Review and approve PRs
- Review and approve workflow changes

**Proposed Members** (from current CODEOWNERS):
- @roytalari
- @HarrisonBoyleThomas
- @Fred83200
- @KranthiRayipudi
- @RohitShuklaPINS
- @harriet-stuart-wd
- @prathapA100
- @DanBrown2025
- @KalyaniNik
- @stef-solirius
- @raamvar

**Entra ID Group**: `ODW-GitHub-Workflow-Approvers` (to be created)
**Proposed Members**: All users currently listed in CODEOWNERS

### ODW-Data-Engineers
**Purpose**: Rest of engineering team with write access
**Permissions**: Write role on all ODW repositories
**Responsibilities**:
- Develop and maintain data pipelines
- Write Synapse notebooks and scripts
- Review and approve PRs

**Proposed Members**: Rest of ODW engineering team (not listed in CODEOWNERS currently)

**Entra ID Group**: `pins-odw-engineers` (existing group - reused)
**Group ID**: 13900d77-c9e6-40e1-9d6e-740cd6c2218c
**Description**: "ODW (Operational Data Warehouse) Engineers"
**Current Members**: ~12-15 engineers (most of the team is already in this group)

## Implementation Prerequisites

### GitHub Requirements
- [ ] Verify Planning-Inspectorate organization is on GitHub Enterprise Cloud
- [ ] Check if SAML SSO with Entra ID is already configured
- [ ] Obtain GitHub organization admin access

### Azure Entra ID Requirements
- [ ] Identify or create security groups for each team
- [ ] Verify all team members have Entra ID accounts
- [ ] Obtain Entra ID admin access (Global Admin or Application Administrator)

### Approvals Required
- [ ] Team structure approval from Rams
- [ ] PINS IT/Security approval for SSO configuration (if not already set up)
- [ ] Support ticket to GitHub administrators

## Entra ID Integration Setup

### Step 1: Configure SAML SSO (if not already configured)
1. Navigate to Azure Portal > Enterprise Applications
2. Create new application: GitHub Enterprise Cloud Organization
3. Configure SAML-based SSO following [Microsoft documentation](https://learn.microsoft.com/en-us/entra/identity/saas-apps/github-tutorial)
4. Test SSO with pilot users

### Step 2: Create/Identify Entra ID Security Groups
1. Navigate to Azure Portal > Azure Active Directory > Groups
2. **Reuse existing group**:
   - `pins-odw-engineers` (ID: 13900d77-c9e6-40e1-9d6e-740cd6c2218c) for ODW-Data-Engineers team
   - This group already exists with ~12-15 members
3. **Create 2 new small security groups**:
   - `ODW-GitHub-Admins` (4 members: beejjacobs, m-juckes-pins, stef-solirius, raamvar)
   - `ODW-GitHub-Workflow-Approvers` (12 members: all current CODEOWNERS members)
4. Add appropriate members to the new groups

### Step 3: Link GitHub Teams to Entra ID Groups
1. Navigate to GitHub Organization Settings > Teams
2. Create GitHub teams if they don't exist
3. Enable team synchronization
4. Link each GitHub team to corresponding Entra ID group
5. Verify sync is working

### Step 4: Configure Repository Access
1. For each ODW repository:
   - Add teams with appropriate permissions
   - Update CODEOWNERS file to reference teams
   - Remove individual collaborator access

## CODEOWNERS Migration

### Current (odw-synapse-workspace)
```
/.github/CODEOWNERS        @beejjacobs @m-juckes-pins @stef-solirius
/.github/workflows         @beejjacobs @m-juckes-pins @roytalari @stef-solirius @HarrisonBoyleThomas @Fred83200 @KranthiRayipudi @RohitShuklaPINS @harriet-stuart-wd @prathapA100 @DanBrown2025 @raamvar @KalyaniNik
/workspace/**              @roytalari @stef-solirius @HarrisonBoyleThomas @Fred83200 @KranthiRayipudi @RohitShuklaPINS @harriet-stuart-wd @prathapA100 @DanBrown2025 @raamvar @KalyaniNik
/pipelines/**              @m-juckes-pins @roytalari @stef-solirius @HarrisonBoyleThomas @Fred83200 @KranthiRayipudi @RohitShuklaPINS @harriet-stuart-wd @prathapA100 @DanBrown2025 @raamvar @KalyaniNik
/odw/**                    @roytalari @stef-solirius @HarrisonBoyleThomas @Fred83200 @KranthiRayipudi @RohitShuklaPINS @harriet-stuart-wd @prathapA100 @DanBrown2025 @raamvar @KalyaniNik
*.py                       @roytalari @stef-solirius @HarrisonBoyleThomas @Fred83200 @KranthiRayipudi @RohitShuklaPINS @harriet-stuart-wd @prathapA100 @DanBrown2025 @raamvar @KalyaniNik
*.ipynb                    @roytalari @stef-solirius @HarrisonBoyleThomas @Fred83200 @KranthiRayipudi @RohitShuklaPINS @harriet-stuart-wd @prathapA100 @DanBrown2025 @raamvar @KalyaniNik
*.sql                      @roytalari @stef-solirius @HarrisonBoyleThomas @Fred83200 @KranthiRayipudi @RohitShuklaPINS @harriet-stuart-wd @prathapA100 @DanBrown2025 @raamvar @KalyaniNik
```

### Proposed (using teams)
```
/.github/CODEOWNERS        @Planning-Inspectorate/ODW-Admins
/.github/workflows         @Planning-Inspectorate/ODW-Workflow-Approvers
/workspace/**              @Planning-Inspectorate/ODW-Workflow-Approvers
/pipelines/**              @Planning-Inspectorate/ODW-Workflow-Approvers
/odw/**                    @Planning-Inspectorate/ODW-Workflow-Approvers
*.py                       @Planning-Inspectorate/ODW-Workflow-Approvers
*.ipynb                    @Planning-Inspectorate/ODW-Workflow-Approvers
*.sql                      @Planning-Inspectorate/ODW-Workflow-Approvers
```

## Access Management Processes

### Onboarding a New Team Member
**With Entra ID Integration:**
1. Add user to appropriate Entra ID security group(s)
2. User is automatically synced to GitHub team(s)
3. User gains access to all repositories assigned to that team
4. **Estimated Time**: < 5 minutes + sync time (usually instant)

**Without Entra ID Integration:**
1. Navigate to GitHub Organization > Teams
2. Select appropriate team(s)
3. Add user as member
4. **Estimated Time**: < 5 minutes

### Offboarding a Team Member
**With Entra ID Integration:**
1. Remove user from Entra ID security group(s)
2. User is automatically removed from GitHub team(s)
3. User loses access to all repositories
4. **Estimated Time**: < 5 minutes + sync time

**Without Entra ID Integration:**
1. Navigate to GitHub Organization > Teams
2. Remove user from all teams
3. Check for any direct repository access and remove
4. **Estimated Time**: 10-15 minutes

### Requesting Access
1. Submit request to team lead or manager
2. Team lead approves and adds user to appropriate Entra ID group (if integrated) or GitHub team
3. User receives notification of access

### Revoking Access
1. Team lead removes user from Entra ID group or GitHub team
2. Verify user no longer has repository access
3. Document reason for revocation (optional)

## Troubleshooting

### Team Sync Not Working
1. Check Azure Portal > Enterprise Applications > GitHub > Provisioning logs
2. Check GitHub Organization > Teams > Team Sync status
3. Manually trigger sync if available
4. As fallback, manually add/remove users in GitHub

### User Can't Access Repository
1. Verify user is member of correct Entra ID group (if using integration)
2. Check GitHub team membership
3. Verify team has correct permissions on repository
4. Check if user needs to accept organization invitation

### CODEOWNERS Approval Not Required
1. Verify branch protection rules include "Require review from Code Owners"
2. Check CODEOWNERS file syntax (teams should use @org/team-name format)
3. Verify team has at least 2-3 members who can approve

## Rollback Plan
If issues arise during implementation:
1. Keep original CODEOWNERS file backed up
2. Restore individual user access if needed
3. Keep teams configured for future migration
4. Document what went wrong and adjust plan

## Resources
- [Microsoft Entra ID GitHub Integration](https://learn.microsoft.com/en-us/entra/identity/saas-apps/github-tutorial)
- [GitHub Teams Documentation](https://docs.github.com/en/organizations/organizing-members-into-teams)
- [GitHub CODEOWNERS Documentation](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-code-owners)
- [GitHub Team Synchronization](https://docs.github.com/en/enterprise-cloud@latest/organizations/managing-saml-single-sign-on-for-your-organization/managing-team-synchronization-for-your-organization)

## Contact
For questions or issues:
- **Team Lead**: Rams
- **GitHub Admins**: Contact PINS IT support
- **Entra ID Admins**: Contact PINS IT support

## Status
- **Plan Created**: 2026-02-04
- **Current Phase**: Phase 1 - Audit and Planning
- **Next Milestone**: Prerequisites verification and approval from Rams

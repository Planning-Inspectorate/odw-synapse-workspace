# GitHub Teams Proposal - ODW Access Management

## Executive Summary
We propose moving from individual user-based access control to team-based access control for ODW GitHub repositories, integrated with Azure Entra ID groups. This will:
- Reduce manual effort for onboarding/offboarding (< 5 minutes instead of 15+ minutes)
- Ensure consistency across all ODW repositories
- Leverage existing PINS identity management via Entra ID
- Provide automatic provisioning/deprovisioning

## Proposed Team Structure

| Team Name | Entra ID Group | Purpose | Permissions | Member Count |
|-----------|----------------|---------|-------------|--------------|
| **ODW-Admins** | `ODW-GitHub-Admins` | Repository administrators | Admin role on all ODW repos | 4 |
| **ODW-Workflow-Approvers** | `ODW-GitHub-Workflow-Approvers` | Senior engineers & code reviewers | Write role + CODEOWNERS approval required | 12 |
| **ODW-Data-Engineers** | `ODW-GitHub-Contributors` | Rest of engineering team | Write role on all ODW repos | TBD |

## Team Membership

### ODW-Admins (4 members)
Repository administrators with full access to manage settings and critical changes.

**Members:**
- @beejjacobs
- @m-juckes-pins
- @stef-solirius
- @raamvar

**Responsibilities:**
- Manage repository settings
- Configure workflows and branch protections
- Approve changes to CODEOWNERS file
- Approve critical infrastructure changes

---

### ODW-Workflow-Approvers (12 members)
Senior engineers who approve all code changes, including GitHub Actions workflows.

**Members:**
- @roytalari
- @HarrisonBoyleThomas
- @Fred83200
- @KranthiRayipudi
- @RohitShuklaPINS
- @harriet-stuart-wd
- @prathapA100
- @DanBrown2025
- @KalyaniNik
- @stef-solirius (also in ODW-Admins)
- @raamvar (also in ODW-Admins)

**Responsibilities:**
- Develop and maintain data pipelines
- Write Synapse notebooks and scripts
- Review and approve all PRs (CODEOWNERS approval required for):
  - `.github/workflows/**` (GitHub Actions)
  - `/workspace/**` (Synapse workspace files)
  - `/pipelines/**` (Azure DevOps pipelines)
  - `/odw/**` (ODW Python package)
  - `*.py`, `*.ipynb`, `*.sql` (Code files)

**Note:** This team replaces the long list of individual users currently in CODEOWNERS.

---

### ODW-Data-Engineers (count TBD)
Rest of engineering team with write access.

**Members:** To be identified - engineers not currently listed in CODEOWNERS but needing repository access.

**Responsibilities:**
- Develop and maintain data pipelines
- Write Synapse notebooks and scripts
- Submit PRs (require approval from ODW-Workflow-Approvers)

**Note:** This team is for future scalability as the team grows.

---

## Key Changes from Current Setup

### Before (Current State)
```
/.github/workflows    @beejjacobs @m-juckes-pins @roytalari @stef-solirius @HarrisonBoyleThomas @Fred83200 @KranthiRayipudi @RohitShuklaPINS @harriet-stuart-wd @prathapA100 @DanBrown2025 @raamvar @KalyaniNik
/workspace/**         @roytalari @stef-solirius @HarrisonBoyleThomas @Fred83200 @KranthiRayipudi @RohitShuklaPINS @harriet-stuart-wd @prathapA100 @DanBrown2025 @raamvar @KalyaniNik
*.py                  @roytalari @stef-solirius @HarrisonBoyleThomas @Fred83200 @KranthiRayipudi @RohitShuklaPINS @harriet-stuart-wd @prathapA100 @DanBrown2025 @raamvar @KalyaniNik
```

### After (Proposed State)
```
/.github/workflows    @Planning-Inspectorate/ODW-Workflow-Approvers
/workspace/**         @Planning-Inspectorate/ODW-Workflow-Approvers
*.py                  @Planning-Inspectorate/ODW-Workflow-Approvers
```

**Impact:** Much cleaner and easier to maintain. Adding/removing team members only requires updating Entra ID group membership.

---

## Benefits

### 1. Reduced Manual Effort
- **Current**: 15+ minutes to update CODEOWNERS file and repository settings when someone joins/leaves
- **Proposed**: < 5 minutes - just add/remove user from Entra ID group (automatic sync to GitHub)

### 2. Consistency
- **Current**: Risk of missing repositories when granting access
- **Proposed**: Single team membership applies to all ODW repositories

### 3. Audit & Compliance
- **Current**: Access control spread across multiple repositories
- **Proposed**: Centralized in Entra ID with audit logs

### 4. Scalability
- **Current**: CODEOWNERS file becomes unwieldy as team grows
- **Proposed**: Team names stay constant regardless of team size

### 5. Integration with PINS Systems
- Leverages existing Entra ID groups
- Consistent with other PINS access management
- Supports MFA and conditional access policies

---

## Implementation Timeline

### Phase 1: Prerequisites & Approval (Week 1)
- [ ] Verify GitHub Enterprise Cloud subscription
- [ ] Check if Entra ID SSO is already configured
- [ ] Get approval from Rams on team structure
- [ ] Submit support ticket to PINS IT

### Phase 2: Entra ID Setup (Week 2)
- [ ] Create Entra ID security groups
- [ ] Add members to groups
- [ ] Configure GitHub SSO (if not already done)
- [ ] Enable team synchronization

### Phase 3: Repository Configuration (Week 3)
- [ ] Create GitHub teams and link to Entra ID groups
- [ ] Grant team permissions to repositories
- [ ] Update CODEOWNERS files
- [ ] Test with pilot users

### Phase 4: Validation & Rollout (Week 4)
- [ ] Full team testing
- [ ] Remove individual user access
- [ ] Monitor for issues
- [ ] Update documentation

---

## Prerequisites

### GitHub Requirements
- Planning-Inspectorate organization must be on GitHub Enterprise Cloud (required for Entra ID integration)
- GitHub organization admin access needed

### Azure Entra ID Requirements
- Create/identify 3 security groups
- Entra ID admin access (Global Admin or Application Administrator role)
- All team members must have Entra ID accounts

### Approvals Required
- **Rams** - Team structure and membership
- **PINS IT** - Entra ID group creation and SSO configuration
- **GitHub Admins** - Organization settings changes

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Accidentally remove critical access | High | Add team access BEFORE removing individual access. Keep backup of current CODEOWNERS. |
| Entra ID integration not supported | Medium | Fall back to manual GitHub teams (still better than current state). Can migrate to Entra ID later. |
| Team sync delays | Low | Test with pilot group first. Keep manual override capability. |
| Users not aware of changes | Low | Communicate in advance. Send notifications before implementation. |

---

## Questions for Rams

1. **Team Membership**: Are the proposed members for each team correct? Any additions/changes?
   
2. **ODW-Data-Engineers Team**: Do we have additional engineers who need repository access but aren't currently in CODEOWNERS?

3. **Entra ID Groups**: Do any of these security groups already exist in Entra ID that we can leverage?

4. **Timeline**: Is the 4-week timeline acceptable, or do you need it faster/slower?

5. **Approvals**: Who should we contact at PINS IT to get this moving?

---

## Next Steps (if approved)

1. **Create support ticket** to PINS IT for:
   - Verify GitHub Enterprise Cloud subscription
   - Entra ID security group creation
   - GitHub SSO configuration support

2. **Request admin access** for:
   - GitHub organization admin (to create teams and configure repos)
   - Entra ID admin (to manage security groups)

3. **Prepare for implementation**:
   - Document current repository access
   - Create draft Entra ID group memberships
   - Schedule implementation window

---

## Contact

**Implementation Lead**: Stefania Deligia (@stef-solirius)  
**Approval Required From**: Rams (Team Lead)  
**Documentation**: See `docs/github-access-management.md` for full technical details

**Date**: 2026-02-04  
**Status**: Awaiting Approval

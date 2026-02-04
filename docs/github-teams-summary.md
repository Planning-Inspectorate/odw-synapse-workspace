# GitHub Teams - Quick Reference

## Final Team Structure & Entra ID Group Mapping

| GitHub Team | Entra ID Group | Status | Members | Purpose |
|-------------|----------------|--------|---------|---------|
| **ODW-Admins** | `ODW-GitHub-Admins` | **NEW** - to be created | 4 | Repository administrators with admin role |
| **ODW-Workflow-Approvers** | `ODW-GitHub-Workflow-Approvers` | **NEW** - to be created | 12 | Senior engineers who approve all code changes via CODEOWNERS |
| **ODW-Data-Engineers** | `pins-odw-engineers` | **EXISTING** - reused | ~12-15 | All engineering team with write access |

## Strategy Rationale

✅ **Reuse Existing Group**: `pins-odw-engineers`
- Most of the ODW team is already in this group
- Group ID: `13900d77-c9e6-40e1-9d6e-740cd6c2218c`
- Description: "ODW (Operational Data Warehouse) Engineers"

✅ **Create Only 2 New Small Groups**:
- `ODW-GitHub-Admins` (4 members)
- `ODW-GitHub-Workflow-Approvers` (12 members from current CODEOWNERS)

✅ **Benefits**:
- Minimizes group proliferation in Entra ID
- Leverages existing access management
- Reduces admin overhead
- Most users already have correct membership

## Current Team Members

### ODW-Admins (4 members)
- @beejjacobs
- @m-juckes-pins
- @stef-solirius
- @raamvar

### ODW-Workflow-Approvers (12 members - current CODEOWNERS)
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
- Plus @m-juckes-pins for pipelines

### ODW-Data-Engineers (~12-15 members)
Already in `pins-odw-engineers` Entra ID group

## CODEOWNERS Mapping

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

## Implementation Steps

1. **Request Entra ID admin to create 2 new groups**:
   ```
   Group 1: ODW-GitHub-Admins
   Members: beejjacobs, m-juckes-pins, stef-solirius, raamvar
   
   Group 2: ODW-GitHub-Workflow-Approvers
   Members: All 12 users from current CODEOWNERS
   ```

2. **Verify `pins-odw-engineers` group membership**
   - Ensure all current team members are in this group
   - Add any missing members

3. **Configure GitHub Enterprise Cloud SSO** (if not already done)
   - Enable SAML SSO with Microsoft Entra ID
   - Test with pilot users

4. **Create GitHub teams and link to Entra ID groups**:
   ```
   Organization: Planning-Inspectorate
   
   Team 1: ODW-Admins → ODW-GitHub-Admins (Entra ID)
   Team 2: ODW-Workflow-Approvers → ODW-GitHub-Workflow-Approvers (Entra ID)
   Team 3: ODW-Data-Engineers → pins-odw-engineers (Entra ID)
   ```

5. **Update repository settings**:
   - Grant teams appropriate permissions
   - Update CODEOWNERS file
   - Test with team members

6. **Remove individual access**:
   - After verifying team access works
   - Keep backup of old CODEOWNERS

## Existing ODW Entra ID Groups Found

For reference, other ODW-related groups that exist:
- `pins-odw-dev-administrators`
- `pins-odw-preprod-administrators`
- `pins-odw-prod-administrators`
- `pins-odw-dev-dataengineers`
- `pins-odw-preprod-dataengineers`
- `pins-odw-prod-dataengineers`
- `pins-odw-engineers` ← **Using this one**
- `pins-odw-engineers-prod`
- `azure-devops-odw-admins`

## Contact for Implementation

- **Entra ID Group Creation**: PINS IT Support
- **GitHub Organization Admin**: PINS IT Support / GitHub Team
- **Team Lead Approval**: Rams
- **Implementation Lead**: Stefania Deligia (@stef-solirius)

## Timeline

- **Week 1**: Prerequisites verification & approval
- **Week 2**: Entra ID group creation & SSO setup
- **Week 3**: GitHub team configuration & repository updates
- **Week 4**: Testing, validation & rollout

## Status

✅ Proposal created  
✅ Existing groups identified  
✅ Strategy finalized (reuse + 2 new groups)  
⏳ Awaiting approval from Rams  
⏳ Awaiting PINS IT support ticket  

# Current vs Proposed Approach - Access Management

## Current Approach

### Access Method
Individual GitHub usernames listed directly in CODEOWNERS file.

### Current CODEOWNERS Structure
```
/.github/CODEOWNERS    @beejjacobs @m-juckes-pins @stef-solirius
/workspace/**          @roytalari @stef-solirius @HarrisonBoyleThomas @Fred83200 ...
*.py                   @roytalari @stef-solirius @HarrisonBoyleThomas @Fred83200 ...
```

### Issues with Current Approach
1. **Manual Maintenance**: Every user addition/removal requires:
   - PR to update CODEOWNERS
   - Review and approval
   - Merge and deployment

2. **No Single Source of Truth**: 
   - GitHub access managed separately from Entra ID
   - Risk of access drift between systems
   - No automatic deprovisioning when users leave

3. **Scalability Issues**:
   - CODEOWNERS file becomes long and hard to maintain
   - Changes affect multiple lines
   - Difficult to audit who has access

4. **Inconsistent with PINS**:
   - Other PINS services use Entra ID for access management
   - ODW is an outlier

5. **No Automatic Deprovisioning**:
   - When users leave or change roles, manual cleanup needed
   - Risk of orphaned access

## Proposed Approach

### Access Method
GitHub teams synced with Azure Entra ID security groups.

### Proposed CODEOWNERS Structure
```
/.github/CODEOWNERS    @Planning-Inspectorate/odw-admins
/workspace/**          @Planning-Inspectorate/odw-developers
*.py                   @Planning-Inspectorate/odw-developers
```

### Proposed Team Structure

| Team Name | Entra ID Group | Purpose | Permissions |
|-----------|----------------|---------|-------------|
| odw-admins | ODW-GitHub-Admins | Repository administrators | Admin access to all ODW repos |
| odw-developers | ODW-GitHub-Developers | Active developers | Write access, code review rights |
| odw-contributors | ODW-GitHub-Contributors | External contributors | Read access, can create PRs |

### Benefits

1. **Single Source of Truth**:
   - Entra ID is the authoritative source
   - GitHub automatically reflects Entra ID changes
   - No manual GitHub updates needed

2. **Automatic Provisioning/Deprovisioning**:
   - Add user to Entra ID group → automatically added to GitHub team
   - Remove from Entra ID group → automatically removed from GitHub team
   - Happens within minutes via SSO sync

3. **Reduced Manual Effort**:
   - No PRs needed to update CODEOWNERS for user changes
   - CODEOWNERS references teams (stable) not individuals (changing)
   - Changes made in Azure Portal or via automation

4. **Consistent with PINS Identity Management**:
   - Aligns with organization-wide access management
   - Uses existing identity infrastructure
   - Centralized access control

5. **Better Auditability**:
   - Entra ID audit logs track all changes
   - Clear group membership history
   - Easier compliance reporting

6. **Simplified CODEOWNERS**:
   - Shorter, more maintainable file
   - Team-based rather than individual-based
   - Easier to understand access structure

## Implementation Comparison

### Current: Adding a New User

**Steps Required**: 5-7 manual steps
1. User requests access
2. Create PR to update CODEOWNERS
3. Add username to all relevant lines
4. Request review
5. Merge PR
6. Wait for deployment
7. Verify access

**Time**: 1-2 days (depending on review cycle)

### Proposed: Adding a New User

**Steps Required**: 1 step
1. Add user to appropriate Entra ID group(s) in Azure Portal

**Time**: 5-10 minutes (SSO sync time)

### Current: Removing a User

**Steps Required**: 5-7 manual steps (same as adding)
1. Create PR to remove from CODEOWNERS
2. Remove username from all relevant lines
3. Review and approval
4. Merge PR
5. Wait for deployment
6. Manually remove from any other GitHub teams
7. Verify removal

**Time**: 1-2 days

**Risk**: Easy to miss removal from all locations

### Proposed: Removing a User

**Steps Required**: 1 step
1. Remove user from Entra ID group(s)

**Time**: 5-10 minutes

**Risk**: None - automatic sync ensures complete removal

## Security Comparison

### Current
- ❌ No automatic deprovisioning
- ❌ Manual process prone to errors
- ❌ Difficult to audit
- ✅ Simple to understand

### Proposed
- ✅ Automatic deprovisioning when user leaves
- ✅ Centralized access control
- ✅ Full audit trail in Entra ID
- ✅ Consistent with enterprise security practices
- ✅ Integration with existing identity lifecycle

## Migration Path

1. **Phase 1**: Spike test (current)
   - Create test group and team
   - Verify sync works
   - Test CODEOWNERS integration

2. **Phase 2**: Setup GitHub Enterprise SSO
   - Configure SAML between GitHub and Entra ID
   - Test automatic sync

3. **Phase 3**: Create Production Groups
   - ODW-GitHub-Admins
   - ODW-GitHub-Developers
   - ODW-GitHub-Contributors

4. **Phase 4**: Migrate Users
   - Add current users to appropriate groups
   - Verify GitHub team membership syncs

5. **Phase 5**: Update CODEOWNERS
   - Replace individual usernames with teams
   - Test code review workflow

6. **Phase 6**: Documentation
   - Update onboarding/offboarding docs
   - Create runbook for access management

## Rollback Plan

If issues arise:
- CODEOWNERS can be quickly reverted to individual usernames
- No loss of access - teams can coexist with individuals
- Entra ID groups can be deleted without affecting current access

## Cost Considerations

### Current
- No additional cost
- High operational cost (manual maintenance time)

### Proposed
- May require GitHub Enterprise (if not already in place)
- Significantly reduced operational cost
- Better security posture reduces risk

## Recommendation

**Proceed with proposed approach** because:
1. Aligns with PINS enterprise identity management
2. Reduces operational overhead significantly
3. Improves security through automatic deprovisioning
4. More scalable as team grows
5. Better audit trail and compliance

The spike test will validate technical feasibility before full rollout.

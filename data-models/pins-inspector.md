#### ODW Data Model

##### entity: pins-inspector

Data model for pins-inspector entity showing data flow from source to curated.

**Note:** This diagram shows the SAP HR-based pins_inspector entity. The horizon_pins_inspector entity (legacy Horizon system) follows a separate data flow path documented in the Harmonised section.

```mermaid

classDiagram

    direction LR

    namespace Sources {

        class sap_hr_monthly_src {
            PersNo: string
            EmployeeNo: string
            FTE: float
            OrganizationalUnit: string
        }

        class inspector_raw_src {
            PINSStaffNumber: int
            GivenNames: string
            FamilyName: string
            Grade: string
        }

        class inspector_address_src {
            StaffNumber: string
            PostalCode: string
            City: string
        }

        class inspector_specialisms_src {
            StaffNumber: string
            QualificationName: string
            Proficien: string
        }

        class entraid_src {
            id: string
            employeeId: string
            givenName: string
        }

        class horizon_pins_inspector_src {
            horizonId: string
            firstName: string
            lastName: string
        }
    }
    
    namespace Standardised {

        class sap_hr_monthly {
            PersNo: string
            EmployeeNo: string
            FTE: float
            OrganizationalUnit: string
        }

        class load_inspector_raw {
            PINSStaffNumber: int
            GivenNames: string
            FamilyName: string
            Grade: string
        }

        class inspector_address {
            StaffNumber: string
            PostalCode: string
            City: string
        }

        class inspector_specialisms_monthly {
            StaffNumber: string
            QualificationName: string
            Proficien: string
        }

        class entraid {
            id: string
            employeeId: string
            givenName: string
        }

        class horizon_pins_inspector_std {
            horizonId: string
            firstName: string
            lastName: string
        }
    }

    namespace Harmonised {

        class hist_sap_hr {
            PersNo: string
            FTE: float
            PersonnelArea: string
            OrganizationalUnit: string
        }

        class live_dim_inspector {
            pins_staff_number: string
            given_names: string
            family_name: string
            grade: string
        }

        class sap_hr_inspector_address {
            StaffNumber: string
            PostalCode: string
            City: string
        }

        class sap_hr_inspector_specialisms {
            StaffNumber: string
            QualificationName: string
            Proficien: string
            Current: int
        }

        class entraid_hrm {
            id: string
            employeeId: string
            isActive: string
        }

        class pins_inspector {
            entraId: string
            sapId: string
            firstName: string
            lastName: string
            specialism: array
            address: struct
        }

        class horizon_pins_inspector {
            horizonId: string
            firstName: string
            lastName: string
            IsActive: string
            ValidTo: timestamp
        }
    }

    namespace Curated {

        class pins_inspector_cu {
            entraId: string
            sapId: string
            firstName: string
            lastName: string
            specialism: array
            address: struct
        }
    }

%% SAP HR Flow
`sap_hr_monthly_src` --> `sap_hr_monthly`
`sap_hr_monthly` --> `hist_sap_hr`

%% Inspector Raw Flow
`inspector_raw_src` --> `load_inspector_raw`
`load_inspector_raw` --> `live_dim_inspector`

%% Inspector Address Flow
`inspector_address_src` --> `inspector_address`
`inspector_address` --> `sap_hr_inspector_address`

%% Inspector Specialisms Flow
`inspector_specialisms_src` --> `inspector_specialisms_monthly`
`inspector_specialisms_monthly` --> `sap_hr_inspector_specialisms`

%% EntraID Flow
`entraid_src` --> `entraid`
`entraid` --> `entraid_hrm`

%% Aggregation to pins_inspector (Harmonised)
`live_dim_inspector` --> `pins_inspector`
`hist_sap_hr` --> `pins_inspector`
`sap_hr_inspector_address` --> `pins_inspector`
`sap_hr_inspector_specialisms` --> `pins_inspector`
`entraid_hrm` --> `pins_inspector`

%% Harmonised to Curated
`pins_inspector` --> `pins_inspector_cu`

%% Horizon Flow (Separate Path)
`horizon_pins_inspector_src` --> `horizon_pins_inspector_std`
`horizon_pins_inspector_std` --> `horizon_pins_inspector`


```

---

### Data Flow Summary

#### SAP HR-based pins_inspector
1. **Raw → Standardised**: Multiple SAP HR sources (HR monthly, inspector raw, address, specialisms) are ingested
2. **Standardised → Harmonised**: 
   - SAP HR data consolidated into `hist_sap_hr` and dimension tables
   - Inspector base data in `live_dim_inspector`
   - Specialisms and addresses in separate harmonised tables
3. **Harmonised Integration**: `py_harmonised_pins_inspector` notebook joins:
   - `live_dim_inspector` (base inspector data)
   - `entraid_hrm` (identity mapping)
   - `hist_sap_hr` (latest HR record per person)
   - `sap_hr_inspector_specialisms` (aggregated as array)
   - `sap_hr_inspector_address` (structured as nested object)
4. **Harmonised → Curated**: `pins_inspector` notebook filters active records (IsActive='Y')

#### Horizon-based horizon_pins_inspector (Separate Entity)
1. **Raw → Standardised**: Horizon legacy system data ingested
2. **Standardised → Harmonised**: `horizon_pins_inspector` notebook implements SCD Type 2 pattern
3. **No Curated Layer**: Remains in harmonised only for historical reference

**Key Point**: These are two separate, unmerged entities serving different purposes.

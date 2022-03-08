from faker import Faker
from faker.providers import BaseProvider
import random
import csv

class HRProvider(BaseProvider):
    def personnel_area(self):
        return random.choice(['Corporate Services', 'Operations', 'Strategy'])
    def pa(self):
        return random.choice(['P200', 'P500', 'P600'])
    def personnel_subarea(self):
        return random.choice(['Human Resources', 'Ops Group 2', 'Strat Change'])
    def psubarea(self):
        return random.choice(['P215', 'P510', 'P615'])
    def organizational_unit(self):
        return random.choice(['Learning & Development', 'Inquiries & Specialist Group 2', 'Change Portfolio','Inquiries & Specialist Group 5','ECT'])
    def organizational_key(self):
        return random.choice(['P2000000090530', 'P5000000090220', 'P6000000090640','P5000000090220','P5000000090420'])
    def workc(self):
        return random.choice(['PE'])
    def workcontract(self):
        return random.choice(['Permanent'])
    def ct(self):
        return random.choice(['01'])
    def contract_type(self):
        return random.choice(['Permanent Contract'])
    def ps_group(self):
        return random.choice(['AA','BAND 3','G7','BAND 2','SEO','HEO'])
    def pay_band_description(self):
        return random.choice(['Administrative Assistant','Band 3 Inspector','Grade 7','Band 2 Inspector','Senior Executive Officer','Higher Executive Officer'])
    def employment_status(self):
        return random.choice(['Active','Inactive'])
    def gender(self):
        return random.choice(['Male','Female'])
    def actr(self):
        return random.choice(['06','01','02'])
    def reason_for_action(self):
        return random.choice(['Restructure','Change to Hours','Promotion'])
    def cost_centre(self):
        return random.choice(['Human Resources','Inspectr Costs','Digital Services'])
    def loc(self):
        return random.choice(['001','003'])
    def location(self):
        return random.choice(['Home','Bristol'])
    def active_status(self):
        return random.choice(['ACTIVE','NON-ACTIVE'])
    def grade(self):
        return random.choice(['HPI','NON-PHPI','B2','NSI','SHPI','B3','CONS'])


Faker.seed(0)
fake = Faker('en_GB')

fake.add_provider(HRProvider)

def generate_employee():
    employee=dict()
    employee['first_name'] = fake.first_name()
    employee['last_name'] = fake.last_name()
    employee['email'] = f"{employee['first_name']}.{employee['last_name']}@planninginspectorate.gov.uk"
    return [fake.job(),fake.random_number(8, True),fake.random_number(8, True),employee['first_name'],employee['last_name'],fake.pa(),fake.personnel_area(),fake.personnel_subarea(),fake.psubarea(),fake.random_number(8, True),fake.organizational_unit(),fake.organizational_key(),None,fake.workc(),fake.workcontract(),fake.ct(),fake.contract_type(),fake.ps_group(),fake.pay_band_description(),fake.randomize_nb_elements(100,True,False),fake.randomize_nb_elements(37,True,False),None,fake.boolean(chance_of_getting_true=50),3,fake.employment_status(),fake.gender(),fake.date_this_century(True,False).strftime('%m-%d-%Y'),fake.date_this_century(True,False).strftime('%m-%d-%Y'),None,None,fake.random_number(6, True),fake.actr(),fake.reason_for_action(),fake.random_number(8, True),fake.job(),fake.random_number(5, True),fake.cost_centre(),fake.iso8601(),fake.date_this_decade(True,False).strftime('%m-%d-%Y'),fake.date_this_decade(True,False).strftime('%m-%d-%Y'),fake.iso8601(),fake.random_number(8, True),fake.name(),fake.random_number(8, True),fake.job(),fake.name(),fake.loc(),fake.location(),fake.iso8601(),fake.date_this_decade(True,False).strftime('%m-%d-%Y'),fake.date_this_decade(True,False).strftime('%m-%d-%Y'),fake.date_this_decade(True,False).strftime('%m-%d-%Y'),1,'Employee','xxxx','GBP',None,fake.past_date('-50y').strftime('%m-%d-%Y'),None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,'PI','PINS',None,fake.random_number(8, True),fake.random_number(8, True),employee['email'],f"{fake.building_number()} {fake.street_name()}",None,fake.city(),fake.county() ,fake.postcode(),fake.cellphone_number(),fake.date_this_decade(True,False).strftime('%m-%d-%Y'),fake.active_status(),fake.boolean(chance_of_getting_true=50),fake.random_number(8, True),fake.grade(),fake.boolean(chance_of_getting_true=50),fake.boolean(chance_of_getting_true=50),fake.boolean(chance_of_getting_true=50),fake.randomize_nb_elements(100,True,False),fake.randomize_nb_elements(100,True,False),fake.location(),fake.location(),fake.future_datetime('+1y').isoformat(),None]

with open('tests/employee-syn.csv', 'w') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Profession','Pers_No','Employee_Number','First_Name','Last_Name','PA','Personnel_Area','Personnel_SubArea','PSubArea','Org_unit','Organizational_Unit','Organizational_Key','Organizational_Key1','WorkC','Work_Contract','CT','Contract_Type','PS_Group','Pay_Band_Description','FTE','Wk_Hrs','Work_Schedule','Is_Part_Time','S','Employment_Status','Gender_Key','TRA_Start_Date','TRA_End_Date','TRA_Status','TRA_Grade','Prev_PersNo','ActR','Reason_For_Action','Position','Position1','Cost_Ctr','Cost_Centre','Civil_Service_Start','Date_To_Current_Job','Seniority_Date','Date_To_Subst_Grade','Pers_No_1','Name_of_Manager','Manager_Position','Manager_Position_Text','Counter_Sign_Manager','Loc','Location','Org_Start_Date','Fix_Term_End_Date','Loan_Start_Date','Loan_End_Date','EEGrp','Employee_Group','Annual_salary','Curr','NI_number','Birth_date','Age_of_employee','EO','Rel','Religious_Denomination_Key','SxO','Ethnic_origin','Disability_Code','Disability_Text','Disability_Code_Description','NID','Wage_Type','Employee_Subgroup','LOA_Abs_Type','LOA_Absence_Type_Text','Scheme_reference','Pension_Scheme_Name','PArea','Payroll_Area','Assignment_Number','Company_Code','CoCd','Email_Address','Address_Line_1','Address_Line_2','Town','County','Postcode','Phone_Number','Annual_Leave_Start','Active_Status','Is_Charting_Officer','Charting_Officer_ID','Grade','Is_Sub_Group_Leader','Is_APO','Is_APOM','FTE-Primary','FTE-Secondary','Location-Primary','Location-Secondary','Leaving_Date','Attribute10'])
    for n in range(1, 10000):
        writer.writerow(generate_employee())

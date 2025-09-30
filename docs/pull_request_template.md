**PR Template**

Note: Run the correct ADO pipeline for this PR - check the list here:
[ODW Repositories](https://pins-ds.atlassian.net/wiki/spaces/ODW/pages/2285764637/ODW+repositories)
 1. JIRA Ticket Reference  :
         <!-- Replace with JIRA ticket number and title -->
    [ Enter JIRA ticket number and Title here]

 2.  Summary of the work : 
         <!-- Replace with a short summary of changes -->
    [ Enter Summary here]
 
 3.  New Source-to-Raw Datasets
	
	 - [ ] New source data has been added
  	    - A trigger has been attached at the appropriate frequency

 4.  New Tables in Standardised Layer

   	 - [ ] New standardised tables have been created
		 -  orchestration.json is updated and tested in Dev, and PR is open or merged to main
		 -  Schema exists in odw-config/standardised-table-definitions or is about to be PRd
 
 5.   New Tables in Harmonised or Curated Layers

      - [ ] New harmonised or curated tables have been created
 		- Script is configured in the pipeline pln_post_deployments
        - Schema exists in odw-config/harmonised-table-definitions or curated-table-definitions or is about to be PRd
 
 6.  Schema or Column Changes
    (Only new columns or columns with changed data types are in scope)

     - [ ] Changes to table structure or columns
		  - py_change_table is set to run in pln_post_deployments
	      - A script has been created to backfill or populate new column(s) in Test and Prod
			 - [ ] Avoid dropping and recreating tables unless strictly necessary

 7. Script Execution in Build
	 - [ ] Scripts have run in isolation in Build
		-  Script has been added to pln_post_deployments
	    -  Script is now part of a scheduled pipeline with correct triggers
	 - [ ] No scripts have run or no action required in Test/Prod

8. Table Creation and Schema Validation
   
  	 - [ ] All required tables have been created
  	 - [ ] Schema has been validated against the requirements

9.  Deployment and Schema Change Documentation
	
  	 - [ ] Deployment steps and rollback procedures are documented
  	 - [ ] Schema change handling is outlined and tested

10. Archiving Process Review

  	 - [ ] Automatic archiving logic has been reviewed
  	 - [ ] Archiving schedules and retention policies are validated
 

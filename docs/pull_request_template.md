**PR Template**

 1. Are there new source to raw datasets?
	
	 - [ ] Has a trigger been attached at the appropriate frequency?

 2. Have any new tables been created in Standardised?
 
	- [ ] orchestration.json has been updated and tested in Dev and has been / is about to be PRd into main
	- [ ] the new schema exists inside *odw-config/standardised-table-definitions* OR is about to be PRd into that folder 
	- [ ] Is the raw-to-standardised python script scheduled to run for this dataset grouping?
 3. Have any new tables been created in Harmonised or Curated?
		 
	 - [ ]  *2-odw-standardised-to-harmonised/py_odw_harmonised_table_creation* OR 4-*odw-harmonised-to-curated/py_odw_curated_table_creation* are set up to run in the pipeline *pln_post_deployments* with the base parameter specifying the correct table
	- [ ] the new schema exists inside *odw-config/standardised-table-definitions* OR is about to be PRd into that folder 
 4. Have any tables changed AND/OR have any columns changed in any scripts?
We only care about new columns or columns that change type.

	- [ ] Please set py_change_table to run in the pipeline *pln_post_deployments*
	- [ ] Please create a script to backdate and fill in this new column in Test and Prod
	Only delete and recreate tables with caution!
 4. Have any scripts run in isolation in dev? Please look at the *"spark.autotune.trackingId"*
		- If these changes need to be reflected in Test and Prod, please add to the pipeline *post-			deployment/pln_post_deployment*
	- [ ] Yes - I have reflected this script in the *pln_post_deployments* pipeline
	- [ ] Yes - This script is part of a scheduled run and has been added to the appropriate end to end pipeline with a trigger at the correct frequency
	- [ ] No - This change does not need to take place in Test and Prod
	- [ ] No - No scripts have run 
	
 

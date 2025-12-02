#!/usr/bin/env python3
"""
Script to validate E2E test results from the logging database.
Uses the existing SynapseUtil class to check test results and fail if any test status is not PASSED.
"""

import sys
import os
import time
import argparse
from datetime import datetime

# Add the root project directory to sys.path for proper imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

# Add the tests directory to sys.path
tests_dir = os.path.join(project_root, 'tests')
sys.path.insert(0, tests_dir)

from tests.util.synapse_util import SynapseUtil


def validate_e2e_results(env: str, hours_back: int = 2, max_wait_minutes: int = 10, verbose: bool = False) -> bool:
    """
    Validate E2E test results from the logging database.
    
    Args:
        env: Environment name (e.g., 'dev', 'test')
        hours_back: How many hours back to look for results
        max_wait_minutes: Maximum time to wait for results
        verbose: Enable verbose debugging output
        
    Returns:
        True if all tests passed, False otherwise
    """
    print("Fetching E2E Tests results...")
    print("=" * 52)
    print(f"Environment: {env}")
    print(f"Database: logging")
    print(f"Looking back: {hours_back} hours")
    print(f"Max wait time: {max_wait_minutes} minutes")
    print("")
    
    start_time = datetime.now()
    max_wait_seconds = max_wait_minutes * 60
    
    while True:
        try:
            # Get connection to logging database using existing SynapseUtil
            server = f"pins-synw-odw-{env.lower()}-uks-ondemand.sql.azuresynapse.net"
            if verbose:
                print(f"Connecting to server: {server}")
                print(f"Database: logging")
                
                # Debug: Check available ODBC drivers
                import pyodbc
                print(f"Available ODBC drivers: {[x for x in pyodbc.drivers() if 'SQL Server' in x]}")
            else:
                import pyodbc
            
            connection = SynapseUtil._get_connection(server, "logging")
            
            # SQL query to get recent E2E test results
            sql_query = f"""
            SELECT entity, test_status, test_timestamp, 
                   std_count, hrm_count, hrm_active_count, cur_count, 
                   std_to_hrm_match, hrm_to_cur_match, cur_unique_check 
            FROM e2e_test_results 
            WHERE test_timestamp >= DATEADD(hour, -{hours_back}, GETDATE()) 
            ORDER BY test_timestamp DESC
            """
            
            print("Executing SQL query against Synapse serverless pool...")
            results = SynapseUtil.submit_sql_query(connection, sql_query)
            
            if results and len(results) > 0:
                print(f"Found {len(results)} E2E test results!")
                print("")
                print("Test Results Summary:")
                print("=" * 50)
                
                all_passed = True
                entities_tested = []
                failed_count = 0
                
                for row in results:
                    entity = row[0] if len(row) > 0 else "Unknown"
                    test_status = row[1] if len(row) > 1 else "Unknown"
                    timestamp = row[2] if len(row) > 2 else "Unknown"
                    entities_tested.append(entity)
                    
                    status_indicator = "PASS" if test_status == "PASSED" else "FAIL"
                    print(f"[{status_indicator}] Entity: {entity}, Status: {test_status}, Time: {timestamp}")
                    
                    # Log additional metrics if available
                    if len(row) > 3:
                        metrics = ['std_count', 'hrm_count', 'hrm_active_count', 'cur_count', 
                                  'std_to_hrm_match', 'hrm_to_cur_match', 'cur_unique_check']
                        for i, metric in enumerate(metrics, start=3):
                            if i < len(row) and row[i] is not None:
                                print(f"  {metric}: {row[i]}")
                    
                    if test_status != "PASSED":
                        all_passed = False
                        failed_count += 1
                
                print("=" * 50)
                unique_entities = list(set(entities_tested))
                total_tests = len(unique_entities)
                print(f"Summary: {total_tests} entities tested")
                print(f"Entities: {', '.join(sorted(unique_entities))}")
                print("")
                
                if all_passed:
                    print("SUCCESS: All E2E tests PASSED!")
                    print("")
                    print("E2E Test Verification: PASSED")
                    print("All tests passed - pipeline can proceed.")
                    return True
                else:
                    print(f"FAILURE: {failed_count} out of {total_tests} E2E tests FAILED!")
                    print("")
                    print("E2E Test Verification: FAILED")
                    return False
            else:
                print("No test results found in the specified time window")
        
        except Exception as e:
            print(f"Error executing query: {str(e)}")
        
        # Check if we should continue waiting
        elapsed = datetime.now() - start_time
        if elapsed.total_seconds() >= max_wait_seconds:
            print(f"ERROR: No E2E test results found after waiting {max_wait_minutes} minutes")
            print("")
            print("Manual Verification Instructions:")
            print("Please check the test results manually in Synapse Analytics Studio:")
            print("")
            print("1. Open Synapse Analytics Studio: https://web.azuresynapse.net/")
            print("2. Navigate to Data > Databases > logging > Tables > e2e_test_results")
            print("3. Verify that test_status shows 'PASSED' for recent runs")
            return False
        
        elapsed_seconds = int(elapsed.total_seconds())
        print(f"No recent test results found. Waiting... ({elapsed_seconds}s/{max_wait_seconds}s)")
        time.sleep(30)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate E2E test results from the logging database")
    parser.add_argument("env", nargs="?", default=os.environ.get('ENV', 'dev'),
                       help="Environment name (e.g., 'dev', 'test'). Defaults to ENV environment variable or 'dev'")
    parser.add_argument("--hours-back", type=int, default=2,
                       help="How many hours back to look for results (default: 2)")
    parser.add_argument("--max-wait-minutes", type=int, default=10,
                       help="Maximum time to wait for results in minutes (default: 10)")
    parser.add_argument("-v", "--verbose", action="store_true",
                       help="Enable verbose debugging output")
    
    args = parser.parse_args()
    
    print(f"Starting E2E test validation for environment: {args.env}")
    if args.verbose:
        print(f"Verbose mode: enabled")
        print(f"Hours back: {args.hours_back}")
        print(f"Max wait minutes: {args.max_wait_minutes}")
        print("")
    
    try:
        success = validate_e2e_results(args.env, args.hours_back, args.max_wait_minutes, args.verbose)
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        sys.exit(1)

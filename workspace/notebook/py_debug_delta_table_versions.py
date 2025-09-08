"""
Delta Table Version Debugger

This script helps diagnose issues with Delta table versioning,
particularly when tables are being completely overwritten instead of incrementally updated.
"""

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, when
import json

def debug_delta_table_versions(table_name, max_versions_to_check=5):
    """
    Debug function to analyze Delta table versions and understand overwrite behavior
    
    Args:
        table_name (str): Full table name (e.g., 'odw_curated_db.listed_building')
        max_versions_to_check (int): Maximum number of versions to analyze
    """
    spark = SparkSession.builder.getOrCreate()
    
    print(f"=== DEBUGGING DELTA TABLE: {table_name} ===")
    
    # Get table path
    try:
        table_path = spark.sql(f"DESCRIBE DETAIL {table_name}").select("location").first()["location"]
        print(f"Table Path: {table_path}")
    except Exception as e:
        print(f"ERROR: Could not get table path: {e}")
        return
    
    # Get Delta table object
    try:
        delta_table = DeltaTable.forPath(spark, table_path)
        history_df = delta_table.history()
        
        print("\n=== DELTA TABLE HISTORY ===")
        history_df.select("version", "timestamp", "operation", "operationParameters", "readVersion", "isBlindAppend").show(max_versions_to_check, truncate=False)
        
        # Get version information
        versions = history_df.select("version").orderBy("version", ascending=False).collect()
        latest_version = versions[0]["version"]
        
        print(f"\nLatest Version: {latest_version}")
        print(f"Total Versions: {len(versions)}")
        
    except Exception as e:
        print(f"ERROR: Could not access Delta table history: {e}")
        return
    
    # Analyze specific versions
    print(f"\n=== VERSION ANALYSIS (checking up to {max_versions_to_check} versions) ===")
    
    versions_to_check = min(max_versions_to_check, len(versions))
    
    for i in range(versions_to_check):
        version_num = versions[i]["version"]
        
        try:
            version_df = spark.read.format("delta").option("versionAsOf", version_num).load(table_path)
            row_count = version_df.count()
            
            print(f"Version {version_num}: {row_count} rows")
            
            # Show sample of data schema
            if i == 0:  # Only show schema for latest version
                print(f"Schema for version {version_num}:")
                version_df.printSchema()
                print(f"Sample data (first 3 rows):")
                version_df.show(3, truncate=True)
                
        except Exception as e:
            print(f"ERROR reading version {version_num}: {e}")
    
    # Compare versions if we have at least 2
    if latest_version > 0 and len(versions) >= 2:
        print(f"\n=== COMPARING VERSIONS {latest_version} vs {latest_version-1} ===")
        
        try:
            current_df = spark.read.format("delta").option("versionAsOf", latest_version).load(table_path)
            previous_df = spark.read.format("delta").option("versionAsOf", latest_version-1).load(table_path)
            
            current_count = current_df.count()
            previous_count = previous_df.count()
            
            print(f"Current version ({latest_version}): {current_count} rows")
            print(f"Previous version ({latest_version-1}): {previous_count} rows")
            
            # Check for overwrites by comparing subtract operations
            creates = current_df.subtract(previous_df).count()
            deletes = previous_df.subtract(current_df).count()
            
            print(f"Records in current but not previous (creates): {creates}")
            print(f"Records in previous but not current (deletes): {deletes}")
            
            # Analyze if this looks like an overwrite
            if creates == current_count and deletes == previous_count:
                print("*** DIAGNOSIS: This appears to be a COMPLETE OVERWRITE ***")
                print("    - All current records are 'new' (not in previous version)")
                print("    - All previous records are 'deleted' (not in current version)")
                print("    - This suggests the table is being written with mode='overwrite'")
            elif creates == 0 and deletes == 0:
                print("*** DIAGNOSIS: No changes detected between versions ***")
            else:
                print("*** DIAGNOSIS: This appears to be incremental changes ***")
                print(f"    - {creates} genuine creates")
                print(f"    - {deletes} genuine deletes")
            
            # Show operation details from history
            print(f"\nOperation details for version {latest_version}:")
            latest_operation = history_df.filter(col("version") == latest_version).collect()[0]
            print(f"Operation: {latest_operation['operation']}")
            if latest_operation['operationParameters']:
                print(f"Parameters: {latest_operation['operationParameters']}")
                
        except Exception as e:
            print(f"ERROR comparing versions: {e}")

def get_table_write_mode_recommendation(table_name):
    """
    Provide recommendations for how the table should be written based on analysis
    """
    print(f"\n=== RECOMMENDATIONS FOR {table_name} ===")
    print("1. If this table should track changes over time:")
    print("   - Use mode='append' or Delta merge operations")
    print("   - Consider using SCD (Slowly Changing Dimension) patterns")
    print("   - Add change tracking columns (created_date, modified_date, is_active)")
    print("")
    print("2. If complete overwrite is intended:")
    print("   - Current behavior is correct")
    print("   - But change detection logic needs to be updated")
    print("   - Consider comparing with a 'baseline' or 'previous snapshot' table")
    print("")
    print("3. For the current change detection issue:")
    print("   - The subtract() operation will return all records when tables are overwritten")
    print("   - Need to implement business logic to determine real vs. structural changes")
    print("   - Consider using a hash/checksum approach to detect actual data changes")

# Example usage
if __name__ == "__main__":
    # Example: Debug the listed_building table
    debug_delta_table_versions("odw_curated_db.listed_building")
    get_table_write_mode_recommendation("odw_curated_db.listed_building")

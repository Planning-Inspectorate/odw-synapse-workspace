"""
Improved Delta Table Change Detection Utility

This utility handles both incremental and overwrite scenarios for Delta table change detection.
It includes intelligent logic to detect when tables are being overwritten vs. incrementally updated.
"""

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, when, hash, concat, coalesce
from pyspark.sql.types import StringType
import json

def get_delta_table_path(table_name):
    """Get the location/path of a delta table"""
    spark = SparkSession.builder.getOrCreate()
    table_path = spark.sql(f"DESCRIBE DETAIL {table_name}").select("location").first()["location"]
    return table_path

def get_delta_table_latest_version(table_path):
    """Get the int value of the latest version of a delta table"""
    spark = SparkSession.builder.getOrCreate()
    delta_table = DeltaTable.forPath(spark, table_path)
    history_df = delta_table.history()
    version = history_df.select("version").orderBy("version", ascending=False).first()["version"]
    return version

def detect_table_operation_type(table_path, current_version, previous_version):
    """
    Detect if the table operation was an overwrite or incremental update
    
    Returns:
        str: 'overwrite', 'incremental', or 'unknown'
    """
    spark = SparkSession.builder.getOrCreate()
    
    try:
        delta_table = DeltaTable.forPath(spark, table_path)
        history_df = delta_table.history()
        
        # Get operation details for the current version
        current_operation = history_df.filter(col("version") == current_version).collect()
        
        if current_operation:
            operation = current_operation[0]['operation']
            operation_params = current_operation[0]['operationParameters']
            
            # Check for overwrite indicators
            if operation == 'WRITE' and operation_params:
                mode = operation_params.get('mode', '')
                if mode.lower() == 'overwrite':
                    return 'overwrite'
                elif mode.lower() in ['append', 'merge']:
                    return 'incremental'
            elif operation in ['MERGE', 'UPDATE', 'DELETE']:
                return 'incremental'
                
        return 'unknown'
        
    except Exception as e:
        print(f"Warning: Could not detect operation type: {e}")
        return 'unknown'

def create_record_hash(df, exclude_columns=None):
    """
    Create a hash for each record to enable better change detection
    
    Args:
        df: DataFrame to hash
        exclude_columns: List of columns to exclude from hash (e.g., timestamps)
    
    Returns:
        DataFrame with added 'record_hash' column
    """
    if exclude_columns is None:
        exclude_columns = ['dateReceived', 'timestamp', 'ingestion_date', 'created_date', 'modified_date']
    
    # Get columns to include in hash (exclude system columns)
    hash_columns = [col for col in df.columns if col not in exclude_columns]
    
    # Create hash from concatenated values
    if hash_columns:
        hash_expr = hash(concat(*[coalesce(col(c).cast(StringType()), lit("")) for c in hash_columns]))
        return df.withColumn("record_hash", hash_expr)
    else:
        # If no columns to hash, use a constant
        return df.withColumn("record_hash", lit(0))

def detect_changes_intelligent(table_name, primary_key, exclude_from_hash=None, enable_hash_comparison=True):
    """
    Intelligent change detection that handles both overwrite and incremental scenarios
    
    Args:
        table_name (str): Full table name
        primary_key (str): Primary key column name
        exclude_from_hash (list): Columns to exclude from hash comparison
        enable_hash_comparison (bool): Whether to use hash-based comparison for overwrite scenarios
        
    Returns:
        DataFrame: Changes with EventType column
    """
    spark = SparkSession.builder.getOrCreate()
    
    print(f"Starting intelligent change detection for {table_name}")
    
    # Get table information
    table_path = get_delta_table_path(table_name)
    latest_version = get_delta_table_latest_version(table_path)
    
    print(f"Latest version: {latest_version}")
    
    # Handle first version case
    if latest_version == 0:
        print("First version detected - all records are creates")
        current_df = spark.read.format("delta").option("versionAsOf", 0).load(table_path)
        changes_df = current_df.withColumn("EventType", lit("Create"))
        return changes_df
    
    # Load current and previous versions
    previous_version = latest_version - 1
    current_df = spark.read.format("delta").option("versionAsOf", latest_version).load(table_path)
    previous_df = spark.read.format("delta").option("versionAsOf", previous_version).load(table_path)
    
    current_count = current_df.count()
    previous_count = previous_df.count()
    
    print(f"Current version ({latest_version}): {current_count} rows")
    print(f"Previous version ({previous_version}): {previous_count} rows")
    
    # Detect operation type
    operation_type = detect_table_operation_type(table_path, latest_version, previous_version)
    print(f"Detected operation type: {operation_type}")
    
    # Basic subtract-based change detection
    create_df = current_df.subtract(previous_df)
    delete_df = previous_df.subtract(current_df)
    
    creates_count = create_df.count()
    deletes_count = delete_df.count()
    
    print(f"Basic subtract analysis: {creates_count} creates, {deletes_count} deletes")
    
    # Determine if this looks like an overwrite
    is_likely_overwrite = (creates_count == current_count and deletes_count == previous_count)
    
    if is_likely_overwrite and operation_type == 'overwrite':
        print("Overwrite scenario detected - using intelligent comparison")
        
        if enable_hash_comparison:
            return handle_overwrite_with_hash_comparison(
                current_df, previous_df, primary_key, exclude_from_hash
            )
        else:
            return handle_overwrite_basic(current_df, previous_df, primary_key)
    
    elif is_likely_overwrite:
        print("Potential overwrite detected based on data pattern")
        print("Consider enabling hash comparison or check your table write mode")
        
        if enable_hash_comparison:
            return handle_overwrite_with_hash_comparison(
                current_df, previous_df, primary_key, exclude_from_hash
            )
        else:
            # Fall back to treating as incremental
            return handle_incremental_changes(create_df, delete_df, primary_key)
    
    else:
        print("Incremental changes detected")
        return handle_incremental_changes(create_df, delete_df, primary_key)

def handle_incremental_changes(create_df, delete_df, primary_key):
    """Handle normal incremental changes"""
    # Find updated rows (same primary key in both create and delete)
    update_df = create_df.join(delete_df.select(primary_key), on=primary_key, how="inner") \
                        .withColumn("EventType", lit("Update"))
    
    # Remove updated rows from create and delete sets
    create_df = create_df.join(update_df.select(primary_key), on=primary_key, how="left_anti") \
                        .withColumn("EventType", lit("Create"))
    
    delete_df = delete_df.join(update_df.select(primary_key), on=primary_key, how="left_anti") \
                        .withColumn("EventType", lit("Delete"))
    
    # Union all changes
    changes_df = create_df.union(update_df).union(delete_df)
    
    print(f"Incremental changes: {create_df.count()} creates, {update_df.count()} updates, {delete_df.count()} deletes")
    
    return changes_df

def handle_overwrite_basic(current_df, previous_df, primary_key):
    """Handle overwrite scenario with basic logic"""
    print("Using basic overwrite handling - comparing by primary key only")
    
    # Get primary keys from both versions
    current_keys = current_df.select(primary_key).distinct()
    previous_keys = previous_df.select(primary_key).distinct()
    
    # Find truly new records (primary key not in previous)
    new_keys = current_keys.subtract(previous_keys)
    create_df = current_df.join(new_keys, on=primary_key, how="inner") \
                         .withColumn("EventType", lit("Create"))
    
    # Find deleted records (primary key not in current)
    deleted_keys = previous_keys.subtract(current_keys)
    delete_df = previous_df.join(deleted_keys, on=primary_key, how="inner") \
                          .withColumn("EventType", lit("Delete"))
    
    # All other records in current are potential updates
    existing_keys = current_keys.intersect(previous_keys)
    update_df = current_df.join(existing_keys, on=primary_key, how="inner") \
                         .withColumn("EventType", lit("Update"))
    
    changes_df = create_df.union(update_df).union(delete_df)
    
    print(f"Basic overwrite analysis: {create_df.count()} creates, {update_df.count()} updates, {delete_df.count()} deletes")
    
    return changes_df

def handle_overwrite_with_hash_comparison(current_df, previous_df, primary_key, exclude_from_hash=None):
    """Handle overwrite scenario with hash-based comparison to detect real changes"""
    print("Using hash-based overwrite handling - detecting actual data changes")
    
    # Add hash columns to both dataframes
    current_with_hash = create_record_hash(current_df, exclude_from_hash)
    previous_with_hash = create_record_hash(previous_df, exclude_from_hash)
    
    # Get primary keys and hashes
    current_keys_hash = current_with_hash.select(primary_key, "record_hash")
    previous_keys_hash = previous_with_hash.select(primary_key, "record_hash")
    
    # Find new records (primary key not in previous)
    current_keys = current_keys_hash.select(primary_key).distinct()
    previous_keys = previous_keys_hash.select(primary_key).distinct()
    
    new_keys = current_keys.subtract(previous_keys)
    create_df = current_with_hash.join(new_keys, on=primary_key, how="inner") \
                                .withColumn("EventType", lit("Create"))
    
    # Find deleted records (primary key not in current)
    deleted_keys = previous_keys.subtract(current_keys)
    delete_df = previous_with_hash.join(deleted_keys, on=primary_key, how="inner") \
                                 .withColumn("EventType", lit("Delete"))
    
    # Find records that exist in both but have different hashes (real updates)
    common_keys = current_keys.intersect(previous_keys)
    
    current_common = current_keys_hash.join(common_keys, on=primary_key, how="inner")
    previous_common = previous_keys_hash.join(common_keys, on=primary_key, how="inner")
    
    # Find keys where hash changed
    changed_keys = current_common.subtract(previous_common).select(primary_key).distinct()
    
    update_df = current_with_hash.join(changed_keys, on=primary_key, how="inner") \
                                .withColumn("EventType", lit("Update"))
    
    # Remove hash column from results
    create_df = create_df.drop("record_hash")
    update_df = update_df.drop("record_hash")
    delete_df = delete_df.drop("record_hash")
    
    changes_df = create_df.union(update_df).union(delete_df)
    
    print(f"Hash-based overwrite analysis: {create_df.count()} creates, {update_df.count()} updates, {delete_df.count()} deletes")
    
    return changes_df

# Main function for backward compatibility
def get_delta_table_changes(db_name, table_name, primary_key, enable_intelligent_detection=True):
    """
    Main function to get delta table changes with improved logic
    
    Args:
        db_name (str): Database name
        table_name (str): Table name
        primary_key (str): Primary key column name
        enable_intelligent_detection (bool): Use intelligent detection vs. basic subtract
        
    Returns:
        list: Formatted changes for downstream processing
    """
    spark = SparkSession.builder.getOrCreate()
    table_name_full = f"{db_name}.{table_name}"
    
    try:
        if enable_intelligent_detection:
            changes_df = detect_changes_intelligent(table_name_full, primary_key)
        else:
            # Fall back to original logic
            table_path = get_delta_table_path(table_name_full)
            latest_version = get_delta_table_latest_version(table_path)
            
            if latest_version == 0:
                current_df = spark.read.format("delta").option("versionAsOf", 0).load(table_path)
                changes_df = current_df.withColumn("EventType", lit("Create"))
            else:
                current_df = spark.read.format("delta").option("versionAsOf", latest_version).load(table_path)
                previous_df = spark.read.format("delta").option("versionAsOf", latest_version-1).load(table_path)
                
                create_df = current_df.subtract(previous_df).withColumn("EventType", lit("Create"))
                delete_df = previous_df.subtract(current_df).withColumn("EventType", lit("Delete"))
                
                changes_df = create_df.union(delete_df)
        
        # Format for downstream processing
        formatted_data = [
            {
                'Body': str({k: v for k, v in row.asDict().items() if k != 'EventType'}),
                'UserProperties': {'type': row['EventType']}
            }
            for row in changes_df.collect()
        ]
        
        return formatted_data
        
    except Exception as e:
        print(f"Error processing changes for {table_name_full}: {e}")
        return []

# Example usage
if __name__ == "__main__":
    # Test with improved detection
    changes = get_delta_table_changes(
        "odw_curated_db", 
        "listed_building", 
        "entity",  # or whatever your primary key is
        enable_intelligent_detection=True
    )
    print(f"Found {len(changes)} changes")

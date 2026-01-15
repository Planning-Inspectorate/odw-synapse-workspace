from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.util.table_util import TableUtil
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, IntegerType
import pyspark.sql.functions as F


class IngestionFunctions:
    """
    Contains many utility functions for ingesting data

    This is just a copy of the `py_spark_df_ingestion_functions` notebook - these functions probably need to be refactored
    """

    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    @LoggingUtil.logging_to_appins
    def apply_df_to_table(self, df: DataFrame, db_name: str, table_name: str) -> None:
        # Write the DataFrame with the new column to a new temporary table
        temp_table_name: str = f"{table_name}_tmp"
        df.write.mode("overwrite").format("delta").saveAsTable(f"{db_name}.{temp_table_name}")

        # Drop the original table and empty the location
        TableUtil.delete_table(db_name=db_name, table_name=table_name)

        # Rename the temporary table to replace the original table
        self.spark.sql(f"ALTER TABLE {db_name}.{temp_table_name} RENAME TO {db_name}.{table_name}")

    @LoggingUtil.logging_to_appins
    def normalise_field(self, field: StructField):
        """Normalize field for comparison by ignoring nullable differences and sorting fields."""
        if isinstance(field.dataType, StructType):
            # Sort fields within the StructType and convert to tuple
            return (field.name, tuple(sorted(self.normalise_field(f) for f in field.dataType.fields)))
        elif isinstance(field.dataType, ArrayType):
            # Normalize the elementType and convert to tuple
            return (field.name, ("ArrayType", self.normalise_field(StructField("element", field.dataType.elementType, True))))
        else:
            return (field.name, field.dataType.simpleString().replace("NullableType", ""))

    @LoggingUtil.logging_to_appins
    def compare_schemas(self, schema1: StructType, schema2: StructType) -> tuple:
        def get_field_set(schema: StructType):
            """Convert schema fields to a set ignoring nullable differences and order of fields."""
            return set(self.normalise_field(field) for field in schema.fields)

        fields1 = get_field_set(schema1)
        fields2 = get_field_set(schema2)

        in_schema1_not_in_schema2 = fields1 - fields2
        in_schema2_not_in_schema1 = fields2 - fields1

        # Sort for consistent output
        in_schema1_not_in_schema2_sorted = sorted(in_schema1_not_in_schema2, key=lambda x: x[0])
        in_schema2_not_in_schema1_sorted = sorted(in_schema2_not_in_schema1, key=lambda x: x[0])

        return in_schema1_not_in_schema2_sorted, in_schema2_not_in_schema1_sorted

    @LoggingUtil.logging_to_appins
    def merge_schema(
        self, table_name: str, table_df: DataFrame, in_current_not_in_existing: tuple, in_existing_not_in_current: tuple, reference_df: DataFrame
    ) -> None:
        # removing columns from the existing table that are not in the new df
        for item in in_existing_not_in_current:
            LoggingUtil().log_info(f"Removing column '{item[0]}' from {table_name} since it's not in the new data's schema")
            table_df = table_df.drop(item[0])

        # map of reference df field -> DataType for correct casting (supports arrays/structs)
        ref_types = {f.name: f.dataType for f in reference_df.schema.fields}

        # adding new columns in the existing table
        for item in in_current_not_in_existing:
            col_name = item[0]
            dtype = ref_types.get(col_name)
            if dtype is None:
                LoggingUtil().log_info(f"No dtype found for '{col_name}' in reference schema; defaulting to string")
                dtype = StringType()
            LoggingUtil().log_info(f"Adding new column '{col_name}' of type '{dtype}' in {table_name} since it's not in the table's schema")
            table_df = table_df.withColumn(col_name, lit(None).cast(dtype))

        self.apply_df_to_table(table_df, table_name.split(".")[0], table_name.split(".")[1])

    @LoggingUtil.logging_to_appins
    def compare_and_merge_schema(self, df: DataFrame, table_name: str):
        # get the existing table's df
        table_df: DataFrame = self.spark.table(table_name)

        # compare the schemas
        in_current_not_in_existing, in_existing_not_in_current = self.compare_schemas(df.schema, table_df.schema)

        if in_current_not_in_existing or in_existing_not_in_current:
            LoggingUtil().log_info("Schemas do not match")
            print("#" * 100)
            print("Columns in new data not in existing table: \n", in_current_not_in_existing)
            print("#" * 100)
            print("Columns in existing table not in data: \n", in_existing_not_in_current)
            print("#" * 100)

            # merging schema if there is any schema mismatch
            self.merge_schema(table_name, table_df, in_current_not_in_existing, in_existing_not_in_current, df)

        else:
            print("Schemas match.")

    @LoggingUtil.logging_to_appins
    def collect_all_raw_sb_data(self, folder_name: str, schema: StructType) -> DataFrame:
        """
        Function to loop through all sub-folders in a given folder path and collect all
        json data into a single DataFrame. To be used for a reload of an entity from all
        data in the RAW layer.

        Args:
            folder_name: the name of the folder in the storage account. May be different from the
            entity name.
            schema: a spark schema of type StructType to apply to the RAW data.

        Returns:
            A spark DataFrame of all json data from the RAW layer for a given root folder
        """

        storage_account: str = Util.get_storage_account()

        folder: str = f"abfss://odw-raw@{storage_account}ServiceBus/{folder_name}"

        df: DataFrame = (
            self.spark.read.format("json").schema(schema).option("recursiveFileLookup", "true").option("pathGlobFilter", "*.json").load(folder)
        )

        df_with_filename: DataFrame = df.withColumn("input_file", F.input_file_name())

        return df_with_filename

    @LoggingUtil.logging_to_appins
    def collect_all_raw_sb_data_historic(self, folder_name: str) -> DataFrame:
        """
        Function to loop through all sub-folders in a given folder path and collect all
        json data into a single DataFrame. To be used for a reload of an entity from all
        data in the RAW layer.

        Args:
            folder_name: the name of the folder in the storage account. May be different from the
            entity name.

        Returns:
            A spark DataFrame of all json data from the RAW layer for a given root folder
        """

        storage_account: str = Util.get_storage_account()

        folder: str = f"abfss://odw-raw@{storage_account}ServiceBus/{folder_name}"

        df: DataFrame = self.spark.read.format("json").option("recursiveFileLookup", "true").option("pathGlobFilter", "*.json").load(folder)

        df_with_filename: DataFrame = df.withColumn("input_file", F.input_file_name())

        return df_with_filename

    @LoggingUtil.logging_to_appins
    def is_array_of_struct(self, field: StructField):
        field_type = field.dataType
        return isinstance(field_type, ArrayType) and isinstance(field_type.elementType, StructType)

    @LoggingUtil.logging_to_appins
    def get_empty_array_of_structs(self, schema: StructType):
        empty_struct = F.struct([F.lit("").cast(field.dataType).alias(field.name) for field in schema.fields])
        return F.array(empty_struct)

    @LoggingUtil.logging_to_appins
    def cast_null_columns(self, df: DataFrame, schema: StructType) -> DataFrame:
        """
        Function to casts the null columns in a df to their actual type according to the schema.

        Args:
            df: the spark data frame
            schema: the data-model schema

        Returns:
            A spark DataFrame with all the casted columns
        """
        # Get the schema of the DataFrame as a dictionary
        df_schema = dict(df.dtypes)

        # Convert the provided StructType schema into a dictionary with column names as keys and data types as values
        schema_dict = {field.name: field.dataType for field in schema}

        # Identify columns that have a type mismatch
        columns_to_cast = [col for col in df_schema.keys() if col in schema_dict and df_schema[col] != schema_dict[col].simpleString()]

        # Iterate over these columns and cast them if they are null
        for col in columns_to_cast:
            # Cast the column if it's null
            all_null = df.filter(F.col(col).isNotNull()).count() == 0
            if all_null or df_schema[col] == "bigint":
                result = False
                try:
                    df = df.withColumn(col, F.col(col).cast(schema_dict[col]))
                    result = True
                except:
                    if self.is_array_of_struct(schema[col]):
                        try:
                            df = df.withColumn(col, self.get_empty_array_of_structs(schema[col].dataType.elementType))
                            result = True
                        except Exception as e:
                            print(e)
                    elif isinstance(schema_dict[col], ArrayType):
                        df = df.withColumn(col, F.expr("array()").cast(ArrayType(StringType())))
                        result = True

                if result:
                    print(f"Column '{col}' was cast from {df_schema[col]} to {schema_dict[col]}.")
                else:
                    print(f"Failed to cast Column '{col}' from {df_schema[col]} to {schema_dict[col]}.")

        return df

    @LoggingUtil.logging_to_appins
    def read_csvs_recursively(self, storage_account: str = None, folder: str = None) -> DataFrame:
        """
        Example of how to read csv files in a folder recursively.
        Each file is combined into a final DataFrame.
        The input_file column is added to show which file that record came from.
        Amend the folder path accordingly.
        """
        storage_account: str = Util.get_storage_account()

        folder: str = f"abfss://odw-raw@{storage_account}AIEDocumentData/2024-09-24"

        df: DataFrame = (
            self.spark.read.format("csv")
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", "*.csv")
            .option("header", "true")
            .option("quote", '"')
            .option("escape", '"')
            .option("encoding", "UTF-8")
            .option("multiLine", "true")
            .option("ignoreLeadingWhiteSpace", "true")
            .option("ignoreTrailingWhiteSpace", "true")
            .load(folder)
        )

        df_with_path: DataFrame = df.withColumn("input_file", F.input_file_name())

        return df_with_path

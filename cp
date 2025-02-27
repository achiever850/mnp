import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_timestamp, when
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, BooleanType, TimestampType

## Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

## Configurations
s3_certificateapps_input_path = "s3://hcd-ec2-windows-servers-file-transfer-bucket/usa_staffing_csv/certificateapplications/"
redshift_connection = "hcd_dev_redshift_connection"
redshift_tmp_dir = "s3://aws-glue-assets-094737541415-us-gov-west-1/temporary/"
redshift_database = "hcd-dev-db"
redshift_certificateapps_table = "usastaffing_staging.certificateapplication"

## Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## Define certificateapplication schema (aligned with updated Redshift DDL)
certificateapps_schema = StructType([
    StructField("tenantId", IntegerType(), False),
    StructField("rankingListId", IntegerType(), False),
    StructField("applicationId", LongType(), False),  # Updated to NOT NULL
    StructField("listApplicationId", LongType(), False),  # Updated to NOT NULL
    StructField("applicationNumber", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("middleName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("suffix", StringType(), True),
    StructField("applicationName", StringType(), True),
    StructField("startDateTime", TimestampType(), True),
    StructField("priorityDescription", StringType(), True),
    StructField("rankOrder", IntegerType(), False),  # Updated to NOT NULL
    StructField("rating", StringType(), True),
    StructField("recordStatusCode", StringType(), True),
    StructField("recordStatusCodeDescription", StringType(), True),
    StructField("addedFlag", BooleanType(), True),
    StructField("addedDateTime", TimestampType(), True),
    StructField("auditCode", StringType(), True),
    StructField("auditDateTime", TimestampType(), True),
    StructField("certifiedDateTime", TimestampType(), True),
    StructField("eligibilityAdjudicationStatus", StringType(), True),
    StructField("eligibilityClaimed", StringType(), True),
    StructField("eligibleSeries", StringType(), True),
    StructField("eligibilityStartDate", TimestampType(), True),
    StructField("eligibilityEndDate", TimestampType(), True),
    StructField("expiredFlag", BooleanType(), True),
    StructField("veteranPreferenceCode", StringType(), True),
    StructField("veteranPreferenceDescription", StringType(), True),
    StructField("hiredPDNumber", StringType(), True),
    StructField("hiredPositionTitle", StringType(), True),
    StructField("hiredSeries", StringType(), True),
    StructField("hiredSeriesTitle", StringType(), True),
    StructField("hiredCity", StringType(), True),
    StructField("hiredCounty", StringType(), True),
    StructField("hiredState", StringType(), True),
    StructField("hiredCountry", StringType(), True),
    StructField("hiredLocationDescription", StringType(), True),
    StructField("markedAsFavoriteFlag", BooleanType(), True),
    StructField("markedForFollowUpFlag", BooleanType(), True),
    StructField("reorderedFlag", BooleanType(), True),
    StructField("returnStatus", StringType(), True),
    StructField("usaHireCompletedDate", TimestampType(), True),
    StructField("originallySubmittedDateTime", TimestampType(), True),
    StructField("lastSubmittedDateTime", TimestampType(), True),
    StructField("lastModifiedDateTime", TimestampType(), True),
    StructField("dwLastModifiedDateTime", TimestampType(), True)
])

## Process data function (with table existence check)
def process_data(input_path, schema, table_name, connection, temp_dir):
    try:
        # Read the CSV into a DataFrame
        df = spark.read.option("header", "true") \
            .option("delimiter", ",") \
            .schema(schema) \
            .csv(input_path)
        
        # Debug: Verify DataFrame
        print(f"Schema for {table_name}:")
        df.printSchema()
        print(f"Sample data for {table_name} (raw):")
        df.show(5, truncate=False)
        print(f"Row count before filter: {df.count()}")

        # Hardcoded filter for certificateapplication, updated for NOT NULL columns
        filtered_df = df.filter(
            (col("tenantId").isNotNull()) & 
            (col("rankingListId").isNotNull()) & 
            (col("applicationId").isNotNull()) & 
            (col("listApplicationId").isNotNull()) & 
            (col("rankOrder").isNotNull())
        )

        # Debug: Check filtered data
        print(f"Sample data after filter for {table_name}:")
        filtered_df.show(5, truncate=False)
        print(f"Row count after filter: {filtered_df.count()}")

        # Transform timestamp columns
        timestamp_columns = ["startDateTime", "addedDateTime", "auditDateTime", "certifiedDateTime",
                            "eligibilityStartDate", "eligibilityEndDate", "usaHireCompletedDate",
                            "originallySubmittedDateTime", "lastSubmittedDateTime",
                            "lastModifiedDateTime", "dwLastModifiedDateTime"]
        for ts_col in timestamp_columns:
            filtered_df = filtered_df.withColumn(ts_col, to_timestamp(col(ts_col), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS"))

        # Cast boolean fields
        boolean_cols = ["addedFlag", "expiredFlag", "markedAsFavoriteFlag",
                        "markedForFollowUpFlag", "reorderedFlag"]
        for b_col in boolean_cols:
            filtered_df = filtered_df.withColumn(b_col, when(col(b_col) == "true", True)
                                                .when(col(b_col) == "false", False)
                                                .otherwise(None).cast(BooleanType()))

        # Debug: Final transformed data
        print(f"Sample data after transformations for {table_name}:")
        filtered_df.show(5, truncate=False)

        # Convert to DynamicFrame
        dynamic_frame = DynamicFrame.fromDF(filtered_df, glueContext, f"{table_name}_redshift_frame")

        # Explicit mapping to enforce schema
        mappings = [(field.name, field.dataType.simpleString(), field.name, field.dataType.simpleString()) 
                    for field in schema.fields]
        mapped_frame = ApplyMapping.apply(frame=dynamic_frame, mappings=mappings)

        # Check if table exists in Redshift
        check_query = f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'usastaffing_staging' 
            AND table_name = 'certificateapplication'
        """
        check_df = glueContext.read.format("jdbc") \
            .option("url", "jdbc:redshift://<your-redshift-endpoint>:5439/{redshift_database}".format(redshift_database=redshift_database)) \
            .option("driver", "com.amazon.redshift.jdbc.Driver") \
            .option("query", check_query) \
            .option("user", "<your-redshift-username>") \
            .option("password", "<your-redshift-password>") \
            .load()
        
        table_exists = check_df.collect()[0][0] > 0
        if not table_exists:
            raise ValueError(f"Table {table_name} does not exist in Redshift. Aborting job.")

        # Write to Redshift
        glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=mapped_frame,
            catalog_connection=connection,
            connection_options={
                "dbtable": table_name,
                "database": redshift_database,
                "preactions": f"TRUNCATE TABLE {table_name}",
                "redshiftTmpDir": temp_dir
            }
        )
        print(f"Data successfully loaded into {table_name}")
        
    except Exception as e:
        print(f"Error processing {table_name}: {str(e)}")
        raise

## Process certificateapplication data
process_data(
    s3_certificateapps_input_path, certificateapps_schema, redshift_certificateapps_table,
    redshift_connection, redshift_tmp_dir
)

## Commit the job
job.commit()

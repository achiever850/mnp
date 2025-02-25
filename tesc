import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, BooleanType, DateType, DoubleType

## Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

## Configurations
s3_certificate_input_path = "s3://hcd-ec2-windows-servers-file-transfer-bucket/usa_staffing_csv/certificates/"
redshift_certificate_table = "usastaffing_staging.certificate"

## Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## Define schema for certificate
certificate_schema = StructType([
    StructField("tenantId", IntegerType(), False),
    StructField("rankingListId", IntegerType(), False),
    StructField("certificateNumber", StringType(), True),
    StructField("certificateType", StringType(), True),
    StructField("certificateStatus", StringType(), True),
    StructField("certificateOrder", StringType(), True),
    StructField("certificatepriorityOrder", StringType(), True),
    StructField("applicantListName", StringType(), True),
    StructField("signedDateTime", StringType(), True),
    StructField("issueDateTime", StringType(), True),
    StructField("issuer", StringType(), False),
    StructField("certificateAuditedFlag", BooleanType(), True),
    StructField("initialAuditCompleteDateTime", StringType(), True),
    StructField("finalAuditCompleteDateTime", StringType(), True),
    StructField("auditedBy", StringType(), True),
    StructField("refermethod", StringType(), True),
    StructField("refermethodNumber", DoubleType(), True),
    StructField("certificateAmendedFlag", BooleanType(), True),
    StructField("certificatecancelledFlag", BooleanType(), True),
    StructField("certificateExpiredFlag", BooleanType(), True),
    StructField("certificateExpirationDate", StringType(), True),
    StructField("ctapictapwellQualifiedScore", DoubleType(), True),
    StructField("rankBy", StringType(), True),
    StructField("tieBreaker", StringType(), True),
    StructField("candidateInventoryEnabledFlag", BooleanType(), True),
    StructField("candidateInventoryStartDate", StringType(), True),
    StructField("candidateInventoryEndDate", StringType(), True),
    StructField("lastModifiedDateTime", StringType(), True),
    StructField("dwLastModifiedDateTime", StringType(), True)
])

## Read and process certificate data
certificate_df = spark.read.option("header", "true") \
    .option("delimiter", ",") \
    .schema(certificate_schema) \
    .csv(s3_certificate_input_path)

certificate_filtered_df = certificate_df.filter(
    (col("tenantId").isNotNull()) & (col("rankingListId").isNotNull())
)

## Convert string date columns to timestamp
certificate_filtered_df = certificate_filtered_df.withColumn(
    "signedDateTime", to_timestamp(col("signedDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS")
).withColumn(
    "issueDateTime", to_timestamp(col("issueDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS")
).withColumn(
    "initialAuditCompleteDateTime", to_timestamp(col("initialAuditCompleteDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS")
).withColumn(
    "finalAuditCompleteDateTime", to_timestamp(col("finalAuditCompleteDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS")
).withColumn(
    "certificateExpirationDate", to_timestamp(col("certificateExpirationDate"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS")
).withColumn(
    "candidateInventoryStartDate", to_timestamp(col("candidateInventoryStartDate"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS")
).withColumn(
    "candidateInventoryEndDate", to_timestamp(col("candidateInventoryEndDate"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS")
).withColumn(
    "lastModifiedDateTime", to_timestamp(col("lastModifiedDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS")
).withColumn(
    "dwLastModifiedDateTime", to_timestamp(col("dwLastModifiedDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS")
)

## Write certificate data to Redshift
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=DynamicFrame.fromDF(certificate_filtered_df, glueContext, "certificate_redshift_frame"),
    catalog_connection=redshift_connection,
    connection_options={
        "dbtable": redshift_certificate_table,
        "database": redshift_database,
        "preactions": f"TRUNCATE TABLE {redshift_certificate_table}"
    },
    redshift_tmp_dir=redshift_temp_dir
)

## Commit the job
job.commit()

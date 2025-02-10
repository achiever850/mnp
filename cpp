import sys
from awsglue.transforms import * 
from awsglue.utils import getResolvedOptions 
from pyspark.context import SparkContext 
from awsglue.context import GlueContext 
from awsglue.job import Job 
from awsglue import DynamicFrame

# Get job arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Initialize GlueContext and Spark session
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Define API sources
api_sources = {
    "certificationapplicationapplicationids_test": {
        "headers": ["tenantid", "CA_rankinglistid", "CA_applicationid", "applicationid"]
    },
    "certificationapplicationnewhireids_test": {
        "headers": ["tenantid", "CA_rankinglistid", "CA_applicationid", "newhireid"]
    },
    "certificationapplicationRankinglistids_test": {
        "headers": ["tenantid", "CA_rankinglistid", "CA_applicationid", "rankinglistid"]
    },
    "certificationapplicationrequestsids_test": {
        "headers": ["tenantid", "CA_rankinglistid", "CA_applicationid", "requestid"]
    }
}

# Loop through each API source
for api_name, config in api_sources.items():
    # Read CSV from S3
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="csv",
        format_options={"withHeader": True, "separator": ","},
        connection_options={
            "paths": [f"s3://hcd-ec2-windows-servers-file-transfer-bucket/usa_staffing_csv/{api_name}/"],
            "recurse": True
        },
        transformation_ctx=f"AmazonS3_{api_name}"
    )

    # Write to Amazon Redshift
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame, 
        connection_type="redshift",
        connection_options={
            "redshiftTmpDir": "s3://aws-glue-assets-094737541415-us-gov-west-1/temporary/",
            "useConnectionProperties": "True",
            "dbtable": f"usastaffing_staging.{api_name}",
            "connectionName": "hcd_dev_redshift_connection",
            "preactions": f"""
                CREATE TABLE IF NOT EXISTS usastaffing_staging.{api_name} (
                    tenantid VARCHAR, 
                    CA_rankinglistid VARCHAR, 
                    CA_applicationid VARCHAR, 
                    {config["headers"][-1]} VARCHAR
                ); 
                TRUNCATE TABLE usastaffing_staging.{api_name};
            """
        },
        transformation_ctx=f"AmazonRedshift_{api_name}"
    )

# Commit the job
job.commit()

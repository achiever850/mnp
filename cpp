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

# Define API sources with their headers and target Redshift tables
api_sources = {
    "certificationapplicationnewhireids": {
        "headers": ["tenantid", "CA_rankinglistid", "CA_applicationid", "newhireid"],
        "redshift_table": "usastaffing_staging.certificationapplicationnewhireids"
    },
    "certificationapplicationRankinglistids": {
        "headers": ["tenantid", "CA_rankinglistid", "CA_applicationid", "rankinglistid"],
        "redshift_table": "usastaffing_staging.certificationapplicationrankinglistids"
    },
    "certificationapplicationrequestsids": {
        "headers": ["tenantid", "CA_rankinglistid", "CA_applicationid", "requestid"],
        "redshift_table": "usastaffing_staging.certificationapplicationrequestsids"
    }
}

# Loop through each API dataset and load it
for api_name, config in api_sources.items():
    
    # Read data from S3
    api_dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="csv",
        format_options={"withHeader": True, "separator": ","},
        connection_options={
            "paths": [f"s3://hcd-ec2-windows-servers-file-transfer-bucket/usa_staffing_csv/{api_name}/"],
            "recurse": True
        },
        transformation_ctx=f"{api_name}_node"
    )

    # Write data to Redshift
    glueContext.write_dynamic_frame.from_options(
        frame=api_dynamic_frame, 
        connection_type="redshift",
        connection_options={
            "redshiftTmpDir": "s3://aws-glue-assets-094737541415-us-gov-west-1/temporary/",
            "useConnectionProperties": True,
            "dbtable": config["redshift_table"],
            "connectionName": "hcd_dev_redshift_connection",
            "preactions": f"""
                CREATE TABLE IF NOT EXISTS {config["redshift_table"]} (
                    tenantid VARCHAR, 
                    CA_rankinglistid VARCHAR, 
                    CA_applicationid VARCHAR, 
                    {config["headers"][-1]} VARCHAR
                ); 
                TRUNCATE TABLE {config["redshift_table"]};
            """
        },
        transformation_ctx=f"{api_name}_redshift_node"
    )

# Commit the job
job.commit()

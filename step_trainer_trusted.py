import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step trainer landing
Steptrainerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://mihir-glue/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Steptrainerlanding_node1",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1673715168650 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1673715168650",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=Steptrainerlanding_node1,
    frame2=CustomerTrusted_node1673715168650,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Fields
DropFields_node1673715606622 = DropFields.apply(
    frame=ApplyMapping_node2,
    paths=[
        "customername",
        "email",
        "phone",
        "birthdate",
        "registrationdate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "lastupdatedate",
        "serialnumber",
    ],
    transformation_ctx="DropFields_node1673715606622",
)

# Script generated for node Amazon S3
AmazonS3_node1673715633688 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1673715606622,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://mihir-glue/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1673715633688",
)

job.commit()

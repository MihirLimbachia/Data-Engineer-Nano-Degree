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
Steptrainerlanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="Steptrainerlanding_node1",
)

# Script generated for node Customer Curated
CustomerCurated_node1673715168650 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1673715168650",
)

# Script generated for node Rename customer fields
Renamecustomerfields_node1673800145494 = ApplyMapping.apply(
    frame=CustomerCurated_node1673715168650,
    mappings=[
        ("customername", "string", "`(right) customername`", "string"),
        ("email", "string", "`(right) email`", "string"),
        ("phone", "string", "`(right) phone`", "string"),
        ("birthday", "string", "birthday", "string"),
        ("serialnumber", "string", "`(right) serialnumber`", "string"),
        ("registrationdate", "long", "`(right) registrationdate`", "long"),
        ("lastupdatedate", "long", "`(right) lastupdatedate`", "long"),
        (
            "sharewithresearchasofdate",
            "long",
            "`(right) sharewithresearchasofdate`",
            "long",
        ),
        (
            "sharewithpublicasofdate",
            "long",
            "`(right) sharewithpublicasofdate`",
            "long",
        ),
    ],
    transformation_ctx="Renamecustomerfields_node1673800145494",
)

# Script generated for node Join Step trainer and customer curated
JoinSteptrainerandcustomercurated_node2 = Join.apply(
    frame1=Steptrainerlanding_node1,
    frame2=Renamecustomerfields_node1673800145494,
    keys1=["serialnumber"],
    keys2=["`(right) serialnumber`"],
    transformation_ctx="JoinSteptrainerandcustomercurated_node2",
)

# Script generated for node Drop Fields
DropFields_node1673715606622 = DropFields.apply(
    frame=JoinSteptrainerandcustomercurated_node2,
    paths=[
        "`(right) customername`",
        "`(right) email`",
        "`(right) phone`",
        "`(right) serialnumber`",
        "`(right) registrationdate`",
        "`(right) lastupdatedate`",
        "`(right) sharewithresearchasofdate`",
        "`(right) sharewithpublicasofdate`",
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

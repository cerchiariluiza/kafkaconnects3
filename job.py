import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
	from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "d", table_name = "sampledata_csv", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "d", table_name = "sampledata_csv", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("orderdate", "string", "orderdate", "string"), ("region", "string", "region", "string"), ("manager", "string", "manager", "string"), ("item", "string", "item", "string"), ("units", "long", "units", "long"), ("unitcost", "double", "unitcost", "double")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("orderdate", "string", "orderdate", "string"), ("region", "string", "region", "string"), ("manager", "string", "manager", "string"), ("item", "string", "item", "string"), ("units", "long", "units", "long"), ("unitcost", "double", "unitcost", "double")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["orderdate", "region", "manager", "item", "units", "unitcost"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["orderdate", "region", "manager", "item", "units", "unitcost"], transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "d", table_name = "sampledata_csv", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "d", table_name = "sampledata_csv", transformation_ctx = "resolvechoice3")


dropDF=resolveChoice3.toDF();
dropDF1=dropDF.filter(dropDF["unit-cost"]>10)
dynamic_dFrame=DynamicFrame.fromDF(dropDF1, glueContext, "dynamic_df")
## @type: DataSink
## @args: [database = "d", table_name = "sampledata_csv", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = resolvechoice3]
datasink4 = glueContext.write_dynamic_frame.from_catalog(frame = dynamic_dFrame, database = "d", table_name = " ", transformation_ctx = "datasink4")
job.commit()

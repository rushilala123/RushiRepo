import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from pyspark.sql.functions import *
from pyspark.sql import *


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['bucket', 'file'])
bucket = args['bucket']
obj = args['file']
data = "s3a://{}/{}".format(bucket,obj)
print(data)
sc = SparkContext()
glueContext = GlueContext(sc) 
spark = glueContext.spark_session

def extract(obj):
    start_index = 0
    end_index = obj.find('/')
    tab = obj[start_index:end_index]
    return tab

tab = extract(obj)

Redshift_table = f"public.{tab}"
final_table = f"CALL final.{tab}();"

# Configure AWS credentials
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                     "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

#lode data from s3 bucket
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(data)

# Write new records to Redshift
df.write \
    .format("jdbc") \
    .option("url", "jdbc:redshift://redshift-cluster-1.cvyigj4q55uc.ap-south-1.redshift.amazonaws.com:5439/dev") \
    .option("dbtable", Redshift_table) \
    .option("user", "awsuser") \
    .option("password", "Rj143143") \
    .option("aws_iam_role","arn:aws:iam::039643697933:role/red-s3_Access") \
    .option("aws_region","ap-south-1") \
    .mode("append") \
    .save()
    
# Call a stored procedure in Redshift
def call_stored_procedure():
    
    # Create a Redshift client
    client = boto3.client('redshift-data',
                          region_name='ap-south-1')
    
    # Call the stored procedure
    print("till line 49")
    response = client.execute_statement(
        ClusterIdentifier='redshift-cluster-1',
        Database='dev',
        DbUser='awsuser',
        Sql=final_table
    )
    
    # Print the response
    print("this is response line 58")
    print(response)

# Call the stored procedure
print("call_stored_procedure at line 62")
call_stored_procedure()

# Stop the SparkSession
print("stop spark at line66")
spark.stop()
    

    
    
    
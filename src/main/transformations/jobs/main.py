import os
import sys

from resources.dev import config
from src.main.utility.encrypt_decrypt import *
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *
from src.main.utility.my_sql_session import *
from src.main.read.aws_read import *
from src.main.download.aws_file_download import *
from src.main.utility.spark_session import spark_session

# Get S3 client

aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

# Now you can use your s3_client for your s3 operations

response = s3_client.list_buckets()
print(response)
logger.info("List of Buckets: %s", response ['Buckets'])

# check if local directory has already a file
# if file is present then check the same file is present in staging area with status as A
# if so then dont delete and try to re-run else give an error and not process the file

csv_files = [ file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append((file))

    statement = f""" select distinct file_name from 
                    {config.database_name}.{config.product_staging_table}
                    where file_name in ({str(total_csv_files)[1:-1]}) and status = 'I'
    """
    logger.info(f"dynamically statement created : {statement}")
    cursor.execute(statement)
    data = cursor.fetchall()

    if data:
        logger.info("Your last run was failed please check ")
    else:
        logger.info("No record match")
else:
    logger.info("Last run was successful")

try:
    s3_reader = S3Reader()
    # bucket name should come from table

    folder_path = config.s3_source_directory
    s3_absolute_path = s3_reader.list_files(s3_client, config.bucket_name, folder_path)
    logger.info("Absolute path of S3 bucket for csv file %s", s3_absolute_path)

    if not s3_absolute_path:
        logger.info(f"file not available at {folder_path}")
        raise Exception('No Data available to process ')
except Exception as e :
    logger.error(' Exited with error: - %s', e)
    raise e

bucket_name = config.bucket_name
local_directory = config.local_directory

prefix = f"s3://{bucket_name}/"
file_paths = [url[len(prefix):] for url in s3_absolute_path]

logging.info(" File path available on S3 under %s bucket and folder name is %s", bucket_name) # check
logging.info(f' File path available on S3 under {bucket_name} bucket and folder name is {file_paths} ')

try:
    downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error("File download error: %s", e)
    sys.exit()

#get the list of all the files in the local directory
all_files = os.listdir(local_directory)
logger.info(f"List of files present at my local directory after download {all_files}")

# Filter files with ".csv" in their names and create absolute path

if all_files:
    csv_files = []
    error_files = []

    for files in all_files:
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory, files)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory, files)))

    if not csv_files:
        logger.error("No csv data available to process the request")
        raise Exception("No csv data available to process the request")
else:
    logger.error("There is no data to process")
    raise Exception("There is no data to process")

####### make csv lines convert into a list of comma separated #############

logger.info("****************** Listing the File **********************")
logger.info("List of csv files that needs to be processed %s", csv_files)

logger.info("****************** Creating Spark Session **********************")

spark = spark_session()

logger.info("****************** Spark Session Created **********************")

correct_files = []
for data in csv_files:
    data_schema = spark.read.format("csv")\
        .option("header", "true")\
        .load(data).columns
    logger.info( f" Schema for the {data} is {data_schema}")
    logger.info(f" Mandatory column Schema is {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f" missing columns are {missing_columns}")

    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f" No missing column for the {data}")
        correct_files.append(data)

logger.info(f" *********** list of correct files ************ {correct_files}")
logger.info(f" *********** list of error files ************ {error_files}")
logger.info(f" *********** Moving error data to error directory if any ************ ")


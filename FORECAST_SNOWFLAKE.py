# coding=utf-8
import snowflake.connector
import boto3
import subprocess
from time import sleep


#DEFINE PROCESSOR PARAMETERS
PROCESSOR_ACCOUNT =  '<ACCOUNT_NAME>'
PROCESSOR_USER =  '<USER_NAME>'
PROCESSOR_PASSWORD = '<PASSWORD>'
PROCESSOR_WAREHOUSE = 'FORECAST_WH'
PROCESSOR_DATABASE =  'FORECAST_RESULTS'
PROCESSOR_SCHEMA = 'FORECAST_DATA'
PROCESSOR_TABLE = 'FORECAST_RESULTS'
PROCESSOR_STAGE = 'FORECAST_STAGE'
PROCESSOR_S3 = '<S3_PATH>'
PROCESSOR_S3_PUBLIC_KEY = '<OWNER_PUBLIC_KEY>'
PROCESSOR_S3_PRIVATE_KEY = '<OWNER_PRIVATE_KEY>'


#DEFINE OWNER PARAMETERS
OWNER_ACCOUNT = '<ACCOUNT_NAME>'
OWNER_USER = '<USER_NAME>'
OWNER_PASSWORD = '<PASSWORD>'
OWNER_WAREHOUSE = 'FORECAST_WH'
OWNER_DATABASE = 'FORECAST_DATA'
OWNER_SCHEMA = 'FORECAST_DATA'
OWNER_TABLE = 'FORECAST_DATA'
OWNER_STAGE = 'FORECAST_STAGE'
OWNER_S3 = '<S3_PATH>'
OWNER_S3_PUBLIC_KEY = '<OWNER_PUBLIC_KEY>'
OWNER_S3_PRIVATE_KEY = '<OWNER_PRIVATE_KEY>'

#DEFINE FORECAST VARIABLES
S3_BUCKET = PROCESSOR_S3
FILENAME = 'data_0_0_0.csv.gz'

DATASETNAME = 'snowflake_ds_1'
DATASETGROUPNAME = 'snowflake_dsg_1'
PREDICTORNAME = 'snowflake_f_1'
RAW_FILEPATH = PROCESSOR_S3 + '/raw/' + FILENAME

def snowflake_connect(ACCOUNT, USER, PASSWORD, WAREHOUSE):
       con = snowflake.connector.connect(
       account = ACCOUNT,
       user = USER,
       password = PASSWORD,
       )

       cur = con.cursor()
       cur.execute('USE ROLE ACCOUNTADMIN')
       cur.execute('USE WAREHOUSE ' + WAREHOUSE)

       return con.cursor()

def setup_owner():
       #SETUP INITIAL DATASET
       cursor = snowflake_connect(OWNER_ACCOUNT, OWNER_USER, OWNER_PASSWORD, OWNER_WAREHOUSE)
       cursor.execute('CREATE OR REPLACE DATABASE ' + OWNER_DATABASE)
       cursor.execute('CREATE OR REPLACE SCHEMA ' + OWNER_SCHEMA)
       cursor.execute('CREATE OR REPLACE STAGE ' + OWNER_STAGE + ' url=\'' + OWNER_S3 + '\' credentials=(aws_key_id=\'' + OWNER_S3_PUBLIC_KEY + '\' aws_secret_key=\'' + OWNER_S3_PRIVATE_KEY + '\')')
       cursor.execute('CREATE OR REPLACE TABLE ' + OWNER_TABLE + ' (ts timestamp, demand float, id string)')
       cursor.execute('COPY INTO ' + OWNER_TABLE + ' FROM @' + OWNER_STAGE + '/item-demand-time.csv')

       #CREATE SHARE
       cursor.execute('CREATE OR REPLACE SHARE FORECAST_SHARE')
       cursor.execute('GRANT USAGE ON DATABASE ' + OWNER_DATABASE + ' TO SHARE FORECAST_SHARE')
       cursor.execute('GRANT USAGE ON SCHEMA ' + OWNER_SCHEMA + ' TO SHARE FORECAST_SHARE')
       cursor.execute('GRANT SELECT ON TABLE ' + OWNER_TABLE + ' TO SHARE FORECAST_SHARE')
       cursor.execute('ALTER SHARE FORECAST_SHARE ADD ACCOUNTS = ' + PROCESSOR_ACCOUNT)

def setup_processor():
       #CREATE INITIAL SHARE
       cursor = snowflake_connect(PROCESSOR_ACCOUNT, PROCESSOR_USER, PROCESSOR_PASSWORD, PROCESSOR_WAREHOUSE)
       cursor.execute('CREATE OR REPLACE DATABASE ' + OWNER_DATABASE + ' FROM SHARE ' + OWNER_ACCOUNT + '.FORECAST_SHARE')

 #CREATE RESULT SHARE
       cursor.execute('CREATE OR REPLACE DATABASE ' + PROCESSOR_DATABASE)
       cursor.execute('CREATE OR REPLACE SCHEMA ' + PROCESSOR_SCHEMA)
       cursor.execute('CREATE OR REPLACE STAGE ' + PROCESSOR_STAGE + ' url=\'' + PROCESSOR_S3 + '\' credentials=(aws_key_id=\'' + PROCESSOR_S3_PUBLIC_KEY + '\' aws_secret_key=\'' + PROCESSOR_S3_PRIVATE_KEY + '\')')
       cursor.execute('CREATE OR REPLACE TABLE ' + PROCESSOR_TABLE + ' (date datetime, item_id string, first_observation_date datetime, last_observation_date datetime, mean float, p10 float, p50 float, p90 float)')
       cursor.execute('CREATE OR REPLACE SHARE FORECAST_RESULT_SHARE')
       cursor.execute('GRANT USAGE ON DATABASE ' + PROCESSOR_DATABASE + ' TO SHARE FORECAST_RESULT_SHARE')
       cursor.execute('GRANT USAGE ON SCHEMA ' + PROCESSOR_SCHEMA + ' TO SHARE FORECAST_RESULT_SHARE')
       cursor.execute('GRANT SELECT ON TABLE ' + PROCESSOR_TABLE + ' TO SHARE FORECAST_RESULT_SHARE')
       cursor.execute('ALTER SHARE FORECAST_RESULT_SHARE ADD ACCOUNTS = ' + OWNER_ACCOUNT)


#Instantiate Forecast Session
session = boto3.Session(region_name='us-west-2')
forecast = session.client(service_name='forecast')
forecastquery = session.client(service_name='forecastquery')

s3 = session.client('s3')
accountId = boto3.client('sts').get_caller_identity().get('Account')
ROLE_ARN = 'arn:aws:iam::%s:role/amazonforecast'%accountId

#Unload Data From Snowflake to S3

def unload_data_snowflake():
       cursor = snowflake_connect(PROCESSOR_ACCOUNT, PROCESSOR_USER, PROCESSOR_PASSWORD, PROCESSOR_WAREHOUSE)
       cursor.execute('USE DATABASE ' + PROCESSOR_DATABASE)
       cursor.execute('USE SCHEMA ' + PROCESSOR_SCHEMA)
       cursor.execute('COPY INTO @' + PROCESSOR_STAGE + '/raw/ FROM (select * FROM ' + OWNER_DATABASE + '.' + OWNER_SCHEMA +'.' + OWNER_TABLE + ')')

#Create Dataset 
def create_dataset():
        #forecast.delete_dataset(DatasetName=DATASETNAME)
       schema ={
          "Attributes":[
             {
                "AttributeName":"timestamp",
                "AttributeType":"timestamp"
             },
             {
                "AttributeName":"target_value",
                "AttributeType":"float"
             },
             {
                "AttributeName":"item_id",
                "AttributeType":"string"
             }
          ]
       }

       response = forecast.create_dataset( Domain="CUSTOM", DatasetType='TARGET_TIME_SERIES', DataFormat='CSV', DatasetName=DATASETNAME, DataFrequency="H", TimeStampFormat="yyyy-MM-dd hh:mm:ss", Schema=schema)

#Create dataset_group
def create_dataset_group():
        forecast.create_dataset_group(DatasetGroupName=DATASETGROUPNAME, RoleArn=ROLE_ARN, DatasetNames=[DATASETNAME])

#Import DataSet
def import_dataset():
       ds_import_job_response=forecast.create_dataset_import_job(DatasetName=DATASETNAME, Delimiter=',', DatasetGroupName=DATASETGROUPNAME, S3Uri=RAW_FILEPATH)
       ds_versionId=ds_import_job_response['VersionId']

       #Wait for File To Finish Loading
       while True:
               dataImportStatus = forecast.describe_dataset_import_job(DatasetName=DATASETNAME, VersionId=ds_versionId)['Status']
               if (dataImportStatus != 'ACTIVE') and dataImportStatus != 'FAILED':
                       sleep(30)
               else:
                      break

#Create Recipe
def create_predictor():
       createPredictorResponse = forecast.create_predictor(RecipeName='forecast_MQRNN', DatasetGroupName=DATASETGROUPNAME, PredictorName=PREDICTORNAME, ForecastHorizon = 24)
       predictorVersionId=createPredictorResponse['VersionId']

 #Wait for Predictor To Be Created
       while True:
              predictorStatus = forecast.describe_predictor(PredictorName=PREDICTORNAME, 
              VersionId=predictorVersionId)['Status']
              if predictorStatus != 'ACTIVE' and predictorStatus != 'FAILED':
                       sleep(30)
              else:
                       break

                       #Deploy Predictor
def deploy_predictor():
       forecast.deploy_predictor(PredictorName=PREDICTORNAME)

 #Wait for Predictor To Be Deployed
       while True:
              deployedPredictorStatus = forecast.describe_deployed_predictor(PredictorName=PREDICTORNAME)['Status']
              if deployedPredictorStatus != 'ACTIVE' and deployedPredictorStatus != 'FAILED':
                     sleep(30)
              else:
                     break

def get_forecast():
       forecastReponse = forecastquery.get_forecast(
               PredictorName=PREDICTORNAME,
               Interval="hour",
               Filters={"item_id":"client_12"}
       )

def export_data():
       forecastInfoList = forecast.list_forecasts(PredictorName=PREDICTORNAME)['ForecastInfoList']
       forecastId = forecastInfoList[0]['ForecastId']

 #Drop Data Back In S3
       outputPath=S3_BUCKET + "/output"
       forecastExportResponse = forecast.create_forecast_export_job(ForecastId=forecastId, OutputPath={"S3Uri": outputPath, "RoleArn":ROLE_ARN})
       forecastExportJobId = forecastExportResponse['ForecastExportJobId']

 #Wait for Forecast to be Unloaded
       while True:
              forecastExportStatus = forecast.describe_forecast_export_job(ForecastExportJobId=forecastExportJobId)['Status']
              if forecastExportStatus != 'ACTIVE' and forecastExportStatus != 'FAILED':
                     sleep(30)
              else:
                     break


def load_data_snowflake():
       cursor = snowflake_connect(PROCESSOR_ACCOUNT, PROCESSOR_USER, PROCESSOR_PASSWORD, PROCESSOR_WAREHOUSE)
       cursor.execute('USE DATABASE ' + PROCESSOR_DATABASE)
       cursor.execute('USE SCHEMA ' + PROCESSOR_SCHEMA)
       cursor.execute('COPY INTO ' + PROCESSOR_TABLE + ' FROM @' + PROCESSOR_STAGE + '/output file_format=(skip_header=1)')

setup_owner()
setup_processor()
unload_data_snowflake()
create_dataset()
create_dataset_group()
import_dataset()
create_predictor()
deploy_predictor()
get_forecast()
export_data()
load_data_snowflake()

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
import censusdata
import boto3
import time

def extractAndAddIdCols(row):
    values=str(row['geoid']).split(':')
    if (row['geo_level']=='county'):
        row['geo_id'] = values[len(values)-2].strip().strip('> county:')+values[-1].strip()
        row['geo_name'] = values[0].strip()
    elif (row['geo_level']=='city_cdp'):
        row['geo_id'] = values[-1].strip()
        row['geo_name'] = values[0].strip()
    elif (row['geo_level']=='ctract'):
        row['geo_id'] = values[-1].strip()
        row['geo_name'] = values[0].strip()
    elif (row['geo_level']=='zcta'):
        row['geo_id']=values[-1].strip()
        row['geo_name'] = values[0].strip()
    else:
        row['geo_id']='NA'
        row['geo_name'] = 'NA'
    return row


def loadCencusData(args):
    year= int(args['year'])
    age_sex_col= ['B01001_001E', 'B01001_002E', 'B01001_003E', 'B01001_004E', 'B01001_005E', 'B01001_006E', 'B01001_007E', 'B01001_008E',
                  'B01001_009E', 'B01001_010E', 'B01001_011E', 'B01001_012E', 'B01001_013E', 'B01001_014E', 'B01001_015E', 'B01001_016E',
                  'B01001_017E', 'B01001_018E', 'B01001_019E', 'B01001_020E', 'B01001_021E', 'B01001_022E', 'B01001_023E', 'B01001_024E',
                  'B01001_025E', 'B01001_026E', 'B01001_027E', 'B01001_028E', 'B01001_029E', 'B01001_030E', 'B01001_031E', 'B01001_032E',
                  'B01001_033E', 'B01001_034E', 'B01001_035E', 'B01001_036E', 'B01001_037E', 'B01001_038E', 'B01001_039E', 'B01001_040E',
                  'B01001_041E', 'B01001_042E', 'B01001_043E', 'B01001_044E', 'B01001_045E', 'B01001_046E', 'B01001_047E', 'B01001_048E',
                  'B01001_049E']

    agg_age_sex_county = censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'), ('county', '*')]),age_sex_col)
    agg_age_sex_county['geo_level']='county'
    agg_age_sex_place = censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'),('place','*')]),age_sex_col)
    agg_age_sex_place['geo_level']='city_cdp'
    agg_age_sex_tract = censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'),('county', '*'),('tract','*')]),age_sex_col)
    agg_age_sex_tract['geo_level']='ctract'
    agg_age_sex_zipode = censusdata.download('acs5', year, censusdata.censusgeo([('zip code tabulation area', '*')]),age_sex_col)
    agg_age_sex_zipode['geo_level']='zcta'
    agg_age_sex_adults = pd.concat([agg_age_sex_county,agg_age_sex_place,agg_age_sex_tract,agg_age_sex_zipode])
    agg_age_sex_adults.index.rename('geoid', inplace=True)
    agg_age_sex_adults.reset_index(drop=False,inplace=True)
    agg_age_sex_adults = agg_age_sex_adults.rename(columns={
        'B01001_001E': 't_pop', 'B01001_002E': 'm_pop', 'B01001_003E': 'm_lt5', 'B01001_004E': 'm_5_9',
        'B01001_005E': 'm_10_14', 'B01001_006E': 'm_15_17', 'B01001_007E': 'm_18_19', 'B01001_008E': 'm_20',
        'B01001_009E': 'm_21', 'B01001_010E': 'm_22_24', 'B01001_011E': 'm_25_29', 'B01001_012E': 'm_30_34',
        'B01001_013E': 'm_35_39', 'B01001_014E': 'm_40_44', 'B01001_015E': 'm_45_49', 'B01001_016E': 'm_50_54',
        'B01001_017E': 'm_55_59', 'B01001_018E': 'm_60_61', 'B01001_019E': 'm_62_64', 'B01001_020E': 'm_65_66',
        'B01001_021E': 'm_67_69', 'B01001_022E': 'm_70_74', 'B01001_023E': 'm_75_79', 'B01001_024E': 'm_80_84',
        'B01001_025E': 'm_85p', 'B01001_026E': 'f_pop', 'B01001_027E': 'f_lt5', 'B01001_028E': 'f_5_9',
        'B01001_029E': 'f_10_14', 'B01001_030E': 'f_15_17', 'B01001_031E': 'f_18_19', 'B01001_032E': 'f_20',
        'B01001_033E': 'f_21', 'B01001_034E': 'f_22_24', 'B01001_035E': 'f_25_29', 'B01001_036E': 'f_30_34',
        'B01001_037E': 'f_35_39', 'B01001_038E': 'f_40_44', 'B01001_039E': 'f_45_49', 'B01001_040E': 'f_50_54',
        'B01001_041E': 'f_55_59', 'B01001_042E': 'f_60_61', 'B01001_043E': 'f_62_64', 'B01001_044E': 'f_65_66',
        'B01001_045E': 'f_67_69', 'B01001_046E': 'f_70_74', 'B01001_047E': 'f_75_79', 'B01001_048E': 'f_80_84',
        'B01001_049E': 'f_85p'
    })
    age_sex_kids_col=['B09001_001E','B09001_003E','B09001_004E', 'B09001_005E', 'B09001_006E', 'B09001_007E', 'B09001_008E', 'B09001_009E']
    agg_age_sex_kids_county = censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'), ('county', '*')]),age_sex_kids_col)
    agg_age_sex_kids_county['geo_level']='county'
    agg_age_sex_kids_place = censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'),('place','*')]),age_sex_kids_col)
    agg_age_sex_kids_place['geo_level']='city_cdp'
    agg_age_sex_kids_tract = censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'),('county', '*'),('tract','*')]),age_sex_kids_col)
    agg_age_sex_kids_tract['geo_level']='ctract'
    agg_age_sex_kids_zipode = censusdata.download('acs5', year, censusdata.censusgeo([('zip code tabulation area', '*')]),age_sex_kids_col)
    agg_age_sex_kids_zipode['geo_level']='zcta'
    agg_age_sex_kids = pd.concat([agg_age_sex_kids_county,agg_age_sex_kids_place,agg_age_sex_kids_tract,agg_age_sex_kids_zipode])
    agg_age_sex_kids.index.rename('geoid', inplace=True)
    agg_age_sex_kids.reset_index(drop=False,inplace=True)
    agg_age_sex_kids = agg_age_sex_kids.rename(columns={
        'B09001_001E':'t_0_17','B09001_003E':'t_lt3','B09001_004E':'t_3_4',
        'B09001_005E':'t_5', 'B09001_006E':'t_6_8', 'B09001_007E':'t_9_11',
        'B09001_008E':'t_12_14', 'B09001_009E':'t_15_17'
    })
    print('Executing : Census Data Load Transformation')
    #Join data fro all ages
    agg_age_sex = pd.merge(agg_age_sex_adults, agg_age_sex_kids, how='outer', on = ['geoid'])\
        .drop('geo_level_x', axis=1).rename({'geo_level_y': 'geo_level'}, axis=1)
    age_sex_16plus_col=['B23001_001E','B23001_002E','B23001_088E']
    agg_age_sex_16plus_county = censusdata.download('acs5', 2021, censusdata.censusgeo([('state', '06'), ('county', '*')]),age_sex_16plus_col)
    agg_age_sex_16plus_county['geo_level']='county'
    agg_age_sex_16plus_place = censusdata.download('acs5', 2021, censusdata.censusgeo([('state', '06'),('place','*')]),age_sex_16plus_col)
    agg_age_sex_16plus_place['geo_level']='city_cdp'
    agg_age_sex_16plus_tract = censusdata.download('acs5', 2021, censusdata.censusgeo([('state', '06'),('county', '*'),('tract','*')]),age_sex_16plus_col)
    agg_age_sex_16plus_tract['geo_level']='ctract'
    agg_age_sex_16plus_zipode = censusdata.download('acs5', 2021, censusdata.censusgeo([('zip code tabulation area', '*')]),age_sex_16plus_col)
    agg_age_sex_16plus_zipode['geo_level']='zcta'
    agg_age_sex_16plus = pd.concat([agg_age_sex_16plus_county,agg_age_sex_16plus_place,agg_age_sex_16plus_tract,agg_age_sex_16plus_zipode])
    agg_age_sex_16plus.index.rename('geoid', inplace=True)
    agg_age_sex_16plus.reset_index(drop=False,inplace=True)
    agg_age_sex_16plus = agg_age_sex_16plus.rename(columns={
        'B23001_001E':'t_16plus','B23001_002E':'m_16plus','B23001_088E':'f_16plus'
    })
    
    agg_age_sex = agg_age_sex_adults.merge(agg_age_sex_kids,on = ['geoid']) \
        .drop('geo_level_x', axis=1).rename({'geo_level_y': 'geo_level'}, axis=1)\
        .merge(agg_age_sex_16plus,on = ['geoid'])\
        .drop('geo_level_x', axis=1).rename({'geo_level_y': 'geo_level'}, axis=1)

    #Extract new Fields
    agg_age_sex_col = list(agg_age_sex.columns.values)
    agg_age_sex=agg_age_sex.apply(extractAndAddIdCols,axis=1)

    #Fix Column Order
    agg_age_sex_col.remove('geo_level')
    agg_age_sex_col.remove('geoid')
    agg_age_sex_col=['geo_id','geo_name','geo_level'] + agg_age_sex_col

    agg_age_sex= agg_age_sex[agg_age_sex_col]
    totalpopulationSparkDF=spark.createDataFrame(agg_age_sex) 
    targetFile=args['targetFile'] + str(year)
    totalpopulationSparkDF.repartition(1).write.mode("overwrite").parquet(targetFile)
    

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','year','targetFile','crawlerName'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)




print('Executing: Census Data Load for Total Population by Age and Sex')
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.precision', 2)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
loadCencusData(args)
print('Census Data Load for Total Population by Age and Sex Completed')
#Invoking crawler
client = boto3.client('glue')
crawlerName=args['crawlerName']

#Check crawler status before starting
while(1):
    response = client.get_crawler(Name=crawlerName)
    if(response['Crawler']['State']=='READY'):
        client.start_crawler(Name=crawlerName)
        break
    else:
        print("Crawler running. Next check in 30 secs")
        time.sleep(30)


#Check crawler completion status 
while(1):
    response = client.get_crawler(Name=crawlerName)
    if(response['Crawler']['State']=='READY'):
        break
    else:
        print("Crawler running. Next check in 30 secs")
        time.sleep(30)

print('Crawler execution successfully completed')
print('Data load execution successfully completed')
job.commit()

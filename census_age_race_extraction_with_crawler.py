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
    row['geo_id'] = values[len(values)-2].strip().strip('> county:')+values[-1].strip()
    row['geo_name'] = values[0].strip()
    return row


def loadCencusData(args):
    year= int(args['year'])
    AAA_population = censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'),('county', '*')]),['B01001B_001E',	'B01001B_002E',	'B01001B_003E',	'B01001B_004E',	'B01001B_005E',	'B01001B_006E',	'B01001B_007E',	'B01001B_008E',	'B01001B_009E',	'B01001B_010E',	'B01001B_011E',	'B01001B_012E',	'B01001B_013E',	'B01001B_014E',	'B01001B_015E',	'B01001B_016E',	'B01001B_017E',	'B01001B_018E',	'B01001B_019E',	'B01001B_020E',	'B01001B_021E',	'B01001B_022E',	'B01001B_023E',	'B01001B_024E',	'B01001B_025E',	'B01001B_026E',	'B01001B_027E',	'B01001B_028E',	'B01001B_029E',	'B01001B_030E',	'B01001B_031E'])
    AAA_population = AAA_population.rename(columns={
    'B01001B_001E': 't_all',
    'B01001B_002E': 'm_all',
    'B01001B_003E': 'm_lt5',
    'B01001B_004E': 'm_5_9',
    'B01001B_005E': 'm_10_14',
    'B01001B_006E': 'm_15_17',
    'B01001B_007E': 'm_18_19',
    'B01001B_008E': 'm_20_24',
    'B01001B_009E': 'm_25_29',
    'B01001B_010E': 'm_30_34',
    'B01001B_011E': 'm_35_44',
    'B01001B_012E': 'm_45_54',
    'B01001B_013E': 'm_55_64',
    'B01001B_014E': 'm_65_74',
    'B01001B_015E': 'm_75_84',
    'B01001B_016E': 'm_85p',
    'B01001B_017E': 'f_all',
    'B01001B_018E': 'f_lt5',
    'B01001B_019E': 'f_5_9',
    'B01001B_020E': 'f_10_14',
    'B01001B_021E': 'f_15_17',
    'B01001B_022E': 'f_18_19',
    'B01001B_023E': 'f_20_24',
    'B01001B_024E': 'f_25_29',
    'B01001B_025E': 'f_30_34',
    'B01001B_026E': 'f_35_44',
    'B01001B_027E': 'f_45_54',
    'B01001B_028E': 'f_55_64',
    'B01001B_029E': 'f_65_74',
    'B01001B_030E': 'f_75_84',
    'B01001B_031E': 'f_85p'})
    AAA_population['raceth'] = 'AAA'
    # ########## American Indian and Alaskan Native ##############
    AIAN_population = censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'),('county', '*')]),['B01001C_001E','B01001C_002E','B01001C_003E',	'B01001C_004E',	'B01001C_005E',	'B01001C_006E',	'B01001C_007E',	'B01001C_008E',	'B01001C_009E',	'B01001C_010E',	'B01001C_011E',	'B01001C_012E',	'B01001C_013E',	'B01001C_014E',	'B01001C_015E',	'B01001C_016E',	'B01001C_017E',	'B01001C_018E',	'B01001C_019E',	'B01001C_020E',	'B01001C_021E',	'B01001C_022E',	'B01001C_023E',	'B01001C_024E',	'B01001C_025E',	'B01001C_026E',	'B01001C_027E',	'B01001C_028E',	'B01001C_029E',	'B01001C_030E',	'B01001C_031E'])
    AIAN_population = AIAN_population.rename(columns={
        'B01001C_001E': 't_all',
        'B01001C_002E': 'm_all',
        'B01001C_003E': 'm_lt5',
        'B01001C_004E': 'm_5_9',
        'B01001C_005E': 'm_10_14',
        'B01001C_006E': 'm_15_17',
        'B01001C_007E': 'm_18_19',
        'B01001C_008E': 'm_20_24',
        'B01001C_009E': 'm_25_29',
        'B01001C_010E': 'm_30_34',
        'B01001C_011E': 'm_35_44',
        'B01001C_012E': 'm_45_54',
        'B01001C_013E': 'm_55_64',
        'B01001C_014E': 'm_65_74',
        'B01001C_015E': 'm_75_84',
        'B01001C_016E': 'm_85p',
        'B01001C_017E': 'f_all',
        'B01001C_018E': 'f_lt5',
        'B01001C_019E': 'f_5_9',
        'B01001C_020E': 'f_10_14',
        'B01001C_021E': 'f_15_17',
        'B01001C_022E': 'f_18_19',
        'B01001C_023E': 'f_20_24',
        'B01001C_024E': 'f_25_29',
        'B01001C_025E': 'f_30_34',
        'B01001C_026E': 'f_35_44',
        'B01001C_027E': 'f_45_54',
        'B01001C_028E': 'f_55_64',
        'B01001C_029E': 'f_65_74',
        'B01001C_030E': 'f_75_84',
        'B01001C_031E': 'f_85p'})
    AIAN_population['raceth'] = 'AIAN'
    # ################# Asian #####################
    Asian_population = censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'),('county', '*')]),['B01001D_001E',	'B01001D_002E',	'B01001D_003E',	'B01001D_004E',	'B01001D_005E',	'B01001D_006E',	'B01001D_007E',	'B01001D_008E',	'B01001D_009E',	'B01001D_010E',	'B01001D_011E',	'B01001D_012E',	'B01001D_013E',	'B01001D_014E',	'B01001D_015E',	'B01001D_016E',	'B01001D_017E',	'B01001D_018E',	'B01001D_019E',	'B01001D_020E',	'B01001D_021E',	'B01001D_022E',	'B01001D_023E',	'B01001D_024E',	'B01001D_025E',	'B01001D_026E',	'B01001D_027E',	'B01001D_028E',	'B01001D_029E',	'B01001D_030E',	'B01001D_031E'])
    Asian_population = Asian_population.rename(columns={
        'B01001D_001E': 't_all',
        'B01001D_002E': 'm_all',
        'B01001D_003E': 'm_lt5',
        'B01001D_004E': 'm_5_9',
        'B01001D_005E': 'm_10_14',
        'B01001D_006E': 'm_15_17',
        'B01001D_007E': 'm_18_19',
        'B01001D_008E': 'm_20_24',
        'B01001D_009E': 'm_25_29',
        'B01001D_010E': 'm_30_34',
        'B01001D_011E': 'm_35_44',
        'B01001D_012E': 'm_45_54',
        'B01001D_013E': 'm_55_64',
        'B01001D_014E': 'm_65_74',
        'B01001D_015E': 'm_75_84',
        'B01001D_016E': 'm_85p',
        'B01001D_017E': 'f_all',
        'B01001D_018E': 'f_lt5',
        'B01001D_019E': 'f_5_9',
        'B01001D_020E': 'f_10_14',
        'B01001D_021E': 'f_15_17',
        'B01001D_022E': 'f_18_19',
        'B01001D_023E': 'f_20_24',
        'B01001D_024E': 'f_25_29',
        'B01001D_025E': 'f_30_34',
        'B01001D_026E': 'f_35_44',
        'B01001D_027E': 'f_45_54',
        'B01001D_028E': 'f_55_64',
        'B01001D_029E': 'f_65_74',
        'B01001D_030E': 'f_75_84',
        'B01001D_031E': 'f_85p'})
    Asian_population['raceth'] = 'Asian'
    ############### Native Hawaiian and other Pacific Islander ############
    PI_population = censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'),('county', '*')]),['B01001E_001E',	'B01001E_002E',	'B01001E_003E',	'B01001E_004E',	'B01001E_005E',	'B01001E_006E',	'B01001E_007E',	'B01001E_008E',	'B01001E_009E',	'B01001E_010E',	'B01001E_011E',	'B01001E_012E',	'B01001E_013E',	'B01001E_014E',	'B01001E_015E',	'B01001E_016E',	'B01001E_017E',	'B01001E_018E',	'B01001E_019E',	'B01001E_020E',	'B01001E_021E',	'B01001E_022E',	'B01001E_023E',	'B01001E_024E',	'B01001E_025E',	'B01001E_026E',	'B01001E_027E',	'B01001E_028E',	'B01001E_029E',	'B01001E_030E',	'B01001E_031E'])
    PI_population = PI_population.rename(columns={
        'B01001E_001E': 't_all',
        'B01001E_002E': 'm_all',
        'B01001E_003E': 'm_lt5',
        'B01001E_004E': 'm_5_9',
        'B01001E_005E': 'm_10_14',
        'B01001E_006E': 'm_15_17',
        'B01001E_007E': 'm_18_19',
        'B01001E_008E': 'm_20_24',
        'B01001E_009E': 'm_25_29',
        'B01001E_010E': 'm_30_34',
        'B01001E_011E': 'm_35_44',
        'B01001E_012E': 'm_45_54',
        'B01001E_013E': 'm_55_64',
        'B01001E_014E': 'm_65_74',
        'B01001E_015E': 'm_75_84',
        'B01001E_016E': 'm_85p',
        'B01001E_017E': 'f_all',
        'B01001E_018E': 'f_lt5',
        'B01001E_019E': 'f_5_9',
        'B01001E_020E': 'f_10_14',
        'B01001E_021E': 'f_15_17',
        'B01001E_022E': 'f_18_19',
        'B01001E_023E': 'f_20_24',
        'B01001E_024E': 'f_25_29',
        'B01001E_025E': 'f_30_34',
        'B01001E_026E': 'f_35_44',
        'B01001E_027E': 'f_45_54',
        'B01001E_028E': 'f_55_64',
        'B01001E_029E': 'f_65_74',
        'B01001E_030E': 'f_75_84',
        'B01001E_031E': 'f_85p'})
    PI_population['raceth'] = 'PI'
    ###################### Some Other Race ##################
    Sother_population = censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'),('county', '*')]),['B01001F_001E',	'B01001F_002E',	'B01001F_003E',	'B01001F_004E',	'B01001F_005E',	'B01001F_006E',	'B01001F_007E',	'B01001F_008E',	'B01001F_009E',	'B01001F_010E',	'B01001F_011E',	'B01001F_012E',	'B01001F_013E',	'B01001F_014E',	'B01001F_015E',	'B01001F_016E',	'B01001F_017E',	'B01001F_018E',	'B01001F_019E',	'B01001F_020E',	'B01001F_021E',	'B01001F_022E',	'B01001F_023E',	'B01001F_024E',	'B01001F_025E',	'B01001F_026E',	'B01001F_027E',	'B01001F_028E',	'B01001F_029E',	'B01001F_030E',	'B01001F_031E'])
    Sother_population = Sother_population.rename(columns={
        'B01001F_001E': 't_all',
        'B01001F_002E': 'm_all',
        'B01001F_003E': 'm_lt5',
        'B01001F_004E': 'm_5_9',
        'B01001F_005E': 'm_10_14',
        'B01001F_006E': 'm_15_17',
        'B01001F_007E': 'm_18_19',
        'B01001F_008E': 'm_20_24',
        'B01001F_009E': 'm_25_29',
        'B01001F_010E': 'm_30_34',
        'B01001F_011E': 'm_35_44',
        'B01001F_012E': 'm_45_54',
        'B01001F_013E': 'm_55_64',
        'B01001F_014E': 'm_65_74',
        'B01001F_015E': 'm_75_84',
        'B01001F_016E': 'm_85p',
        'B01001F_017E': 'f_all',
        'B01001F_018E': 'f_lt5',
        'B01001F_019E': 'f_5_9',
        'B01001F_020E': 'f_10_14',
        'B01001F_021E': 'f_15_17',
        'B01001F_022E': 'f_18_19',
        'B01001F_023E': 'f_20_24',
        'B01001F_024E': 'f_25_29',
        'B01001F_025E': 'f_30_34',
        'B01001F_026E': 'f_35_44',
        'B01001F_027E': 'f_45_54',
        'B01001F_028E': 'f_55_64',
        'B01001F_029E': 'f_65_74',
        'B01001F_030E': 'f_75_84',
        'B01001F_031E': 'f_85p'})
    Sother_population['raceth'] = 'Sother'
    ################ Two or more races ###############
    Twomore_population = censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'),('county', '*')]),['B01001G_001E',	'B01001G_002E',	'B01001G_003E',	'B01001G_004E',	'B01001G_005E',	'B01001G_006E',	'B01001G_007E',	'B01001G_008E',	'B01001G_009E',	'B01001G_010E',	'B01001G_011E',	'B01001G_012E',	'B01001G_013E',	'B01001G_014E',	'B01001G_015E',	'B01001G_016E',	'B01001G_017E',	'B01001G_018E',	'B01001G_019E',	'B01001G_020E',	'B01001G_021E',	'B01001G_022E',	'B01001G_023E',	'B01001G_024E',	'B01001G_025E',	'B01001G_026E',	'B01001G_027E',	'B01001G_028E',	'B01001G_029E',	'B01001G_030E',	'B01001G_031E'])
    Twomore_population = Twomore_population.rename(columns={
        'B01001G_001E': 't_all',
        'B01001G_002E': 'm_all',
        'B01001G_003E': 'm_lt5',
        'B01001G_004E': 'm_5_9',
        'B01001G_005E': 'm_10_14',
        'B01001G_006E': 'm_15_17',
        'B01001G_007E': 'm_18_19',
        'B01001G_008E': 'm_20_24',
        'B01001G_009E': 'm_25_29',
        'B01001G_010E': 'm_30_34',
        'B01001G_011E': 'm_35_44',
        'B01001G_012E': 'm_45_54',
        'B01001G_013E': 'm_55_64',
        'B01001G_014E': 'm_65_74',
        'B01001G_015E': 'm_75_84',
        'B01001G_016E': 'm_85p',
        'B01001G_017E': 'f_all',
        'B01001G_018E': 'f_lt5',
        'B01001G_019E': 'f_5_9',
        'B01001G_020E': 'f_10_14',
        'B01001G_021E': 'f_15_17',
        'B01001G_022E': 'f_18_19',
        'B01001G_023E': 'f_20_24',
        'B01001G_024E': 'f_25_29',
        'B01001G_025E': 'f_30_34',
        'B01001G_026E': 'f_35_44',
        'B01001G_027E': 'f_45_54',
        'B01001G_028E': 'f_55_64',
        'B01001G_029E': 'f_65_74',
        'B01001G_030E': 'f_75_84',
        'B01001G_031E': 'f_85p'})
    Twomore_population['raceth'] = 'Twomore'
    #################### WhitE', not Hispanic ################
    White_NH_population = censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'),('county', '*')]),['B01001H_001E',	'B01001H_002E',	'B01001H_003E',	'B01001H_004E',	'B01001H_005E',	'B01001H_006E',	'B01001H_007E',	'B01001H_008E',	'B01001H_009E',	'B01001H_010E',	'B01001H_011E',	'B01001H_012E',	'B01001H_013E',	'B01001H_014E',	'B01001H_015E',	'B01001H_016E',	'B01001H_017E',	'B01001H_018E',	'B01001H_019E',	'B01001H_020E',	'B01001H_021E',	'B01001H_022E',	'B01001H_023E',	'B01001H_024E',	'B01001H_025E',	'B01001H_026E',	'B01001H_027E',	'B01001H_028E',	'B01001H_029E',	'B01001H_030E',	'B01001H_031E'])
    White_NH_population = White_NH_population.rename(columns={
        'B01001H_001E': 't_all',
        'B01001H_002E': 'm_all',
        'B01001H_003E': 'm_lt5',
        'B01001H_004E': 'm_5_9',
        'B01001H_005E': 'm_10_14',
        'B01001H_006E': 'm_15_17',
        'B01001H_007E': 'm_18_19',
        'B01001H_008E': 'm_20_24',
        'B01001H_009E': 'm_25_29',
        'B01001H_010E': 'm_30_34',
        'B01001H_011E': 'm_35_44',
        'B01001H_012E': 'm_45_54',
        'B01001H_013E': 'm_55_64',
        'B01001H_014E': 'm_65_74',
        'B01001H_015E': 'm_75_84',
        'B01001H_016E': 'm_85p',
        'B01001H_017E': 'f_all',
        'B01001H_018E': 'f_lt5',
        'B01001H_019E': 'f_5_9',
        'B01001H_020E': 'f_10_14',
        'B01001H_021E': 'f_15_17',
        'B01001H_022E': 'f_18_19',
        'B01001H_023E': 'f_20_24',
        'B01001H_024E': 'f_25_29',
        'B01001H_025E': 'f_30_34',
        'B01001H_026E': 'f_35_44',
        'B01001H_027E': 'f_45_54',
        'B01001H_028E': 'f_55_64',
        'B01001H_029E': 'f_65_74',
        'B01001H_030E': 'f_75_84',
        'B01001H_031E': 'f_85p'})
    White_NH_population['raceth'] = 'White_NH'
    ##################### Latino or Hispanic ######################
    Latino_population = censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'),('county', '*')]),['B01001I_001E',	'B01001I_002E',	'B01001I_003E',	'B01001I_004E',	'B01001I_005E',	'B01001I_006E',	'B01001I_007E',	'B01001I_008E',	'B01001I_009E',	'B01001I_010E',	'B01001I_011E',	'B01001I_012E',	'B01001I_013E',	'B01001I_014E',	'B01001I_015E',	'B01001I_016E',	'B01001I_017E',	'B01001I_018E',	'B01001I_019E',	'B01001I_020E',	'B01001I_021E',	'B01001I_022E',	'B01001I_023E',	'B01001I_024E',	'B01001I_025E',	'B01001I_026E',	'B01001I_027E',	'B01001I_028E',	'B01001I_029E',	'B01001I_030E',	'B01001I_031E'])
    Latino_population = Latino_population.rename(columns={
        'B01001I_001E': 't_all',
        'B01001I_002E': 'm_all',
        'B01001I_003E': 'm_lt5',
        'B01001I_004E': 'm_5_9',
        'B01001I_005E': 'm_10_14',
        'B01001I_006E': 'm_15_17',
        'B01001I_007E': 'm_18_19',
        'B01001I_008E': 'm_20_24',
        'B01001I_009E': 'm_25_29',
        'B01001I_010E': 'm_30_34',
        'B01001I_011E': 'm_35_44',
        'B01001I_012E': 'm_45_54',
        'B01001I_013E': 'm_55_64',
        'B01001I_014E': 'm_65_74',
        'B01001I_015E': 'm_75_84',
        'B01001I_016E': 'm_85p',
        'B01001I_017E': 'f_all',
        'B01001I_018E': 'f_lt5',
        'B01001I_019E': 'f_5_9',
        'B01001I_020E': 'f_10_14',
        'B01001I_021E': 'f_15_17',
        'B01001I_022E': 'f_18_19',
        'B01001I_023E': 'f_20_24',
        'B01001I_024E': 'f_25_29',
        'B01001I_025E': 'f_30_34',
        'B01001I_026E': 'f_35_44',
        'B01001I_027E': 'f_45_54',
        'B01001I_028E': 'f_55_64',
        'B01001I_029E': 'f_65_74',
        'B01001I_030E': 'f_75_84',
        'B01001I_031E': 'f_85p'})
    Latino_population['raceth'] = 'Latino'
    race_population=pd.concat([AAA_population,AIAN_population,Asian_population,PI_population,Sother_population,Twomore_population,White_NH_population,Latino_population])
    race_population.index.rename('geoid', inplace=True)
    race_population.reset_index(drop=False,inplace=True)

    AAA_population_16plus = censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'),('county', '*')]),['C23002B_001E','C23002B_002E','C23002B_015E'])
    AAA_population_16plus = AAA_population_16plus.rename(columns={
    'C23002B_001E': 't_16plus',
    'C23002B_002E': 'm_16plus',
    'C23002B_015E': 'f_16plus'})
    AAA_population_16plus['raceth'] = 'AAA'
    # ########## American Indian and Alaskan Native ##############

    AIAN_population_16plus = censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'),('county', '*')]),['C23002C_001E','C23002C_002E','C23002C_015E'])
    AIAN_population_16plus = AIAN_population_16plus.rename(columns={
    'C23002C_001E': 't_16plus',
    'C23002C_002E': 'm_16plus',
    'C23002C_015E': 'f_16plus'})
    AIAN_population_16plus['raceth'] = 'AIAN'
    # ################# Asian #####################

    Asian_population_16plus = censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'),('county', '*')]),['C23002D_001E','C23002D_002E','C23002D_015E'])
    Asian_population_16plus = Asian_population_16plus.rename(columns={
    'C23002D_001E': 't_16plus',
    'C23002D_002E': 'm_16plus',
    'C23002D_015E': 'f_16plus'})
    Asian_population_16plus['raceth'] = 'Asian'
    ############### Native Hawaiian and other Pacific Islander ############

    PI_population_16plus = censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'),('county', '*')]),['C23002E_001E','C23002E_002E','C23002E_015E'])
    PI_population_16plus = PI_population_16plus.rename(columns={
    'C23002E_001E': 't_16plus',
    'C23002E_002E': 'm_16plus',
    'C23002E_015E': 'f_16plus'})
    PI_population_16plus['raceth'] = 'PI'
    ###################### Some Other Race ##################
    Sother_population_16plus =  censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'),('county', '*')]),['C23002F_001E','C23002F_002E','C23002F_015E'])
    Sother_population_16plus = Sother_population_16plus.rename(columns={
    'C23002F_001E': 't_16plus',
    'C23002F_002E': 'm_16plus',
    'C23002F_015E': 'f_16plus'})
    Sother_population_16plus['raceth'] = 'Sother'
    ################ Two or more races ###############
    Twomore_population_16plus = censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'),('county', '*')]),['C23002G_001E','C23002G_002E','C23002G_015E'])
    Twomore_population_16plus = Twomore_population_16plus.rename(columns={
    'C23002G_001E': 't_16plus',
    'C23002G_002E': 'm_16plus',
    'C23002G_015E': 'f_16plus'})
    Twomore_population_16plus['raceth'] = 'Twomore'
    #################### WhitE', not Hispanic ################
    White_NH_population_16plus = censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'),('county', '*')]),['C23002H_001E','C23002H_002E','C23002H_015E'])
    White_NH_population_16plus = White_NH_population_16plus.rename(columns={
    'C23002H_001E': 't_16plus',
    'C23002H_002E': 'm_16plus',
    'C23002H_015E': 'f_16plus'})
    White_NH_population_16plus['raceth'] = 'White_NH'
    ##################### Latino or Hispanic ######################
    Latino_population_16plus = censusdata.download('acs5', year, censusdata.censusgeo([('state', '06'),('county', '*')]),['C23002I_001E','C23002I_002E','C23002I_015E'])
    Latino_population_16plus = Latino_population_16plus.rename(columns={
    'C23002I_001E': 't_16plus',
    'C23002I_002E': 'm_16plus',
    'C23002I_015E': 'f_16plus'})
    Latino_population_16plus['raceth'] = 'Latino'
    race_population_16plus=pd.concat([AAA_population_16plus,AIAN_population_16plus,Asian_population_16plus,PI_population_16plus,Sother_population_16plus,Twomore_population_16plus,White_NH_population_16plus,Latino_population_16plus])
    race_population_16plus.index.rename('geoid', inplace=True)
    race_population_16plus.reset_index(drop=False,inplace=True)
    
    race_population = race_population.merge(race_population_16plus,on = ['geoid','raceth'])
    
    race_population_cols = list(race_population.columns.values)
    race_population = race_population.apply(extractAndAddIdCols,axis=1).drop('geoid', axis=1)
    race_population_cols.remove("geoid")
    race_population_cols.remove("raceth")
    race_population_cols = ['geo_id', 'geo_name', 'raceth'] + race_population_cols
    race_population = race_population[race_population_cols]
    totalpopulationSparkDF=spark.createDataFrame(race_population) 
    targetFile=args['targetFile'] + 'census_age_sex_race/census_year=' + str(year)
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

import pandas as pd
import censusdata
import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.precision', 2)

spark = SparkSession.builder.master("local[1]") \
                    .appName('meenakshisundaram') \
                    .getOrCreate()
# list to hold all the specified year's census data
totalPopulationPandasDFByYear=[]
for year in [2021,2019]:
    totalpopulation = censusdata.download('acs5', year, censusdata.censusgeo([('county', '*')]),
                                   ['B01001_001E','B01001_026E','B01001_029E','B01001_030E','B01001_031E','B01001_032E',
                                    'B01001_033E','B01001_034E','B01001_035E', 'B01001_036E', 'B01001_037E', 'B01001_038E',
                                    'B01001_039E', 'B01001_028E', 'B01001_040E', 'B01001_041E', 'B01001_042E', 'B01001_043E',
                                    'B01001_044E', 'B01001_045E', 'B01001_046E', 'B01001_047E', 'B01001_048E', 'B01001_049E',
                                    'B01001_027E', 'B01001_002E', 'B01001_005E', 'B01001_006E', 'B01001_007E', 'B01001_008E',
                                    'B01001_009E', 'B01001_010E', 'B01001_011E', 'B01001_012E', 'B01001_013E', 'B01001_014E',
                                    'B01001_015E', 'B01001_004E', 'B01001_016E', 'B01001_017E', 'B01001_018E', 'B01001_019E',
                                    'B01001_020E', 'B01001_021E', 'B01001_022E', 'B01001_023E', 'B01001_024E', 'B01001_025E',
                                    'B01001_003E'])
    totalpopulation['year'] = year
    totalPopulationPandasDFByYear.append(totalpopulation)
# List of dataframes to one big dataframe
totalPopulationPandasDFByYear=pd.concat(totalPopulationPandasDFByYear)

# Renaming the index as it was taking the first column as index and dropping while changing from pandasDF to sparkDF
totalPopulationPandasDFByYear.index.rename('county_desc', inplace=True)
totalPopulationPandasDFByYear.reset_index(drop=False,inplace=True)

# sparkDF from PandasDF
sparkDF = spark.createDataFrame(totalPopulationPandasDFByYear)

# creating multiple columns with the first column
sparkDF = sparkDF.withColumn("stateNumber", sparkDF["county_desc.geo._1._2"])\
                .withColumn("countyNumber", sparkDF["county_desc.geo._2._2"])\
                .withColumn("countyName", split(sparkDF["county_desc.name"], ',').getItem(0))\
                .withColumn("stateName", split(sparkDF["county_desc.name"], ',').getItem(1)).drop(sparkDF['county_desc'])
# Renaming the columns
oldNewNames = {'B01001_001E' : 'est_t', 'B01001_026E' : 'est_t_f', 'B01001_029E' : 'est_t_f_10_14', 'B01001_030E' : 'est_t_f_15_17', 'B01001_031E' : 'est_t_f_18_19', 'B01001_032E' : 'est_t_f_20', 'B01001_033E' : 'est_t_f_21', 'B01001_034E' : 'est_t_f_22_24', 'B01001_035E' : 'est_t_f_25_29', 'B01001_036E' : 'est_t_f_30_34', 'B01001_037E' : 'est_t_f_35_39', 'B01001_038E' : 'est_t_f_40_44', 'B01001_039E' : 'est_t_f_45_49', 'B01001_028E' : 'est_t_f_5_9', 'B01001_040E' : 'est_t_f_50_54', 'B01001_041E' : 'est_t_f_55_59', 'B01001_042E' : 'est_t_f_60_61', 'B01001_043E' : 'est_t_f_62_64', 'B01001_044E' : 'est_t_f_65_66', 'B01001_045E' : 'est_t_f_67_69', 'B01001_046E' : 'est_t_f_70_74', 'B01001_047E' : 'est_t_f_75_79', 'B01001_048E' : 'est_t_f_80_84', 'B01001_049E' : 'est_t_f_85_plus', 'B01001_027E' : 'est_t_f_5_under', 'B01001_002E' : 'est_t_m', 'B01001_005E' : 'est_t_m_10_14', 'B01001_006E' : 'est_t_m_15_17', 'B01001_007E' : 'est_t_m_18_19', 'B01001_008E' : 'est_t_m_20', 'B01001_009E' : 'est_t_m_21', 'B01001_010E' : 'est_t_m_22_24', 'B01001_011E' : 'est_t_m_25_29', 'B01001_012E' : 'est_t_m_30_34', 'B01001_013E' : 'est_t_m_35_39', 'B01001_014E' : 'est_t_m_40_44', 'B01001_015E' : 'est_t_m_45_49', 'B01001_004E' : 'est_t_m_5_9', 'B01001_016E' : 'est_t_m_50_54', 'B01001_017E' : 'est_t_m_55_59', 'B01001_018E' : 'est_t_m_60_61', 'B01001_019E' : 'est_t_m_62_64', 'B01001_020E' : 'est_t_m_65_66', 'B01001_021E' : 'est_t_m_67_69', 'B01001_022E' : 'est_t_m_70_74', 'B01001_023E' : 'est_t_m_75_79', 'B01001_024E' : 'est_t_m_80_84', 'B01001_025E' : 'est_t_m_85_plus', 'B01001_003E' : 'est_t_m_5_under'}
for key, value in oldNewNames.items():
    sparkDF = sparkDF.withColumnRenamed(key, value)
sparkDF.show(5, False)



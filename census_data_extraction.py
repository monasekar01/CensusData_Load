import pandas as pd
import censusdata
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.precision', 2)

# county65plus = censusdata.download('acs1', 2021, censusdata.censusgeo([('county', '*')]),
#                                    ['B01001_001E', 'B01001_020E', 'B01001_021E', 'B01001_022E', 'B01001_023E',
#                                     'B01001_024E', 'B01001_025E', 'B01001_044E', 'B01001_045E', 'B01001_046E',
#                                     'B01001_047E', 'B01001_048E', 'B01001_049E'])

totalpopulation = censusdata.download('acs1', 2021, censusdata.censusgeo([('county', '*')]),
                                   ['B01001_001E','B01001_026E','B01001_029E','B01001_030E','B01001_031E','B01001_032E',
                                    'B01001_033E','B01001_034E','B01001_035E', 'B01001_036E', 'B01001_037E', 'B01001_038E',
                                    'B01001_039E', 'B01001_028E', 'B01001_040E', 'B01001_041E', 'B01001_042E', 'B01001_043E',
                                    'B01001_044E', 'B01001_045E', 'B01001_046E', 'B01001_047E', 'B01001_048E', 'B01001_049E',
                                    'B01001_027E', 'B01001_002E', 'B01001_005E', 'B01001_006E', 'B01001_007E', 'B01001_008E',
                                    'B01001_009E', 'B01001_010E', 'B01001_011E', 'B01001_012E', 'B01001_013E', 'B01001_014E',
                                    'B01001_015E', 'B01001_004E', 'B01001_016E', 'B01001_017E', 'B01001_018E', 'B01001_019E',
                                    'B01001_020E', 'B01001_021E', 'B01001_022E', 'B01001_023E', 'B01001_024E', 'B01001_025E',
                                    'B01001_003E'])

totalpopulation.to_csv("/Users/monasekar/PycharmProjects/pythonProject/census-data/totalpopulation.csv")

# county65plus = censusdata.download('acs5', 2015, censusdata.censusgeo([('county', '*')]),
#                                    ['B01001_001E', 'B01001_020E', 'B01001_021E', 'B01001_022E', 'B01001_023E',
#                                     'B01001_024E', 'B01001_025E', 'B01001_044E', 'B01001_045E', 'B01001_046E',
#                                     'B01001_047E', 'B01001_048E', 'B01001_049E'])
# county65plus.to_csv("/Users/monasekar/PycharmProjects/pythonProject/census-data/county65plus1.csv")
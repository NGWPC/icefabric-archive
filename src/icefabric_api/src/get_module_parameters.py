import pandas as pd
import geopandas as gpd

param_file = 'cfe_params.csv'
gpkg_file = 'gauge_06710385.gpkg'


divides = gpd.read_file(gpkg_file,layer='divides')
divides = divides['divide_id'].to_list()


module_params = pd.read_csv(param_file)
param_values = module_params[['name','default_value']]

for divide in divides:

    cfg_file = f'{divide}_bmi_cfg_cfe.txt'
    f = open(cfg_file, "x")

    for index, row in param_values.iterrows():
        key = row['name']
        value = row['default_value']
        f.write(f'{key}={value}\n')

    f.close()


params_calibratable = module_params.loc[module_params['calibratable'] == 'TRUE']
params_calibratable.to_json('out.json',orient='split')




    





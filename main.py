# -*- coding: utf-8 -*-
# CENSUS 2011: LC3409EW (General health by tenure by age) - Nomis - Official Labour Market Statistics
from gssutils import * 
import pandas as pd
import json
from datetime import datetime

dataE = pd.read_csv("final_df_england.csv") 
dataW = pd.read_csv("final_df_wales.csv")

joined_dat = pd.concat([dataE, dataW])
#print('England: ' + str(dataE['DATE_NAME'].count()))
#print('Wales: ' + str(dataW['DATE_NAME'].count()))
#print('Joined: ' + str(joined_dat['DATE_NAME'].count()))

joined_dat.head(10)

joined_dat = joined_dat[['DATE_NAME', 'GEOGRAPHY_CODE', 'C_TENHUK11_NAME', 'C_AGE_NAME', 'C_HEALTH_NAME', 'OBS_VALUE']]

joined_dat = joined_dat.rename(columns={'OBS_VALUE': 'Value'})

joined_dat['C_TENHUK11_NAME'] = joined_dat['C_TENHUK11_NAME'].apply(pathify)
joined_dat['C_AGE_NAME'] = joined_dat['C_AGE_NAME'].apply(pathify)
joined_dat['C_HEALTH_NAME'] = joined_dat['C_HEALTH_NAME'].apply(pathify)

# +
# Have to add to metadata 
# The Year: 2011
# Geography Type: 2011 output areas
# -

# Output the data to CSV
csvName = 'observations.csv'
out = Path('out')
out.mkdir(exist_ok=True)
joined_dat.drop_duplicates().to_csv(out / csvName, index = False)

# +
scrape.dataset.family = 'census-2011'
scrape.dataset.description = 'CENSUS 2011 - LC3409EW (General health by tenure by age) - Nomis - Official Labour Market Statistics/n' + notes

# Output CSV-W metadata (validation, transform and DSD).
# Output dataset metadata separately for now.

import os
from urllib.parse import urljoin

dataset_path = pathify(os.environ.get('JOB_NAME', 'gss_data/covid-19/' + Path(os.getcwd()).name)) + '-' + pathify(csvName)
scrape.set_base_uri('http://gss-data.org.uk')
scrape.set_dataset_id(dataset_path)
scrape.dataset.title = 'CENSUS 2011 - LC3409EW (General health by tenure by age) - Nomis - Official Labour Market Statistics'
csvw_transform = CSVWMapping()
csvw_transform.set_csv(out / csvName)
csvw_transform.set_mapping(json.load(open('info.json')))
csvw_transform.set_dataset_uri(urljoin(scrape._base_uri, f'data/{scrape._dataset_id}'))
csvw_transform.write(out / f'{csvName}-metadata.json')
with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
    metadata.write(scrape.generate_trig())
# -


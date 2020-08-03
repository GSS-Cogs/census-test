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

joined_dat = joined_dat.drop('Unnamed: 0', 1)
joined_dat = joined_dat.drop('GEOGRAPHY_NAME', 1)
joined_dat = joined_dat.drop('GEOGRAPHY_TYPE', 1)
joined_dat = joined_dat.drop('C_TENHUK11', 1)
joined_dat = joined_dat.drop('C_AGE', 1)
joined_dat = joined_dat.drop('C_HEALTH', 1)
joined_dat = joined_dat.drop('RECORD_OFFSET', 1)
joined_dat = joined_dat.drop('RECORD_COUNT', 1)

joined_dat['C_TENHUK11_NAME'] = joined_dat['C_TENHUK11_NAME'].apply(pathify)
joined_dat['C_TENHUK11_NAME'] = joined_dat['C_AGE_NAME'].apply(pathify)
joined_dat['C_TENHUK11_NAME'] = joined_dat['C_HEALTH_NAME'].apply(pathify)









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
scrape.dataset.family = 'covid-19'
scrape.dataset.description = 'PHE COVID-19 number of outbreaks in care homes – Management Information/n' + notes

# Output CSV-W metadata (validation, transform and DSD).
# Output dataset metadata separately for now.

import os
from urllib.parse import urljoin

dataset_path = pathify(os.environ.get('JOB_NAME', 'gss_data/covid-19/' + Path(os.getcwd()).name)) + '-' + pathify(csvName)
scrape.set_base_uri('http://gss-data.org.uk')
scrape.set_dataset_id(dataset_path)
scrape.dataset.title = 'PHE COVID-19 number of outbreaks in care homes – Management Information'
csvw_transform = CSVWMapping()
csvw_transform.set_csv(out / csvName)
csvw_transform.set_mapping(json.load(open('info.json')))
csvw_transform.set_dataset_uri(urljoin(scrape._base_uri, f'data/{scrape._dataset_id}'))
csvw_transform.write(out / f'{csvName}-metadata.json')
with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
    metadata.write(scrape.generate_trig())
# +
#info = json.load(open('info.json')) 
#codelistcreation = info['transform']['codelists'] 
#print(codelistcreation)
#print("-------------------------------------------------------")
#print(joined_dat.columns)

#codeclass = CSVCodelists()
#for cl in codelistcreation:
#    if cl in joined_dat.columns:
#        joined_dat[cl] = joined_dat[cl].str.replace("-"," ")
#        joined_dat[cl] = joined_dat[cl].str.capitalize()
#        codeclass.create_codelists(pd.DataFrame(joined_dat[cl]), 'codelists', scrape.dataset.family, Path(os.getcwd()).name.lower())

# +
#joined_dat.head(60)
# -



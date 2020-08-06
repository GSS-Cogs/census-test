# -*- coding: utf-8 -*-
# CENSUS 2011: LC3409EW (General health by tenure by age) - Nomis - Official Labour Market Statistics
from gssutils import * 
import pandas as pd
import json
from datetime import datetime

scraper = Scraper(seed='info.json')

#dataE = pd.read_csv("final_df_england.csv.gz", compression='gzip', dtype='category') 
dataW = pd.read_csv("final_df_wales.csv.gz", compression='gzip', dtype='category')
#dataW.head(5)

#----------------------------------------------------------------------------------------
#joined_dat = pd.concat([dataE, dataW])
joined_dat = dataW
#----------------------------------------------------------------------------------------
#print('England: ' + str(dataE['DATE_NAME'].count()))
#print('Wales: ' + str(dataW['DATE_NAME'].count()))
#print('Joined: ' + str(joined_dat['DATE_NAME'].count()))

joined_dat = joined_dat[['GEOGRAPHY_CODE', 'C_TENHUK11_NAME', 'C_AGE_NAME', 'C_HEALTH_NAME', 'OBS_VALUE']]

joined_dat = joined_dat.rename(columns={
    'GEOGRAPHY_CODE': 'Geography',
    'C_TENHUK11_NAME': 'Tenure',
    'C_AGE_NAME': 'Age',
    'C_HEALTH_NAME': 'Health',
    'OBS_VALUE': 'Value'
})

# +
#joined_dat.head(5)
#print(list(joined_dat['Tenure'].unique()))

# +
#joined_dat['C_TENHUK11_NAME'] = joined_dat['C_TENHUK11_NAME'].apply(pathify)
#joined_dat['C_AGE_NAME'] = joined_dat['C_AGE_NAME'].apply(pathify)
#joined_dat['C_HEALTH_NAME'] = joined_dat['C_HEALTH_NAME'].apply(pathify)

# +
# Have to add to metadata 
# The Year: 2011
# Geography Type: 2011 output areas
# -

# LIMIT OUTPUT JUST TO TEST THE JENKINS PIPELINE
print(joined_dat['Tenure'].count())
joined_dat = joined_dat[0:10000]
print(joined_dat['Tenure'].count())
#joined_dat.head(60)

# Output the data to CSV
csvName = 'observations.csv'
out = Path('out')
out.mkdir(exist_ok=True)
joined_dat.drop_duplicates().to_csv(out / (csvName + '.gz'), index = False, compression='gzip')

# +
from urllib.parse import urljoin

scraper.dataset.family = 'census'

dataset_path = pathify(os.environ.get('JOB_NAME', 'gss_data/census-2011'))
scraper.set_base_uri('http://gss-data.org.uk')
scraper.set_dataset_id(dataset_path)
csvw_transform = CSVWMapping()
csvw_transform.set_csv(out / csvName)
csvw_transform.set_mapping(json.load(open('info.json')))
csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
joined_dat[:10].to_csv(out / csvName, index = False)
csvw_transform.write(out / f'{csvName}-metadata.json')
(out / csvName).
with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())
# -



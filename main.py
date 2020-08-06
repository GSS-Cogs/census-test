# -*- coding: utf-8 -*-
# CENSUS 2011: LC3409EW (General health by tenure by age) - Nomis - Official Labour Market Statistics
from gssutils import * 
import pandas as pd
import json
from datetime import datetime

scraper = Scraper(seed='info.json')

england = pd.read_csv("final_df_england.csv.gz", compression='gzip', dtype='category')
england['C_TENHUK11_NAME'].cat.rename_categories(pathify, inplace=True)
england['C_HEALTH_NAME'].cat.rename_categories(pathify, inplace=True)
england['C_AGE_NAME'].cat.rename_categories({
    'Age 0 to 15': 'Y0T15',
    'Age 16 to 49': 'Y16T49',
    'Age 50 to 64': 'Y50T64',
    'Age 65 and over': 'Y_GE65'
}, inplace=True)
wales = pd.read_csv("final_df_wales.csv.gz", compression='gzip', dtype='category')


# +
def concatenate(dfs):
    """Concatenate while preserving categorical columns.

    NB: We change the categories in-place for the input dataframes
    taken from https://stackoverflow.com/questions/45639350/retaining-categorical-dtype-upon-dataframe-concatenation#answer-57809778
    """
    from pandas.api.types import union_categoricals
    import pandas as pd
    # Iterate on categorical columns common to all dfs
    for col in set.intersection(
        *[
            set(df.select_dtypes(include='category').columns)
            for df in dfs
        ]
    ):
        # Generate the union category across dfs for this column
        uc = union_categoricals([df[col] for df in dfs])
        # Change to union category for all dataframes
        for df in dfs:
            df[col] = pd.Categorical(df[col].values, categories=uc.categories)
    return pd.concat(dfs)

joined_dat = concatenate([wales, england])
# -

for c in joined_dat:
    display(c)
    display(joined_dat[c].cat.categories)

joined_dat = joined_dat[['GEOGRAPHY_CODE', 'C_TENHUK11_NAME', 'C_AGE_NAME', 'C_HEALTH_NAME', 'OBS_VALUE']]

joined_dat = joined_dat.rename(columns={
    'GEOGRAPHY_CODE': 'Geography',
    'C_TENHUK11_NAME': 'Tenure',
    'C_AGE_NAME': 'Age',
    'C_HEALTH_NAME': 'Health',
    'OBS_VALUE': 'Value'
})
joined_dat['Value'] = pd.to_numeric(joined_dat['Value'], downcast='integer')

# +
# Have to add to metadata 
# The Year: 2011
# Geography Type: 2011 output areas
# -

# Output the data to CSV
csvName = 'observations.csv'
out = Path('out')
out.mkdir(exist_ok=True)
joined_dat.drop_duplicates()[:10000].to_csv(out / (csvName + '.gz'), index = False, compression='gzip')

# +
from urllib.parse import urljoin

scraper.dataset.family = 'census'

dataset_path = pathify(os.environ.get('JOB_NAME', 'gss_data/census-2011'))
scraper.set_base_uri('http://gss-data.org.uk')
scraper.set_dataset_id(dataset_path)
csvw_transform = CSVWMapping()
joined_dat[:10].to_csv(out / csvName, index = False)
csvw_transform.set_csv(out / csvName)
csvw_transform.set_mapping(json.load(open('info.json')))
csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
csvw_transform.write(out / f'{csvName}-metadata.json')
(out / csvName).unlink()
with open(out / f'{csvName}-metadata.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())
# -



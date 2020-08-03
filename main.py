# -*- coding: utf-8 -*-
# CENSUS 2011: LC3409EW (General health by tenure by age) - Nomis - Official Labour Market Statistics
from gssutils import * 
import pandas as pd
import json
from datetime import datetime

dataE = pd.read_csv("final_df_england.csv") 
dataW = pd.read_csv("final_df_wales.csv")

#----------------------------------------------------------------------------------------
#joined_dat = pd.concat([dataE, dataW])
joined_dat = dataW
#----------------------------------------------------------------------------------------
#print('England: ' + str(dataE['DATE_NAME'].count()))
#print('Wales: ' + str(dataW['DATE_NAME'].count()))
#print('Joined: ' + str(joined_dat['DATE_NAME'].count()))

#joined_dat = joined_dat[['DATE_NAME', 'GEOGRAPHY_CODE', 'C_TENHUK11_NAME', 'C_AGE_NAME', 'C_HEALTH_NAME', 'OBS_VALUE']]
joined_dat = joined_dat[['DATE_NAME', 'GEOGRAPHY_CODE', 'C_TENHUK11_NAME', 'C_AGE_NAME', 'C_HEALTH_NAME']]

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
import gssutils.scrapers
from gssutils.metadata import namespaces, dcat, pmdcat, mimetype
from gssutils.utils import pathify, ensure_list
from rdflib import BNode, URIRef
from rdflib.graph import Dataset as RDFDataset

def create(dataset, base_uri, dataset_id):
    catalog = dcat.Catalog()
    catalog.uri = urljoin(base_uri, 'catalog/datasets')
    metadata_graph = urljoin(base_uri, f'graph/{dataset_id}-metadata')
    catalog.set_graph(metadata_graph)
    catalog.record = pmdcat.CatalogRecord()
    catalog.record.uri = urljoin(base_uri, f'data/{dataset_id}-catalog-record')
    catalog.record.set_graph(metadata_graph)
    catalog.record.label = dataset.label + " Catalog Record"
    catalog.record.metadataGraph = metadata_graph
    catalog.record.issued = dataset.issued
    catalog.record.modified = dataset.modified
    catalog.record.primaryTopic = dataset
    dataset = pmdcat.Dataset('https://www.nomisweb.co.uk/census/2011/lc3409ew')
    # need to ensure that all the pointed to things are in the same graph
    if hasattr(catalog.record.primaryTopic, 'distribution'):
        for dist in ensure_list(catalog.record.primaryTopic.distribution):
            dist.set_graph(metadata_graph)
    dataset.graph = urljoin(base_uri, f'graph/{dataset_id}')
    dataset.datasetContents = pmdcat.DataCube()
    dataset.datasetContents.uri = urljoin(base_uri, f'data/{dataset_id}#dataset')
    dataset.sparqlEndpoint = urljoin(base_uri, '/sparql')
    quads = RDFDataset()
    quads.namespace_manager = namespaces
    catalog.add_to_dataset(quads)
    return quads 


# +
info = json.load(Path('info.json').open())

ds_id = pathify(os.environ.get('JOB_NAME', 'gss_data/covid-19/' + Path(os.getcwd()).name)) + '-' + pathify(csvName)
ds_base = 'http://gss-data.org.uk'

ds = pmdcat.Dataset('https://www.nomisweb.co.uk/census/2011/lc3409ew')
ds.uri = urljoin(ds_base, f'data/{ds_id}')
ds.graph = urljoin(ds_base, f'graph/{ds_id}/metadata')
#ds.inGraph = urljoin(ds_base, f'graph/{ds_id}')
ds.sparqlEndpoint = urljoin(ds_base, '/sparql')
ds.modified = datetime.now()
ds.theme = THEME['census-2011']
ds.family = 'family-census-2011'
ds.title = info['title']
ds.description = info['description']
ds.issued = parse(info['published']).date()
ds.landingPage = info['landingPage']
ds.contactPoint = 'mailto:census.customerservices@ons.gov.uk'
ds.publisher = GOV[info['published']]
ds.rights = ""
ds.license = ""

d = create(ds, ds_base, ds_id).serialize(format='trig')

#display(d)
# -

with open(out / 'observations.csv-metadata.trig', 'wb') as metadata:
     metadata.write(d)

csvw_transform = CSVWMapping()
csvw_transform.set_csv(out / csvName)
csvw_transform.set_mapping(json.load(open('info.json')))
csvw_transform.set_dataset_uri(urljoin(ds_base, f'data/{ds_id}'))
csvw_transform.write(out / f'{csvName}-metadata.json')












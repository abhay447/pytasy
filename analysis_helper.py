import pandas as pd
from country_lookup import venue_to_country, test_hosting_cities
import numpy as np

# Define the lookup function
def lookup_country(city, venue):
    if city is None:
        return venue_to_country.get(venue, None)
    return test_hosting_cities.get(city, None)  # Returns None if city not found

def get_analysis_df(parent_dir):
    dataset_path=f'{parent_dir}/processed_output/delivery_parquet/'
    people_registry_path=f'{parent_dir}/downloads/people.csv'
    people_df = pd.read_csv(people_registry_path)
    
    full_df = pd.read_parquet(dataset_path)
    lookup_country_vec = np.vectorize(lookup_country)
    # Apply the UDF
    full_df['country'] = lookup_country_vec(full_df['city'], full_df['venue_name'])

    full_df['year'] = pd.to_datetime(full_df["dt"]).dt.year.astype('str')
    full_df['dt'] = full_df["dt"].astype('str')
    return (full_df, people_df)
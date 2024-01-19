
from src.utils import timing_decorator
from math import radians, cos, sin, asin, sqrt
import dask.dataframe as _DaskDataFrame
import pandas as pd 
import geopandas as gpd
import math
import time
from datetime import datetime



def haversine(lon1, lat1, lon2, lat2):
    # Convertir les coordonnées en radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # Formule de Haversine
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  # Rayon de la Terre en kilomètres
    return c * r

def calculate_distance_and_speed_old(df):
    df['prev_latitude'] = df['latitude'].shift(1)
    df['prev_longitude'] = df['longitude'].shift(1)
    df['distance'] = df.apply(lambda x: haversine(x['prev_longitude'], x['prev_latitude'], x['longitude'], x['latitude']) if not _np.isnan(x['prev_longitude']) else 0, axis=1)
    df['time_diff'] = df['timestamp'].diff().dt.total_seconds() / 3600
    df['speed'] = df['distance'] / df['time_diff']
    df['speed'] = df['speed'].fillna(0) 
    return df


def calculate_distance_and_speed(df):
    df['prev_latitude'] = df['latitude'].shift(1)
    df['prev_longitude'] = df['longitude'].shift(1)
    # Find the first line of each drivers
    df['is_first_row'] = df['driver'] != df['driver'].shift(1)
    # Haversine
    df['distance'] = df.apply(lambda x: 0 if x['is_first_row'] else haversine(x['prev_longitude'], x['prev_latitude'], x['longitude'], x['latitude']), axis=1)
    # time_diff
    df['time_diff'] = df['timestamp'].diff().dt.total_seconds() / 3600
    # Speed
    df['speed'] = df.apply(lambda x: 0 if x['is_first_row'] or x['time_diff'] <= 0 else (x['distance'] / x['time_diff']), axis=1)
    # Cleaning
    df.drop(['prev_latitude', 'prev_longitude', 'is_first_row'], axis=1, inplace=True)

    return df

@timing_decorator
def compute_distance_and_speed(drivers_gps_dask_df):
    # Convert Dask DataFrame To Pandas
    drivers_gps_pandas_df = drivers_gps_dask_df.compute()
    drivers_gps_pandas_df = drivers_gps_pandas_df.sort_values(['driver', 'timestamp'])

    # Compute....
    drivers_gps_pandas_df = calculate_distance_and_speed(drivers_gps_pandas_df)

    # Convert back Pandas DataFrame to Dask
    drivers_gps_dask_df = _DaskDataFrame.from_pandas(drivers_gps_pandas_df, npartitions=4)

    # Sorting again the Dask Dataframe
    drivers_gps_dask_df = drivers_gps_dask_df.sort_values(by=['timestamp'])
    
    return drivers_gps_dask_df, drivers_gps_pandas_df


@timing_decorator
def average_speed_pandas(drivers_gps_pandas_df):
    # Group drivers and compute results
    grouped_df = drivers_gps_pandas_df.groupby('driver')
 
    # Calculate mean speed and total distance for each driver
    result_df = grouped_df.agg({'speed': 'mean', 'distance': 'sum'}).reset_index()
    
    # Merge the results back to the original DataFrame
    drivers_gps_pandas_df = pd.merge(drivers_gps_pandas_df, result_df, on='driver', how='left')
    
    return result_df

@timing_decorator
def average_speed_dask(drivers_gps_dask_df):
    
    # Group and Calculate mean speed and total distance for each driver
    result_df = drivers_gps_dask_df.groupby('driver').agg({'speed': 'mean', 'distance': 'sum'}).compute()
 
    # Convert result to Dask DataFrame
    result_df_dask = _DaskDataFrame.from_pandas(result_df, npartitions=1)

    # Merge the results back to the original Dask DataFrame
    drivers_gps_dask_df = _DaskDataFrame.merge(drivers_gps_dask_df, result_df_dask, on='driver', how='left')
    
    return result_df



@timing_decorator
def assign_zones_to_drivers(drivers_df, zones_gdf):
    # Convert DataFrame into GeoDataFrame
    gdf_drivers = gpd.GeoDataFrame(drivers_df, geometry=gpd.points_from_xy(drivers_df.longitude, drivers_df.latitude))

    # Use spatial r-tree index
    sindex = zones_gdf.sindex

    def determine_zone_optimized(point, gdf_zones, sindex):
        possible_matches_index = list(sindex.intersection(point.bounds))
        possible_matches = gdf_zones.iloc[possible_matches_index]
        precise_matches = possible_matches[possible_matches.intersects(point)]
        if not precise_matches.empty:
            return precise_matches.iloc[0]['zone_id']
        return 0

    # Nombre de lignes par batch
    batch_size = 1000  # Ajustez cette valeur selon vos besoins et capacités de votre machine

    # Nombre total de batches
    total_batches = math.ceil(len(gdf_drivers) / batch_size)

    # Traiter chaque batch
    start_time = time.time()
    for batch in range(total_batches):
        start_index = batch * batch_size
        end_index = start_index + batch_size

        # Sélectionner le batch actuel
        batch_df = gdf_drivers.iloc[start_index:end_index]

        # Traiter chaque point dans le batch
        for index, row in batch_df.iterrows():
            point = row['geometry']
            zone_id = determine_zone_optimized(point, zones_gdf, sindex)
            gdf_drivers.at[index, 'zone_id'] = zone_id

        elapsed_time = round(time.time() - start_time, 3)
        print(f"Batch {batch + 1}/{total_batches} traité - Execution time : {elapsed_time} seconds - Current Time :", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        if batch >= 6:
            break   

    gdf_drivers['zone_id'] = gdf_drivers['zone_id'].astype(int)

    return gdf_drivers
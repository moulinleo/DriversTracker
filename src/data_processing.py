import dask.bag as db
import dask.dataframe as dd
import geopandas as gpd
from shapely.geometry import Polygon
from src.utils import timing_decorator
import geopandas as gpd
from math import radians, cos, sin, asin, sqrt
import pandas as pd
from datetime import datetime
import math
import geopandas as _gpd
import time
import geopandas as _gpd
from shapely.geometry import Point
import math


def check_valid_polygon(coords):
    # Checks the validity of a polygon using its coordinates.
    try:
        polygon = Polygon(coords)
        return polygon.is_valid
    except:
        return False
    
# Sort by timestamp 
def timestamp_sort(df):
    return df.sort_values(by='timestamp')

@timing_decorator
def polygon_conversion(dask_zones_bag):

    # Polygons conversions
    zones_polygons = dask_zones_bag.map(lambda x: x['zones']).flatten()
    zones_polygons = zones_polygons.map(lambda zone: [(point['lng'],point['lat']) for point in zone['polygon']])
    zones_polygons = zones_polygons.map(lambda polygon: {"polygon": polygon})
    zones_dask_df = zones_polygons.to_dataframe()
    
    # Apply the check_valid_polygon function to each polygon and create a new column 'is_valid' 
    zones_dask_df['is_valid'] = zones_dask_df['polygon'].apply(check_valid_polygon, meta=('is_valid', 'bool'))
    
    # Count the number of valid and invalid polygons
    count_valid = zones_dask_df[zones_dask_df['is_valid']].shape[0].compute()
    count_invalid = zones_dask_df[~zones_dask_df['is_valid']].shape[0].compute()
    
    # Keep only valid polygon
    zones_dask_df = zones_dask_df[zones_dask_df['is_valid']].compute()
    
    # New Zone_id fields
    zones_dask_df['zone_id'] = zones_dask_df.index + 1
    zones_dask_df['zone_id'] = zones_dask_df['zone_id'].astype(int)
    
    print(f"Valid polygon count : {count_valid}")
    print(f"Invalid polygon count : {count_invalid}")
    
    return zones_dask_df


def create_geodataframe(zones_dask_df):
    # Create 'geometry' column by applying lambda function to convert 'polygon' to Polygon
    zones_dask_df['geometry'] = zones_dask_df['polygon'].apply(lambda x: Polygon(x))
    
    # Create GeoDataFrame from the modified Dask DataFrame
    zones_geodataframe = gpd.GeoDataFrame(zones_dask_df, geometry='geometry')
    
    # Set the CRS of the GeoDataFrame to EPSG 4326
    zones_geodataframe.set_crs(epsg=4326, inplace=True)
    
    return zones_geodataframe



def create_unique_drivers(drivers_gps_dask_df, save_flag=False):
    grouped = drivers_gps_dask_df.groupby('driver')

    # Calculer la première et la dernière date pour chaque conducteur
    drivers_dask_df = grouped.agg({'timestamp': ['min', 'max']})

    # Aplatir les colonnes (après l'agrégation, les colonnes seront multi-indexées)
    drivers_dask_df.columns = ['first_date', 'last_date']

    # Réinitialiser l'index pour rendre 'driver' à nouveau une colonne
    drivers_dask_df = drivers_dask_df.reset_index()

    drivers_dask_df = drivers_dask_df.sort_values(by=['driver'])

    # Saving
    if save_flag:
        drivers_dask_df.to_parquet('../intermediate_results/drivers_dask_df.parquet')
        drivers_pandas_df = drivers_dask_df.compute()
        drivers_pandas_df.to_parquet('../intermediate_results/drivers_pandas_df.parquet')
        
    return drivers_dask_df


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
def process_drivers_gps_data(drivers_gps_dask_df):
    # Convert Dask DataFrame To Pandas
    drivers_gps_pandas_df = drivers_gps_dask_df.compute()
    drivers_gps_pandas_df = drivers_gps_pandas_df.sort_values(['driver', 'timestamp'])

    # Compute....
    drivers_gps_pandas_df = calculate_distance_and_speed(drivers_gps_pandas_df)

    # Convert back Pandas DataFrame to Dask
    drivers_gps_dask_df = dd.from_pandas(drivers_gps_pandas_df, npartitions=4)

    # Sorting again the Dask Dataframe    
    drivers_gps_dask_df = drivers_gps_dask_df.groupby('driver').apply(timestamp_sort, meta=drivers_gps_dask_df.compute())

    
    return drivers_gps_dask_df

@timing_decorator
def compute_driver_stats(drivers_gps_pandas_df, drivers_pandas_df, drivers_dask_df, save_flag=False):
    # Group drivers and compute results
    grouped = drivers_gps_pandas_df.groupby('driver')
    mean_speed = grouped['speed'].mean()
    total_distance = grouped['distance'].sum()

    # Create result dataset
    driver_stats = pd.DataFrame({
        'mean_speed': mean_speed,
        'total_distance': total_distance
    }).reset_index()

    # Update
    drivers_pandas_df = drivers_pandas_df.merge(driver_stats, on='driver', how='left')

    drivers_pandas_df = drivers_dask_df.compute()
    
    if save_flag:
        drivers_pandas_df.to_parquet('../intermediate_results/drivers_pandas_df.parquet')

    return drivers_pandas_df

@timing_decorator
def compute_driver_stats_dask(drivers_gps_dask_df, save_flag=False):
    # Dask Version
    mean_speed = drivers_gps_dask_df.groupby('driver')['speed'].mean().compute()
    total_distance = drivers_gps_dask_df.groupby('driver')['distance'].sum().compute()

    # Create result dataset
    driver_stats = pd.DataFrame({
        'driver': mean_speed.index,
        'mean_speed': mean_speed.values,
        'total_distance': total_distance.values
    })

    # Update
    drivers_dask_df = drivers_dask_df.merge(driver_stats, on='driver', how='left')

    drivers_dask_df['mean_speed'] = drivers_dask_df['mean_speed'].astype(float)
    drivers_dask_df['total_distance'] = drivers_dask_df['total_distance'].astype(float)

    if save_flag:
        drivers_pandas_df = drivers_dask_df.compute()
        drivers_pandas_df.to_parquet('../intermediate_results/drivers_pandas_df.parquet')
    
    return drivers_dask_df



@timing_decorator
def iterative_process_zones_id_gps(drivers_gps_pandas_df, zones_geodataframe):
    # Convert DataFrame into GeoDataFrame
    gdf_drivers = _gpd.GeoDataFrame(drivers_gps_pandas_df, geometry=_gpd.points_from_xy(drivers_gps_pandas_df.longitude, drivers_gps_pandas_df.latitude))

    # Use spatial r-tree index
    sindex = zones_geodataframe.sindex

    def determine_zone_optimized(point, gdf_zones, sindex):
        possible_matches_index = list(sindex.intersection(point.bounds))
        possible_matches = gdf_zones.iloc[possible_matches_index]
        precise_matches = possible_matches[possible_matches.intersects(point)]
        if not precise_matches.empty:
            return precise_matches.iloc[0]['zone_id']
        return 0

    i = 0

    # Number of lines per batch
    batch_size = 1000  # Adjust this value according to your environment/memory capacity

    # Total number of batches
    total_batches = math.ceil(len(gdf_drivers) / batch_size)

    # Iterate over all batches
    for batch in range(total_batches):
        start_index = batch * batch_size
        end_index = start_index + batch_size

        # Current batch
        batch_df = gdf_drivers.iloc[start_index:end_index]

        # Iterate over all rows in the batch
        for index, row in batch_df.iterrows():
            point = row['geometry']
            zone_id = determine_zone_optimized(point, zones_geodataframe, sindex)
            gdf_drivers.at[index, 'zone_id'] = zone_id

        if batch >= 6:
            break   

    gdf_drivers['zone_id'] = gdf_drivers['zone_id'].astype(int)
    
    return gdf_drivers


@timing_decorator
def process_partition(partition, zones_gdf):
    # Create geographicak points.
    partition['geometry'] = partition.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)
    
    # Convert to Geodataframe
    drivers_gdf = _gpd.GeoDataFrame(partition, geometry='geometry')
    
    # Check CRS
    drivers_gdf.set_crs("EPSG:4326", inplace=True)

    if drivers_gdf.crs != zones_gdf.crs:
        drivers_gdf = drivers_gdf.to_crs(zones_gdf.crs)
        
    # Spatial joint...
    joined = _gpd.sjoin(drivers_gdf, zones_gdf[['geometry', 'zone_id']], how='left', predicate='within')
    
    # Check if the zone_id is valid (inside a zone) or outside.
    if 'zone_id_right' in joined:
        partition['zone_id'] = joined['zone_id_right']
    else:
        partition['zone_id'] = -1          

    return partition.drop(columns=['geometry'])

@timing_decorator
def determine_zone_id_dask_geo(drivers_gps_dask_df, zones_geodataframe, save_flag=False):
   #  DETERMINE ZONE ID FOR EACH GPS VALUE. PARALLEL METHOD WITH DASK & GEOPANDAS #
    results = drivers_gps_dask_df.map_partitions(process_partition, zones_geodataframe, align_dataframes=False, meta=drivers_gps_dask_df)

    # Update Original dataframe...
    drivers_gps_dask_df = results
    drivers_gps_dask_df.compute()
    drivers_gps_dask_df['zone_id'] = drivers_gps_dask_df['zone_id'].fillna(-1).astype(int)
    drivers_gps_dask_df['zone_id'] = drivers_gps_dask_df['zone_id'].astype(int)
    drivers_gps_dask_df = drivers_gps_dask_df.sort_values(by=['driver', 'timestamp'])

    # Save Result...
    if save_flag:
        drivers_gps_dask_df.to_parquet('../intermediate_results/drivers_gps_dask_df.parquet')
        drivers_gps_pandas_df = drivers_gps_dask_df.compute()
        drivers_gps_pandas_df.to_parquet('../intermediate_results/drivers_gps_pandas_df.parquet')
        
    return drivers_gps_dask_df


@timing_decorator
def compute_mean_time_zone(drivers_gps_dask_df, save_flag=False):
    # Compute time_diff sum
    time_sum = drivers_gps_dask_df.groupby(['driver', 'zone_id'])['time_diff'].sum()

    # Find zone_id with max total time_diff for each driver
    favorite_zone_by_time = time_sum.groupby('driver').idxmax().compute()
    favorite_zone_by_time = favorite_zone_by_time.apply(lambda x: x[1] if pd.notna(x) else None)

    # Count lignes for each drivers and zone_id
    count_zones = drivers_gps_dask_df.groupby(['driver', 'zone_id']).size()

    # Find zone_id with max number for each driver
    favorite_zone_by_values = count_zones.groupby('driver').idxmax().compute()
    favorite_zone_by_values = favorite_zone_by_values.apply(lambda x: x[1] if pd.notna(x) else None)

    # Result
    driver_stats = pd.DataFrame({
        'driver': favorite_zone_by_time.index,
        'favorite_zone_by_time': favorite_zone_by_time.values,
        'favorite_zone_by_values': favorite_zone_by_values.values
    })

    # Update Dataframe
    drivers_dask_df = drivers_dask_df.merge(driver_stats, on='driver', how='left')

    drivers_dask_df['favorite_zone_by_time'] = drivers_dask_df['favorite_zone_by_time'].astype(int)
    drivers_dask_df['favorite_zone_by_values'] = drivers_dask_df['favorite_zone_by_values'].astype(int)

    if save_flag:
        drivers_pandas_df = drivers_dask_df.compute()
        drivers_pandas_df.to_parquet('../intermediate_results/drivers_pandas_df.parquet')
        
    return drivers_pandas_df


@timing_decorator
def count_unique_drivers_per_zone(drivers_gps_pandas_df, drivers_gps_dask_df, zones_geodataframe, save_flag=False):

    # gps_count
    gps_count = drivers_gps_pandas_df['zone_id'].value_counts()

    # unique_drivers
    unique_drivers = drivers_gps_pandas_df.groupby('zone_id')['driver'].nunique()

    # Convert in DataFrame
    gps_count_df = gps_count.reset_index().rename(columns={'index': 'zone_id'})
    gps_count_df = gps_count.reset_index().rename(columns={'count': 'gps_count'})
    unique_drivers_df = unique_drivers.reset_index().rename(columns={'driver': 'unique_drivers'})



    # Fusion DataFrames
    zones_dask_df = zones_dask_df.merge(gps_count_df, on='zone_id', how='left')
    zones_dask_df = zones_dask_df.merge(unique_drivers_df, on='zone_id', how='left')


    # Fill NaN 
    zones_dask_df['gps_count'] = zones_dask_df['gps_count'].fillna(0)
    zones_dask_df['unique_drivers'] = zones_dask_df['unique_drivers'].fillna(0)
    zones_dask_df['gps_count'] = zones_dask_df['gps_count'].astype(int)
    zones_dask_df['unique_drivers'] = zones_dask_df['unique_drivers'].astype(int)

    # Refresh zones_geodataframe
    zones_geodataframe = _gpd.GeoDataFrame(zones_dask_df, geometry='geometry')
    zones_geodataframe.set_crs(epsg=4326, inplace=True)

    total_gps_count = zones_dask_df['gps_count'].sum()
    print("Total GPS Count:", total_gps_count)

    count_zone_id_minus_one = drivers_gps_dask_df[drivers_gps_dask_df['zone_id'] == -1]['zone_id'].count().compute()

    print("Count zone_id = -1 :", count_zone_id_minus_one)

    

def count_gps_values_per_driver(drivers_gps_dask_df):
    
    # Line count
    gps_counts = drivers_gps_dask_df.groupby('driver').size().compute()
    gps_counts = gps_counts.reset_index().rename(columns={0: 'gps_count'})

    # Fusion CPT GPS with orginal Dask DataFrame
    drivers_dask_df = drivers_dask_df.merge(gps_counts, on='driver', how='left')

    drivers_dask_df.compute()

    drivers_pandas_df = drivers_dask_df.compute()
    
    drivers_pandas_df.loc[drivers_pandas_df['mean_speed'] > 150, ['total_distance', 'mean_speed']] = 0
    drivers_pandas_df.loc[drivers_pandas_df['total_distance'] > 200, ['total_distance', 'mean_speed']] = 0
    
    
    return drivers_pandas_df
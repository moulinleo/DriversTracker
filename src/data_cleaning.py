import dask.dataframe as _DaskDataFrame


def handle_missing_values(df, print_=False):
    
    if print_:
         # Count the number of columns before and after dropping missing values
        num_dropped_columns = len(df.columns) - len(df.dropna().compute().columns)
        print(f"Dropping missing values. Number of columns dropped: {num_dropped_columns}")
    
    # Drop missing values
    return df.dropna()

def remove_duplicates(df, print_=False):
    
    if print_:
        # Count the number of duplicate rows
        num_duplicates = len(df[df.duplicated().compute()])
        print(f"Removing duplicates. Number of duplicate rows: {num_duplicates}")
    
    # Remove duplicates
    return df.drop_duplicates()

# Sort by timestamp 
def timestamp_sort(df):
    return df.sort_values(by='timestamp')

def data_cleaning(drivers_gps_dask_df, print_=False):
    # Data Cleaning Pipeline. The argument print_=False prints the result of each step.
    
    # Convert to timestamps to datetime
    drivers_gps_dask_df['timestamp'] = _DaskDataFrame.to_datetime(drivers_gps_dask_df['timestamp'])
    
    # Convert Lon and Lat to float
    drivers_gps_dask_df['latitude'] = drivers_gps_dask_df['latitude'].astype(float)
    drivers_gps_dask_df['longitude'] = drivers_gps_dask_df['longitude'].astype(float)
    
    # Remove duplicates
    drivers_gps_dask_df = remove_duplicates(drivers_gps_dask_df, print_=print_)
    
    # Drop NaNs values
    # drivers_gps_dask_df = handle_missing_values(drivers_gps_dask_df, print_=print_)
     
    # Convert zone_id to Int
    drivers_gps_dask_df['zone_id'] = 0
    drivers_gps_dask_df['zone_id'] = drivers_gps_dask_df['zone_id'].astype(int)
    
    # Sort by timestamp and driver
    drivers_gps_dask_df = drivers_gps_dask_df.groupby('driver').apply(timestamp_sort, meta=drivers_gps_dask_df.compute())

    # Reset index
    drivers_gps_dask_df = drivers_gps_dask_df.reset_index(drop=True)

    return drivers_gps_dask_df
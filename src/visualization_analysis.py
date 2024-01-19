import matplotlib.pyplot as plt 
import seaborn as sns
import pandas as pd


def plot_mean_speed_distribution(drivers_pandas_df):
    # Plot the distribution of mean speeds
    plt.figure(figsize=(10, 6))
    sns.histplot(drivers_pandas_df['mean_speed'], bins=30, kde=True)
    plt.title('Distribution of Mean Speeds')
    plt.xlabel('Mean Speed (km/h)')
    plt.ylabel('Number of Drivers')
    plt.show()

def plot_total_distance_distribution(drivers_pandas_df):
    # Plot the distribution of total distances traveled
    plt.figure(figsize=(10, 6))
    sns.histplot(drivers_pandas_df['total_distance'], bins=30, kde=True)
    plt.title('Distribution of Total Distances Traveled')
    plt.xlabel('Total Distance (km)')
    plt.ylabel('Number of Drivers')
    plt.show()

def plot_gps_count_distribution(drivers_pandas_df):
    # Plot the distribution of GPS counts
    plt.figure(figsize=(10, 6))
    sns.histplot(drivers_pandas_df['gps_count'], bins=30, kde=True)
    plt.title('Distribution of GPS Counts')
    plt.xlabel('Number of GPS Records')
    plt.ylabel('Number of Drivers')
    plt.show()

def plot_speed_distance_relationship(drivers_pandas_df):
    # Plot the relationship between mean speed and total distance
    plt.figure(figsize=(10, 6))
    sns.scatterplot(x='mean_speed', y='total_distance', data=drivers_pandas_df)
    plt.title('Speed / Total Distance Relationship')
    plt.xlabel('Mean Speed (km/h)')
    plt.ylabel('Total Distance (km)')
    plt.show()

def plot_favorite_zone_by_time(drivers_pandas_df):
    # Plot the distribution of favorite zones by time spent
    plt.figure(figsize=(12, 6))
    sns.countplot(data=drivers_pandas_df, x='favorite_zone_by_time')
    plt.title('Favorite Zones by Time Spent')
    plt.xlabel('Zone ID')
    plt.ylabel('Number of Drivers')
    plt.xticks(rotation=45)
    plt.show()

def plot_favorite_zone_by_values(drivers_pandas_df):
    # Plot the distribution of favorite zones by number of records
    plt.figure(figsize=(12, 6))
    sns.countplot(data=drivers_pandas_df, x='favorite_zone_by_values')
    plt.title('Favorite Zones by Number of Records')
    plt.xlabel('Zone ID')
    plt.ylabel('Number of Drivers')
    plt.xticks(rotation=45)
    plt.show()

def plot_gps_count_by_zone(zones_pandas_df):
    # Plot the number of GPS lines per zone
    plt.figure(figsize=(12, 6))
    sns.barplot(data=zones_pandas_df, x='zone_id', y='gps_count')
    plt.title('Number of GPS Lines per Zone')
    plt.xlabel('Zone ID')
    plt.ylabel('Number of GPS Lines')
    plt.xticks(rotation=45)
    plt.show()

def plot_unique_drivers_by_zone(zones_pandas_df):
    # Plot the number of unique drivers per zone
    plt.figure(figsize=(12, 6))
    sns.barplot(data=zones_pandas_df, x='zone_id', y='unique_drivers')
    plt.title('Number of Unique Drivers per Zone')
    plt.xlabel('Zone ID')
    plt.ylabel('Number of Unique Drivers')
    plt.xticks(rotation=45)
    plt.show()


def plot_gps_data_distribution(drivers_gps_pandas_df):
    # Plot the distribution of GPS data by hour
    drivers_gps_pandas_df['hour'] = drivers_gps_pandas_df['timestamp'].dt.hour

    plt.figure(figsize=(10, 6))
    plt.hist(drivers_gps_pandas_df['hour'], bins=24, range=(8, 21), edgecolor='black')
    plt.title('Distribution of GPS Data by Hour')
    plt.xlabel('Hour of the Day')
    plt.ylabel('Number of GPS Points')
    plt.xticks(range(0, 25))
    plt.grid(axis='y', alpha=0.75)
    plt.show()

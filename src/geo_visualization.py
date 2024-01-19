import folium
from shapely.geometry import Point, Polygon
import geopandas as gpd
from folium.plugins import HeatMap


# Function to compute mean coordinates from the centroid of the geometry
def compute_mean_coordinates(geodataframe):
    geodataframe['centroid'] = geodataframe['geometry'].centroid
    mean_latitude = geodataframe['centroid'].y.mean()
    mean_longitude = geodataframe['centroid'].x.mean()
    return mean_latitude, mean_longitude

# Function to select a specific driver from the Dask DataFrame
def select_specific_driver(drivers_gps_dask_df, driver_id):
    specific_driver_df = drivers_gps_dask_df[drivers_gps_dask_df['driver'] == driver_id].compute()
    specific_driver_df = specific_driver_df.sort_values(by='timestamp')
    start_location = specific_driver_df.iloc[0][['latitude', 'longitude']]
    return specific_driver_df, start_location

# Function to create a Folium map object
def create_folium_map(mean_latitude, mean_longitude):
    return folium.Map(location=[mean_latitude, mean_longitude], zoom_start=9.5)

# Function to draw zones on the Folium map
def draw_zones_on_map(zones_geodataframe, oMap):
    for _, row in zones_geodataframe.iterrows():
        simpl_geo = row['geometry'].simplify(tolerance=0.001, preserve_topology=True)
        folium.GeoJson(simpl_geo).add_to(oMap)

        centroid = row['geometry'].centroid
        # Marker with DivIcon to display zone_id
        folium.Marker(
            [centroid.y, centroid.x],
            icon=folium.DivIcon(
                icon_size=(150, 36),
                icon_anchor=(7, 20),
                html=f'<div style="font-size: 12pt; color : black">{row["zone_id"]}</div>'
            )
        ).add_to(oMap)
    return oMap

# Function to draw GPS dots and lines on the Folium map
def draw_gps_dots_and_lines(specific_driver_df, oMap):
    for _, row in specific_driver_df.iterrows():
        folium.CircleMarker(location=[row['latitude'], row['longitude']], radius=5, color='blue').add_to(oMap)

    points = specific_driver_df[['latitude', 'longitude']].values
    folium.PolyLine(points, color='blue').add_to(oMap)
    return oMap

def create_heatmap(drivers_gps_pandas_df):
    # Create a Folium map object with a heatmap
    mean_lat = drivers_gps_pandas_df['latitude'].mean()
    mean_lon = drivers_gps_pandas_df['longitude'].mean()
    m = folium.Map(location=[mean_lat, mean_lon], zoom_start=12)
    HeatMap(data=drivers_gps_pandas_df[['latitude', 'longitude']], radius=10).add_to(m)
    return m

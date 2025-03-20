import geocoder
import pandas as pd
from geopy.geocoders import ArcGIS
from geopy.extra.rate_limiter import RateLimiter
import time

# Initialize geocoder with rate limiting
geolocator = ArcGIS()
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

def get_lat_lon(address):
    """Get latitude, longitude, and true address from an address string."""
    try:
        location = geocode(address)
        if location:
            return location.latitude, location.longitude, location.address
        return None, None, None
    except Exception as e:
        print(f"Geocoding error for {address}: {e}")
        return None, None, None

def cleanup_df(df, city):
    """Clean up DataFrame and add URL and geographic metadata."""
    df['address'] = df['address'].str.replace(', NE', ', NEBRASKA')
    df = df[['key', 'address', 'i']].drop_duplicates()
    df['County'] = 'Sarpy'
    df['address'] = df['address'].str.replace(', NEBRASKA', '')
    df['URL'] = df['i'].apply(lambda x: BASE_URL.format(x))
    df['state'] = 'Nebraska'
    df['city'] = city
    df['concat_address'] = df['address'] + ", " + df['city'] + ", " + df['state']
    return df

def add_geolocation(df, address_column):
    """Add latitude and longitude to DataFrame based on address column."""
    latitudes, longitudes, true_addresses = [], [], []
    for i, address in enumerate(df[address_column], 1):
        print(f"Processing record {i}: {address}")
        lat, lon, true_addr = get_lat_lon(address)
        latitudes.append(lat)
        longitudes.append(lon)
        true_addresses.append(true_addr)
        time.sleep(1)  # Respect rate limits
    df['Latitude'] = latitudes
    df['Longitude'] = longitudes
    df['True_Address'] = true_addresses
    return df

# Load scraped data and process
file_path = 'scraped_property_data.csv'
df = pd.read_csv(file_path)
df = cleanup_df(df, 'Bellevue')  # Example city
result_df = add_geolocation(df, 'concat_address')
result_df.to_csv('geolocated_property_data.csv', index=False)
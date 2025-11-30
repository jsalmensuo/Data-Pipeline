import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
from math import sqrt
import json
import time

# --- New libraries ---
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from fmiopendata.wfs import download_stored_query
import pytz

# --- 1. SETTINGS AND DATA LOADING ---
FILE_PATH = r'E:\projects\python\data-pipeline\data\processed\outage_data.json'
CACHED_FILE_PATH = r'E:\projects\python\data-pipeline\data\processed\outage_data_with_weather.json'
CACHE_HIT = False

try:
    df = pd.read_json(CACHED_FILE_PATH, encoding='utf-8')
    required_cols = ['lampotila_celsius', 'latitude', 'duration_hours']
    if all(col in df.columns for col in required_cols):
        print(f"✅ Using cache! Data loaded from: {CACHED_FILE_PATH}")
        CACHE_HIT = True
    else:
        print("⚠️ Cache found but missing required columns. Will fetch API data.")
except FileNotFoundError:
    print("⚠️ Cache not found. Fetching API data.")
except Exception as e:
    print(f"❌ Error reading cache: {e}")

if not CACHE_HIT:
    try:
        df = pd.read_json(FILE_PATH, encoding='utf-8')
        print(f"✅ Original data loaded: {FILE_PATH}")
    except Exception as e:
        print(f"❌ Error reading original data: {e}")
        exit()

# Initialize missing columns
for col in ['lampotila_celsius', 'latitude', 'longitude', 'station_id']:
    if col not in df.columns:
        df[col] = np.nan

# --- 2. DURATION CALCULATION ---
def calculate_duration(row):
    try:
        year = str(row['year']).zfill(4)
        time_start = row['time_start'].replace('.', ':')
        time_end = row['time_end'].replace('.', ':')
        
        start_str = f"{year}-{row['month']}-{row['day']} {time_start}"
        end_str = f"{year}-{row['month']}-{row['day']} {time_end}"
        
        time_format_start = "%Y-%m-%d %H:%M" if ':' in time_start else "%Y-%m-%d %H"
        time_format_end = "%Y-%m-%d %H:%M" if ':' in time_end else "%Y-%m-%d %H"
        
        time_start_obj = datetime.strptime(start_str, time_format_start)
        time_end_obj = datetime.strptime(end_str, time_format_end)
        
        duration = (time_end_obj - time_start_obj).total_seconds() / 3600
        return duration, time_start_obj
    except Exception:
        return np.nan, pd.NaT

df[['duration_hours', 'start_timestamp']] = df.apply(lambda row: pd.Series(calculate_duration(row)), axis=1)
df = df.dropna(subset=['duration_hours'])

# --- 3.A GEOCODING ---
if not CACHE_HIT:
    geolocator = Nominatim(user_agent="keskeytys_analyysi_app_v2")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.5)
    location_cache = {}

    def get_lat_lon(location):
        if location in location_cache: return location_cache[location]
        try:
            full_query = f"{location}, Finland"
            result = geocode(full_query)
            if result:
                location_cache[location] = (result.latitude, result.longitude)
                return result.latitude, result.longitude
        except Exception:
            time.sleep(2)
        return None, None

    print("\n--- Geocoding locations ---")
    locations_to_geocode = df['location'].unique()
    lat_lon_results = {}
    
    for i, loc in enumerate(locations_to_geocode):
        lat_lon_results[loc] = get_lat_lon(loc)
        if (i + 1) % 10 == 0: print(f" ...Geocoded {i + 1}/{len(locations_to_geocode)}")

    df['latitude'] = df['location'].apply(lambda x: lat_lon_results.get(x, (None, None))[0])
    df['longitude'] = df['location'].apply(lambda x: lat_lon_results.get(x, (None, None))[1])
    df = df.dropna(subset=['latitude', 'longitude'])
    print(f"✅ Geocoding complete. Rows: {len(df)}")

# --- 3.B FMI WEATHER FETCH ---
if not CACHE_HIT:
    weather_cache = {}

    def fetch_weather_for_row(row, radius=10.0):
        """Fetch t2m from FMI with large bbox and small buffer for time."""
        timestamp_local = row['start_timestamp']
        if pd.isna(timestamp_local): return np.nan, np.nan

        helsinki_tz = pytz.timezone('Europe/Helsinki')
        if timestamp_local.tzinfo is None:
            local_dt = helsinki_tz.localize(timestamp_local)
        else:
            local_dt = timestamp_local
        utc_dt = local_dt.astimezone(pytz.utc)

        # Round to nearest hour
        if utc_dt.minute >= 30:
            utc_dt += timedelta(hours=1)
        utc_dt = utc_dt.replace(minute=0, second=0, microsecond=0)

        lat, lon = row['latitude'], row['longitude']
        cache_key = ((lat, lon), utc_dt)
        if cache_key in weather_cache: return weather_cache[cache_key]

        # Bounding box
        bbox = f"{lon-radius},{lat-radius},{lon+radius},{lat+radius}"

        # Time window ±10 min
        start_time_str = (utc_dt - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_time_str   = (utc_dt + timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")

        try:
            obs = download_stored_query(
                "fmi::observations::weather::multipointcoverage",
                args=[
                    f"bbox={bbox}",
                    f"starttime={start_time_str}",
                    f"endtime={end_time_str}",
                    "parameters=t2m",
                    "timeseries=True"
                ],
            )

            if not obs.data:
                weather_cache[cache_key] = (np.nan, np.nan)
                return np.nan, np.nan

            # Find first valid t2m
            for station, var_dict in obs.data.items():
                if "t2m" in var_dict:
                    values = var_dict["t2m"]["values"]
                    for val in values:
                        if val is not None and not np.isnan(val):
                            weather_cache[cache_key] = (float(val), station)
                            return float(val), station

            weather_cache[cache_key] = (np.nan, np.nan)
            return np.nan, np.nan
        except Exception:
            time.sleep(0.5)
            return np.nan, np.nan

    print("\n--- Fetching FMI weather ---")
    temperatures = []
    station_ids = []

    for i, (index, row) in enumerate(df.iterrows()):
        temp, statid = fetch_weather_for_row(row)
        temperatures.append(temp)
        station_ids.append(statid)

        if (i + 1) % 20 == 0:
            temp_disp = f"{temp:.1f}°C" if pd.notna(temp) else "NaN"
            print(f" ...{i + 1}/{len(df)} | {row['location']} | {temp_disp}")

    df['lampotila_celsius'] = temperatures
    df['station_id'] = station_ids

    # Save cache
    try:
        df_save = df.copy()
        if 'start_timestamp' in df_save.columns:
            df_save['start_timestamp'] = df_save['start_timestamp'].astype(str)
        df_save.to_json(CACHED_FILE_PATH, orient='records', lines=False, indent=4)
        print(f"\n✅ Data saved to cache: {CACHED_FILE_PATH}")
    except Exception as e:
        print(f"❌ Error saving cache: {e}")

# --- 4. DATA CLEANUP ---
if 'tags' in df.columns:
    df['Is_Huolto'] = df['tags'].apply(lambda x: 1 if isinstance(x, list) and 'huollosta' in x else 0)
    df['Is_Saneeraus'] = df['tags'].apply(lambda x: 1 if isinstance(x, list) and 'saneeraustöistä' in x else 0)
else:
    df['Is_Huolto'] = 0
    df['Is_Saneeraus'] = 0

if 'month' in df.columns:
    df['season'] = df['month'].apply(lambda m: 'Talvi' if m in [12,1,2] else 'Kevat' if m in [3,4,5] else 'Kesa' if m in [6,7,8] else 'Syksy')
else:
    df['season'] = 'Tuntematon'

df['year_int'] = pd.to_numeric(df['year'], errors='coerce').fillna(2024)
median_temp = df['lampotila_celsius'].median()
if pd.isna(median_temp): median_temp = 0.0
df['lampotila_celsius'] = df['lampotila_celsius'].fillna(median_temp)

# --- 5. PREP FOR ML ---
cols_to_drop = ['start_timestamp', 'day', 'month', 'year', 'tags', 'station_id']
df_ml = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors='ignore')

categorical_cols = ['weekday', 'location', 'season']
for col in categorical_cols:
    if col not in df_ml.columns: df_ml[col] = 'Tuntematon'

df_ml = pd.get_dummies(df_ml, columns=categorical_cols, drop_first=False)
df_ml = df_ml.dropna()

# --- 6. TRAIN RANDOM FOREST ---
TARGET = 'duration_hours'
features = [c for c in df_ml.columns if c != TARGET]

X = df_ml[features]
y = df_ml[TARGET]

if len(X) == 0:
    print("❌ No data for modeling.")
    exit()

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

print("\n--- Training Random Forest ---")
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

y_pred = model.predict(X_test)
rmse = sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)

print(f"RMSE: {rmse:.2f} h")
print(f"R2: {r2:.2f}")

results = pd.DataFrame({'Actual': y_test, 'Predicted': y_pred.round(2)})
print("\nSample predictions:")
print(results.head(5))

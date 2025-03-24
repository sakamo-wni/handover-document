import os
import json
import urllib.request
import urllib.error
from datetime import datetime, timezone
import boto3
import uuid
from time import sleep
import socket

s3_client_eu = boto3.client("s3", region_name="eu-central-1")
s3_client_jp = boto3.client("s3", region_name="ap-northeast-1")
raw_data_bucket = os.getenv("RawDataBucket")   
converted_bucket = os.getenv("ConvertedBucket")   
tagid = os.getenv("tagid")
base_url = os.getenv("URL")
memory_cache = {}
CACHE_EXPIRY = 3600 
WEATHER_CODES = None  
STATION_TEMPLATE = None  
MISSING_VALUES = {
    "INT8": -99,
    "INT16": -9999,
    "INT32": -999999999,
    "STR": ""
}

INVALID_VALUES = {
    "INT8": -111,
    "INT16": -11111,
    "INT32": -1111111111,
    "STR": ""
}
MISSING_INT32 = MISSING_VALUES["INT32"]
MISSING_INT16 = MISSING_VALUES["INT16"]
MISSING_INT8 = MISSING_VALUES["INT8"]

def get_memory_cache(key):
    try:
        if key in memory_cache:
            cache_data = memory_cache[key]
            if datetime.now(timezone.utc).timestamp() - cache_data['timestamp'] < CACHE_EXPIRY:
                return cache_data['data']
            del memory_cache[key]
    except Exception as e:
        print(f"Cache access error: {e}")
    return None

def set_memory_cache(key, data):
    try:
        memory_cache[key] = {
            'timestamp': datetime.now(timezone.utc).timestamp(),
            'data': data
        }
        return True
    except Exception as e:
        print(f"Cache save error: {e}")
        return False

def cleanup_memory_cache():
    current_time = datetime.now(timezone.utc).timestamp()
    expired_keys = [
        key for key, cache_data in memory_cache.items()
        if current_time - cache_data['timestamp'] > CACHE_EXPIRY
    ]
    for key in expired_keys:
        del memory_cache[key]

def validate_env_vars():
    required_vars = [
        "RawDataBucket",    
        "ConvertedBucket",  
        "tagid", 
        "URL", 
        "APIKey"
    ]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

def save_to_s3_raw(bucket, key, body):
    try:
        s3_client_eu.put_object(
            Body=body,
            Bucket=bucket,
            Key=key,
            ContentType='application/json'
        )
        print(f"Successfully saved raw data to EU S3: {bucket}/{key}")
        return True
    except Exception as error:
        raise ValueError(f"Failed to save raw data to EU S3: {str(error)}")

def save_to_s3_converted(bucket, key, body):
    try:
        s3_client_jp.put_object(
            Body=body,
            Bucket=bucket,
            Key=key,
            ContentType='application/json'
        )
        print(f"Successfully saved converted data to JP S3: {bucket}/{key}")
        return True
    except Exception as error:
        raise ValueError(f"Failed to save converted data to JP S3: {str(error)}")

def create_ruheader(announced, header_comment, dataname, dataid16, data_size=0, data_format="GeoJSON"):
    RU_HEADER_BEG_SIGNATURE = "WN\n"
    RU_HEADER_END_SIGNATURE = "\x04\x1a"
    created = datetime.now(timezone.utc)
    rhd = RU_HEADER_BEG_SIGNATURE
    rhd += "header_version=1.00\n"
    rhd += f"data_name={dataname}\n"
    rhd += f"global_id={dataid16[0:4]}\n"
    rhd += f"category={dataid16[4:8]}\n"
    rhd += f"data_id={dataid16[8:16]}\n"
    rhd += f"created={created.strftime('%Y/%m/%d %H:%M:%S GMT')}\n"
    rhd += f"announced={created.strftime('%Y/%m/%d %H:%M:%S GMT')}\n" 
    rhd += "revision=1\n"
    rhd += f"data_size={data_size}\n"
    rhd += f"header_comment={header_comment}\n"
    rhd += f"format={data_format}\n"
    rhd += RU_HEADER_END_SIGNATURE
    return rhd

def generate_raw_s3_key(tagid, filename):
    return f"{tagid}/{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/{filename}"

def generate_json_s3_key(tagid, filename):
    return f"data/{tagid}/{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/{filename}"

def get_weather_description(ww_code):
    weather_codes = {
        0: "Cloud development not observed or not observable",
        1: "Clouds generally dissolving or becoming less developed",
        2: "State of sky on the whole unchanged",
        3: "Clouds generally forming or developing",
        4: "Visibility reduced by smoke",
        5: "Haze",
        6: "Widespread dust in suspension in the air",
        7: "Dust or sand raised by wind at or near the station",
        8: "Well developed dust whirl(s) or sand whirl(s)",
        9: "Duststorm or sandstorm within sight",
        10: "Mist",
        11: "Patches of shallow fog",
        12: "More or less continuous shallow fog",
        13: "Lightning visible, no thunder heard",
        14: "Precipitation within sight, not reaching the ground",
        15: "Precipitation within sight, reaching the ground but distant",
        16: "Precipitation within sight, reaching the ground near station",
        17: "Thunderstorm, but no precipitation",
        18: "Squalls at or within sight",
        19: "Funnel cloud(s)",
        20: "Drizzle or snow grains",
        21: "Rain (not freezing)",
        22: "Snow",
        23: "Rain and snow or ice pellets",
        24: "Freezing drizzle or freezing rain",
        25: "Shower(s) of rain",
        26: "Shower(s) of snow, or of rain and snow",
        27: "Shower(s) of hail",
        28: "Fog or ice fog",
        29: "Thunderstorm",
        30: "Slight or moderate duststorm or sandstorm - has decreased during the preceding hour",
        31: "Slight or moderate duststorm or sandstorm - no appreciable change during the preceding hour",
        32: "Slight or moderate duststorm or sandstorm - has begun or has increased during the preceding hour",
        33: "Severe duststorm or sandstorm - has decreased during the preceding hour",
        34: "Severe duststorm or sandstorm - no appreciable change during the preceding hour",
        35: "Severe duststorm or sandstorm - has begun or has increased during the preceding hour",
        36: "Slight or moderate blowing snow generally low (below eye level)",
        37: "Heavy drifting snow",
        38: "Slight or moderate blowing snow generally high (above eye level)",
        39: "Heavy drifting snow",
        40: "Fog or ice fog at a distance at the time of observation",
        41: "Fog or ice fog in patches",
        42: "Fog or ice fog, sky visible, has become thinner during the preceding hour",
        43: "Fog or ice fog, sky invisible, has become thinner during the preceding hour",
        44: "Fog or ice fog, sky visible, no appreciable change during the preceding hour",
        45: "Fog or ice fog, sky invisible, no appreciable change during the preceding hour",
        46: "Fog or ice fog, sky visible, has begun or has become thicker during the preceding hour",
        47: "Fog or ice fog, sky invisible, has begun or has become thicker during the preceding hour",
        48: "Fog, depositing rime, sky visible",
        49: "Fog, depositing rime, sky invisible",
        50: "Drizzle, not freezing, intermittent, slight at time of observation",
        51: "Drizzle, not freezing, continuous, slight at time of observation",
        52: "Drizzle, not freezing, intermittent, moderate at time of observation",
        53: "Drizzle, not freezing, continuous, moderate at time of observation",
        54: "Drizzle, not freezing, intermittent, heavy at time of observation",
        55: "Drizzle, not freezing, continuous, heavy at time of observation",
        56: "Drizzle, freezing, slight",
        57: "Drizzle, freezing, moderate or heavy",
        58: "Drizzle and rain, slight",
        59: "Drizzle and rain, moderate or heavy",
        60: "Rain, not freezing, intermittent, slight at time of observation",
        61: "Rain, not freezing, continuous, slight at time of observation",
        62: "Rain, not freezing, intermittent, moderate at time of observation",
        63: "Rain, not freezing, continuous, moderate at time of observation",
        64: "Rain, not freezing, intermittent, heavy at time of observation",
        65: "Rain, not freezing, continuous, heavy at time of observation",
        66: "Rain, freezing, slight",
        67: "Rain, freezing, moderate or heavy",
        68: "Rain or drizzle and snow, slight",
        69: "Rain or drizzle and snow, moderate or heavy",
        70: "Intermittent fall of snowflakes, slight at time of observation",
        71: "Continuous fall of snowflakes, slight at time of observation",
        72: "Intermittent fall of snowflakes, moderate at time of observation",
        73: "Continuous fall of snowflakes, moderate at time of observation",
        74: "Intermittent fall of snowflakes, heavy at time of observation",
        75: "Continuous fall of snowflakes, heavy at time of observation",
        76: "Diamond dust (with or without fog)",
        77: "Snow grains (with or without fog)",
        78: "Isolated star-like snow crystals (with or without fog)",
        79: "Ice pellets",
        80: "Rain shower(s), slight",
        81: "Rain shower(s), moderate or heavy",
        82: "Rain shower(s), violent",
        83: "Shower(s) of rain and snow mixed, slight",
        84: "Shower(s) of rain and snow mixed, moderate or heavy",
        85: "Snow shower(s), slight",
        86: "Snow shower(s), moderate or heavy",
        87: "Shower(s) of snow pellets or small hail, with or without rain or rain and snow mixed - slight",
        88: "Shower(s) of snow pellets or small hail, with or without rain or rain and snow mixed - moderate or heavy",
        89: "Shower(s) of hail, with or without rain or rain and snow mixed, not associated with thunder - slight",
        90: "Shower(s) of hail, with or without rain or rain and snow mixed, not associated with thunder - moderate or heavy",
        91: "Slight rain at time of observation, Thunderstorm during the preceding hour but not at time of observation",
        92: "Moderate or heavy rain at time of observation, Thunderstorm during the preceding hour but not at time of observation",
        93: "Slight snow, or rain and snow mixed or hail at time of observation, Thunderstorm during the preceding hour but not at time of observation",
        94: "Moderate or heavy snow, or rain and snow mixed or hail at time of observation, Thunderstorm during the preceding hour but not at time of observation",
        95: "Thunderstorm, slight or moderate, without hail but with rain and/or snow at time of observation",
        96: "Thunderstorm, slight or moderate, with hail at time of observation",
        97: "Thunderstorm, heavy, without hail but with rain and/or snow at time of observation",
        98: "Thunderstorm combined with duststorm or sandstorm at time of observation",
        99: "Thunderstorm, heavy, with hail at time of observation",
        100: "No significant weather observed",
        101: "Clouds generally dissolving or becoming less developed during the past hour",
        102: "State of sky unchanged during the past hour",
        103: "Clouds generally forming or developing during the past hour",
        104: "Haze or smoke, visibility equal to, or greater than 1km",
        105: "Haze or smoke, visibility less than 1 km",
        110: "Mist",
        111: "Diamond dust",
        112: "Distant lightning",
        118: "Squalls",
        120: "Fog",
        121: "Precipitation",
        122: "Drizzle or snow grains",
        123: "Rain (not freezing)",
        124: "Snow",
        125: "Freezing drizzle or freezing rain",
        126: "Thunderstorm",
        127: "Blowing or drifting snow or sand",
        128: "Blowing or drifting snow or sand, visibility equal to or greater than 1km",
        129: "Blowing or drifting snow or sand, visibility less than 1 km",
        130: "Fog",
        131: "Fog or ice fog in patches",
        132: "Fog or ice fog, has become thinner during the past hour",
        133: "Fog or ice fog, no appreciable change during the past hour",
        134: "Fog or ice fog, has begun or become thicker during the past hour",
        135: "Fog, depositing rime",
        140: "Precipitation",
        141: "Precipitation, slight or moderate",
        142: "Precipitation, heavy",
        143: "Liquid precipitation, slight or moderate",
        144: "Liquid precipitation, heavy",
        145: "Solid precipitation, slight or moderate",
        146: "Solid precipitation, heavy",
        147: "Freezing precipitation, slight or moderate",
        148: "Freezing precipitation, heavy",
        150: "Drizzle",
        151: "Drizzle, not freezing, slight",
        152: "Drizzle, not freezing, moderate",
        153: "Drizzle, not freezing, heavy",
        154: "Drizzle, freezing, slight",
        155: "Drizzle, freezing, moderate",
        156: "Drizzle, freezing, heavy",
        157: "Drizzle and rain, slight",
        158: "Drizzle and rain, moderate or heavy",
        160: "Rain",
        161: "Rain, not freezing, slight",
        162: "Rain, not freezing, moderate",
        163: "Rain, not freezing, heavy",
        164: "Rain, freezing, slight",
        165: "Rain, freezing, moderate",
        166: "Rain, freezing, heavy",
        167: "Rain (or drizzle) and snow, slight",
        168: "Rain (or drizzle) and snow, moderate or heavy",
        170: "Snow",
        171: "Snow, slight",
        172: "Snow, moderate",
        173: "Snow, heavy",
        174: "Ice pellets, slight",
        175: "Ice pellets, moderate",
        176: "Ice pellets, heavy",
        177: "Snow grains",
        178: "Ice crystals",
        180: "Shower(s) or intermittent precipitation",
        181: "Rain shower(s) or intermittent rain, slight",
        182: "Rain shower(s) or intermittent rain, moderate",
        183: "Rain shower(s) or intermittent rain, heavy",
        184: "Rain shower(s) or intermittent rain, violent",
        185: "Snow shower(s) or intermittent snow, slight",
        186: "Snow shower(s) or intermittent snow, moderate",
        187: "Snow shower(s) or intermittent snow, heavy",
        189: "Hail",
        190: "Thunderstorm",
        191: "Thunderstorm, slight or moderate, with no precipitation",
        192: "Thunderstorm, slight or moderate, with rain showers and/or snow showers",
        193: "Thunderstorm, slight or moderate, with hail",
        194: "Thunderstorm, heavy, with no precipitation",
        195: "Thunderstorm, heavy, with rain showers and/or snow showers",
        196: "Thunderstorm, heavy, with hail",
        199: "Tornado"
    }
    
    try:
        code = int(ww_code)
        return weather_codes.get(code, "Unknown weather code")
    except (ValueError, TypeError):
        return "Invalid weather code"

def map_cloud_cover(value):
    cloud_cover_mapping = {
        0: 0,    # 0 oktas
        10: 1,   # 1 okta
        25: 2,   # 2 oktas
        40: 3,   # 3 oktas
        50: 4,   # 4 oktas
        60: 5,   # 5 oktas
        75: 6,   # 6 oktas
        90: 7,   # 7 oktas
        100: 8,  # 8 oktas
        112: 9   # Sky obscured
    }
    
    closest = min(cloud_cover_mapping.keys(), key=lambda x: abs(x - value))
    return cloud_cover_mapping[closest]

def map_parameter_value(parameter_id, value):
    parameter_mapping = {
        "temp_dry": ("AIRTMP", lambda x: int(x * 10), "INT16"),
        "temp_max_past1h": ("AIRTMP_1HOUR_MAX", lambda x: int(x * 10), "INT16"),  
        "temp_mean_past1h": ("AIRTMP_1HOUR_AVG", lambda x: int(x * 10), "INT16"),  
        "temp_min_past1h": ("AIRTMP_1HOUR_MINI", lambda x: int(x * 10), "INT16"),  
        "cloud_cover": ("AMTCLD_8", lambda x: map_cloud_cover(x), "INT16"),  
        "humidity": ("RHUM", lambda x: int(x * 10), "INT16"),
        "precip_past10min": ("PRCRIN_10MIN", lambda x: int(x * 10), "INT16"),
        "precip_past1h": ("PRCRIN_1HOUR", lambda x: int(x * 10), "INT16"),
        "pressure": ("ARPRSS", lambda x: int(x * 10), "INT16"),
        "pressure_at_sea": ("SSPRSS", lambda x: int(x * 10), "INT16"),  
        "temp_dew": ("DEWTMP", lambda x: int(x * 10), "INT16"),
        "visibility": ("HVIS", lambda x: int(x), "INT32"),
        "radia_glob": ("GLBRAD_10MIN", lambda x: int(x * 600 / 10000), "INT16"),  # W/m² → J/cm² (10 minutes)
        "radia_glob_past1h": ("GLBRAD_1HOUR", lambda x: int(x * 3600 / 10000), "INT16"),  # W/m² → J/cm² (1 hour)
        "sun_last10min_glob": ("SUNDUR_10MIN", lambda x: int(x), "INT16"),  
        "wind_dir": ("WNDDIR", lambda x: int(x), "INT16"),  
        "wind_max": ("WNDSPD_10MIN_MAX", lambda x: int(x * 10), "INT16"),  
        "wind_max_per10min_past1h": ("WNDSPD_1HOUR_MAX", lambda x: int(x * 10), "INT16"),  
        "wind_speed": ("WNDSPD", lambda x: int(x * 10), "INT16"),  
        "weather": ("WX_original", lambda x: get_weather_description(x), "STR")
    }
    
    if parameter_id in parameter_mapping:
        field_name, converter, value_type = parameter_mapping[parameter_id]
        try:
            if value is None:
                return field_name, get_missing_value(value_type)
            converted_value = converter(value)
            return field_name, converted_value
        except (TypeError, ValueError):
            return field_name, get_invalid_value(value_type)
    return None, None

def get_missing_value(value_type):
    return MISSING_VALUES.get(value_type, None)

def get_invalid_value(value_type):
    return INVALID_VALUES.get(value_type, None)

def initialize_station_data(station_id):
    global STATION_TEMPLATE
    
    if STATION_TEMPLATE is None:  
        STATION_TEMPLATE = {
            "LCLID": "",
            "ID_GLOBAL_MNET": "",
            "HVIS": MISSING_INT32,  
            "HVIS_AQC": MISSING_INT8,
            "AMTCLD_8": MISSING_INT16,  
            "AMTCLD_8_AQC": MISSING_INT8,  
            "AIRTMP": MISSING_INT16,
            "AIRTMP_AQC": MISSING_INT8,
            "AIRTMP_1HOUR_MAX": MISSING_INT16,  
            "AIRTMP_1HOUR_MAX_AQC": MISSING_INT8,  
            "AIRTMP_1HOUR_AVG": MISSING_INT16,  
            "AIRTMP_1HOUR_AVG_AQC": MISSING_INT8,  
            "AIRTMP_1HOUR_MINI": MISSING_INT16,  
            "AIRTMP_1HOUR_MINI_AQC": MISSING_INT8,  
            "RHUM": MISSING_INT16,
            "RHUM_AQC": MISSING_INT8,
            "DEWTMP": MISSING_INT16,
            "DEWTMP_AQC": MISSING_INT8,
            "ARPRSS": MISSING_INT16,
            "ARPRSS_AQC": MISSING_INT8,
            "SSPRSS": MISSING_INT16, 
            "SSPRSS_AQC": MISSING_INT8,  
            "PRCRIN_10MIN": MISSING_INT16,
            "PRCRIN_10MIN_AQC": MISSING_INT8,
            "PRCRIN_1HOUR": MISSING_INT16,
            "PRCRIN_1HOUR_AQC": MISSING_INT8,
            "GLBRAD_10MIN": MISSING_INT16,  
            "GLBRAD_10MIN_AQC": MISSING_INT8,  
            "GLBRAD_1HOUR": MISSING_INT16,  
            "GLBRAD_1HOUR_AQC": MISSING_INT8,  
            "SUNDUR_10MIN": MISSING_INT16,  
            "SUNDUR_10MIN_AQC": MISSING_INT8,  
            "WNDDIR": MISSING_INT16,  
            "WNDDIR_AQC": MISSING_INT8,  
            "WNDSPD_10MIN_MAX": MISSING_INT16,  
            "WNDSPD_10MIN_MAX_AQC": MISSING_INT8,  
            "WNDSPD_1HOUR_MAX": MISSING_INT16,  
            "WNDSPD_1HOUR_MAX_AQC": MISSING_INT8,  
            "WNDSPD": MISSING_INT16,  
            "WNDSPD_AQC": MISSING_INT8,  
            "WX_original": "",
            "WX_original_AQC": MISSING_INT8
        }
    
    data = STATION_TEMPLATE.copy()
    data["LCLID"] = str(station_id)
    data["ID_GLOBAL_MNET"] = f"DMI_{station_id}"
    return data

def initialize_parameter_counts():
    return {
        "temp_dry": 0,
        "temp_max_past1h": 0,  
        "temp_mean_past1h": 0,  
        "temp_min_past1h": 0,  
        "cloud_cover": 0,
        "humidity": 0,
        "precip_past10min": 0,
        "precip_past1h": 0,
        "pressure": 0,
        "pressure_at_sea": 0,  
        "temp_dew": 0,
        "visibility": 0,
        "radia_glob": 0,  
        "radia_glob_past1h": 0,  
        "sun_last10min_glob": 0,  
        "wind_dir": 0,  
        "wind_max": 0,  
        "wind_max_per10min_past1h": 0, 
        "wind_speed": 0,  
        "weather": 0
    }

def fetch_all_data():

    cache_key = f"api_data_{datetime.now(timezone.utc).strftime('%Y%m%d_%H')}"
    
    cached_data = get_memory_cache(cache_key)
    if cached_data:
        print("Using cached API data")
        return cached_data

    params = {
        "period": "latest-hour",
        "bbox-crs": "https://www.opengis.net/def/crs/OGC/1.3/CRS84",
        "api-key": os.getenv("APIKey")
    }

    all_features = []
    page_count = 1
    current_url = base_url
    total_pages = None
    max_retries = 3 
    timeout = 30     

    try:
        while True:
            
            if page_count == 1:
                param_str = '&'.join(f"{key}={value}" for key, value in params.items())
                request_url = f"{current_url}?{param_str}"
            else:
                request_url = current_url

            retry_count = 0
            while retry_count < max_retries:
                try:
                    req = urllib.request.Request(request_url)
                    with urllib.request.urlopen(req, timeout=timeout) as response:
                        data = json.loads(response.read().decode())
                    break  
                except urllib.error.HTTPError as e:
                    if e.code == 504: 
                        retry_count += 1
                        if retry_count < max_retries:
                            print(f"Gateway timeout, retrying... (Attempt {retry_count + 1}/{max_retries})")
                            sleep(retry_count * 5)  
                            continue
                    print(f"HTTP Error: {e.code} - {e.reason}")
                    raise
                except urllib.error.URLError as e:
                    print(f"URL Error: {e.reason}")
                    raise
                except socket.timeout:
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"Request timed out, retrying... (Attempt {retry_count + 1}/{max_retries})")
                        sleep(retry_count * 5)
                        continue
                    raise urllib.error.URLError("Timeout after multiple retries")

            if retry_count >= max_retries:
                raise Exception("Maximum retry attempts reached")

            if 'features' not in data:
                print("Warning: No 'features' found in response")
                break

            features = data['features']
            if not features:
                print("Warning: Empty features array received")
                break

            all_features.extend(features)

            if 'numberMatched' in data and page_count == 1:
                total_records = data['numberMatched']
                records_per_page = len(features)
                total_pages = -(-total_records // records_per_page)
                print(f"Expected total pages: {total_pages}")

            next_link = None
            if 'links' in data:
                for link in data['links']:
                    if link['rel'] == 'next':
                        next_link = link['href']
                        break

            if not next_link:
                print("No more pages available")
                break

            current_url = next_link
            page_count += 1
            sleep(2)  

        print(f"Completed fetching {page_count} pages with total {len(all_features)} records")
        
        if total_pages and page_count != total_pages:
            print(f"Warning: Expected {total_pages} pages but got {page_count} pages")

        result = (all_features, page_count)
        if all_features:
            set_memory_cache(cache_key, result)
            print(f"API data cached with key: {cache_key}")

        return result

    except Exception as e:
        print(f"Error fetching data: {e}")
        raise


def process_feature(feature, station_data, parameter_counts):
    props = feature["properties"]
    station_id = props["stationId"]
    parameter_id = props["parameterId"]
    value = props["value"]
    observed = props.get("observed")

    if parameter_id in parameter_counts:
        parameter_counts[parameter_id] += 1

    if station_id not in station_data:
        station_data[station_id] = initialize_station_data(station_id)

    if observed:
        observed_dt = datetime.fromisoformat(observed.replace('Z', '+00:00'))
        current_timestamp = station_data[station_id].get(f"{parameter_id}_timestamp")
        if not current_timestamp or observed_dt > current_timestamp:
            field_name, converted_value = map_parameter_value(parameter_id, value)
            if field_name:
                station_data[station_id][field_name] = converted_value
                station_data[station_id][f"{field_name}_AQC"] = -99
                station_data[station_id][f"{parameter_id}_timestamp"] = observed_dt


def create_converted_json(station_data, observed_time):
    cleaned_station_data = []
    for station_id, data in station_data.items():
        cleaned_data = {key: value for key, value in data.items() if not key.endswith("_timestamp")}
        cleaned_station_data.append(cleaned_data)

    return {
        "tagid": "441000125",
        "announced": observed_time.strftime("%Y-%m-%dT%H:%M:00Z"),
        "created": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "original": {
            "observation_date": {
                "year": observed_time.year,
                "month": observed_time.month,
                "day": observed_time.day,
                "hour": observed_time.hour,
                "min": observed_time.minute,
                "sec": 0
            },
            "point_count": len(cleaned_station_data),
            "point_data": cleaned_station_data
        }
    }

def process_and_save_data():
    try:

        validate_env_vars()
        cleanup_memory_cache()

        current_time = datetime.now(timezone.utc)
        cache_key = f"processed_data_{current_time.strftime('%Y%m%d_%H')}"

        cached_result = get_memory_cache(cache_key)
        if cached_result:
            print("Using cached processed data")
            return cached_result

        print("Fetching data from API...")
        all_features, page_count = fetch_all_data()

        if not all_features:
            raise ValueError("No features found in API response")

        latest_observed = None
        for feature in all_features:
            observed = feature['properties'].get('observed')
            if observed:
                try:
                    observed_dt = datetime.fromisoformat(observed.replace('Z', '+00:00'))
                    if not latest_observed or observed_dt > latest_observed:
                        latest_observed = observed_dt
                except ValueError:
                    continue

        if not latest_observed:
            latest_observed = datetime.now(timezone.utc)

        complete_dataset = {
            "type": "FeatureCollection",
            "features": all_features
        }

        created = datetime.now(timezone.utc) 

        dataname = "DMI_OBS_AWS_raw"
        dataid16 = "0200600041000125"
        data_size = len(json.dumps(complete_dataset).encode('utf-8'))
        header_comment = base_url
        ruheader = create_ruheader(created, header_comment, dataname, dataid16, data_size)

        raw_json_data = json.dumps(complete_dataset, indent=4, ensure_ascii=False).encode('utf-8')
        combined_data = ruheader.encode('utf-8') + raw_json_data

        output_file_name = datetime.now(timezone.utc).strftime('%Y%m%d%H%M')
        raw_s3_key = generate_raw_s3_key(tagid, output_file_name)

        save_to_s3_raw(
            raw_data_bucket,
            raw_s3_key,
            combined_data
        )

        station_data = {}
        parameter_counts = initialize_parameter_counts()

        print("\nProcessing features...")
        for feature in all_features:
            process_feature(feature, station_data, parameter_counts)

        print(f"Processed {len(station_data)} unique stations")

        converted_result = create_converted_json(station_data, latest_observed)

        current_time = datetime.now()
        random_suffix = str(uuid.uuid4())
        json_file_name = f"{current_time.strftime('%Y%m%d%H%M%S')}.{random_suffix}"
        conv_s3_key = generate_json_s3_key(tagid, json_file_name)

        save_to_s3_converted(
            converted_bucket,
            conv_s3_key,
            json.dumps(converted_result, ensure_ascii=False, indent=2).encode('utf-8')
        )

        result = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data successfully processed and saved',
                'statistics': {
                    'total_records': len(all_features),
                    'total_stations': len(station_data),
                    'raw_data_location': f"s3://{raw_data_bucket}/{raw_s3_key}",
                    'converted_data_location': f"s3://{converted_bucket}/{conv_s3_key}",
                    'announced': created.strftime('%Y-%m-%dT%H:%M:%SZ'), 
                    'parameter_counts': parameter_counts
                }
            })
        }

        set_memory_cache(cache_key, result)
        print(f"Processing result cached with key: {cache_key}")

        return result

    except Exception as e:
        error_message = f"Error in process_and_save_data: {str(e)}"
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': error_message,
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
        }

def main(event=None, context=None):
    try:
        print("Starting data processing...")
        result = process_and_save_data()
        
        if result['statusCode'] == 200:
            stats = json.loads(result['body'])['statistics']
            print("\nProcessing Summary:")
            print(f"Total records processed: {stats['total_records']}")
            print(f"Total stations: {stats['total_stations']}")
            print(f"Raw data saved to: {stats['raw_data_location']}")
            print(f"Converted data saved to: {stats['converted_data_location']}")
        
        return result

    except Exception as e:
        error_message = f"Fatal error in main function: {str(e)}"
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': error_message,
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
        }

if __name__ == "__main__":
    main({}, {})
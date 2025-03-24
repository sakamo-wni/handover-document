import os
import json
import uuid
import csv
import bz2
import urllib.request
from datetime import datetime, timezone
import boto3

s3_client_eu = boto3.client("s3", region_name="ap-northeast-1")
s3_client_jp = boto3.client("s3", region_name="ap-northeast-1")

def validate_env_vars():
    """Validate required environment variables"""
    required_vars = ["RawDataBucket", "ConvertedBucket", "tagid", "URL"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

class Constants:
    MISSING_INT8 = -99
    MISSING_INT16 = -9999
    MISSING_INT32 = -999999
    INVALID_INT16 = -11111
    INVALID_INT32 = -1111111

def find_value_in_nested_list(nested, target_key):
    """Find a value with the given key in a nested data structure"""
    if isinstance(nested, dict):
        if nested.get("key") == target_key:
            return nested.get("value")
        for v in nested.values():
            result = find_value_in_nested_list(v, target_key)
            if result is not None:
                return result
    elif isinstance(nested, list):
        for item in nested:
            result = find_value_in_nested_list(item, target_key)
            if result is not None:
                return result
    return None

def count_precipitation_data(data):
    """Count different types of precipitation data in the raw BUFR JSON"""
    direct_10min_count = 0
    one_min_aggregated_count = 0
    one_hour_count = 0
    six_hour_count = 0  
    twelve_hour_count = 0  
    twenty_four_hour_count = 0
    other_time_period_count = 0
    
    def traverse(node, path=[]):
        nonlocal direct_10min_count, one_min_aggregated_count, one_hour_count, six_hour_count, twelve_hour_count, twenty_four_hour_count, other_time_period_count

        if isinstance(node, list):
            has_10min_period = False
            has_precipitation = False
            has_1hour_period = False
            has_6hour_period = False
            has_12hour_period = False
            has_24hour_period = False
            
            other_time_period = False
            other_time_period_value = None
            
            for item in node:
                if isinstance(item, dict):
                    if item.get("key") == "timePeriod":
                        time_period = item.get("value")
                        time_unit = item.get("units")
                        
                        if time_period == -10 and time_unit == "min":
                            has_10min_period = True
                        elif (time_period == -60 and time_unit == "min") or (time_period == -1 and time_unit == "h"):
                            has_1hour_period = True

                        elif (time_period == -360 and time_unit == "min") or (time_period == -6 and time_unit == "h"):
                            has_6hour_period = True
                        elif (time_period == -720 and time_unit == "min") or (time_period == -12 and time_unit == "h"):  # 追加
                            has_12hour_period = True  

                        elif (time_period == -24 and time_unit == "h") or (time_period == -1440 and time_unit == "min"):
                            has_24hour_period = True
                        elif time_unit == "min" or time_unit == "h":
                            other_time_period = True
                            other_time_period_value = f"{time_period} {time_unit}"
                    
                    elif item.get("key") == "totalPrecipitationOrTotalWaterEquivalent" and item.get("value") is not None:
                        has_precipitation = True
            
            if has_10min_period and has_precipitation:
                direct_10min_count += 1
            
            elif has_1hour_period and has_precipitation:
                one_hour_count += 1

            elif has_6hour_period and has_precipitation:  
                six_hour_count += 1  

            elif has_12hour_period and has_precipitation:  
                twelve_hour_count += 1  

            elif has_24hour_period and has_precipitation:
                twenty_four_hour_count += 1
        
            elif other_time_period and has_precipitation:
                other_time_period_count += 1
                
            if len(node) >= 2:
                has_1min_period = False
                has_time_increment = False
                has_replication_factor = False
                precipitation_values = []
                
                for item in node:
                    if isinstance(item, dict) and item.get("key") == "timePeriod" and item.get("value") == -1 and item.get("units") == "min":
                        has_1min_period = True
                        break
                
                if has_1min_period:
                    for item in node:
                        if isinstance(item, list):
                            for subitem in item:
                                if isinstance(subitem, dict):
                                    if subitem.get("key") == "timeIncrement" and subitem.get("value") == 1:
                                        has_time_increment = True
                                    elif subitem.get("key") == "delayedDescriptorReplicationFactor" and subitem.get("value") == 10:
                                        has_replication_factor = True
                                    elif subitem.get("key") == "totalPrecipitationOrTotalWaterEquivalent":
                                        precipitation_values.append(subitem)
                
                if has_1min_period and has_time_increment and has_replication_factor and len(precipitation_values) == 10:
                    one_min_aggregated_count += 1
            
            for item in node:
                traverse(item, path + [node])
                
        elif isinstance(node, dict):
            for value in node.values():
                traverse(value, path + [node])
    
    for message in data.get("messages", []):
        traverse(message)
    
    return {
        "direct_10min_count": direct_10min_count,
        "one_min_aggregated_count": one_min_aggregated_count,
        "one_hour_count": one_hour_count,
        "six_hour_count": six_hour_count,
        "twelve_hour_count": twelve_hour_count,
        "twenty_four_hour_count": twenty_four_hour_count,
        "other_time_period_count": other_time_period_count
    }

def convert_kelvin_to_tenths_celsius(value_str):
    """Convert Kelvin to tenths of Celsius"""
    if value_str in [None, "", "--", "-"]:
        return Constants.MISSING_INT16
    try:
        val_k = float(value_str)
        val_c = val_k - 273.15
        return int(val_c * 10)
    except ValueError:
        return Constants.MISSING_INT16

def convert_to_int(str_value, missing=Constants.MISSING_INT16):
    """Convert string value to integer"""
    if str_value in [None, "", "--", "-"]:
        return missing
    try:
        return int(float(str_value))
    except ValueError:
        return missing

def convert_to_int_with_factor(value_str, factor=1):
    """Convert string value to integer with multiplication factor"""
    if value_str in [None, "", "--", "-"]:
        return Constants.MISSING_INT16
    try:
        val = float(value_str)
        return int(val * factor)
    except ValueError:
        return Constants.MISSING_INT16

def convert_pa_to_tenths_hpa(value_str):
    """Convert Pascal to tenths of hectopascal"""
    if value_str in [None, "", "--", "-"]:
        return Constants.MISSING_INT16
    try:
        val_pa = float(value_str)
        val_hpa = val_pa / 100.0
        return int(val_hpa * 10)
    except ValueError:
        return Constants.MISSING_INT16

def convert_m_to_cm(value_str):
    """Convert meters to centimeters"""
    if value_str in [None, "", "--", "-"]:
        return Constants.MISSING_INT16
    try:
        val_m = float(value_str)
        val_cm = val_m * 100
        return int(val_cm)
    except ValueError:
        return Constants.MISSING_INT16

def load_weather_codes(csv_path=None):
    """Load weather code descriptions from CSV file"""
    if csv_path is None:
        csv_path = os.path.join(os.path.dirname(__file__), "DwdPresentWeather.csv")
    
    if not os.path.exists(csv_path):
        return {}
        
    mapping = {}
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code_str = row["Code"].strip()
            try:
                code = int(code_str)
            except ValueError:
                continue
            description = row["Description"].strip()
            sub_desc = row.get("Sub-description", "").strip()
            if sub_desc:
                full_description = f"{description} ({sub_desc})"
            else:
                full_description = description
            mapping[code] = full_description
    return mapping

def create_geojson_from_raw_data(data):
    """Create GeoJSON from raw BUFR data with actual coordinates"""
    latest_stations = {}
    
    for message in data.get("messages", []):
        header_time = {
            "year": find_value_in_nested_list(message, "typicalYear") or -9999,
            "month": find_value_in_nested_list(message, "typicalMonth") or -9999,
            "day": find_value_in_nested_list(message, "typicalDay") or -9999,
            "hour": find_value_in_nested_list(message, "typicalHour") or -9999,
            "minute": find_value_in_nested_list(message, "typicalMinute") or -9999
        }
        
        subsets = []
        for item in message:
            if isinstance(item, list):
                subsets = item
                break
                
        for subset in subsets:
            station_name = find_value_in_nested_list(subset, "stationOrSiteName") or "UNKNOWN"
            
            lat_raw = find_value_in_nested_list(subset, "latitude")
            lon_raw = find_value_in_nested_list(subset, "longitude")
            alt_raw = find_value_in_nested_list(subset, "heightOfStationGroundAboveMeanSeaLevel")
            
            if lat_raw in [None, ""] or lon_raw in [None, ""]:
                lat_f, lon_f, alt_f = 0.0, 0.0, 0.0
                include_alt = False
            else:
                try:
                    lat_f = float(lat_raw)
                    lon_f = float(lon_raw)
                except ValueError:
                    lat_f, lon_f, alt_f = 0.0, 0.0, 0.0
                    include_alt = False
                else:
                    if alt_raw in [None, ""]:
                        alt_f = None
                        include_alt = False
                    else:
                        try:
                            alt_f = float(alt_raw)
                            include_alt = True
                        except ValueError:
                            alt_f = None
                            include_alt = False
            
            timestamp_str = f"{header_time['year']:04d}-{header_time['month']:02d}-{header_time['day']:02d} {header_time['hour']:02d}:{header_time['minute']:02d}"
            latest_stations[station_name] = {
                "timestamp": timestamp_str,
                "lon": lon_f,
                "lat": lat_f,
                "alt": alt_f,
                "include_alt": include_alt
            }
    
    features = []
    for st_name, info in latest_stations.items():
        if info.get("include_alt"):
            coordinates = [info["lon"], info["lat"], info["alt"]]
        else:
            coordinates = [info["lon"], info["lat"]]
        
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": coordinates
            },
            "properties": {
                "LCLID": st_name,
                "LNAME": st_name,
                "CNTRY": "DE"
            }
        }
        features.append(feature)
    
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    return geojson

def process_structured_json(bufr_data, tagid):
    """Create structured JSON from BUFR data"""
    try:
        weather_mapping = load_weather_codes()
    except Exception as e:
        print(f"Warning: Could not load weather codes: {str(e)}")
        weather_mapping = {}
    
    unique_records = {}
    for message in bufr_data.get("messages", []):
        header_year = find_value_in_nested_list(message, "typicalYear") or -9999
        header_month = find_value_in_nested_list(message, "typicalMonth") or -9999
        header_day = find_value_in_nested_list(message, "typicalDay") or -9999
        header_hour = find_value_in_nested_list(message, "typicalHour") or -9999
        header_minute = find_value_in_nested_list(message, "typicalMinute") or -9999

        subsets = []
        for item in message:
            if isinstance(item, list):
                subsets = item
                break

        for subset in subsets:
            station_name = find_value_in_nested_list(subset, "stationOrSiteName") or "UNKNOWN"
            year = find_value_in_nested_list(subset, "year") or header_year
            month = find_value_in_nested_list(subset, "month") or header_month
            day = find_value_in_nested_list(subset, "day") or header_day
            hour = find_value_in_nested_list(subset, "hour") or header_hour
            minute = find_value_in_nested_list(subset, "minute") or header_minute
            
            timestamp_str = f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}"
            
            if station_name in unique_records and timestamp_str <= unique_records[station_name]["timestamp"]:
                continue

            point_dict = {}
            point_dict["LCLID"] = station_name
            point_dict["ID_GLOBAL_MNET"] = f"DWD_{station_name}"

            airtmp_at_2m = None
            
            def find_temperature_at_2m(node):
                if isinstance(node, list):
                    has_2m_height = False
                    temp_value = None
                    
                    for item in node:
                        if isinstance(item, dict):
                            if item.get("key") == "heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform" and item.get("value") == 2:
                                has_2m_height = True
                            elif has_2m_height and item.get("key") == "airTemperature":
                                temp_value = item.get("value")
                                return temp_value
                    
                    for item in node:
                        result = find_temperature_at_2m(item)
                        if result is not None:
                            return result
                    
                elif isinstance(node, dict):
                    for value in node.values():
                        if isinstance(value, (list, dict)):
                            result = find_temperature_at_2m(value)
                            if result is not None:
                                return result
                
                return None
            
            airtmp_at_2m = find_temperature_at_2m(subset)
            
            if airtmp_at_2m is not None:
                point_dict["AIRTMP"] = convert_kelvin_to_tenths_celsius(str(airtmp_at_2m))
            else:
                airtmp_str = find_value_in_nested_list(subset, "airTemperature")
                point_dict["AIRTMP"] = convert_kelvin_to_tenths_celsius(airtmp_str)
            
            point_dict["AIRTMP_AQC"] = Constants.MISSING_INT8

            point_dict["PRCRIN_10MIN"] = Constants.MISSING_INT16
            point_dict["PRCRIN_10MIN_AQC"] = Constants.MISSING_INT8
            point_dict["PRCRIN_1HOUR"] = Constants.MISSING_INT16
            point_dict["PRCRIN_1HOUR_AQC"] = Constants.MISSING_INT8
            point_dict["PRCRIN_24HOUR"] = Constants.MISSING_INT16
            point_dict["PRCRIN_24HOUR_AQC"] = Constants.MISSING_INT8

            precip_data = []
            
            def find_precipitation_data(node, precip_data):
                """Find and categorize precipitation data by time period"""
                if isinstance(node, list):
                    time_period = None
                    time_unit = None
                    has_time_increment = False
                    has_replication_factor = False
                    precipitation_values = []
                    
                    for item in node:
                        if isinstance(item, dict) and item.get("key") == "timePeriod":
                            time_period = item.get("value")
                            time_unit = item.get("units")
                    
                    if time_period == -10 and time_unit == "min":
                        for item in node:
                            if isinstance(item, dict) and item.get("key") == "totalPrecipitationOrTotalWaterEquivalent":
                                precip_data.append({"type": "10min_direct", "value": item.get("value")})
                    elif (time_period == -60 and time_unit == "min") or (time_period == -1 and time_unit == "h"):
                        for item in node:
                            if isinstance(item, dict) and item.get("key") == "totalPrecipitationOrTotalWaterEquivalent":
                                precip_data.append({"type": "1hour", "value": item.get("value")})

                    elif (time_period == -360 and time_unit == "min") or (time_period == -6 and time_unit == "h"): 
                        for item in node:  
                            if isinstance(item, dict) and item.get("key") == "totalPrecipitationOrTotalWaterEquivalent": 
                                precip_data.append({"type": "6hour", "value": item.get("value")})  

                    elif (time_period == -720 and time_unit == "min") or (time_period == -12 and time_unit == "h"):
                        for item in node:  
                            if isinstance(item, dict) and item.get("key") == "totalPrecipitationOrTotalWaterEquivalent":  
                                precip_data.append({"type": "12hour", "value": item.get("value")}) 

                    elif (time_period == -24 and time_unit == "h") or (time_period == -1440 and time_unit == "min"):
                        for item in node:
                            if isinstance(item, dict) and item.get("key") == "totalPrecipitationOrTotalWaterEquivalent":
                                precip_data.append({"type": "24hour", "value": item.get("value")})
                    elif time_period is not None and time_unit in ("min", "h") and time_period not in (-10, -60, -1, -6, -360, -12, -720, -24, -1440):

                        for item in node:
                            if isinstance(item, dict) and item.get("key") == "totalPrecipitationOrTotalWaterEquivalent":
                                other_period = f"{time_period} {time_unit}"
                                print(f"[WARNING] Found precipitation data with non-standard time period: {other_period}")
                                precip_data.append({"type": "other_period", "value": item.get("value"), "period": other_period})
                    elif time_period == -1 and time_unit == "min":
                        for item in node:
                            if isinstance(item, list):
                                increment_val = None
                                replication_val = None
                                local_precip_values = []
                                
                                for subitem in item:
                                    if isinstance(subitem, dict):
                                        if subitem.get("key") == "timeIncrement":
                                            increment_val = subitem.get("value")
                                        elif subitem.get("key") == "delayedDescriptorReplicationFactor":
                                            replication_val = subitem.get("value")
                                        elif subitem.get("key") == "totalPrecipitationOrTotalWaterEquivalent":
                                            local_precip_values.append(subitem.get("value"))
                                
                                if increment_val == 1 and replication_val == 10 and len(local_precip_values) == 10:
                                    total_value = sum(float(val) for val in local_precip_values if val is not None)
                                    precip_data.append({"type": "1min_aggregated", "value": total_value})
                    
                    for item in node:
                        find_precipitation_data(item, precip_data)
                elif isinstance(node, dict):
                    for value in node.values():
                        if isinstance(value, (list, dict)):
                            find_precipitation_data(value, precip_data)

            find_precipitation_data(subset, precip_data)

            point_dict["PRCRIN_10MIN"] = Constants.MISSING_INT16
            point_dict["PRCRIN_10MIN_AQC"] = Constants.MISSING_INT8
            point_dict["PRCRIN_1HOUR"] = Constants.MISSING_INT16
            point_dict["PRCRIN_1HOUR_AQC"] = Constants.MISSING_INT8
            point_dict["PRCRIN_6HOUR"] = Constants.MISSING_INT16
            point_dict["PRCRIN_6HOUR_AQC"] = Constants.MISSING_INT8
            point_dict["PRCRIN_12HOUR"] = Constants.MISSING_INT16  
            point_dict["PRCRIN_12HOUR_AQC"] = Constants.MISSING_INT8  
            point_dict["PRCRIN_24HOUR"] = Constants.MISSING_INT16
            point_dict["PRCRIN_24HOUR_AQC"] = Constants.MISSING_INT8
            for data in precip_data:
                if data["value"] is not None:
                    try:
                        if data["type"] in ("10min_direct", "1min_aggregated"):
                            point_dict["PRCRIN_10MIN"] = int(float(data["value"]))
                        elif data["type"] == "1hour":
                            point_dict["PRCRIN_1HOUR"] = int(float(data["value"]))
                        elif data["type"] == "6hour":  
                            point_dict["PRCRIN_6HOUR"] = int(float(data["value"]))  
                        elif data["type"] == "12hour":  
                            point_dict["PRCRIN_12HOUR"] = int(float(data["value"])) 
                        elif data["type"] == "24hour":
                            point_dict["PRCRIN_24HOUR"] = int(float(data["value"]))
                        elif data["type"] == "other_period":
                            print(f"[WARNING] Station {station_name}: Using INVALID_INT16 for precipitation with period {data['period']}")
                            point_dict["PRCRIN_10MIN"] = Constants.INVALID_INT16
                            point_dict["PRCRIN_1HOUR"] = Constants.INVALID_INT16
                            point_dict["PRCRIN_24HOUR"] = Constants.INVALID_INT16
                    except (ValueError, TypeError):
                        pass  

            point_dict["SNWDPT"] = Constants.MISSING_INT16
            point_dict["SNWDPT_AQC"] = Constants.MISSING_INT8

            snow_depth_str = find_value_in_nested_list(subset, "totalSnowDepth")
            point_dict["SNWDPT"] = convert_m_to_cm(snow_depth_str)
            point_dict["SNWDPT_AQC"] = Constants.MISSING_INT8

            cloud_str = find_value_in_nested_list(subset, "cloudCoverTotal")
            if cloud_str is not None:
                try:
                    point_dict["AMTCLD"] = int(float(cloud_str))
                except ValueError:
                    point_dict["AMTCLD"] = Constants.MISSING_INT16
            else:
                point_dict["AMTCLD"] = Constants.MISSING_INT16
            point_dict["AMTCLD_AQC"] = Constants.MISSING_INT8

            hvis_str = find_value_in_nested_list(subset, "horizontalVisibility")
            if hvis_str in [None, "", "--", "-"]:
                point_dict["HVIS"] = Constants.MISSING_INT32
            else:
                point_dict["HVIS"] = convert_to_int_with_factor(hvis_str, factor=1)
            point_dict["HVIS_AQC"] = Constants.MISSING_INT8

            gust_dir_str = find_value_in_nested_list(subset, "maximumWindGustDirection")
            point_dict["GUSTD"] = convert_to_int_with_factor(gust_dir_str, factor=1)
            point_dict["GUSTD_AQC"] = Constants.MISSING_INT8

            gust_speed_str = find_value_in_nested_list(subset, "maximumWindGustSpeed")
            if gust_speed_str is not None:
                try:
                    point_dict["GUSTS"] = int(float(gust_speed_str) * 10)
                except ValueError:
                    point_dict["GUSTS"] = Constants.MISSING_INT16
            else:
                point_dict["GUSTS"] = Constants.MISSING_INT16
            point_dict["GUSTS_AQC"] = Constants.MISSING_INT8

            w10m_str = find_value_in_nested_list(subset, "maximumWindSpeed10MinuteMeanWind")
            point_dict["WNDSPD_10MIN_AVG"] = convert_to_int_with_factor(w10m_str, factor=10)
            point_dict["WNDSPD_10MIN_AVG_AQC"] = Constants.MISSING_INT8

            mini_tmp_str = find_value_in_nested_list(subset, "minimumTemperatureAt2M")
            point_dict["AIRTMP_1HOUR_MINI"] = convert_kelvin_to_tenths_celsius(mini_tmp_str)
            point_dict["AIRTMP_1HOUR_MINI_AQC"] = Constants.MISSING_INT8

            max_tmp_str = find_value_in_nested_list(subset, "maximumTemperatureAt2M")
            point_dict["AIRTMP_1HOUR_MAX"] = convert_kelvin_to_tenths_celsius(max_tmp_str)
            point_dict["AIRTMP_1HOUR_MAX_AQC"] = Constants.MISSING_INT8

            dew_str = find_value_in_nested_list(subset, "dewpointTemperature")
            point_dict["DEWTMP"] = convert_kelvin_to_tenths_celsius(dew_str)
            point_dict["DEWTMP_AQC"] = Constants.MISSING_INT8

            rhum_str = find_value_in_nested_list(subset, "relativeHumidity")
            if rhum_str is not None:
                try:
                    point_dict["RHUM"] = int(float(rhum_str) * 10)
                except ValueError:
                    point_dict["RHUM"] = Constants.MISSING_INT16
            else:
                point_dict["RHUM"] = Constants.MISSING_INT16
            point_dict["RHUM_AQC"] = Constants.MISSING_INT8

            press_str = find_value_in_nested_list(subset, "nonCoordinatePressure")
            if press_str is not None:
                try:
                    point_dict["ARPRSS"] = int(float(press_str) / 100.0 * 10)
                except ValueError:
                    point_dict["ARPRSS"] = Constants.MISSING_INT16
            else:
                point_dict["ARPRSS"] = Constants.MISSING_INT16
            point_dict["ARPRSS_AQC"] = Constants.MISSING_INT8

            mslp_str = find_value_in_nested_list(subset, "pressureReducedToMeanSeaLevel")
            if mslp_str is not None:
                try:
                    point_dict["SSPRSS"] = int(float(mslp_str) / 100.0 * 10)
                except ValueError:
                    point_dict["SSPRSS"] = Constants.MISSING_INT16
            else:
                point_dict["SSPRSS"] = Constants.MISSING_INT16
            point_dict["SSPRSS_AQC"] = Constants.MISSING_INT8

            windspd_str = find_value_in_nested_list(subset, "windSpeed")
            if windspd_str is not None:
                try:
                    point_dict["WNDSPD"] = int(float(windspd_str) * 10)
                except ValueError:
                    point_dict["WNDSPD"] = Constants.MISSING_INT16
            else:
                point_dict["WNDSPD"] = Constants.MISSING_INT16
            point_dict["WNDSPD_AQC"] = Constants.MISSING_INT8

            rad_str = find_value_in_nested_list(subset, "globalSolarRadiationIntegratedOverPeriodSpecified")
            if rad_str is not None:
                try:
                    point_dict["GLBRAD_1HOUR"] = int(float(rad_str))
                except ValueError:
                    point_dict["GLBRAD_1HOUR"] = Constants.MISSING_INT16
            else:
                point_dict["GLBRAD_1HOUR"] = Constants.MISSING_INT16
            point_dict["GLBRAD_1HOUR_AQC"] = Constants.MISSING_INT8

            wx_str = find_value_in_nested_list(subset, "presentWeather")
            code = convert_to_int(wx_str, missing=Constants.MISSING_INT16)
            if code == Constants.MISSING_INT16 or code not in weather_mapping:
                weather_str = ""
            else:
                weather_str = weather_mapping.get(code, "")
            point_dict["WX_original"] = weather_str
            point_dict["WX_original_AQC"] = Constants.MISSING_INT8

            unique_records[station_name] = {"timestamp": timestamp_str, "data": point_dict}

    point_data_list = [record["data"] for record in unique_records.values()]
    point_count = len(point_data_list)

    now_utc = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    announced_str = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    created_str = announced_str
    observation_date = {
        "year": now_utc.year,
        "month": now_utc.month,
        "day": now_utc.day,
        "hour": now_utc.hour,
        "min": now_utc.minute,
        "sec": now_utc.second
    }

    final_json = {
            "tagid": tagid,
            "announced": announced_str,
            "created": created_str,
            "original": {
                "observation_date": observation_date,
                "point_count": point_count,
                "point_data": point_data_list
            }
        }
    
    precip_counts = count_precipitation_data(bufr_data)
    print(f"[PRECIP LOG] 10分間降水量(直接): {precip_counts['direct_10min_count']}件")
    print(f"[PRECIP LOG] 1分値*10データから合計した10分間降水量: {precip_counts['one_min_aggregated_count']}件")
    print(f"[PRECIP LOG] 1時間降水量: {precip_counts['one_hour_count']}件")
    print(f"[PRECIP LOG] 6時間降水量: {precip_counts['six_hour_count']}件")
    print(f"[PRECIP LOG] 12時間降水量: {precip_counts['twelve_hour_count']}件") 
    print(f"[PRECIP LOG] 24時間降水量: {precip_counts['twenty_four_hour_count']}件")
    print(f"[PRECIP LOG] その他の時間降水量: {precip_counts['other_time_period_count']}件")
    return final_json

def process_full_data():
    try:
        validate_env_vars()
        raw_bucket = os.getenv("RawDataBucket")
        converted_bucket = os.getenv("ConvertedBucket")
        tagid = os.getenv("tagid")
        base_url = os.getenv("URL")

        print(f"Downloading data from: {base_url}")
        with urllib.request.urlopen(base_url) as response:
            if response.status != 200:
                raise RuntimeError(f"Failed to download file. status code={response.status}")
            compressed_data = response.read()

        print("Decompressing bz2 data in memory...")
        decompressed_data = bz2.decompress(compressed_data)

        now = datetime.now(timezone.utc)
        raw_key = f"{tagid}/{now.strftime('%Y/%m/%d')}/{now.strftime('%Y%m%d%H%M%S')}_raw.json"
        s3_client_eu.put_object(
            Bucket=raw_bucket,
            Key=raw_key,
            Body=decompressed_data,
            ContentType='application/json'
        )
        print(f"Raw data saved to s3://{raw_bucket}/{raw_key}")

        bufr_data = json.loads(decompressed_data.decode('utf-8'))

        structured_json = process_structured_json(bufr_data, tagid)
        structured_json_str = json.dumps(structured_json, ensure_ascii=False, indent=2)
        obs_filename = f"{now.strftime('%Y%m%d%H%M%S')}.{str(uuid.uuid4())}"
        structured_key = f"data/{tagid}/{now.strftime('%Y/%m/%d')}/{obs_filename}"
        s3_client_jp.put_object(
            Bucket=converted_bucket,
            Key=structured_key,
            Body=structured_json_str.encode('utf-8'),
            ContentType='application/json'
        )
        print(f"Structured JSON saved to s3://{converted_bucket}/{structured_key}")

        geojson_data = create_geojson_from_raw_data(bufr_data)
        geojson_str = json.dumps(geojson_data, ensure_ascii=False, indent=2)
        geojson_key = "metadata/spool/DWD_SYNOP/metadata.json"
        s3_client_jp.put_object(
            Bucket=converted_bucket,
            Key=geojson_key,
            Body=geojson_str.encode('utf-8'),
            ContentType='application/json'
        )
        print(f"GeoJSON saved to s3://{converted_bucket}/{geojson_key}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Data processed successfully",
                "raw_data_location": f"s3://{raw_bucket}/{raw_key}",
                "structured_json_location": f"s3://{converted_bucket}/{structured_key}",
                "geojson_location": f"s3://{converted_bucket}/{geojson_key}"
            })
        }
    except Exception as e:
        error_message = f"Error in process_full_data: {str(e)}"
        print(error_message)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": error_message,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
        }

def process_observation_data():
    try:
        validate_env_vars()
        raw_bucket = os.getenv("RawDataBucket")
        converted_bucket = os.getenv("ConvertedBucket")
        tagid = os.getenv("tagid")
        base_url = os.getenv("URL")

        print(f"Downloading data from: {base_url}")
        with urllib.request.urlopen(base_url) as response:
            if response.status != 200:
                raise RuntimeError(f"Failed to download file. status code={response.status}")
            compressed_data = response.read()

        print("Decompressing bz2 data in memory...")
        decompressed_data = bz2.decompress(compressed_data)

        now = datetime.now(timezone.utc)
        raw_key = f"{tagid}/{now.strftime('%Y/%m/%d')}/{now.strftime('%Y%m%d%H%M%S')}_raw.json"
        s3_client_eu.put_object(
            Bucket=raw_bucket,
            Key=raw_key,
            Body=decompressed_data,
            ContentType='application/json'
        )
        print(f"Raw data saved to s3://{raw_bucket}/{raw_key}")

        bufr_data = json.loads(decompressed_data.decode('utf-8'))

        structured_json = process_structured_json(bufr_data, tagid)
        structured_json_str = json.dumps(structured_json, ensure_ascii=False, indent=2)
        obs_filename = f"{now.strftime('%Y%m%d%H%M%S')}.{str(uuid.uuid4())}"
        structured_key = f"data/{tagid}/{now.strftime('%Y/%m/%d')}/{obs_filename}"
        s3_client_jp.put_object(
            Bucket=converted_bucket,
            Key=structured_key,
            Body=structured_json_str.encode('utf-8'),
            ContentType='application/json'
        )
        print(f"Structured JSON saved to s3://{converted_bucket}/{structured_key}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Observation data processed successfully",
                "raw_data_location": f"s3://{raw_bucket}/{raw_key}",
                "structured_json_location": f"s3://{converted_bucket}/{structured_key}"
            })
        }
    except Exception as e:
        error_message = f"Error in process_observation_data: {str(e)}"
        print(error_message)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": error_message,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
        }

def process_json_data(json_file):
    try:
        print(f"Reading local file: {json_file}")
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        if "point_data" in data and "tagid" in data:
            print("File is already in structured JSON format")
            tagid = data.get("tagid", "DWD_SYNOP")
            
            geojson_data = {
                "type": "FeatureCollection",
                "features": []
            }
            
            for point in data.get("point_data", []):
                station_name = point.get("LCLID", "UNKNOWN")
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [0.0, 0.0]  
                    },
                    "properties": {
                        "LCLID": station_name,
                        "LNAME": station_name,
                        "CNTRY": "DE"
                    }
                }
                geojson_data["features"].append(feature)
                        
        else:
            print("Processing raw data format")
            tagid = "DWD_SYNOP" 
            
            precip_counts = count_precipitation_data(data)
            print(f"[PRECIP LOG] 10分間降水量(直接): {precip_counts['direct_10min_count']}件")
            print(f"[PRECIP LOG] 1分値*10 個から合計した10分間降水量: {precip_counts['one_min_aggregated_count']}件")
            print(f"[PRECIP LOG] 1時間降水量: {precip_counts['one_hour_count']}件")
            print(f"[PRECIP LOG] 6時間降水量: {precip_counts['six_hour_count']}件")
            print(f"[PRECIP LOG] 12時間降水量: {precip_counts['twelve_hour_count']}件")
            print(f"[PRECIP LOG] 24時間降水量: {precip_counts['twenty_four_hour_count']}件")
            print(f"[PRECIP LOG] その他の時間降水量: {precip_counts['other_time_period_count']}件")
            
            structured_json = process_structured_json(data, tagid)
            data = structured_json
            
            geojson_data = create_geojson_from_raw_data(data)
        
        geojson_str = json.dumps(geojson_data, ensure_ascii=False, indent=2)
        geojson_filename = "metadata.json"
        with open(geojson_filename, "w", encoding="utf-8") as f:
            f.write(geojson_str)
        print(f"GeoJSON saved to {geojson_filename}")
        
        now = datetime.now()
        new_filename = f"{now.strftime('%Y%m%d%H%M%S')}.{str(uuid.uuid4())}.json"
        
        if isinstance(data, dict) and "point_data" in data:
            data["announced"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            data["created"] = data["announced"]
            data["observation_date"] = {
                "year": now.year,
                "month": now.month,
                "day": now.day,
                "hour": now.hour,
                "min": now.minute,
                "sec": now.second
            }
            
            structured_json_str = json.dumps(data, ensure_ascii=False, indent=2)
            with open(new_filename, "w", encoding="utf-8") as f:
                f.write(structured_json_str)
            print(f"Updated JSON saved to {new_filename}")
        
        return {
            "status": "success",
            "structured_json_location": new_filename,
            "geojson_location": geojson_filename
        }
    except Exception as e:
        error_message = f"Error in process_json_data: {str(e)}"
        print(error_message)
        return {
            "status": "error",
            "error": error_message
        }

def main(event, context):
    try:
        resources = event.get('resources', [])
        if not resources:
            print("No resources found in event. Running default process.")
            return process_full_data()
            
        rule_name = resources[0].split('/')[-1]
        
        if 'StationRule' in rule_name:
            print("Triggered by StationRule: Processing both station and observation data.")
            return process_full_data()
        elif 'ObservationRule' in rule_name:
            print("Triggered by ObservationRule: Processing observation data only.")
            return process_observation_data()
        else:
            print(f"Unknown rule triggered: {rule_name}. Running default process.")
            return process_full_data()
    except Exception as e:
        error_message = f"Error in lambda_handler: {str(e)}"
        print(error_message)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": error_message,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
        }

if __name__ == "__main__":
    dummy_event = {}
    print(main(dummy_event, None))
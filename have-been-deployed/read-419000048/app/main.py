import json
import datetime
import xml.etree.ElementTree as ET
import re
from datetime import datetime, timezone
import urllib.request
import boto3
import os
import uuid

s3 = boto3.client('s3')
save_bucket = os.environ.get("save_bucket", None)
tagid = os.environ.get("tagid")

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

WEATHER_BASE_URL = os.environ.get("WEATHER_BASE_URL", "https://dd.weather.gc.ca/observations/xml")
provinces = ['AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'NU', 'ON', 'PE', 'QC', 'SK', 'YT']

memory_cache = {}
CACHE_EXPIRY = 3600  

def get_memory_cache(key):
    try:
        if key in memory_cache:
            cache_data = memory_cache[key]
            if datetime.now(timezone.utc).timestamp() - cache_data['timestamp'] < CACHE_EXPIRY:
                print(f"Cache hit for key: {key}")
                return cache_data['data']
            else:
                print(f"Cache expired for key: {key}")
                del memory_cache[key]
    except Exception as e:
        print(f"Error accessing cache: {e}")
    return None

def set_memory_cache(key, data): 
    try:
        memory_cache[key] = {
            'timestamp': datetime.now(timezone.utc).timestamp(),
            'data': data
        }
        print(f"Cache saved for key: {key}")
        return True
    except Exception as e:
        print(f"Error saving to cache: {e}")
        return False

def cleanup_memory_cache():
    current_time = datetime.now(timezone.utc).timestamp()
    expired_keys = [
        key for key, cache_data in memory_cache.items()
        if current_time - cache_data['timestamp'] > CACHE_EXPIRY
    ]
    for key in expired_keys:
        del memory_cache[key]
    if expired_keys:
        print(f"Cleaned up {len(expired_keys)} expired cache entries")

def fetch_xml_data(province):
    try:
        url = f"{WEATHER_BASE_URL}/{province}/hourly/"
        print(f"Accessing directory: {url}")
        
        try:
            response = urllib.request.urlopen(url, timeout=30)
            response_data = response.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            print(f"HTTP Error for {province}: {e.code} - {e.reason}")
            return None
        except urllib.error.URLError as e:
            print(f"URL Error for {province}: {e.reason}")
            return None
        
        latest_files = re.findall(r'href="hourly_[a-z]{2}_(\d{10}_e\.xml)"', response_data)
        
        if latest_files:
            latest_file = latest_files[-1]
            file_url = f"{url}hourly_{province.lower()}_{latest_file}"
            print(f"Attempting to fetch: {file_url}")
            
            try:
                response = urllib.request.urlopen(file_url, timeout=30)
                xml_content = response.read().decode("utf-8")
                
                if xml_content:
                    print(f"Successfully retrieved XML for {province} ({len(xml_content)} bytes)")
                    return xml_content
                    
            except urllib.error.HTTPError as e:
                print(f"HTTP Error fetching file for {province}: {e.code} - {e.reason}")
                return None
            except urllib.error.URLError as e:
                print(f"URL Error fetching file for {province}: {e.reason}")
                return None
                
        print(f"No matching files found for province {province}")
        return None
            
    except Exception as e:
        print(f"Error fetching data for {province}: {str(e)}")
        return None

def parse_xml_to_dict(xml_data):
    try:
        if not xml_data:
            print("Empty XML data received")
            return [], None

        root = ET.fromstring(xml_data)
        stations_data = []
        latest_observation_time = None
        
        namespaces = {
            'om': 'http://www.opengis.net/om/1.0',
            'gml': 'http://www.opengis.net/gml',
            'xlink': 'http://www.w3.org/1999/xlink',
            'default': 'http://dms.ec.gc.ca/schema/point-observation/2.1'
        }

        members = root.findall('.//om:member', namespaces)
        print(f"Found {len(members)} members in XML")

        for member in members:
            try:
                observation_time_elem = member.find('.//default:element[@name="observation_date_utc"]', namespaces)
                if observation_time_elem is not None:
                    try:
                        observation_time = datetime.strptime(
                            observation_time_elem.get('value'),
                            '%Y-%m-%dT%H:%M:%S.%fZ'
                        ).replace(tzinfo=timezone.utc)
                        
                        if latest_observation_time is None or observation_time > latest_observation_time:
                            latest_observation_time = observation_time
                            print(f"Updated latest observation time: {latest_observation_time}")
                    except ValueError as e:
                        print(f"Error parsing observation time: {e}")

                identification = member.find('.//default:identification-elements', namespaces)
                if identification is None:
                    continue

                station_id = None
                for element in identification.findall('default:element', namespaces):
                    if element.get('name') == 'climate_station_number':
                        station_id = element.get('value')
                        break

                if station_id is None:
                    continue

                elements = member.find('.//default:elements', namespaces)
                if elements is None:
                    continue

                def get_element_value(elements, name):
                    element = elements.find(f"default:element[@name='{name}']", namespaces)
                    if element is not None and element.get('value') and element.get('value').strip():
                        try:
                            value = element.get('value').strip()
                            if value not in ['', '--', 'null']:
                                if name == 'total_cloud_cover':
                                    if value == '/':
                                        return INVALID_VALUES["INT16"]
                                    try:
                                        return int(value)
                                    except ValueError:
                                        return INVALID_VALUES["INT16"]

                                if name in ['wind_speed', 'wind_gust_speed']:
                                    try:
                                        speed_kmh = float(value)
                                        speed_ms = speed_kmh * 0.277778  # km/hからm/sに変換
                                        return int(speed_ms * 10)
                                    except ValueError:
                                        return MISSING_VALUES["INT16"]

                                if name == 'horizontal_visibility':
                                    try:
                                        vis_km = float(value)
                                        return int(vis_km * 1000)  # kmからmに変換
                                    except ValueError:
                                        return MISSING_VALUES["INT32"]

                                if name == 'mean_sea_level':
                                    try:
                                        pressure_kpa = float(value)
                                        pressure_hpa = pressure_kpa * 10  # kPaからhPaに変換
                                        return int(pressure_hpa * 10)
                                    except ValueError:
                                        return MISSING_VALUES["INT16"]

                                if name == 'wind_direction':
                                    direction_map = {
                                        'N': 16, 'NNE': 1, 'NE': 2, 'ENE': 3,
                                        'E': 4, 'ESE': 5, 'SE': 6, 'SSE': 7,
                                        'S': 8, 'SSW': 9, 'SW': 10, 'WSW': 11,
                                        'W': 12, 'WNW': 13, 'NW': 14, 'NNW': 15
                                    }
                                    return direction_map.get(value, MISSING_VALUES["INT16"])

                                if name in ['air_temperature', 'relative_humidity', 'dew_point']:
                                    try:
                                        return int(float(value) * 10)
                                    except ValueError:
                                        return MISSING_VALUES["INT16"]

                        except (ValueError, TypeError):
                            pass
                    return MISSING_VALUES["INT16"] if name != 'horizontal_visibility' else MISSING_VALUES["INT32"]

                    
                def get_present_weather(elements):
                    element = elements.find(f"default:element[@name='present_weather']", namespaces)
                    if element is not None and element.get('value'):
                        return str(element.get('value')).strip()
                    return ""

                station_data = {
                    "LCLID": str(station_id),
                    "ID_GLOBAL_MNET": f"MSC_{station_id}",
                    "HVIS": get_element_value(elements, 'horizontal_visibility'),
                    "HVIS_AQC": MISSING_INT8,
                    "WNDSPD": get_element_value(elements, 'wind_speed'),
                    "WNDSPD_AQC": MISSING_INT8,
                    "GUSTS": get_element_value(elements, 'wind_gust_speed'),
                    "GUSTS_AQC": MISSING_INT8,
                    "WNDDIR_16": get_element_value(elements, 'wind_direction'),
                    "WNDDIR_16_AQC": MISSING_INT8,
                    "AIRTMP": get_element_value(elements, 'air_temperature'),
                    "AIRTMP_AQC": MISSING_INT8,
                    "DEWTMP": get_element_value(elements, 'dew_point'),
                    "DEWTMP_AQC": MISSING_INT8,
                    "RHUM": get_element_value(elements, 'relative_humidity'),
                    "RHUM_AQC": MISSING_INT8,
                    "AMTCLD_8": get_element_value(elements, 'total_cloud_cover'),
                    "AMTCLD_8_AQC": MISSING_INT8,
                    "SSPRSS": get_element_value(elements, 'mean_sea_level'),
                    "SSPRSS_AQC": MISSING_INT8,
                    "WX_original": get_present_weather(elements),
                    "WX_original_AQC": MISSING_INT8
                }

                stations_data.append(station_data)

            except Exception as e:
                print(f"Error processing station: {str(e)}")
                continue

        return stations_data, latest_observation_time

    except Exception as e:
        print(f"Error parsing XML: {str(e)}")
        return [], None

def combine_province_data(provinces, execution_time=None):
    all_stations_data = []
    latest_observation_time = None
    
    for province in provinces:
        try:
            print(f"\nProcessing province: {province}")
            xml_content = fetch_xml_data(province)
            if xml_content:
                province_data, xml_observation_time = parse_xml_to_dict(xml_content)
                if province_data:
                    all_stations_data.extend(province_data)
                    if xml_observation_time:
                        print(f"XML observation time for {province}: {xml_observation_time}")
                    if latest_observation_time is None or (xml_observation_time and xml_observation_time > latest_observation_time):
                        latest_observation_time = xml_observation_time
                    print(f"Successfully processed {len(province_data)} stations from {province}")
                else:
                    print(f"No valid data parsed for province {province}")
            else:
                print(f"No XML content received for province {province}")
            
        except Exception as e:
            print(f"Error processing province {province}: {str(e)}")
            continue

    observation_time = execution_time or datetime.now(timezone.utc)
    
    print(f"Latest XML observation time: {latest_observation_time}")
    print(f"Execution time used: {observation_time}")

    json_data = {
        "tagid": tagid,
        "announced": observation_time.strftime("%Y-%m-%dT%H:%M:00Z"),
        "created": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "original": {
            "observation_date": {
                "year": observation_time.year,
                "month": observation_time.month,
                "day": observation_time.day,
                "hour": observation_time.hour,
                "min": observation_time.minute,
                "sec": 0
            },
            "point_count": len(all_stations_data),
            "point_data": all_stations_data
        }
    }
    
    return json_data

def save_to_s3(data):
    try:
        if not save_bucket:
            raise ValueError("save_bucket environment variable not set")

        current_time = datetime.now(timezone.utc)
        random_suffix = str(uuid.uuid4())
        file_name = f"{current_time.strftime('%Y%m%d%H%M%S')}.{random_suffix}"
        save_key = f"data/{tagid}/{current_time.strftime('%Y/%m/%d')}/{file_name}"

        json_data = json.dumps(data, ensure_ascii=False, indent=2)
        
        s3.put_object(
            Bucket=save_bucket,
            Key=save_key,
            Body=json_data.encode('utf-8'),
            ContentType='application/json'
        )
        
        print(f"Successfully saved combined data to s3://{save_bucket}/{save_key}")
        return save_key
    except Exception as e:
        print(f"Error saving to S3: {e}")
        return None

def main(event, context):
    try:
        cleanup_memory_cache()
        
        event_time = None
        if event and isinstance(event, dict) and 'time' in event:
            try:
                event_time = datetime.strptime(
                    event['time'], 
                    '%Y-%m-%dT%H:%M:%SZ'
                ).replace(tzinfo=timezone.utc)
                print(f"EventBridge execution time: {event_time}")
            except (ValueError, KeyError) as e:
                print(f"Could not parse event time: {e}")
        
        combined_data = combine_province_data(provinces, event_time)
        
        if combined_data and combined_data["original"]["point_count"] > 0:
            save_key = save_to_s3(combined_data)
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Processing completed successfully',
                    'total_provinces': len(provinces),
                    'total_stations': combined_data["original"]["point_count"],
                    'save_key': save_key,
                    'execution_time': event_time.isoformat() if event_time else None
                }, ensure_ascii=False)
            }
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'No valid data collected',
                    'execution_time': event_time.isoformat() if event_time else None
                }, ensure_ascii=False)
            }

    except Exception as e:
        print(f"Error in main: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'execution_time': event_time.isoformat() if event_time else None
            }, ensure_ascii=False)
        }

if __name__ == '__main__':
    main({}, {})
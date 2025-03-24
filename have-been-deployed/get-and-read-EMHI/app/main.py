import os
import json
import uuid
import urllib.request
import urllib.error
from datetime import datetime, timezone
from xml.etree import ElementTree
import boto3

s3_client_eu = boto3.client("s3", region_name="eu-central-1")
s3_client_jp = boto3.client("s3", region_name="ap-northeast-1")
raw_data_bucket = os.getenv("RawDataBucket")
converted_bucket = os.getenv("ConvertedBucket")
tagid = os.getenv("tagid")
base_url = os.getenv("URL")

class Constants:
    MISSING_INT8 = -99
    MISSING_INT16 = -9999
    MISSING_INT32 = -999999
    INVALID_INT16 = -11111
    INVALID_INT32 = -1111111

    OBSERVATION_DATANAME = "EMHI_OBS_TABLE_AWS_raw"
    OBSERVATION_DATAID16 = "0200600041000140"

def validate_env_vars():
    required_vars = [
        "RawDataBucket",
        "ConvertedBucket",
        "tagid",
        "URL"
    ]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

def normalize_datetime(dt):
    minute = (dt.minute // 10) * 10  
    return dt.replace(minute=minute, second=0, microsecond=0)

def download_file(url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'
        }
        request = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(request) as response:
            return response.read().decode('utf-8')
    except Exception as e:
        print(f"Download error: {e}")
        return None

def save_to_s3_raw(bucket, key, body):
    try:
        s3_client_eu.put_object(
            Body=body,
            Bucket=bucket,
            Key=key,
            ContentType='application/xml'
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

def create_ruheader(dataname, dataid16, announced, data_size=0, data_format="xml"):

    RU_HEADER_BEG_SIGNATURE = "WN\n"
    RU_HEADER_END_SIGNATURE = "\x04\x1a"
    created = datetime.now(timezone.utc)

    rhd = RU_HEADER_BEG_SIGNATURE
    rhd += "header_version=1.00\n"
    rhd += f"data_name={dataname}\n"
    rhd += f"global_id={dataid16[:4]}\n"
    rhd += f"category={dataid16[4:8]}\n"
    rhd += f"data_id={dataid16[8:16]}\n"
    rhd += f"created={created.strftime('%Y/%m/%d %H:%M:%S GMT')}\n"
    rhd += f"announced={announced}\n"
    rhd += "revision=1\n"
    rhd += f"data_size={data_size}\n"
    rhd += f"header_comment={dataname}\n"
    rhd += f"format={data_format}\n"
    rhd += RU_HEADER_END_SIGNATURE
    return rhd

def generate_station_s3_key(tagid):
    return f"metadata/spool/EMHI/metadata.json"

def generate_raw_s3_key(tagid, filename):

    return f"{tagid}/{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/{filename}"

def generate_observation_s3_key(tagid, filename):
    return f"data/{tagid}/{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/{filename}"

def parse_stations_to_geojson(xml_content):

    root = ElementTree.fromstring(xml_content)
    features_list = []

    d = dict()
    d['type'] = 'FeatureCollection'

    for station in root.findall("station"):
        try:
            coords = [
                float(station.find("longitude").text),
                float(station.find("latitude").text)
            ]
            
            features_dict = dict()
            features_dict['type'] = 'Feature'
            
            geometry_dict = dict()
            geometry_dict['type'] = 'Point'
            geometry_dict['coordinates'] = coords
            
            properties_dict = dict()
            name = station.find("name").text.strip()
            wmocode = station.find("wmocode").text.strip() if station.find("wmocode") is not None and station.find("wmocode").text else ""
            
            properties_dict['LCLID'] = name
            properties_dict['LNAME'] = name
            properties_dict['CNTRY'] = "EE"
            properties_dict['WMO_ID'] = wmocode
            
            features_dict['geometry'] = geometry_dict
            features_dict['properties'] = properties_dict
            features_dict['type'] = "Feature"
            
            features_list.append(features_dict)
            
        except Exception as e:
            print(f"XMLデータ解析エラー: {e}")

    d['features'] = features_list
    
    return d

def safe_convert_to_int(value, default=Constants.MISSING_INT16):
    try:
        if value not in [None, "", "--"]:
            return int(float(value))
        return default
    except (ValueError, TypeError):
        return default

def parse_observations(xml_content):
    root = ElementTree.fromstring(xml_content)
    observations = []

    for station in root.findall("station"):
        obs = {
            "LCLID": station.find("name").text if station.find("name") is not None else "",
            "HVIS": station.find("visibility").text if station.find("visibility") is not None else "",
            "AIRTMP": station.find("airtemperature").text if station.find("airtemperature") is not None else "",
            "WNDDIR": station.find("winddirection").text if station.find("winddirection") is not None else "",
            "WNDSPD": station.find("windspeed").text if station.find("windspeed") is not None else "",
            "WNDSPD_10MIN_MAX": station.find("windspeedmax").text if station.find("windspeedmax") is not None else "",
            "PRCRIN_10MIN": station.find("precipitations").text if station.find("precipitations") is not None else "",
            "SUNDUR_10MIN": station.find("sunshineduration").text if station.find("sunshineduration") is not None else "",
            "ARPRSS": station.find("airpressure").text if station.find("airpressure") is not None else "",
            "RHUM": station.find("relativehumidity").text if station.find("relativehumidity") is not None else "",
            "WX_original": station.find("phenomenon").text if station.find("phenomenon") is not None else ""
        }
        if obs["WX_original"] is None or obs["WX_original"].strip() == "":
            obs["WX_original"] = ""
        observations.append(obs)

    return observations

def create_observation_json(observations, timestamp):
    observation_date = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
    normalized_date = normalize_datetime(observation_date)  
    
    point_data_list = []
    for obs in observations:
        point_dict = dict()
        point_dict['LCLID'] = obs.get("LCLID", "")
        point_dict['ID_GLOBAL_MNET'] = f"EMHI_{obs.get('LCLID', '')}"

        hvis_km = obs.get("HVIS")
        if hvis_km not in [None, "", "--"]:
            try:
                hvis_m = str(float(hvis_km) * 1000)  
                point_dict['HVIS'] = safe_convert_to_int(hvis_m, default=Constants.MISSING_INT32)
            except (ValueError, TypeError):
                point_dict['HVIS'] = Constants.MISSING_INT32
        else:
            point_dict['HVIS'] = Constants.MISSING_INT32
        point_dict['HVIS_AQC'] = Constants.MISSING_INT8

        for key, value in obs.items():
            if key == "RHUM":
                try:
                    if value not in [None, "", "--"]:
                        if int(float(value)) == 99:
                            point_dict["RHUM"] = Constants.INVALID_INT16
                        else:
                            point_dict["RHUM"] = int(float(value) * 10)
                    else:
                        point_dict["RHUM"] = Constants.MISSING_INT16
                except (ValueError, TypeError):
                    point_dict["RHUM"] = Constants.MISSING_INT16
                point_dict["RHUM_AQC"] = Constants.MISSING_INT8

            elif value not in [None, "", "--"]:
                try:
                    if key in ["AIRTMP", "WNDSPD", "WNDSPD_10MIN_MAX", "PRCRIN_10MIN", "ARPRSS"]:
                        point_dict[key] = int(float(value) * 10)
                        point_dict[f"{key}_AQC"] = Constants.MISSING_INT8

                    elif key in ["SUNDUR_10MIN", "WNDDIR"]:
                        point_dict[key] = int(float(value))
                        point_dict[f"{key}_AQC"] = Constants.MISSING_INT8
                except (ValueError, TypeError):
                    if key in ["AIRTMP", "WNDSPD", "WNDSPD_10MIN_MAX", "PRCRIN_10MIN", "ARPRSS", "SUNDUR_10MIN", "WNDDIR"]:
                        point_dict[key] = Constants.MISSING_INT16
                        point_dict[f"{key}_AQC"] = Constants.MISSING_INT8
            else:
                if key in ["AIRTMP", "WNDSPD", "WNDSPD_10MIN_MAX", "PRCRIN_10MIN", "ARPRSS", "SUNDUR_10MIN", "WNDDIR"]:
                    point_dict[key] = Constants.MISSING_INT16
                    point_dict[f"{key}_AQC"] = Constants.MISSING_INT8

        point_dict['WX_original'] = obs.get("WX_original", "")
        point_dict['WX_original_AQC'] = Constants.MISSING_INT8

        point_data_list.append(point_dict)

    observation_date_dict = {
        'year': normalized_date.year,
        'month': normalized_date.month,
        'day': normalized_date.day,
        'hour': normalized_date.hour,
        'min': normalized_date.minute,
        'sec': normalized_date.second
    }

    return {
        'tagid': tagid,
        'announced': normalized_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        'created': normalize_datetime(datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        'original': {
            'observation_date': observation_date_dict,
            'point_count': len(point_data_list),
            'point_data': point_data_list
        }
    }

def save_observation_data(xml_content, timestamp):
    announced_dt = datetime.fromtimestamp(int(timestamp), tz=timezone.utc) if timestamp.isdigit() else datetime.now(timezone.utc)
    normalized_dt = normalize_datetime(announced_dt)
    announced_str = normalized_dt.strftime('%Y/%m/%d %H:%M:%S GMT')

    ruheader = create_ruheader(
        Constants.OBSERVATION_DATANAME,
        Constants.OBSERVATION_DATAID16,
        announced_str,
        data_size=len(xml_content)
    )

    combined_data = ruheader + xml_content
    raw_filename = f"{datetime.now(timezone.utc).strftime('%Y%m%d%H%M')}"
    raw_s3_key = generate_raw_s3_key(tagid, raw_filename)
    save_to_s3_raw(raw_data_bucket, raw_s3_key, combined_data.encode('utf-8'))

    observations = parse_observations(xml_content)
    observation_json = create_observation_json(observations, timestamp)
    random_suffix = str(uuid.uuid4())
    obs_filename = f"{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.{random_suffix}"
    obs_key = generate_observation_s3_key(tagid, obs_filename)
    save_to_s3_converted(
        converted_bucket,
        obs_key,
        json.dumps(observation_json, ensure_ascii=False, indent=2).encode('utf-8')
    )

    return {
        'raw_data_location': f"s3://{raw_data_bucket}/{raw_s3_key}",
        'observation_data_location': f"s3://{converted_bucket}/{obs_key}",
        'observation_count': len(observation_json['original']['point_data'])
    }

def process_data():
    try:
        validate_env_vars()

        xml_content = download_file(base_url)
        if not xml_content:
            raise ValueError("Failed to download data")

        root = ElementTree.fromstring(xml_content)
        timestamp = root.attrib.get("timestamp", "0")

        observation_result = save_observation_data(xml_content, timestamp)

        station_geojson = parse_stations_to_geojson(xml_content)
        station_key = generate_station_s3_key(tagid)
        save_to_s3_converted(
            converted_bucket,
            station_key,
            json.dumps(station_geojson, ensure_ascii=False, indent=2).encode('utf-8')
        )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data successfully processed and saved',
                **observation_result,
                'station_data_location': f"s3://{converted_bucket}/{station_key}"
            })
        }

    except Exception as e:
        error_message = f"Error in process_data: {str(e)}"
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': error_message,
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
        }

def process_observation_data():
    try:
        validate_env_vars()

        xml_content = download_file(base_url)
        if not xml_content:
            raise ValueError("Failed to download observation data")

        root = ElementTree.fromstring(xml_content)
        timestamp = root.attrib.get("timestamp", "0")

        observation_result = save_observation_data(xml_content, timestamp)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Observation data successfully processed and saved',
                **observation_result
            })
        }

    except Exception as e:
        error_message = f"Error in process_observation_data: {str(e)}"
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

        resources = event.get('resources', [])
        if not resources:
            error_message = "No resources found in event. Defaulting to both data processing."
            print(error_message)
            return process_data()
        rule_name = resources[0].split('/')[-1]
        if 'StationRule' in rule_name:
            print("Triggered by StationRule: Processing both station and observation data.")
            return process_data()  
        elif 'ObservationRule' in rule_name:
            print("Triggered by ObservationRule: Processing observation data only.")
            return process_observation_data()  
        else:
            print(f"Unknown rule triggered: {rule_name}. Defaulting to both data processing.")
            return process_data()

    except Exception as e:
        error_message = f"Fatal error in main: {str(e)}"
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
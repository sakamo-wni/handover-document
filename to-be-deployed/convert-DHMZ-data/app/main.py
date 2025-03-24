import urllib.request
from xml.etree import ElementTree
import json
from datetime import datetime, timezone
import uuid
import os
import boto3

s3_client = boto3.client("s3", region_name="ap-northeast-1")

raw_bucket = os.getenv("RawDataBucket")
converted_bucket = os.getenv("ConvertedBucket")
tagid = os.getenv("tagid")
base_url = os.getenv("URL")

class Constants:
    MISSING_INT8 = -99
    MISSING_INT16 = -9999
    MISSING_INT32 = -999999
    INVALID_INT16 = -11111
    INVALID_INT32 = -1111111

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

def save_to_s3(bucket, key, body, content_type='application/json'):
    try:
        s3_client.put_object(
            Body=body,
            Bucket=bucket,
            Key=key,
            ContentType=content_type
        )
        print(f"Successfully saved data to S3: {bucket}/{key}")
        return True
    except Exception as error:
        raise ValueError(f"Failed to save data to S3: {str(error)}")

def generate_raw_s3_key(tagid):
    return f"{tagid}/{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_raw.xml"

def generate_station_s3_key(tagid):
    return f"metadata/spool/DHMZ/metadata.json"

def generate_observation_s3_key(tagid, filename):
    return f"data/{tagid}/{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/{filename}"

def safe_convert_to_int(value, default=Constants.MISSING_INT16):
    try:
        if value not in [None, "", "--", "-"]:
            return int(float(value))
        return default
    except (ValueError, TypeError):
        return default

def parse_stations_to_geojson(xml_content):
    root = ElementTree.fromstring(xml_content)
    features_list = []
    geojson = {
        'type': 'FeatureCollection',
        'features': features_list
    }
    
    for grad in root.findall(".//Grad"):
        try:
            latitude = float(grad.find("Lat").text.strip())
            longitude = float(grad.find("Lon").text.strip())
            
            grad_ime = grad.find("GradIme").text.strip()
            
            feature = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [longitude, latitude]  
                },
                'properties': {
                    'LCLID': grad_ime,
                    'LNAME': grad_ime,
                    'CNTRY': 'HR'
                }
            }
            
            features_list.append(feature)
            
        except Exception as e:
            print(f"地点データ解析エラー: {e}")
    
    return geojson

def parse_observations(xml_content):
    root = ElementTree.fromstring(xml_content)
    observations = []
    
    date_data = root.find(".//DatumTermin")
    date_str = date_data.find("Datum").text.strip() if date_data and date_data.find("Datum") is not None else None
    termin = date_data.find("Termin").text.strip() if date_data and date_data.find("Termin") is not None else None
    
    observation_date = None
    if date_str:
        try:
            day, month, year = map(int, date_str.split('.'))
            hour = int(termin) if termin else 0
            observation_date = datetime(year, month, day, hour, 0, 0, tzinfo=timezone.utc)
        except Exception as e:
            print(f"日付解析エラー: {e}")
            observation_date = datetime.now(timezone.utc)
    else:
        observation_date = datetime.now(timezone.utc)
    
    for grad in root.findall(".//Grad"):
        try:
            grad_ime = grad.find("GradIme").text.strip()
            podatci = grad.find("Podatci")
            
            if podatci is not None:
                temp = podatci.find("Temp").text.strip() if podatci.find("Temp") is not None else ""
                vlaga = podatci.find("Vlaga").text.strip() if podatci.find("Vlaga") is not None else ""
                tlak = podatci.find("Tlak").text.strip() if podatci.find("Tlak") is not None else ""
                vjetar_smjer = podatci.find("VjetarSmjer").text.strip() if podatci.find("VjetarSmjer") is not None else ""
                vjetar_brzina = podatci.find("VjetarBrzina").text.strip() if podatci.find("VjetarBrzina") is not None else ""
                vrijeme = podatci.find("Vrijeme").text.strip() if podatci.find("Vrijeme") is not None else ""
                
                obs = {
                    "LCLID": grad_ime,
                    "AIRTMP": temp,
                    "RHUM": vlaga,
                    "ARPRSS": tlak,
                    "WNDDIR": vjetar_smjer,
                    "WNDSPD": vjetar_brzina,
                    "WX_original": vrijeme
                }
                
                observations.append(obs)
        except Exception as e:
            print(f"観測データ解析エラー: {e}")
    
    return observations, observation_date

def create_observation_json(observations, observation_date):
    normalized_date = normalize_datetime(observation_date)
    
    point_data_list = []
    for obs in observations:
        point_dict = dict()
        point_dict['LCLID'] = obs.get("LCLID", "")
        point_dict['ID_GLOBAL_MNET'] = f"DHMZ_{obs.get('LCLID', '')}" 
        
        airtmp = obs.get("AIRTMP")
        try:
            if airtmp not in [None, "", "--", "-"]:
                point_dict["AIRTMP"] = int(float(airtmp) * 10)
            else:
                point_dict["AIRTMP"] = Constants.MISSING_INT16
        except (ValueError, TypeError):
            point_dict["AIRTMP"] = Constants.MISSING_INT16
        point_dict["AIRTMP_AQC"] = Constants.MISSING_INT8
        
        rhum = obs.get("RHUM")
        try:
            if rhum not in [None, "", "--", "-"]:
                if int(float(rhum)) == 99:
                    point_dict["RHUM"] = Constants.INVALID_INT16
                else:
                    point_dict["RHUM"] = int(float(rhum) * 10)
            else:
                point_dict["RHUM"] = Constants.MISSING_INT16
        except (ValueError, TypeError):
            point_dict["RHUM"] = Constants.MISSING_INT16
        point_dict["RHUM_AQC"] = Constants.MISSING_INT8
        
        arprss = obs.get("ARPRSS")
        try:
            if arprss not in [None, "", "--", "-"]:
                # "*"記号を削除して処理
                arprss = arprss.replace("*", "")
                point_dict["ARPRSS"] = int(float(arprss) * 10)
            else:
                point_dict["ARPRSS"] = Constants.MISSING_INT16
        except (ValueError, TypeError):
            point_dict["ARPRSS"] = Constants.MISSING_INT16
        point_dict["ARPRSS_AQC"] = Constants.MISSING_INT8
        
        # 風向 (WNDDIR) - 方角を1~8の値に変換（8方位）
        wnddir = obs.get("WNDDIR")
        wind_dir_map = {
            "N": 1, "NNE": 1, "NE": 2, "ENE": 2,
            "E": 3, "ESE": 3, "SE": 4, "SSE": 4,
            "S": 5, "SSW": 5, "SW": 6, "WSW": 6,
            "W": 7, "WNW": 7, "NW": 8, "NNW": 8,
            "-": Constants.MISSING_INT16
        }
        point_dict["WNDDIR_8"] = wind_dir_map.get(wnddir, Constants.MISSING_INT16)
        point_dict["WNDDIR_8_AQC"] = Constants.MISSING_INT8
        
        # 風速 (WNDSPD) - 10倍にして整数化
        wndspd = obs.get("WNDSPD")
        try:
            if wndspd not in [None, "", "--", "-"]:
                point_dict["WNDSPD"] = int(float(wndspd) * 10)
            else:
                point_dict["WNDSPD"] = Constants.MISSING_INT16
        except (ValueError, TypeError):
            point_dict["WNDSPD"] = Constants.MISSING_INT16
        point_dict["WNDSPD_AQC"] = Constants.MISSING_INT8
        
        # 天気記号 (WX_original)
        wx_original = obs.get("WX_original", "")
        point_dict["WX_original"] = wx_original if wx_original not in [None, "", "--", "-"] else ""
        point_dict["WX_original_AQC"] = Constants.MISSING_INT8
        
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

def process_observation_data():
    try:
        validate_env_vars()

        xml_content = download_file(base_url)
        if not xml_content:
            raise ValueError("Failed to download observation data")

        # 生データをS3に保存（追加した部分）
        now = datetime.now(timezone.utc)
        raw_key = generate_raw_s3_key(tagid)
        save_to_s3(
            raw_bucket,
            raw_key,
            xml_content.encode('utf-8'),
            content_type='application/xml'
        )
        print(f"Raw XML data saved to s3://{raw_bucket}/{raw_key}")

        observations, observation_date = parse_observations(xml_content)
        
        observation_json = create_observation_json(observations, observation_date)
        
        random_suffix = str(uuid.uuid4())
        obs_filename = f"{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.{random_suffix}"
        obs_key = generate_observation_s3_key(tagid, obs_filename)
        save_to_s3(
            converted_bucket,
            obs_key,
            json.dumps(observation_json, ensure_ascii=False, indent=2).encode('utf-8')
        )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Observation data successfully processed and saved',
                'raw_data_location': f"s3://{raw_bucket}/{raw_key}",
                'observation_data_location': f"s3://{converted_bucket}/{obs_key}",
                'observation_count': len(observation_json['original']['point_data'])
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

def process_station_data():
    try:
        validate_env_vars()

        xml_content = download_file(base_url)
        if not xml_content:
            raise ValueError("Failed to download station data")

        # 生データをS3に保存（追加した部分）
        now = datetime.now(timezone.utc)
        raw_key = generate_raw_s3_key(tagid)
        save_to_s3(
            raw_bucket,
            raw_key,
            xml_content.encode('utf-8'),
            content_type='application/xml'
        )
        print(f"Raw XML data saved to s3://{raw_bucket}/{raw_key}")

        # 地点データをGeoJSONに変換
        station_geojson = parse_stations_to_geojson(xml_content)
        
        # S3に保存
        station_key = generate_station_s3_key(tagid)
        save_to_s3(
            converted_bucket,
            station_key,
            json.dumps(station_geojson, ensure_ascii=False, indent=2).encode('utf-8')
        )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Station data successfully processed and saved',
                'raw_data_location': f"s3://{raw_bucket}/{raw_key}",
                'station_data_location': f"s3://{converted_bucket}/{station_key}"
            })
        }

    except Exception as e:
        error_message = f"Error in process_station_data: {str(e)}"
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
            error_message = "No resources found in event. Defaulting to observation data processing."
            print(error_message)
            return process_observation_data()
        
        rule_name = resources[0].split('/')[-1]
        if 'StationRule' in rule_name:
            print("Triggered by StationRule: Processing station data.")
            return process_station_data()  # 地点データのみ処理
        elif 'ObservationRule' in rule_name:
            print("Triggered by ObservationRule: Processing observation data.")
            return process_observation_data()  # 観測データのみ処理
        else:
            print(f"Unknown rule triggered: {rule_name}. Defaulting to observation data processing.")
            return process_observation_data()

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
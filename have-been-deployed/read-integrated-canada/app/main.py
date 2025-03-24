import json
import csv
import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

account_id = boto3.client("sts").get_caller_identity()["Account"]
s3 = boto3.client("s3")
date_path = datetime.now(timezone.utc).strftime("%Y/%m/%d")
today = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
input_bucket = os.environ.get("stock_s3", None)
metadata_bucket = os.environ.get("md_bucket", None)
MESSAGE_EXPIRATION_HOURS = 12  


def validate_environment() -> None:
    if not input_bucket or not metadata_bucket:
        raise EnvironmentError(
            "Required environment variables are not set: stock_s3 and/or md_bucket"
        )


def extract_data_from_s3(bucket: str, key: str) -> Optional[List[Dict[str, Any]]]:

    try:
        print(f"Reading data from s3://{bucket}/{key}")
        response = s3.get_object(Bucket=bucket, Key=key)
        file_content = response["Body"].read()

        if b"\x04\x1a" in file_content:
            parts = file_content.split(b"\x04\x1a")
            data = parts[1].decode("utf-8")
            first_line = data.splitlines()[0]
            is_first_type = "WMO_ID" in first_line
            return parse_csv_content(data, is_first_type)
        else:
            print("No RU header found in file")
            return None

    except Exception as e:
        print(f"Error reading from S3: {e}")
        return None


def parse_csv_content(content: str, is_first_type: bool) -> Optional[List[Dict[str, Any]]]:
    try:
        reader = csv.DictReader(content.splitlines())
        csv_data = []
        for row in reader:
            if is_first_type:  # awsの場合
                station_data = {
                    "Longitude": row.get("Longitude", ""),
                    "Latitude": row.get("Latitude", ""),
                    "Elevation": row.get("Elevation", ""),
                    "Station Name": row.get("Name", ""),
                    "Province": row.get("Province", ""),
                    "LCLID": row.get("MSC_ID", ""),
                    "WMO_ID": row.get("WMO_ID", ""),
                }
            else:  # climateの場合
                station_data = {
                    "Longitude": row.get("Longitude", ""),
                    "Latitude": row.get("Latitude", ""),
                    "Elevation": row.get("Elevation", ""),
                    "Station Name": row.get("Station Name", ""),
                    "Province": row.get("Province", ""),
                    "LCLID": row.get("Climate ID", ""),
                    "WMO_ID": row.get("WMO Identifier", ""),
                }
            csv_data.append(station_data)

        print(
            f"Extracted {len(csv_data)} stations from file (type: {'first' if is_first_type else 'second'})"
        )
        return csv_data

    except Exception as e:
        print(f"Error parsing CSV content: {e}")
        return None


def store_message_in_temporary_storage(message_key: str) -> bool:
    #メッセージを一時的にS3に保存する
    try:
        timestamp = datetime.now(timezone.utc).isoformat()
        key = f"temp/{timestamp}_{message_key}"
        body = json.dumps({"timestamp": timestamp, "key": message_key})
        s3.put_object(Bucket=metadata_bucket, Key=key, Body=body)
        print(f"Stored message with key: {message_key}")
        return True
    except Exception as e:
        print(f"Error storing message: {e}")
        return False


def get_stored_messages() -> List[Dict[str, Any]]:
    messages = []
    try:
        response = s3.list_objects_v2(Bucket=metadata_bucket, Prefix="temp/")
        expiration_time = datetime.now(timezone.utc) - timedelta(hours=MESSAGE_EXPIRATION_HOURS)

        for obj in response.get("Contents", []):
            obj_key = obj["Key"]
            obj_body = s3.get_object(Bucket=metadata_bucket, Key=obj_key)["Body"].read()
            message_data = json.loads(obj_body)
            message_time = datetime.fromisoformat(message_data["timestamp"])
            if message_time > expiration_time:
                messages.append(message_data)
            else:
                s3.delete_object(Bucket=metadata_bucket, Key=obj_key)
        return messages

    except Exception as e:
        print(f"Error retrieving messages: {e}")
        return []


def cleanup_processed_messages(processed_keys: List[str]) -> None:
    try:
        response = s3.list_objects_v2(Bucket=metadata_bucket, Prefix="temp/")
        for obj in response.get("Contents", []):
            obj_key = obj["Key"]
            obj_body = s3.get_object(Bucket=metadata_bucket, Key=obj_key)["Body"].read()
            message_data = json.loads(obj_body)
            if message_data["key"] in processed_keys:
                s3.delete_object(Bucket=metadata_bucket, Key=obj_key)
    except Exception as e:
        print(f"Error cleaning up messages: {e}")

import time

def merge_station_data(
    data1: List[Dict[str, Any]], data2: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    2つのデータセットを統合し、ID、座標、名前のいずれかが重複する場合、優先度の高いデータのみを残す
    優先度は以下の順で判断:
    1. 高度(Elevation)データの有無
    2. 空でないフィールドの数
    """
    all_stations = []
    start_time = time.time()
    
    parent = []  # 親ノードを保持する配列
    
    id_to_index = {}
    name_to_index = {}
    coord_to_index = {}
    
    def has_elevation(station: Dict[str, Any]) -> bool:
        elev = station.get("Elevation", "")
        return bool(str(elev).strip())
    
    def get_non_empty_field_count(station: Dict[str, Any]) -> int:
        return sum(1 for value in station.values() if value and str(value).strip())
    
    def normalize_coords(lon: float, lat: float) -> str:
        return f"{round(lon, 6)},{round(lat, 6)}"
    
    def normalize_name(name: str) -> str:
        return name.lower().strip()
    
    def find(x: int) -> int:
        if parent[x] != x:
            parent[x] = find(parent[x])  
        return parent[x]
    
    def union(x: int, y: int) -> None:
        root_x = find(x)
        root_y = find(y)
        parent[root_x] = root_y
    
    for source_name, data in [("data1", data1), ("data2", data2)]:
        for idx, station in enumerate(data):
            try:
                station_id = str(station.get("LCLID", "")).strip()
                
                name = str(station.get("Station Name", "")).strip()
                normalized_name = normalize_name(name)
                
                try:
                    lon = float(station["Longitude"]) if station["Longitude"] else 0.0
                    lat = float(station["Latitude"]) if station["Latitude"] else 0.0
                    coord_key = normalize_coords(lon, lat)
                except (ValueError, TypeError):
                    lon, lat = 0.0, 0.0
                    coord_key = "invalid_coord"
                
                current_idx = len(all_stations)
                
                all_stations.append({
                    'source': source_name,
                    'station': station,
                    'priority': (
                        1 if has_elevation(station) else 0,  
                        get_non_empty_field_count(station)    
                    )
                })
                
                parent.append(current_idx)
                
                if station_id:
                    if station_id in id_to_index:
                        union(current_idx, id_to_index[station_id])
                    else:
                        id_to_index[station_id] = current_idx
                
                if normalized_name:
                    if normalized_name in name_to_index:
                        union(current_idx, name_to_index[normalized_name])
                    else:
                        name_to_index[normalized_name] = current_idx
                
                if coord_key != "invalid_coord":
                    if coord_key in coord_to_index:
                        union(current_idx, coord_to_index[coord_key])
                    else:
                        coord_to_index[coord_key] = current_idx
                
            except Exception as e:
                print(f"Warning: Error processing station from {source_name}: {e}")
                continue
    
    prep_time = time.time() - start_time
    print(f"Total stations before deduplication: {len(all_stations)} (準備時間: {prep_time:.2f}秒)")
    root_start = time.time()
    
    roots = [find(i) for i in range(len(all_stations))]
    
    groups = {}
    for i, root in enumerate(roots):
        if root not in groups:
            groups[root] = []
        groups[root].append(all_stations[i])
    
    group_time = time.time() - root_start
    print(f"Number of unique station groups: {len(groups)} (グループ化時間: {group_time:.2f}秒)")
    select_start = time.time()
    
    merged_data = []
    for group in groups.values():
        best_station = max(group, key=lambda x: x['priority'])
        merged_data.append(best_station['station'])
    
    station_to_source = {}
    for station in merged_data:
        station_to_source[id(station)] = None
    
    for group in groups.values():
        best_station = max(group, key=lambda x: x['priority'])
        station_to_source[id(best_station['station'])] = best_station['source']
    
    data1_count = sum(1 for src in station_to_source.values() if src == 'data1')
    data2_count = sum(1 for src in station_to_source.values() if src == 'data2')
    
    duplicates_total = len(all_stations) - len(merged_data)
    
    print(f"重複排除により削除された地点数: {duplicates_total}")
    print(f"統合後の地点数: {len(merged_data)} (データ1: {data1_count}, データ2: {data2_count})")
    
    return merged_data

def convert_to_geojson(stations: List[Dict[str, Any]]) -> Dict[str, Any]:
    features_list = []
    geojson: Dict[str, Any] = {"type": "FeatureCollection"}

    for station in stations:
        try:
            # 経度・緯度の座標作成
            longitude = float(station["Longitude"]) if station["Longitude"] else 0.0
            latitude = float(station["Latitude"]) if station["Latitude"] else 0.0
            coords = [longitude, latitude]

            # Elevationが存在し、数値に変換可能なら追加
            if station["Elevation"] and str(station["Elevation"]).strip():
                try:
                    elevation = float(station["Elevation"])
                    coords.append(elevation)
                except (ValueError, TypeError):
                    pass

            geometry_dict = {"type": "Point", "coordinates": coords}
            properties_dict = {
                "LCLID": str(station.get("LCLID", "")).strip(),
                "LNAME": str(station.get("Station Name", "")).strip(),
                "CNTRY": "CA",
                "WMO_ID": station.get("WMO_ID", ""),
            }
            features_dict = {"type": "Feature", "geometry": geometry_dict, "properties": properties_dict}
            features_list.append(features_dict)

        except (KeyError, ValueError, TypeError) as e:
            print(f"Warning: Error processing station {station.get('Station Name', '')}: {e}")
            continue

    geojson["features"] = features_list
    return geojson


def save_to_s3(bucket: str, save_key: str, data: Any) -> bool:
    try:
        if not all([bucket, save_key, data]):
            raise ValueError("Missing required parameters for S3 save")
        json_data = json.dumps(data, indent=2, ensure_ascii=False)
        s3.put_object(
            Body=json_data.encode("utf-8"),
            Bucket=bucket,
            Key=save_key,
            ContentType="application/json",
        )
        print(f"Data successfully saved to s3://{bucket}/{save_key}")
        return True
    except Exception as e:
        print(f"Error saving to S3: {e}")
        return False

def process_messages(messages: List[Dict[str, Any]]) -> bool:
    try:
        if len(messages) < 2:
            return False

        sorted_messages = sorted(messages, key=lambda x: x["timestamp"], reverse=True)[:2]
        keys = [msg["key"] for msg in sorted_messages]

        data1 = extract_data_from_s3(input_bucket, keys[0])
        data2 = extract_data_from_s3(input_bucket, keys[1])
        if not data1 or not data2:
            return False

        merged_data = merge_station_data(data1, data2)
        geojson_data = convert_to_geojson(merged_data)

        s3_key = "metadata/spool/MSC/metadata.json"
        if save_to_s3(metadata_bucket, s3_key, geojson_data):
            cleanup_processed_messages(keys)
            return True

        return False

    except Exception as e:
        print(f"Error processing messages: {e}")
        return False


def main(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        validate_environment()
        print(f"Processing event: {json.dumps(event, ensure_ascii=False)}")
        current_keys: List[str] = []

        for record in event.get("Records", []):
            try:
                print(f"Record body content: {record['body']}")
                
                body = json.loads(record["body"])
                
                print(f"Parsed body structure: {body}")
                
                message_str = body.get("Message")
                print(f"Message field content: {message_str}")
                
                if not isinstance(message_str, str):
                    print(f"Message is not a string, it's a {type(message_str)}")
                    continue
                    
                try: 
 
                    message_obj = json.loads(message_str)
                    if isinstance(message_obj, dict) and "Records" in message_obj:
                        for rec in message_obj["Records"]:
                            if "s3" in rec:
                                current_keys.append(rec["s3"]["object"]["key"])
                    else:
                        current_keys.append(message_str)
                except json.JSONDecodeError:
                    current_keys.append(message_str)
            except Exception as e:
                print(f"Error processing record: {e}")
                continue

        for key in current_keys:
            store_message_in_temporary_storage(key)

        stored_messages = get_stored_messages()
        if process_messages(stored_messages):
            return {
                "statusCode": 200,
                "body": json.dumps("Processing completed successfully"),
            }
        else:
            return {
                "statusCode": 200,
                "body": json.dumps("Waiting for more messages or processing failed"),
            }

    except Exception as e:
        print(f"Fatal error in main function: {e}")
        return {"statusCode": 500, "body": json.dumps(f"Error: {str(e)}")}

if __name__ == "__main__":
    main({}, {})

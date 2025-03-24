import os
import json
from datetime import datetime, timezone, timedelta
import boto3
from botocore.exceptions import ClientError

MISSING_VALUES = ""

input_bucket = os.environ.get("stock_s3")
metadata_bucket = os.environ.get("md_bucket")
s3 = boto3.client('s3')

MESSAGE_EXPIRATION_HOURS = 24  

def extract_data_from_s3(bucket, key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
    except ClientError as e:
        print(f"Could not retrieve S3 object: s3://{bucket}/{key}")
        print("Error:", e)
        return None, None

    # ヘッダー終端を検索
    header_end = content.find('\x04\x1a') + 2
    if header_end < 2:
        print(f"Header end marker not found in object: {key}")
        return None, None

    header_section = content[:header_end]
    announced_dt = None
    for line in header_section.split('\n'):
        if line.startswith('announced='):
            announced_dt = line.split('=')[1].strip()
            break

    try:
        json_data = json.loads(content[header_end:])
    except json.JSONDecodeError as je:
        print(f"JSON Decode Error for object: {key}")
        print("Error:", je)
        return None, None

    return json_data, announced_dt

def convert_to_geojson(stations):

    geojson = {
        "type": "FeatureCollection",
        "features": []
    }

    for item in stations:
        # 標高を取得。存在しなければ MISSING_VALUES("") にする
        altitude_raw = item.get('VL_ALTITUDE', None)
        if altitude_raw:
            try:
                altitude_val = float(altitude_raw)
            except ValueError:
                altitude_val = MISSING_VALUES
        else:
            altitude_val = MISSING_VALUES

        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [
                    float(item.get('VL_LONGITUDE', MISSING_VALUES)),
                    float(item.get('VL_LATITUDE', MISSING_VALUES)),
                    altitude_val
                ]
            },
            "properties": {
                "LCLID": item.get('CD_ESTACAO', MISSING_VALUES).strip(),
                "LNAME": item.get('DC_NOME', MISSING_VALUES).strip(),
                "CNTRY": "BR",
                "OBS_BEGIND": item.get('DT_INICIO_OPERACAO', MISSING_VALUES),
                "OBS_ENDD": item.get('DT_FIM_OPERACAO', "") if item.get('DT_FIM_OPERACAO') else ""
            }
        }
        geojson["features"].append(feature)

    return geojson

def merge_station_data(data1, data2):
    return data1 + data2

def save_metadata_to_s3(bucket, key, data):
    try:
        json_str = json.dumps(data, ensure_ascii=False, indent=2)
        s3.put_object(
            Body=json_str.encode('utf-8'),
            Bucket=bucket,
            Key=key,
            ContentType='application/json'
        )
        print(f"Metadata saved to s3://{bucket}/{key}")
        return True
    except ClientError as e:
        print("Failed to save metadata to S3:", e)
        return False

def store_message_in_temporary_storage(message_key):
    try:
        timestamp = datetime.now(timezone.utc).isoformat()
        temp_key = f"temp_inmet/{timestamp}_{os.path.basename(message_key)}"
        body = {
            "timestamp": timestamp,
            "key": message_key
        }
        s3.put_object(
            Bucket=metadata_bucket,
            Key=temp_key,
            Body=json.dumps(body, ensure_ascii=False)
        )
        print(f"[store_message] key={message_key} -> {temp_key}")
        return True
    except Exception as e:
        print(f"Error storing message key: {e}")
        return False

def get_stored_messages():
    messages = []
    expiration_time = datetime.now(timezone.utc) - timedelta(hours=MESSAGE_EXPIRATION_HOURS)
    try:
        resp = s3.list_objects_v2(Bucket=metadata_bucket, Prefix="temp_inmet/")
        for obj in resp.get("Contents", []):
            temp_key = obj["Key"]
            content = s3.get_object(Bucket=metadata_bucket, Key=temp_key)["Body"].read()
            data = json.loads(content)
            msg_time = datetime.fromisoformat(data["timestamp"])
            if msg_time < expiration_time:

                s3.delete_object(Bucket=metadata_bucket, Key=temp_key)
            else:
                messages.append(data)
    except ClientError as e:
        print(f"Error listing or reading temp/ folder: {e}")

    return messages

def cleanup_processed_messages(processed_keys):
    try:
        resp = s3.list_objects_v2(Bucket=metadata_bucket, Prefix="temp_inmet/")
        for obj in resp.get("Contents", []):
            temp_key = obj["Key"]
            content = s3.get_object(Bucket=metadata_bucket, Key=temp_key)["Body"].read()
            data = json.loads(content)
            if data["key"] in processed_keys:
                s3.delete_object(Bucket=metadata_bucket, Key=temp_key)
                print(f"[cleanup] removed {temp_key}")
    except ClientError as e:
        print(f"Error cleaning up messages: {e}")

def process_messages(messages):

    if len(messages) < 2:
        print("Not enough messages to merge (need 2).")
        return False
    
    sorted_msgs = sorted(messages, key=lambda x: x["timestamp"], reverse=True)
    use_msgs = sorted_msgs[:2] 
    
    keys = [m["key"] for m in use_msgs]
    print(f"[process_messages] keys to merge => {keys}")

    merged_data = []
    for s3_key in keys:
        data, _announced = extract_data_from_s3(input_bucket, s3_key)
        if data:
            merged_data.extend(data)
    
    if not merged_data:
        print("No valid station data found after merging both keys.")
        return False
    
    geojson_data = convert_to_geojson(merged_data)
    output_key = "metadata/spool/INMET/metadata.json"
    if save_metadata_to_s3(metadata_bucket, output_key, geojson_data):

        cleanup_processed_messages(keys)
        return True
    else:
        return False

def main(event, context):
    print(json.dumps(event, ensure_ascii=False))

    s3_keys = []
    for record in event.get("Records", []):
        body_str = record["body"]
        try:
            body_obj = json.loads(body_str)
        except json.JSONDecodeError:
            print(f"Invalid JSON in message body: {body_str}")
            continue
        
        if "Message" in body_obj:
            try:
                message_obj = json.loads(body_obj["Message"])
                if "Records" in message_obj:
                    for s3_record in message_obj["Records"]:
                        s3_key = s3_record.get("s3", {}).get("object", {}).get("key")
                        if s3_key:
                            s3_keys.append(s3_key)
                else:
                    s3_keys.append(body_obj["Message"])
            except json.JSONDecodeError:
                s3_keys.append(body_obj["Message"])
        else:
            print(f"No 'Message' key found in event body: {body_obj}")

    for k in s3_keys:
        store_message_in_temporary_storage(k)

    stored_msgs = get_stored_messages()

    if process_messages(stored_msgs):
        return {
            "statusCode": 200,
            "body": "Merged station data saved."
        }
    else:
        return {
            "statusCode": 200,
            "body": "No merge done yet (need 2 messages)."
        }


if __name__ == '__main__':
    main({}, {})

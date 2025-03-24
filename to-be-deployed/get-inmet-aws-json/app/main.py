import os
import json
import uuid
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
import traceback

input_bucket = os.environ.get("stock_s3")  # 入力バケット
output_bucket = os.environ.get("md_bucket")  # 出力バケット
tagid = os.environ.get("tagid", "460032015")

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

s3_client_jp = boto3.client("s3", region_name="ap-northeast-1")


def generate_json_s3_key(tagid: str, filename: str) -> str:

    return (
        f"data/{tagid}/"
        f"{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/"
        f"{filename}"
    )


def save_to_s3_converted(bucket: str, key: str, body: bytes) -> bool:

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
        raise ValueError(
            f"Failed to save converted data to JP S3: {str(error)}"
        )


def extract_observation_data_from_s3(bucket: str, key: str):

    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
    except ClientError as e:
        print(f"Failed to get object from s3://{bucket}/{key}")
        print("Error:", e)
        return None, None


    header_end = content.find('\x04\x1a') + 2
    if header_end < 2:
        print("Header end marker not found.")
        return None, None


    header_section = content[:header_end]
    announced_dt = None
    for line in header_section.split('\n'):
        if line.startswith('announced='):
            announced_dt = line.split('=')[1].strip()
            break

    # ヘッダー以降を JSON として読み込む
    try:
        json_data = json.loads(content[header_end:])
    except json.JSONDecodeError as je:
        print(f"JSON Decode Error for object: {key}")
        print("Error:", je)
        return None, announced_dt

    return json_data, announced_dt


def convert_observation_to_json(observations, announced_dt: str) -> dict:

    from datetime import datetime

    observation_date = ""
    if announced_dt:
        try:
            dt = datetime.strptime(announced_dt, "%Y/%m/%d %H:%M:%S GMT")
            observation_date = {
                "year": dt.year,
                "month": dt.month,
                "day": dt.day,
                "hour": dt.hour,
                "min": dt.minute,
                "sec": dt.second
            }
        except ValueError:
            pass


    formatted_data = {
        "tagid": "460032015",  
        "announced": announced_dt,
        "created": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        "original": {
            "observation_date": observation_date,
            "point_count": len(observations) if observations else 0,
            "point_data": []
        }
    }

    if not observations:
        return formatted_data

    for obs in observations:
        formatted_data["original"]["point_data"].append({
            "LCLID": str(obs.get("CD_ESTACAO", MISSING_VALUES["STR"])),
            "ID_GLOBAL_MNET": f"INMET_{obs.get('CD_ESTACAO', MISSING_VALUES['STR'])}",
            "AIRTMP_1HOUR_MAX": int(float(obs["TEM_MAX"])) * 10 if obs.get("TEM_MAX") is not None else MISSING_VALUES["INT16"],
            "AIRTMP_1HOUR_MAX_AQC": MISSING_VALUES["INT8"],
            "AIRTMP_1HOUR_AVG": int(float(obs["TEM_INS"])) * 10 if obs.get("TEM_INS") is not None else MISSING_VALUES["INT16"],
            "AIRTMP_1HOUR_AVG_AQC": MISSING_VALUES["INT8"],
            "AIRTMP_1HOUR_MIN": int(float(obs["TEM_MIN"])) * 10 if obs.get("TEM_MIN") is not None else MISSING_VALUES["INT16"],
            "AIRTMP_1HOUR_MIN_AQC": MISSING_VALUES["INT8"],
            "RHUM": int(float(obs["UMD_INS"])) * 10 if obs.get("UMD_INS") is not None else MISSING_VALUES["INT16"],
            "RHUM_AQC": MISSING_VALUES["INT8"],
            "DEWTMP_1HOUR_AVG": int(float(obs["PTO_INS"])) * 10 if obs.get("PTO_INS") is not None else MISSING_VALUES["INT16"],
            "DEWTMP_1HOUR_AVG_AQC": MISSING_VALUES["INT8"],
            "ARPRSS_1HOUR_AVG": int(float(obs["PRE_INS"])) * 10 if obs.get("PRE_INS") is not None else MISSING_VALUES["INT16"],
            "ARPRSS_1HOUR_AVG_AQC": MISSING_VALUES["INT8"],
            "WNDDIR_1HOUR_AVG": obs.get("VEN_DIR", MISSING_VALUES["INT16"]) ,
            "WNDDIR_1HOUR_AVG_AQC": MISSING_VALUES["INT8"],
            "WNDSPD_1HOUR_AVG": int(float(obs["VEN_VEL"])) * 10 if obs.get("VEN_VEL") is not None else MISSING_VALUES["INT16"],
            "WNDSPD_1HOUR_AVG_AQC": MISSING_VALUES["INT8"],
            "GUSTS_1HOUR": int(float(obs["VEN_RAJ"])) * 10 if obs.get("VEN_RAJ") is not None else MISSING_VALUES["INT16"],
            "GUSTS_1HOUR_AQC": MISSING_VALUES["INT8"],
            "PRCRIN_1HOUR": int(float(obs["CHUVA"])) * 10 if obs.get("CHUVA") is not None else MISSING_VALUES["INT16"],
            "PRCRIN_1HOUR_AQC": MISSING_VALUES["INT8"],
            "GLBRAD_1HOUR": int(float(obs.get("RAD_GLO", MISSING_VALUES["INT16"])) * 1000) if obs.get("RAD_GLO") else MISSING_VALUES["INT32"],

            "GLBRAD_1HOUR_AQC": MISSING_VALUES["INT8"]
        })

    return formatted_data




def main(event, context):

    try:
        print(f"Processing event: {json.dumps(event, ensure_ascii=False)}")
        keys = []

        for record in event.get("Records", []):
            try:
                body = json.loads(record["body"])
                if isinstance(body.get("Message"), str):
                    try:
                        message_obj = json.loads(body["Message"])
                        if isinstance(message_obj, dict) and "Records" in message_obj:
                            for rec in message_obj["Records"]:
                                if "s3" in rec:
                                    keys.append(rec["s3"]["object"]["key"])
                        else:
                            keys.append(body["Message"])
                    except json.JSONDecodeError:
                        keys.append(body["Message"])
            except Exception as e:
                print(f"Error processing record: {e}")
                continue

        print(f"Processing keys: {keys}")
        processed_results = []

        for key in keys:
            try:
                print(f"Processing key: {key}")
                observations, announced_dt = extract_observation_data_from_s3(input_bucket, key)

                if not observations:
                    print(f"Failed to extract valid data from key: {key}")
                    processed_results.append({'key': key, 'status': 'empty_or_invalid'})
                    continue

                json_data = convert_observation_to_json(observations, announced_dt)

                now_utc_str = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
                random_suffix = str(uuid.uuid4())
                filename = f"{now_utc_str}.{random_suffix}"
                json_s3_key = generate_json_s3_key(tagid, filename)

                if save_to_s3_converted(output_bucket, json_s3_key, json.dumps(json_data, ensure_ascii=False, indent=2).encode('utf-8')):                    processed_results.append({'key': key, 'save_key': json_s3_key, 'status': 'success'})
                else:
                    processed_results.append({'key': key, 'status': 'failed_to_save'})

            except Exception as e:
                print(f"Error processing key {key}: {e}")
                traceback.print_exc()
                processed_results.append({'key': key, 'status': 'error', 'error_message': str(e)})
                continue

        return {"statusCode": 200, "body": json.dumps({"processed_results": processed_results})}

    except Exception as e:
        print(f"Fatal error: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e), "traceback": traceback.format_exc()})}

if __name__ == '__main__':
    main({}, {})

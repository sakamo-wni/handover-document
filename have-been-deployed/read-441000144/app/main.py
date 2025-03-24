import os
import boto3
from botocore.exceptions import ClientError
import botocore
import json
import sys
from datetime import datetime, timezone, timedelta
from urllib.parse import unquote
import uuid
import csv
import io

account_id = boto3.client("sts").get_caller_identity()["Account"]
s3 = boto3.client('s3')
date_path = datetime.now(timezone.utc).strftime("%Y/%m/%d")
tagid = '441000144'
today = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
input_bucket = os.environ.get("stock_s3", None)
metadata_bucket = os.environ.get("md_bucket", None)

MISSING_INT8 = -99
MISSING_INT16 = -9999
MISSING_INT32 = -999999999
INVALID_INT16 = -11111
INVALID_INT32 = -1111111111
Provider = 'HUNMHS'

memory_cache = {}
CACHE_EXPIRY = 3600  

def get_memory_cache(key):
    """メモリキャッシュからデータを取得"""
    try:
        if key in memory_cache:
            cache_data = memory_cache[key]
            # キャッシュの有効期限をチェック
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
    """メモリキャッシュにデータを保存"""
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
    """期限切れのキャッシュを削除"""
    current_time = datetime.now(timezone.utc).timestamp()
    expired_keys = [
        key for key, cache_data in memory_cache.items()
        if current_time - cache_data['timestamp'] > CACHE_EXPIRY
    ]
    for key in expired_keys:
        del memory_cache[key]
    if expired_keys:
        print(f"Cleaned up {len(expired_keys)} expired cache entries")

def validate_environment():
    """環境変数の検証"""
    if not input_bucket or not metadata_bucket:
        raise EnvironmentError("Required environment variables are not set: stock_s3 and/or md_bucket")

def extract_csv(input_bucket, objkey):
    """S3からCSVファイルを読み込み、ヘッダーを処理する"""
    try:
        if not input_bucket or not objkey:
            raise ValueError("Invalid bucket or key")

        print(f'Reading data from S3: {input_bucket}/{objkey}')
        
        try:
            response = s3.get_object(Bucket=input_bucket, Key=objkey)
            file_content = response['Body'].read()
        except Exception as e:
            raise Exception(f"Failed to read from S3: {e}")

        # ヘッダーマーカーの確認
        header_marker_pos = file_content.find(b'\x04\x1a')
        if header_marker_pos == -1:
            # ヘッダーマーカーが見つからない場合、ファイル全体をデータとして扱う
            data_part = file_content
        else:
            # ヘッダーとデータの分離
            header_part = file_content[:header_marker_pos]
            data_part = file_content[header_marker_pos + 2:]

        if not data_part:
            raise ValueError("No data found")

        # データ部分のデコードとCSV処理
        try:
            csv_content = data_part.decode('utf-8')
            csv_reader = csv.DictReader(io.StringIO(csv_content), delimiter=';')
            csv_data = list(csv_reader)
            
            if not csv_data:
                raise ValueError("No CSV data found")

            # デバッグ出力
            print(f"Sample row from CSV: {csv_data[0] if csv_data else 'No data'}")
            print(f"Column names: {list(csv_data[0].keys()) if csv_data else 'No columns'}")

            # スペースを含むTimeカラム名を探す
            time_column = next((col for col in csv_data[0].keys() if col.strip() == 'Time'), None)
            if not time_column:
                raise ValueError("Time column not found in CSV data")

            # Time値の処理
            time_values = [row[time_column].strip() for row in csv_data if time_column in row and row[time_column].strip()]
            
            if not time_values:
                print("Warning: No valid Time values found in CSV data")
                announced_dt = datetime.now(timezone.utc)
            else:
                latest_time = max(time_values)
                try:
                    announced_dt = datetime.strptime(latest_time, '%Y%m%d%H%M')
                    announced_dt = announced_dt.replace(tzinfo=timezone.utc)
                    print(f"Using latest time value from data: {latest_time} -> {announced_dt}")
                except ValueError as e:
                    print(f"Warning: Could not parse Time value: {latest_time}, Error: {e}")
                    announced_dt = datetime.now(timezone.utc)

            return csv_data, announced_dt

        except Exception as e:
            raise Exception(f"Failed to process CSV data: {e}")

    except Exception as e:
        print(f"Error in extract_csv: {e}")
        return None, None

def validate_data(point_data):
    """データの妥当性チェック"""
    if not point_data:
        print("Error: Empty point_data")
        return False

    required_fields = [
        "LCLID", "ID_GLOBAL_MNET",
        "HVIS", "HVIS_AQC",
        "WNDSPD", "WNDSPD_AQC",
        "GUSTS", "GUSTS_AQC",
        "GUSTD", "GUSTD_AQC",
        "WNDDIR", "WNDDIR_AQC",
        "AIRTMP", "AIRTMP_AQC",
        "AIRTMP_10MIN_MAX", "AIRTMP_10MIN_MAX_AQC",
        "AIRTMP_10MIN_AVG", "AIRTMP_10MIN_AVG_AQC",
        "AIRTMP_10MIN_MINI", "AIRTMP_10MIN_MINI_AQC",
        "RHUM", "RHUM_AQC",
        "ARPRSS", "ARPRSS_AQC",
        "PRCRIN_10MIN", "PRCRIN_10MIN_AQC"
    ]

    for point in point_data:
        missing_fields = [field for field in required_fields if field not in point]
        if missing_fields:
            print(f"Missing fields in point data: {missing_fields}")
            return False
    
    return True

def convert_to_json_format(csv_data, announced_dt):
    """CSVデータをJSON形式に変換"""
    try:
        point_data = []

        if csv_data:
            print("Available CSV columns:", list(csv_data[0].keys()))

        for row in csv_data:
            try:
                def check_value(value):
                    if value is None or str(value).strip() in ['', '--', '-999', '-999.0']:
                        return False
                    try:
                        float(str(value).strip())
                        return True
                    except ValueError:
                        return False

                def convert_to_int(value, multiplier=1, default=MISSING_INT16):
                    try:
                        if check_value(value):
                            value_str = str(value).strip()
                            return int(float(value_str) * multiplier)
                        return default
                    except:
                        print(f"Failed to convert value: {value}")
                        return default

                # StationNumberからスペースを除去
                station_number = str(row.get('StationNumber', '')).strip()
                
                station_data = {
                    "LCLID": station_number,
                    "ID_GLOBAL_MNET": f"{Provider}_{station_number}",
                    
                    # 視程 (v column with spaces)
                    "HVIS": convert_to_int(row.get('     v')),
                    "HVIS_AQC": MISSING_INT8,
                    
                    # 風速 (fs column with spaces)
                    "WNDSPD": convert_to_int(row.get('   fs'), 10),
                    "WNDSPD_AQC":  MISSING_INT8,
                    
                    # 最大瞬間風速 (fx column with spaces)
                    "GUSTS": convert_to_int(row.get('   fx'), 10),
                    "GUSTS_AQC": MISSING_INT8,
                    
                    # 最大瞬間風向 (fxd column with spaces)
                    "GUSTD": convert_to_int(row.get(' fxd')),
                    "GUSTD_AQC": MISSING_INT8,
                    
                    # 風向 (fsd column with spaces)
                    "WNDDIR": convert_to_int(row.get(' fsd')),
                    "WNDDIR_AQC": MISSING_INT8,

                    # 瞬間気温 (t column with spaces)
                    "AIRTMP": convert_to_int(row.get('    t'), 10),
                    "AIRTMP_AQC": MISSING_INT8,
                    
                    # 10分間最高気温 (tx column with spaces)
                    "AIRTMP_10MIN_MAX": convert_to_int(row.get('   tx'), 10),
                    "AIRTMP_10MIN_MAX_AQC": MISSING_INT8,
                    
                    # 10分間平均気温 (ta column with spaces)
                    "AIRTMP_10MIN_AVG": convert_to_int(row.get('   ta'), 10),
                    "AIRTMP_10MIN_AVG_AQC": MISSING_INT8,
                    
                    # 10分間最低気温 (tn column with spaces)
                    "AIRTMP_10MIN_MINI": convert_to_int(row.get('   tn'), 10),
                    "AIRTMP_10MIN_MINI_AQC": MISSING_INT8,

                    # 相対湿度 (u column with spaces)
                    "RHUM": convert_to_int(row.get('   u'), 10),
                    "RHUM_AQC": MISSING_INT8,

                    # 気圧 (p column with spaces)
                    "ARPRSS": convert_to_int(row.get('      p'), 10),
                    "ARPRSS_AQC":  MISSING_INT8,

                    # 10分間降水量 (r column with spaces)
                    "PRCRIN_10MIN": convert_to_int(row.get('    r'), 10),
                    "PRCRIN_10MIN_AQC": MISSING_INT8
                }


                if any(value == INVALID_INT16 for value in station_data.values() if isinstance(value, int)):
                    print(f"Invalid data found in row: {row}")
                    continue

                point_data.append(station_data)

            except Exception as e:
                print(f"Error processing row: {e}")
                continue

        if not validate_data(point_data):
            print("Error: Invalid data format")
            return None

        print(f"Processed {len(point_data)} valid records")

        return {
            "tagid": tagid,
            "announced": announced_dt.strftime("%Y-%m-%dT%H:%M:00Z"),
            "created": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "original": {
                "observation_date": {
                    "year": announced_dt.year,
                    "month": announced_dt.month,
                    "day": announced_dt.day,
                    "hour": announced_dt.hour,
                    "min": announced_dt.minute,
                    "sec": 0
                },
                "point_count": len(point_data),
                "point_data": point_data
            }
        }

    except Exception as e:
        print(f"Error in convert_to_json_format: {e}")
        import traceback
        traceback.print_exc()
        return None

def save_to_s3(metadata_bucket, save_key, data):
    """データをS3に保存"""
    try:
        if not all([metadata_bucket, save_key, data]):
            raise ValueError("Missing required parameters for S3 save")
        
        json_data = json.dumps(data, indent=2, ensure_ascii=False) if not isinstance(data, str) else data
        
        s3.put_object(
            Body=json_data.encode("utf-8"),
            Bucket=metadata_bucket,
            Key=save_key,
            ContentType='application/json'
        )
        print(f"Data successfully saved to s3://{metadata_bucket}/{save_key}")
        return True
    except Exception as e:
        print(f"Error saving to S3: {e}")
        return False

def main(event, context):
    try:
        validate_environment()
        print(f"Processing event: {json.dumps(event, ensure_ascii=False)}")
        keys = []
        
        # 期限切れのキャッシュをクリーンアップ
        cleanup_memory_cache()
        
        # イベントレコードの処理
        for record in event["Records"]:
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
                print(f"\nProcessing key: {key}")
                
                # メモリキャッシュをチェック
                cached_data = get_memory_cache(key)
                if cached_data:
                    json_data = cached_data
                    print(f"Using cached data for key: {key}")
                else:
                    # CSVデータの取得と処理
                    csv_data, announced_dt = extract_csv(input_bucket, key)
                    
                    if not csv_data:
                        print(f"Failed to extract valid CSV data from key: {key}")
                        continue

                    json_data = convert_to_json_format(csv_data, announced_dt)
                    
                    if json_data:
                        # 処理結果をメモリキャッシュに保存
                        set_memory_cache(key, json_data)
                        print(f"Data cached for key: {key}")
                    else:
                        print(f"Failed to convert CSV to JSON for key: {key}")
                        continue

                # 保存用のキー生成
                current_time = datetime.now()
                random_suffix = str(uuid.uuid4())
                file_name = f"{current_time.strftime('%Y%m%d%H%M%S')}.{random_suffix}"
                save_key = f"data/{tagid}/{current_time.strftime('%Y/%m/%d')}/{file_name}"
                
                # S3に保存
                if save_to_s3(metadata_bucket, save_key, json_data):
                    print(f"Successfully processed and saved data for key: {key}")
                    processed_results.append({
                        'key': key,
                        'save_key': save_key,
                        'status': 'success',
                        'cached': cached_data is not None
                    })
                else:
                    print(f"Failed to save data for key: {key}")
                    processed_results.append({
                        'key': key,
                        'status': 'failed_to_save',
                        'cached': cached_data is not None
                    })

            except Exception as e:
                print(f"Error processing key {key}: {e}")
                import traceback
                traceback.print_exc()
                processed_results.append({
                    'key': key,
                    'status': 'error',
                    'error_message': str(e)
                })
                continue

        # キャッシュの状態を確認
        cache_stats = {
            'total_entries': len(memory_cache),
            'cache_keys': list(memory_cache.keys())
        }

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processing completed successfully',
                'processed_keys': processed_results,
                'cache_statistics': cache_stats,
                'total_processed': len(processed_results),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }, ensure_ascii=False)
        }

    except Exception as e:
        print(f"Fatal error in main function: {e}")
        error_detail = {
            'error': str(e),
            'traceback': traceback.format_exc(),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        return {
            'statusCode': 500,
            'body': json.dumps(error_detail, ensure_ascii=False)
        }

if __name__ == '__main__':
    main({}, {})
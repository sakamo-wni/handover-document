import os
import boto3
import json
import datetime
import logging
import hashlib
import uuid
from pathlib import Path
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

tagid = os.environ.get("tagid", "460220001")
input_bucket = os.environ.get("stock_s3")
metadata_bucket = os.environ.get("md_bucket")
cache_bucket = os.environ.get("cache_bucket", metadata_bucket)  # キャッシュに使用するバケット

# キャッシュディレクトリ (S3内のプレフィックス)
CACHE_PREFIX = "tmp_RMI/"

def log_message(message):
    print(message)
    logger.info(message)

def get_station_cache_key(station_id):
    return f"{CACHE_PREFIX}station_{station_id}.json"

def get_station_cache(station_id):
    cache_key = get_station_cache_key(station_id)
    try:
        log_message(f"地点キャッシュを確認: バケット={cache_bucket}, キー={cache_key}, 地点ID={station_id}")
        
        response = s3.get_object(Bucket=cache_bucket, Key=cache_key)
        content = response['Body'].read().decode('utf-8')
        
        try:
            cache_data = json.loads(content)
            log_message(f"地点キャッシュからデータを取得: 地点ID={station_id}, サイズ={len(content)}バイト")
            return cache_data
        except json.JSONDecodeError as e:
            log_message(f"地点キャッシュJSONパースエラー: 地点ID={station_id}, エラー={str(e)}")
            return None
            
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        if error_code == 'NoSuchKey' or error_code == '404':
            log_message(f"地点キャッシュがありません: 地点ID={station_id}")
        else:
            log_message(f"地点キャッシュ読み込みエラー: 地点ID={station_id}, エラー={error_code}")
        return None
    except Exception as e:
        log_message(f"地点キャッシュ一般エラー: 地点ID={station_id}, エラー={str(e)}")
        return None

def update_station_cache(station_id, data):
    """観測地点のキャッシュデータを更新"""
    if data is None:
        return False
    
    cache_key = get_station_cache_key(station_id)
    try:
        log_message(f"地点キャッシュに保存: バケット={cache_bucket}, キー={cache_key}, 地点ID={station_id}")
        json_data = json.dumps(data, ensure_ascii=False)
        data_size = len(json_data.encode('utf-8'))
        
        response = s3.put_object(
            Body=json_data.encode('utf-8'),
            Bucket=cache_bucket,
            Key=cache_key,
            ContentType='application/json'
        )
        log_message(f"地点キャッシュにデータを保存しました: 地点ID={station_id}, ETag={response.get('ETag', 'なし')}, サイズ={data_size}バイト")
        
        try:
            verify_response = s3.get_object(Bucket=cache_bucket, Key=cache_key)
            verify_size = verify_response.get('ContentLength', 0)
            log_message(f"地点キャッシュの存在を確認: 地点ID={station_id}, サイズ={verify_size}バイト")
        except Exception as e:
            log_message(f"警告: 地点キャッシュを書き込んだ直後ですが、確認できません: 地点ID={station_id}, エラー={str(e)}")
        
        return True
    except Exception as e:
        log_message(f"地点キャッシュ保存エラー: 地点ID={station_id}, エラー={str(e)}")
        return False

def extract_data_from_s3(bucket, key):
    """S3からデータを取得し、ヘッダーを除去してJSONデータを抽出"""
    try:
        log_message(f"S3からデータを読み込み: {bucket}/{key}")
        
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        
        header_end = content.find(b'\x04\x1a')
        if header_end != -1:
            header_end += 2
        else:
            header_end = 0
        
        try:
            json_data = json.loads(content[header_end:])
            return json_data
        except json.JSONDecodeError as e:
            log_message(f"JSONパースエラー: {str(e)}")
            return None
    
    except Exception as e:
        log_message(f"データ抽出エラー: {str(e)}")
        return None

def is_feature_identical(current_feature, cached_feature):
    current_props = current_feature.get('properties', {})
    cached_props = cached_feature.get('properties', {})
    
    for key, value in current_props.items():
        if key not in cached_props or cached_props[key] != value:
            return False
    
    for key in cached_props:
        if key not in current_props:
            return False
    
    current_geo = current_feature.get('geometry', {})
    cached_geo = cached_feature.get('geometry', {})
    
    return current_geo == cached_geo

def get_station_id_from_feature(feature):
    if not feature or 'properties' not in feature:
        return None
    
    props = feature.get('properties', {})
    return props.get('code')

def convert_to_required_format(data):
    if not data or 'features' not in data:
        log_message("有効なGeoJSONデータがありません")
        return None
    
    updated_features = []
    unchanged_count = 0
    
    now = datetime.datetime.now(datetime.timezone.utc)
    
    for feature in data['features']:
        if 'properties' not in feature or 'geometry' not in feature:
            continue
        
        props = feature['properties']
        station_id = props.get('code')
        if not station_id:
            continue
        
        # 地点キャッシュからデータを取得
        cached_feature = get_station_cache(station_id)
        
        # 完全一致する場合はスキップ
        if cached_feature and is_feature_identical(feature, cached_feature):
            unchanged_count += 1
            continue
        
        # 観測データの抽出と10倍して整数化
        point_data = {
            "LCLID": str(station_id),
            "ID_GLOBAL_MNET": f"RMI_{station_id}",
            "WNDSPD": int(round(props.get('wind_speed_10m', 0) * 10)) if props.get('wind_speed_10m') is not None else 0,
            "WNDSPD_AQC": -99,
            "GUSTS": int(round(props.get('wind_gusts_speed', 0) * 10)) if props.get('wind_gusts_speed') is not None else 0,
            "GUSTS_AQC": -99,
            "AIRTMP_1HOUR_AVG": int(round(props.get('temp_dry_shelter_avg', 0) * 10)) if props.get('temp_dry_shelter_avg') is not None else 0,
            "AIRTMP_1HOUR_AVG_AQC": -99,
            "RHUM_1HOUR_AVG": int(round(props.get('humidity_rel_shelter_avg', 0) * 10)) if props.get('humidity_rel_shelter_avg') is not None else 0,
            "RHUM_1HOUR_AQC": -99,
            "ARPRSS_1HOUR_AVG": int(round(props.get('pressure', 0) * 10)) if props.get('pressure') is not None else 0,
            "ARPRSS_1HOUR_AVG_AQC": -99,
            "PRCRIN_1HOUR": int(round(props.get('precip_quantity', 0) * 10)) if props.get('precip_quantity') is not None else 0,
            "PRCRIN_1HOUR_AQC": -99
        }
        
        updated_features.append(point_data)
        
        # 地点キャッシュを更新
        update_station_cache(station_id, feature)
    
    timestamp = now
    if data['features'] and 'properties' in data['features'][0] and 'timestamp' in data['features'][0]['properties']:
        timestamp_str = data['features'][0]['properties']['timestamp']
        try:
            timestamp = datetime.datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ")
            timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
        except:
            pass
    
    if not updated_features:
        log_message(f"更新された地点はありません")
        return None
    
    result = {
        "tagid": tagid,
        "announced": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "created": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "original": {
            "observation_date": {
                "year": timestamp.year,
                "month": timestamp.month,
                "day": timestamp.day,
                "hour": timestamp.hour,
                "min": timestamp.minute,
                "sec": timestamp.second
            },
            "point_count": len(updated_features),
            "point_data": updated_features
        }
    }
    
    log_message(f"更新した地点数: {len(updated_features)}")
    log_message(f"キャッシュで除外した地点数: {unchanged_count}")
    log_message(f"合計地点数: {len(data['features'])}")
    
    return result

def save_to_s3(bucket, key, data):
    try:
        log_message(f"S3に保存: {bucket}/{key}")
        
        json_data = json.dumps(data, ensure_ascii=False, indent=2)
        s3.put_object(
            Body=json_data.encode('utf-8'),
            Bucket=bucket,
            Key=key,
            ContentType='application/json'
        )
        return True
    
    except Exception as e:
        log_message(f"S3保存エラー: {str(e)}")
        return False

def process_s3_file(bucket, key):
    try:
        log_message(f"処理開始: {bucket}/{key}")
        
        data = extract_data_from_s3(bucket, key)
        if not data:
            log_message(f"データ抽出失敗: {bucket}/{key}")
            return None
        
        log_message(f"抽出したデータ: 特徴点数={len(data.get('features', []))}個")
        
        result = convert_to_required_format(data)
        
        if not result:
            log_message(f"変更なしでスキップ: {bucket}/{key}")
            return None
        
        return result
    
    except Exception as e:
        log_message(f"処理エラー: {str(e)}")
        import traceback
        log_message(f"詳細: {traceback.format_exc()}")
        return None

def main(event, context):
    try:
        log_message(f"環境変数: input_bucket={input_bucket}, metadata_bucket={metadata_bucket}, cache_bucket={cache_bucket}")
        log_message(f"キャッシュプレフィックス: {CACHE_PREFIX}")
        
        if not input_bucket or not metadata_bucket:
            log_message("環境変数が設定されていません: stock_s3 または md_bucket")
            return {
                'statusCode': 500,
                'body': json.dumps('環境変数が設定されていません')
            }
        
        log_message("===== 処理開始 =====")
        
        keys = []
        for record in event.get("Records", []):
            try:
                body = json.loads(record.get("body", "{}"))
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
            except:
                continue
        
        if not keys:
            log_message("処理するキーがありません")
            return {
                'statusCode': 200,
                'body': json.dumps('処理するキーがありません')
            }
        
        log_message(f"処理対象キー: {keys}")
        
        for key in keys:
            result = process_s3_file(input_bucket, key)
            
            if not result:
                continue
            
            current_time = datetime.datetime.now()
            random_suffix = str(uuid.uuid4())
            file_name = f"{current_time.strftime('%Y%m%d%H%M%S')}.{random_suffix}"
            save_key = f"data/{tagid}/{current_time.strftime('%Y/%m/%d')}/{file_name}"
            
            save_to_s3(metadata_bucket, save_key, result)
        
        log_message("===== 処理完了 =====")
        
        return {
            'statusCode': 200,
            'body': json.dumps('処理が完了しました')
        }
    
    except Exception as e:
        log_message(f"致命的なエラー: {str(e)}")
        import traceback
        log_message(f"詳細: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'エラー: {str(e)}')
        }

if __name__ == "__main__":
    # ローカルテスト用
    main({"Records": []}, None)
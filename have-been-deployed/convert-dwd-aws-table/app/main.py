import os
import boto3
from botocore.exceptions import ClientError
import json
from datetime import datetime, timezone
import io

# AWS クライアントの初期化
s3 = boto3.client('s3')
account_id = boto3.client("sts").get_caller_identity()["Account"]

# 環境変数の取得
input_bucket = os.environ.get("stock_s3")
metadata_bucket = os.environ.get("md_bucket")

def validate_environment():
    """環境変数の検証"""
    if not input_bucket or not metadata_bucket:
        raise EnvironmentError("Required environment variables are not set: stock_s3 and/or md_bucket")

def extract_data(input_bucket, objkey):
    """S3からデータを読み込み、ヘッダーを除去"""
    try:
        if not input_bucket or not objkey:
            print("Invalid bucket or key")
            return None, None

        # S3からファイルを読み込む
        response = s3.get_object(Bucket=input_bucket, Key=objkey)
        content = response['Body'].read().decode('latin1')
        
        # ヘッダー部の終わりを探す
        header_end = content.find('\x04\x1a') + 2
        
        # ヘッダーが見つからない場合
        if header_end < 2:
            print("Header end marker not found")
            return None, None
        
        # メタデータを抽出（例：announced日時）
        announced_dt = None
        for line in content[:header_end].split('\n'):
            if line.startswith('announced='):
                announced_dt = line.split('=')[1].strip()
                break
        
        # ヘッダー部分を除去したデータを返す
        data_text = content[header_end:]
        
        return data_text, announced_dt
        
    except Exception as e:
        print(f"Error extracting data: {e}")
        return None, None

def convert_date_format(date_str):
    """日付フォーマットを yyyymmdd から yyyy-mm-ddT00:00:00Z に変換"""
    try:
        if not date_str or date_str == "99999999":  # 無効な日付の場合
            return None
        
        # yyyymmdd を datetime オブジェクトに変換
        date_obj = datetime.strptime(date_str, '%Y%m%d')
        
        # ISO 8601フォーマットに変換
        return date_obj.strftime('%Y-%m-%dT00:00:00Z')
    except ValueError as e:
        print(f"Error converting date {date_str}: {e}")
        return None

def convert_to_geojson(data_text):
    """テキストデータをGeoJSONに変換"""
    features_list = []
    station_count = 0
    
    # 基本構造の初期化
    geojson = {
        'type': 'FeatureCollection',
        'features': features_list
    }
    
    # データを行ごとに処理
    lines = data_text.split('\n')
    
    # 実データの開始行を探す（破線の次の行から）
    start_line = 0
    for i, line in enumerate(lines):
        if '----' in line:
            start_line = i + 1
            break
    
    # 実データの処理
    for line in lines[start_line:]:
        if not line.strip():  # 空行をスキップ
            continue
            
        try:
            # スペースで分割
            parts = line.split()
            
            # 基本データの抽出
            station_id = parts[0]
            operation_from = parts[1]
            operation_to = parts[2]
            station_height = parts[3]
            latitude = parts[4]
            longitude = parts[5]
            
            # 施設名と州名の構築
            # Stationsname は7列目、Bundesland は8列目に固定
            name = parts[6].strip()  # Stationsname
            bundesland = parts[7].strip()  # Bundesland
            
            # coordinates の作成
            coords = [
                float(longitude),  # Longitude
                float(latitude),   # Latitude
                float(station_height)  # Elevation
            ]
            
            # Feature辞書の作成
            features_dict = dict()
            features_dict['type'] = 'Feature'
            
            # geometry辞書の作成
            geometry_dict = dict()
            geometry_dict['type'] = 'Point'
            geometry_dict['coordinates'] = coords
            
            # properties辞書の作成
            properties_dict = dict()
            properties_dict['LCLID'] = station_id
            properties_dict['LNAME'] = name
            properties_dict['CNTRY'] = "DE"
            properties_dict['states'] = bundesland
            properties_dict['OBS_BEGIND'] = convert_date_format(operation_from)
            properties_dict['OBS_ENDD'] = convert_date_format(operation_to)
            
            # 各辞書を結合
            features_dict['geometry'] = geometry_dict
            features_dict['properties'] = properties_dict
            
            # features_listに追加
            features_list.append(features_dict)
            
            # カウンターをインクリメント
            station_count += 1
            
        except (IndexError, ValueError) as e:
            print(f"Error processing line: {line.strip()}")
            print(f"Error: {e}")
            continue
    
    print(f"Total number of stations processed: {station_count}")
    return geojson

def save_to_s3(metadata_bucket, save_key, data):
    """データをS3に保存"""
    try:
        if not all([metadata_bucket, save_key, data]):
            raise ValueError("Missing required parameters for S3 save")
                
        json_data = json.dumps(data, ensure_ascii=False, indent=2)

        s3.put_object(
            Body=json_data.encode('utf-8'),
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
        
        # イベントからキーを抽出
        for record in event["Records"]:
            try:
                print(f"Processing record: {record}")
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
        
        for key in keys:
            try:
                # データの抽出とヘッダー除去
                data_text, announced_dt = extract_data(input_bucket, key)
                
                if not data_text:
                    print(f"No data found for key: {key}")
                    continue

                # GeoJSONに変換
                json_object = convert_to_geojson(data_text)
                if not json_object:
                    print(f"Failed to convert to GeoJSON for key: {key}")
                    continue

                # S3に保存
                s3_key = 'metadata/spool/DWD_AWS/metadata.json'
                if save_to_s3(metadata_bucket, s3_key, json_object):
                    print(f"Successfully processed and saved data for key: {key}")
                else:
                    print(f"Failed to save data for key: {key}")
                
            except Exception as e:
                print(f"Error processing key {key}: {e}")
                continue

        return {
            'statusCode': 200,
            'body': json.dumps('Processing completed successfully')
        }

    except Exception as e:
        print(f"Fatal error in main function: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

if __name__ == '__main__':
    main({}, {})
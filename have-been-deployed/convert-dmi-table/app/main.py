import os
import boto3
from botocore.exceptions import ClientError
import json
from datetime import datetime, timezone
import io

s3 = boto3.client('s3')
account_id = boto3.client("sts").get_caller_identity()["Account"]

input_bucket = os.environ.get("stock_s3")
metadata_bucket = os.environ.get("md_bucket")

def validate_environment():
    if not input_bucket or not metadata_bucket:
        raise EnvironmentError("Required environment variables are not set: stock_s3 and/or md_bucket")

def extract_data(input_bucket, objkey):
    try:
        if not input_bucket or not objkey:
            print("Invalid bucket or key")
            return None, None

        response = s3.get_object(Bucket=input_bucket, Key=objkey)
        content = response['Body'].read().decode('utf-8')
        
        # ヘッダー部の終わりを探し、見つからなければエラーを返す!
        header_end = content.find('\x04\x1a') + 2
        
        if header_end < 2:
            print("Header end marker not found")
            return None, None
        
        announced_dt = None
        for line in content[:header_end].split('\n'):
            if line.startswith('announced='):
                announced_dt = line.split('=')[1].strip()
                break
        
        # JSONデータの解析!
        try:
            json_data = json.loads(content[header_end:])
            print(f"Extracted data contains {len(json_data.get('features', []))} stations")
            return json_data, announced_dt
        except json.JSONDecodeError as e:
            print(f"JSON parse error: {e}")
            return None, None
        
    except Exception as e:
        print(f"Error extracting data: {e}")
        return None, None

def convert_to_geojson(stations):
    try:
        if not isinstance(stations, dict) or 'features' not in stations:
            print(f"Invalid input data format: {type(stations)}")
            return None
        
        # まず全ての地点情報を取得
        all_stations = []
        
        input_count = len(stations['features'])
        print(f"Input station count: {input_count}")
        
        for item in stations['features']:
            try:
                # 座標情報の取得と正規化
                lon = float(item['geometry']['coordinates'][0])
                lat = float(item['geometry']['coordinates'][1])
                
                # 高度情報の取得
                height_value = item['properties'].get('stationHeight')
                altitude_present = False
                
                # 高度情報がある場合のみ、座標に追加する
                if height_value is not None:
                    try:
                        elevation = float(height_value)
                        altitude_present = True
                        coords = [lon, lat, elevation]
                    except ValueError:
                        # 変換できない場合は高度情報なしとして扱う
                        coords = [lon, lat]
                else:
                    # 高度情報がない場合
                    coords = [lon, lat]
                
                # 緯度・経度のみの座標キー (重複判定用)
                coord_key = f"{lon:.6f},{lat:.6f}"
                
                # ステーション情報の取得
                station_id = item['properties']['stationId'].strip()
                station_name = item['properties']['name'].strip()
                
                # 日付情報の取得と変換
                obs_begin_str = item['properties']['operationFrom']
                obs_end_str = item['properties']['operationTo'] if item['properties']['operationTo'] else ""
                
                try:
                    # タイムゾーン情報を含む場合はそれを削除して統一する
                    if obs_begin_str:
                        obs_begin_dt = datetime.fromisoformat(obs_begin_str)
                        if obs_begin_dt.tzinfo is not None:
                            # タイムゾーン情報を削除（ナイーブなdatetimeに変換）
                            obs_begin_dt = obs_begin_dt.replace(tzinfo=None)
                    else:
                        obs_begin_dt = datetime.min
                except ValueError:
                    obs_begin_dt = datetime.min
                
                try:
                    if obs_end_str:
                        obs_end_dt = datetime.fromisoformat(obs_end_str)
                        if obs_end_dt.tzinfo is not None:
                            # タイムゾーン情報を削除（ナイーブなdatetimeに変換）
                            obs_end_dt = obs_end_dt.replace(tzinfo=None)
                    else:
                        obs_end_dt = datetime.max
                except ValueError:
                    obs_end_dt = datetime.max
                
                # プロパティ辞書の作成
                properties_dict = {
                    'LCLID': station_id,
                    'LNAME': station_name,
                    'CNTRY': "DK",
                    'OBS_BEGIND': obs_begin_str,
                    'OBS_ENDD': obs_end_str
                }
                
                # GeoJSONフィーチャーの作成
                feature = {
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': coords
                    },
                    'properties': properties_dict
                }
                
                # 優先順位の計算 - これを基に重複排除時に選択する
                priority = (
                    # 終了日が設定されてないものを優先
                    1 if not obs_end_str else 0,
                    # 終了日が新しいものを優先
                    obs_end_dt,
                    # 高度情報があるものを優先
                    1 if altitude_present else 0,
                    # 開始日が新しいものを優先
                    obs_begin_dt
                )
                
                # 地点情報を追加
                all_stations.append({
                    'id': station_id,
                    'name': station_name,
                    'coord_key': coord_key,
                    'feature': feature,
                    'priority': priority
                })
                
            except (KeyError, ValueError, TypeError) as e:
                print(f"Warning: Error processing feature: {e}")
                continue
        
        # IDによる重複排除
        station_by_id = {}
        for station in all_stations:
            station_id = station['id']
            if station_id in station_by_id:
                if station['priority'] > station_by_id[station_id]['priority']:
                    station_by_id[station_id] = station
            else:
                station_by_id[station_id] = station
        
        # IDによる重複排除後のリスト
        deduped_by_id = list(station_by_id.values())
        print(f"After ID deduplication: {len(deduped_by_id)} stations")
        
        # 座標による重複排除
        station_by_coord = {}
        for station in deduped_by_id:
            coord_key = station['coord_key']
            if coord_key in station_by_coord:
                if station['priority'] > station_by_coord[coord_key]['priority']:
                    station_by_coord[coord_key] = station
            else:
                station_by_coord[coord_key] = station
        
        # 座標による重複排除後のリスト
        deduped_by_coord = list(station_by_coord.values())
        print(f"After coordinate deduplication: {len(deduped_by_coord)} stations")
        
        # 名前による重複排除
        station_by_name = {}
        for station in deduped_by_coord:
            name = station['name']
            if name in station_by_name:
                if station['priority'] > station_by_name[name]['priority']:
                    station_by_name[name] = station
            else:
                station_by_name[name] = station
        
        # 名前による重複排除後のリスト
        deduped_by_name = list(station_by_name.values())
        print(f"After name deduplication: {len(deduped_by_name)} stations")
        
        # 最終的な地点リストの作成
        features_list = [station['feature'] for station in deduped_by_name]
        output_count = len(features_list)
        print(f"Successfully converted stations count after deduplication: {output_count}")
        print(f"Removed {input_count - output_count} duplicates")
        
        return {
            'type': 'FeatureCollection',
            'features': features_list
        }
        

    except Exception as e:
        print(f"Error in convert_to_geojson: {e}")
        return None

def save_to_s3(metadata_bucket, save_key, data):
    """データをS3に保存"""
    try:
        if not all([metadata_bucket, save_key, data]):
            raise ValueError("Missing required parameters for S3 save")
                
        json_data = json.dumps(data, ensure_ascii=False, indent=2)

        station_count = len(data.get('features', []))

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
        
        for record in event.get("Records", []):
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
                data_text, announced_dt = extract_data(input_bucket, key)
                
                if not data_text:
                    print(f"No data found for key: {key}")
                    continue

                json_object = convert_to_geojson(data_text)
                if not json_object:
                    print(f"Failed to convert to GeoJSON for key: {key}")
                    continue

                final_count = len(json_object['features'])
                print(f"Total number of stations processed: {final_count}")

                s3_key = 'metadata/spool/DMI/metadata.json'
                if save_to_s3(metadata_bucket, s3_key, json_object):
                    print(f"Successfully processed and saved {final_count} stations for key: {key}")
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
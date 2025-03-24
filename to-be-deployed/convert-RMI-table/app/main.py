import os
import boto3
from botocore.exceptions import ClientError
import json
from datetime import datetime
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
            return None
        
        response = s3.get_object(Bucket=input_bucket, Key=objkey)
        content = response['Body'].read().decode('utf-8')
        
        header_end = content.find('\x04\x1a')
        if header_end != -1:
            header_end += 2
        else:
            header_end = 0
        
        try:
            json_data = json.loads(content[header_end:])
            print(f"抽出されたデータには {len(json_data.get('features', []))} 件の地点情報が含まれています")
            return json_data
        except json.JSONDecodeError as e:
            print(f"JSONパースエラー: {e}")
            return None
        
    except Exception as e:
        print(f"データ抽出エラー: {e}")
        return None

def convert_to_geojson(stations):

    try:
        if not isinstance(stations, dict) or 'features' not in stations:
            print(f"無効な入力データ形式: {type(stations)}")
            return None
        
        all_stations = []
        
        input_count = len(stations['features'])
        print(f"入力地点数: {input_count}")
        
        for item in stations['features']:
            try:
                # 座標情報の取得
                lon = float(item['geometry']['coordinates'][0])
                lat = float(item['geometry']['coordinates'][1])
                
                # 高度情報の取得
                altitude = item['properties'].get('altitude')
                altitude_present = False
                
                # 高度情報がある場合のみ、座標に追加する
                if altitude is not None:
                    try:
                        elevation = float(altitude)
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
                
                # 地点情報の取得
                station_id = str(item['properties']['code'])
                station_name = item['properties']['name'].strip()
                
                # 日付情報の取得
                obs_begin_str = item['properties'].get('date_begin', '')
                obs_end_str = item['properties'].get('date_end', '')
                
                if obs_end_str is None:
                    obs_end_str = ''
                
                # プロパティ辞書の作成
                properties_dict = {
                    'LCLID': station_id,
                    'LNAME': station_name,
                    'CNTRY': "BE",
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
                
                # 開始日と終了日のdatetimeオブジェクト作成（重複排除の優先順位用）
                try:
                    if obs_begin_str:
                        obs_begin_dt = datetime.fromisoformat(obs_begin_str.replace('Z', '+00:00'))
                    else:
                        obs_begin_dt = datetime.min
                except ValueError:
                    obs_begin_dt = datetime.min
                
                try:
                    if obs_end_str:
                        obs_end_dt = datetime.fromisoformat(obs_end_str.replace('Z', '+00:00'))
                    else:
                        # 終了日が設定されていない = 現在も稼働中 = 優先度高
                        obs_end_dt = datetime.max
                except ValueError:
                    obs_end_dt = datetime.max
                
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
                print(f"警告: 特徴の処理中にエラーが発生しました: {e}")
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
        print(f"ID重複排除後: {len(deduped_by_id)} 地点")
        
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
        print(f"座標重複排除後: {len(deduped_by_coord)} 地点")
        
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
        print(f"名前重複排除後: {len(deduped_by_name)} 地点")
        
        # 最終的な地点リストの作成
        features_list = [station['feature'] for station in deduped_by_name]
        output_count = len(features_list)
        print(f"重複排除後の変換成功地点数: {output_count}")
        print(f"{input_count - output_count} 件の重複を削除しました")
        
        return {
            'type': 'FeatureCollection',
            'features': features_list
        }
        
    except Exception as e:
        print(f"convert_to_geojson でエラーが発生しました: {e}")
        return None

def save_to_s3(metadata_bucket, save_key, data):
    try:
        if not all([metadata_bucket, save_key, data]):
            raise ValueError("S3保存に必要なパラメータが不足しています")
                
        json_data = json.dumps(data, ensure_ascii=False, indent=2)

        station_count = len(data.get('features', []))

        s3.put_object(
            Body=json_data.encode('utf-8'),
            Bucket=metadata_bucket,
            Key=save_key,
            ContentType='application/json'
        )
        print(f"データを s3://{metadata_bucket}/{save_key} に正常に保存しました（{station_count} 地点）")        
        return True
    except Exception as e:
        print(f"S3保存エラー: {e}")
        return False

def main(event, context):
    try:
        validate_environment()
        print(f"イベント処理: {json.dumps(event, ensure_ascii=False)}")
        keys = []
        
        for record in event.get("Records", []):
            try:
                print(f"レコード処理: {record}")
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
                print(f"レコード処理エラー: {e}")
                continue

        print(f"処理するキー: {keys}")
        
        for key in keys:
            try:
                data = extract_data(input_bucket, key)
                
                if not data:
                    print(f"キー {key} のデータが見つかりませんでした")
                    continue

                json_object = convert_to_geojson(data)
                if not json_object:
                    print(f"キー {key} のGeoJSON変換に失敗しました")
                    continue

                final_count = len(json_object['features'])
                print(f"処理された総地点数: {final_count}")

                s3_key = 'metadata/spool/RMI/metadata.json'
                if save_to_s3(metadata_bucket, s3_key, json_object):
                    print(f"キー {key} の {final_count} 地点の処理と保存が成功しました")
                else:
                    print(f"キー {key} のデータ保存に失敗しました")
                
            except Exception as e:
                print(f"キー {key} の処理中にエラーが発生しました: {e}")
                continue

        return {
            'statusCode': 200,
            'body': json.dumps('処理が正常に完了しました')
        }

    except Exception as e:
        print(f"main関数で致命的なエラーが発生しました: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'エラー: {str(e)}')
        }

if __name__ == '__main__':
    main({"Records": []}, {})
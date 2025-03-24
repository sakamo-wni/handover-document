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
        content = response['Body'].read()
        
        header_end = content.find(b'\x04\x1a')
        if header_end != -1:
            header_end += 2
        else:
            header_end = 0
        
        announced_dt = None
        if header_end > 2:  
            header_text = content[:header_end].decode('utf-8', errors='ignore')
            for line in header_text.split('\n'):
                if line.startswith('announced='):
                    announced_dt = line.split('=')[1].strip()
                    break
        
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
        
        all_stations = []
        
        input_count = len(stations['features'])
        print(f"Input station count: {input_count}")
        
        for item in stations['features']:
            try:
                features = item['features']
                
                lon = float(features['geometry']['coordinates'][0])
                lat = float(features['geometry']['coordinates'][1])
                
                altitude = features['properties'].get('altitud')
                altitude_present = False
                
                if altitude is not None:
                    try:
                        elevation = float(altitude)
                        altitude_present = True
                        coords = [lon, lat, elevation]
                    except ValueError:
                        coords = [lon, lat]
                else:
                    coords = [lon, lat]
                
                coord_key = f"{lon:.6f},{lat:.6f}"
                
                codigo_nacional = features['properties']['CodigoNacional']
                nombre_estacion = features['properties']['nombreEstacion'].strip()
                codigo_wigos = features['properties'].get('codigoWIGOS', '')
                codigo_omm = features['properties'].get('CodigoOMM', '')
                
                fecha_instalacion = features['properties'].get('fechaInstalacion', '')
                
                obs_begin = ''
                try:
                    if fecha_instalacion:
                        obs_begin_dt = datetime.strptime(fecha_instalacion, '%Y-%m-%d %H:%M:%S')
                        obs_begin = obs_begin_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                except ValueError:
                    print(f"Warning: Could not parse date: {fecha_instalacion}")
                
                properties_dict = {
                    'LCLID': str(codigo_nacional),  
                    'LNAME': nombre_estacion,
                    'WIGOS_ID': codigo_wigos,
                    'WMO_ID': str(codigo_omm) if codigo_omm else "",
                    'LATD': lat,
                    'LOND': lon,
                    'ALT': altitude if altitude_present else None,
                    'OBS_BEGIND': obs_begin,
                    'CNTRY': "CL"  # 国コードは常にCL
                }
                
                feature = {
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': coords
                    },
                    'properties': properties_dict
                }
                
                # 優先順位の計算 - 重複排除時に選択するために使用
                priority = (
                    # 高度情報があるものを優先
                    1 if altitude_present else 0,
                    obs_begin
                )
                
                all_stations.append({
                    'id': str(codigo_nacional),
                    'name': nombre_estacion,
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

                s3_key = 'metadata/spool/DMC/metadata.json'
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
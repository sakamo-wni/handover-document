import os
import boto3
from botocore.exceptions import ClientError
import json
from datetime import datetime, timezone, timedelta
from urllib.parse import unquote
import netCDF4
import uuid

account_id = boto3.client("sts").get_caller_identity()["Account"]
s3 = boto3.client('s3')
date_path = datetime.now(timezone.utc).strftime("%Y/%m/%d")
tagid = '441000163'
today = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
input_bucket = os.environ.get("stock_s3", None)
metadata_bucket = os.environ.get("md_bucket", None)

# 基準日（2009年2月10日）
BASE_DATE = datetime(2009, 2, 10)

def validate_environment():
    if not input_bucket or not metadata_bucket:
        raise EnvironmentError("Required environment variables are not set: stock_s3 and/or md_bucket")

def extract_netcdf(input_bucket, objkey):
    if not input_bucket or not objkey:
        print("Invalid bucket or key")
        return None, None

    try:
        print(f'Reading data: {objkey}')
        response = s3.get_object(Bucket=input_bucket, Key=objkey)
        file_content = response['Body'].read()
        
        if b'\x04\x1a' in file_content:
            header = file_content.split(b'\x04\x1a')[0].decode('utf-8')
            announced_dt = None
            for line in header.splitlines():
                if '=' in line:
                    key, value = line.split('=', 1)
                    if 'date' in key.lower():
                        announced_dt = datetime.strptime(value.strip(), '%Y/%m/%d %H:%M:%S GMT')
                        break

            data = file_content.split(b'\x04\x1a')[1]
            temp_path = f'/tmp/temp_data_{uuid.uuid4()}.nc'
            with open(temp_path, 'wb') as f:
                f.write(data)
            
            return temp_path, announced_dt
        else:
            print("No valid NetCDF data found after header.")
            return None, None

    except Exception as e:
        print(f"Error reading or extracting NetCDF data: {e}")
        return None, None

def format_time(time_value):
    try:
        time_value = int(float(time_value))
        if abs(time_value) > 100000:
            return None
        
        time_obj = BASE_DATE + timedelta(days=time_value)
        return time_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    except Exception as e:
        print(f"Error formatting time: {e} (time_value: {time_value})")
        return None

def netcdf_to_geojson(file_path):
    try:
        dataset = netCDF4.Dataset(file_path, 'r')
        print(f"NetCDF Variables: {dataset.variables.keys()}")
        
        data_list = []
        lat = [float(x) for x in dataset.variables['lat'][:]]
        lon = [float(x) for x in dataset.variables['lon'][:]]
        height = [float(x) for x in dataset.variables['height'][:]]
        time = [float(x) for x in dataset.variables['time'][:]]
        name = [str(x).strip() for x in dataset.variables['name'][:]]
        WMO = [str(x).strip() for x in dataset.variables['WMO'][:]]
        WSI = [str(x).strip() for x in dataset.variables['WSI'][:]]
        
        for i in range(len(lat)):
            data_list.append({
                'Longitude': lon[i],
                'Latitude': lat[i],
                'Elevation': height[i],
                'StationNumber': str(WMO[i]).strip(), 
                'StationName': name[i],
                'WIGOS_ID': str(WSI[i]).strip(),  
                'StartDate': time[i]
            })
        
        print(f'Current NetCDF - stations_count: {len(data_list)}')
        
        features_list = []
        d = dict()
        d['type'] = 'FeatureCollection'

        for item in data_list:
            try:
                coords = [float(item['Longitude']), float(item['Latitude']), float(item['Elevation'])]
                features_dict = dict()
                features_dict['type'] = 'Feature'
                
                geometry_dict = dict()
                geometry_dict['type'] = 'Point'
                geometry_dict['coordinates'] = coords
                
                properties_dict = dict()
                properties_dict['LCLID'] = str(item['StationNumber']).strip()  
                properties_dict['LNAME'] = item['StationName']
                properties_dict['CNTRY'] = "NL"
                properties_dict['WMO_ID'] = str(item['StationNumber']).strip()  
                properties_dict['WIGOS_ID'] = item['WIGOS_ID']
                properties_dict['OBS_BEGIND'] = format_time(item['StartDate'])
                
                features_dict['geometry'] = geometry_dict
                features_dict['properties'] = properties_dict
                
                features_list.append(features_dict)
                
            except KeyError as e:
                print(f"Warning: Missing key in item: {e}")
            except ValueError as e:
                print(f"Warning: Invalid value in item: {e}")

        d['features'] = features_list
        return d

    except Exception as e:
        print(f"Error processing NetCDF data: {e}")
        return None
    finally:
        if 'dataset' in locals():
            dataset.close()

def save_to_s3(metadata_bucket, save_key, data):
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

def parse_sqs_message(record):
    try:
     
        body = record.get('body', '').strip()
        if not body:
            return []

        if '/' in body and not body.startswith('{'):
            return [body]
        try:
            body_json = json.loads(body)
            if isinstance(body_json, dict) and 'Message' in body_json:
                message = body_json['Message']
                if isinstance(message, str):
                    return [message.strip()]
        except json.JSONDecodeError:
            return [body]

        return []
    except Exception as e:
        print(f"Error parsing SQS message: {e}")
        return []

def main(event, context):
    try:
        validate_environment()
        print(f"Input event: {json.dumps(event, ensure_ascii=False)}")
        keys = []

        if "Records" in event:
            for record in event["Records"]:
                print(f"Processing raw message: {record.get('body', '')}")
                extracted_keys = parse_sqs_message(record)
                keys.extend(extracted_keys)

        keys = list(filter(None, set(keys)))
        print(f"Processing S3 keys: {keys}")
        for key in keys:
            try:
                print(f"Processing S3 object: {key}")
                netcdf_path, announced_dt = extract_netcdf(input_bucket, key)
                
                if not netcdf_path:
                    print(f"Failed to extract NetCDF data from: {key}")
                    continue

                json_object = netcdf_to_geojson(netcdf_path)
                if not json_object:
                    print(f"Failed to convert NetCDF to GeoJSON: {key}")
                    continue
                
                try:
                    os.remove(netcdf_path)
                except Exception as e:
                    print(f"Warning: Failed to remove temporary file: {e}")

                s3_key = 'metadata/spool/KNMI/metadata.json'
                if save_to_s3(metadata_bucket, s3_key, json_object):
                    print(f"Successfully saved metadata for: {key}")
                else:
                    print(f"Failed to save metadata for: {key}")
                
            except Exception as e:
                print(f"Error processing S3 object {key}: {e}")
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
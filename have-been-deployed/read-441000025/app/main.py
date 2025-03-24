import os
import netCDF4
import boto3
from botocore.exceptions import ClientError
import botocore
import json
import sys
from datetime import datetime, timezone, timedelta
from urllib.parse import unquote
import uuid

account_id = boto3.client("sts").get_caller_identity()["Account"]
s3 = boto3.client('s3')
date_path = datetime.now(timezone.utc).strftime("%Y/%m/%d")
tagid = '441000025'
today = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
input_bucket = os.environ.get("stock_s3", None)
metadata_bucket = os.environ.get("md_bucket", None)

MISSING_INT8  = -99
MISSING_INT16 = -9999
MISSING_INT32 = -999999999
INVALID_INT16 = -11111
INVALID_INT32 = -1111111111
Provider = 'KNMI'
# グローバル変数としてメモリキャッシュを追加
memory_cache = {}
CACHE_EXPIRY = 3600  # キャッシュの有効期限（秒）

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

def check_value(value):

    return (
        value is not None and
        value != -9999.0 and 
        str(value) != '--' and
        not hasattr(value, 'mask')  
    )

def safe_convert_to_int(value, scale=1, default=MISSING_INT16):

    try:
        if check_value(value):
            return int(float(value) * scale)
        return default
    except (ValueError, TypeError, AttributeError):
        return default


def validate_environment():
    """環境変数の検証"""
    if not input_bucket or not metadata_bucket:
        raise EnvironmentError("Required environment variables are not set: stock_s3 and/or md_bucket")

def extract_netcdf(input_bucket, objkey):
    """S3からnetCDFファイルを読み込み、ヘッダーを除去してデータを抽出"""
    temp_path = None

    try:
        if not input_bucket or not objkey:
            raise ValueError("Invalid bucket or key")

        print(f'Reading data from S3: {input_bucket}/{objkey}')
        
        # S3からのデータ取得
        try:
            response = s3.get_object(Bucket=input_bucket, Key=objkey)
            file_content = response['Body'].read()
        except Exception as e:
            raise Exception(f"Failed to read from S3: {e}")

        # ヘッダーマーカーの確認
        header_marker_pos = file_content.find(b'\x04\x1a')
        if header_marker_pos == -1:
            raise ValueError("Header marker not found in file")

        # ヘッダーとデータの分離
        header_part = file_content[:header_marker_pos]
        data_part = file_content[header_marker_pos + 2:]  # +2 でマーカーをスキップ

        if not data_part:
            raise ValueError("No data found after header")

        # ヘッダーの解析
        try:
            header = header_part.decode('utf-8')
            announced_dt = None
            for line in header.splitlines():
                if '=' in line:
                    key, value = line.split('=', 1)
                    if 'announced' in key.lower():  
                        announced_dt = datetime.strptime(value.strip(), '%Y/%m/%d %H:%M:%S GMT')
                        break
        except Exception as e:
            print(f"Warning: Error parsing header: {e}")
        print(f"announced date is: {announced_dt}")

        # HDF5シグネチャの確認
        if not data_part.startswith(b'\x89HDF'):
            print("Warning: Data does not start with HDF5 signature")
            # データの先頭部分を16進数で表示（デバッグ用）
            print(f"Data starts with: {data_part[:16].hex()}")

        # 一時ファイルの作成
        temp_path = f'/tmp/temp_data_{uuid.uuid4()}.nc'
        try:
            with open(temp_path, 'wb') as f:
                f.write(data_part)
        except Exception as e:
            raise Exception(f"Failed to write temporary file: {e}")

        # NetCDFファイルの検証
        try:
            test_dataset = netCDF4.Dataset(temp_path, 'r')
            required_vars = ['station', 'lat', 'lon']
            missing_vars = [var for var in required_vars if var not in test_dataset.variables]
            if missing_vars:
                raise ValueError(f"Missing required variables: {missing_vars}")
            test_dataset.close()
        except Exception as e:
            raise Exception(f"Invalid NetCDF file: {e}")

        print(f"Successfully extracted and validated NetCDF data to: {temp_path}")
        return temp_path, announced_dt

    except Exception as e:
        print(f"Error in extract_netcdf: {e}")
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
                print(f"Cleaned up temporary file: {temp_path}")
            except Exception as cleanup_error:
                print(f"Failed to clean up temporary file: {cleanup_error}")
        return None, None
        
def validate_data(point_data):
    """データの妥当性チェック"""
    if not point_data:
        print("Error: Empty point_data")
        return False

    required_fields = [
        "LCLID", "ID_GLOBAL_MNET",
        "HVIS", "HVIS_AQC",
        "AMTCLD_8", "AMTCLD_8_AQC",
        "WNDSPD_MD", "WNDSPD_MD_AQC",
        "GUSTS", "GUSTS_AQC",
        "WNDSPD_1HOUR_MAX", "WNDSPD_1HOUR_MAX_AQC",
        "WNDSPD_1HOUR_AVG", "WNDSPD_1HOUR_AVG_AQC",
        "GUSTS_1HOUR", "GUSTS_1HOUR_AQC",
        "WNDDIR_MD", "WNDDIR_MD_AQC",
        "AIRTMP_10MIN_MAX", "AIRTMP_10MIN_MAX_AQC",
        "AIRTMP", "AIRTMP_AQC",
        "AIRTMP_10MIN_MINI", "AIRTMP_10MIN_MINI_AQC",
        "RHUM", "RHUM_AQC",
        "DEWTMP", "DEWTMP_AQC",
        "ARPRSS", "ARPRSS_AQC",
        "PRCINT", "PRCINT_AQC",
        "PRCRIN_1HOUR", "PRCRIN_1HOUR_AQC",
        "WX_original", "WX_original_AQC"
    ]

    for point in point_data:
        missing_fields = [field for field in required_fields if field not in point]
        if missing_fields:
            print(f"Missing fields in point data: {missing_fields}")
            return False
    
    return True

def get_weather_description(ww_code):
    """
    現在天気コードからオランダ語の説明文を返す関数
    """
    weather_codes = {
        0: "Helder",
        1: "Bewolking afnemend over het afgelopen uur",
        2: "Bewolking onveranderd over het afgelopen uur",
        3: "Bewolking toenemend over het afgelopen uur",
        4: "Heiigheid of rook, of stof zwevend in de lucht",
        5: "Heiigheid of rook, of stof zwevend in de lucht",
        10: "Nevel",
        12: "Onweer op afstand",
        18: "Squalls",
        20: "Mist",
        21: "Neerslag",
        22: "Motregen (niet onderkoeld) of Motsneeuw",
        23: "Regen (niet onderkoeld)",
        24: "Sneeuw",
        25: "Onderkoelde (mot)regen",
        26: "Onweer met of zonder neerslag",
        30: "Mist",
        32: "Mist of ijsmist, dunner geworden gedurende het afgelopen uur",
        33: "Mist of ijsmist, geen merkbare verandering gedurende het afgelopen uur",
        34: "Mist of ijsmist, opgekomen of dikker geworden gedurende het afgelopen uur",
        35: "Mist met aanzetting van ruige rijp",
        40: "NEERSLAG",
        41: "Neerslag, licht of middelmatig",
        42: "Neerslag, zwaar",
        50: "MOTREGEN",
        51: "Motregen niet onderkoeld, licht",
        52: "Motregen niet onderkoeld, matig",
        53: "Motregen niet onderkoeld, dicht",
        54: "Motregen onderkoeld, licht",
        55: "Motregen onderkoeld, matig",
        56: "Motregen onderkoeld, dicht",
        57: "Motregen en regen, licht",
        58: "Motregen en regen, matig of zwaar",
        60: "REGEN",
        61: "Regen niet onderkoeld, licht",
        62: "Regen niet onderkoeld, matig",
        63: "Regen niet onderkoeld, zwaar",
        64: "Regen onderkoeld, licht",
        65: "Regen onderkoeld, matig",
        66: "Regen onderkoeld, zwaar",
        67: "Regen of motregen en sneeuw, licht",
        68: "Regen of motregen en sneeuw, matig of zwaar",
        70: "SNEEUW",
        71: "Sneeuw, licht",
        72: "Sneeuw, matig",
        73: "Sneeuw, zwaar",
        74: "IJsregen, licht",
        75: "IJsregen, matig",
        76: "IJsregen, zwaar",
        77: "Motsneeuw",
        78: "IJskristallen",
        80: "Bui of neerslag onderbroken",
        81: "Regen(bui) of regen onderbroken, licht",
        82: "Regen(bui) of regen onderbroken, matig",
        83: "Regen(bui) of regen onderbroken, zwaar",
        84: "Regen(bui) of regen onderbroken, zeer zwaar",
        85: "Sneeuw(bui) of sneeuw onderbroken, licht",
        86: "Sneeuw(bui) of sneeuw onderbroken, matig",
        87: "Sneeuw(bui) of sneeuw onderbroken, zwaar",
        89: "Hagel(bui) of hagel onderbroken",
        90: "Onweer",
        91: "Onweer, licht of matig, zonder neerslag",
        92: "Onweer, licht of matig, met regen en/of sneeuw(buien)",
        93: "Onweer, licht of matig, met hagel",
        94: "Onweer, zwaar, zonder neerslag",
        95: "Onweer, zwaar, met regen en/of sneeuw(buien)",
        96: "Onweer, zwaar, met hagel"
    }
    
    return weather_codes.get(ww_code, "Onbekend weer")

def convert_to_json_format(dataset, announced_dt):
    """NetCDFデータをJSON形式に変換"""
    try:
        # データセットの変数を確認
        print(f"Available variables: {list(dataset.variables.keys())}")

        # 基本データの取得
        stations = dataset.variables['station'][:]
        lat = dataset.variables['lat'][:]
        lon = dataset.variables['lon'][:]

        # 観測データの取得
        dd = dataset.variables['dd'][:] # 風向10分平均 (degree)
        dn = dataset.variables['dn'][:] # 風速最小時風向 (degree)
        dx = dataset.variables['dx'][:] # 最大瞬間風速時風向 (degree)
        ffs = dataset.variables['ffs'][:] # 風速10分平均 (m/s)
        fxs = dataset.variables['fxs'][:] # 最大瞬間風速 (m/s)
        Sav1H = dataset.variables['Sav1H'][:] # 1時間平均風速 (m/s)
        Sax1H = dataset.variables['Sax1H'][:] # 1時間最大風速 (m/s)
        Sx1H = dataset.variables['Sx1H'][:] # 1時間最大瞬間風速 (m/s)
        ta = dataset.variables['ta'][:] # 気温10分平均 (degrees Celsius)
        tx = dataset.variables['tx'][:] # 気温10分最大 (degrees Celsius)
        tn = dataset.variables['tn'][:] # 最低気温 (degrees Celsius)
        rh = dataset.variables['rh'][:] # 相対湿度 (%)
        td = dataset.variables['td'][:] # 露点温度 (degrees Celsius)
        p0 = dataset.variables['p0'][:] # 気圧 (hPa)
        vv = dataset.variables['vv'][:] # 視程 (m)
        nc = dataset.variables['nc'][:] # 全雲量 (okta)
        R1H = dataset.variables['R1H'][:] # 1時間降水量 (mm)
        dr = dataset.variables['dr'][:] # 降水時間（Rain Gauge) (sec)
        pr = dataset.variables['pr'][:] # 降水時間　(PWS) (sec)
        rg = dataset.variables['rg'][:] # 降水強度(Rain Gaude) (mm/h)
        pg = dataset.variables['pg'][:] # 降水強度(PWS)(mm/h)
        ww = dataset.variables['ww'][:] # 現在天気

        point_data = []

        for i in range(len(stations)):
            try:
                def check_value(value):
                    return value != -9999.0 and str(value) != '--'

                # 降水強度の処理（rgを優先）
                if 'rg' in dataset.variables and 'pg' in dataset.variables:
                    prcint = dataset.variables['rg'][:][i][0] if check_value(dataset.variables['rg'][:][i][0]) else MISSING_INT16
                elif 'rg' in dataset.variables:
                    prcint = dataset.variables['rg'][:][i][0] if check_value(dataset.variables['rg'][:][i][0]) else MISSING_INT16
                elif 'pg' in dataset.variables:
                    prcint = dataset.variables['pg'][:][i][0] if check_value(dataset.variables['pg'][:][i][0]) else MISSING_INT16
                else:
                    prcint = MISSING_INT16

                # 降水時間の処理（prを優先）
                if 'pr' in dataset.variables and 'dr' in dataset.variables:
                    prcint_sec = dataset.variables['pr'][:][i][0] if check_value(dataset.variables['pr'][:][i][0]) else MISSING_INT32
                elif 'pr' in dataset.variables:
                    prcint_sec = dataset.variables['pr'][:][i][0] if check_value(dataset.variables['pr'][:][i][0]) else MISSING_INT32
                elif 'dr' in dataset.variables:
                    prcint_sec = dataset.variables['dr'][:][i][0] if check_value(dataset.variables['dr'][:][i][0]) else MISSING_INT32
                else:
                    prcint_sec = MISSING_INT32

                station_data = {
                    "LCLID": str(stations[i]).strip(),
                    "ID_GLOBAL_MNET": f"{Provider}_{str(stations[i]).strip()}",
                    
                    # 視程
                    "HVIS": safe_convert_to_int(vv[i][0]),
                    "HVIS_AQC": MISSING_INT8,
                    
                    # 全雲量
                    "AMTCLD_8": safe_convert_to_int(nc[i][0]),
                    "AMTCLD_8_AQC": MISSING_INT8,
                    
                    # 風向・風速
                    "WNDSPD_MD": safe_convert_to_int(ffs[i][0], scale=10),
                    "WNDSPD_MD_AQC": MISSING_INT8,
                    
                    "GUSTS": safe_convert_to_int(fxs[i][0], scale=10),
                    "GUSTS_AQC": MISSING_INT8,
                    
                    "WNDSPD_1HOUR_MAX": safe_convert_to_int(Sax1H[i][0], scale=10),
                    "WNDSPD_1HOUR_MAX_AQC": MISSING_INT8,
                    
                    "WNDSPD_1HOUR_AVG": safe_convert_to_int(Sav1H[i][0], scale=10),
                    "WNDSPD_1HOUR_AVG_AQC": MISSING_INT8,
                    
                    "GUSTS_1HOUR": safe_convert_to_int(Sx1H[i][0], scale=10),
                    "GUSTS_1HOUR_AQC": MISSING_INT8,
                    
                    "WNDDIR_MD": safe_convert_to_int(dd[i][0]),
                    "WNDDIR_MD_AQC": MISSING_INT8,
                    
                    # 気温
                    "AIRTMP_10MIN_MAX": safe_convert_to_int(tx[i][0], scale=10),
                    "AIRTMP_10MIN_MAX_AQC": MISSING_INT8,
                    
                    "AIRTMP": safe_convert_to_int(ta[i][0], scale=10),
                    "AIRTMP_AQC": MISSING_INT8,
                    
                    "AIRTMP_10MIN_MINI": safe_convert_to_int(tn[i][0], scale=10),
                    "AIRTMP_10MIN_MINI_AQC": MISSING_INT8,
                    
                    # 湿度
                    "RHUM": safe_convert_to_int(rh[i][0], scale=10),
                    "RHUM_AQC": MISSING_INT8,
                    
                    # 露点温度
                    "DEWTMP": safe_convert_to_int(td[i][0], scale=10),
                    "DEWTMP_AQC": MISSING_INT8,
                    
                    # 気圧
                    "ARPRSS": safe_convert_to_int(p0[i][0], scale=10),
                    "ARPRSS_AQC": MISSING_INT8,
                    
                    # 降水
                    "PRCINT": safe_convert_to_int(prcint, scale=10),
                    "PRCINT_AQC": MISSING_INT8,
                    
                    # 1時間降水量
                    "PRCRIN_1HOUR": safe_convert_to_int(R1H[i][0], scale=10),
                    "PRCRIN_1HOUR_AQC": MISSING_INT8,
                    
                    # 現在天気
                    "WX_original": get_weather_description(safe_convert_to_int(ww[i][0])),
                    "WX_original_AQC": MISSING_INT8
                }
                
                point_data.append(station_data)
                
            except Exception as e:
                print(f"Error processing station {i}: {e}")
                continue

        # データの検証
        if not validate_data(point_data):
            print("Error: Invalid data format")
            return None

        if announced_dt is None:
            announced_dt = datetime.now(timezone.utc)

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
        print(f"Error converting data: {e}")
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
        processed_results = []
        for key in keys:
            temp_file = None
            try:
                print(f"\nProcessing key: {key}")
                
                # メモリキャッシュをチェック
                cached_data = get_memory_cache(key)
                if cached_data:
                    json_data = cached_data
                    print(f"Using cached data for key: {key}")
                else:
                    # S3からファイルを取得して処理
                    temp_file, announced_dt = extract_netcdf(input_bucket, key)
                    
                    if not temp_file:
                        print(f"Failed to extract valid NetCDF data from key: {key}")
                        processed_results.append({
                            'key': key,
                            'status': 'failed_to_extract',
                            'cached': False
                        })
                        continue

                    try:
                        print(f"Opening temporary file for processing: {temp_file}")
                        dataset = netCDF4.Dataset(temp_file, 'r')
                        json_data = convert_to_json_format(dataset, announced_dt)
                        dataset.close()
                        
                        if json_data:
                            # 処理結果をメモリキャッシュに保存
                            set_memory_cache(key, json_data)
                        else:
                            print(f"Failed to convert NetCDF to JSON for key: {key}")
                            processed_results.append({
                                'key': key,
                                'status': 'failed_to_convert',
                                'cached': False
                            })
                            continue
                            
                    finally:
                        if temp_file and os.path.exists(temp_file):
                            try:
                                os.remove(temp_file)
                                print(f"Temporary file removed: {temp_file}")
                            except Exception as e:
                                print(f"Warning: Failed to remove temporary file: {e}")

                if not json_data:
                    print(f"Failed to process data for key: {key}")
                    processed_results.append({
                        'key': key,
                        'status': 'no_data',
                        'cached': cached_data is not None
                    })
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
                
                if temp_file and os.path.exists(temp_file):
                    try:
                        os.remove(temp_file)
                        print(f"Cleaned up temporary file after error: {temp_file}")
                    except Exception as cleanup_error:
                        print(f"Failed to clean up temporary file: {cleanup_error}")
                
                processed_results.append({
                    'key': key,
                    'status': 'error',
                    'error_message': str(e),
                    'cached': cached_data is not None
                })
                continue

        # キャッシュの状態を含むレスポンスを返す
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processing completed successfully',
                'processed_results': processed_results,
                'cache_statistics': {
                    'total_entries': len(memory_cache),
                    'cache_keys': list(memory_cache.keys())
                },
                'total_processed': len(processed_results),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }, ensure_ascii=False)
        }

    except Exception as e:
        print(f"Fatal error in main function: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'traceback': traceback.format_exc(),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }, ensure_ascii=False)
        }

if __name__ == '__main__':
    main({}, {})
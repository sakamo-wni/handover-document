import os
import boto3
import json
import datetime
import logging
import hashlib
import re
from pathlib import Path

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

# 環境変数
tagid = os.environ.get("tagid", "460320021")
input_bucket = os.environ.get("stock_s3")
metadata_bucket = os.environ.get("md_bucket")

# 定数定義
MISSING_INT8 = -99
MISSING_INT16 = -9999
MISSING_INT32 = -999999999
INVALID_INT16 = -11111
INVALID_INT32 = -1111111111
Provider = "DMC"  

# キャッシュディレクトリ
CACHE_DIR = "/tmp/tmp_DMC/"
os.makedirs(CACHE_DIR, exist_ok=True)

def log_message(message):
    print(message)
    logger.info(message)

def create_cache_key(bucket, key):
    combined = f"{bucket}:{key}"
    # SHA-256ハッシュを使ってファイル名に安全な形式に変換
    hashed = hashlib.sha256(combined.encode()).hexdigest()
    return os.path.join(CACHE_DIR, hashed)

def get_file_cache(bucket, key):
    cache_path = create_cache_key(bucket, key)
    if not os.path.exists(cache_path):
        return None
    
    try:
        with open(cache_path, 'r', encoding='utf-8') as f:
            cache_data = json.load(f)
            log_message(f"キャッシュから前回データを取得")
            return cache_data
    except Exception as e:
        log_message(f"キャッシュ読み込みエラー: {str(e)}")
        try:
            os.remove(cache_path)  # 壊れたキャッシュは削除する!
        except:
            pass
    
    return None

def set_file_cache(bucket, key, data):
    if data is None:
        return False
    
    cache_path = create_cache_key(bucket, key)
    try:
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
        log_message(f"キャッシュに保存しました")
        return True
    except Exception as e:
        log_message(f"キャッシュ保存エラー: {str(e)}")
        try:
            os.remove(cache_path)  # キャッシュが壊れたら削除!
        except:
            pass
        return False

def extract_data_from_s3(bucket, key):
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

def convert_value(value, multiplier=1):
    if value is None:
        return None
    
    # 数値部分を抽出するための正規表現
    match = re.search(r'(-?\d+\.?\d*)', str(value))
    if match:
        try:
            # 浮動小数点数に変換して乗数をかけ、整数化
            return int(float(match.group(1)) * multiplier)
        except (ValueError, TypeError):
            return None
    return None

def convert_knots_to_ms(value):
    # ノットから m/s への変換係数
    KNOTS_TO_MS = 0.51444
    
    raw_value = convert_value(value)
    if raw_value is not None:
        # 10倍スケールで保存するため、変換後に10をかける
        return int(raw_value * KNOTS_TO_MS * 10)
    return None

def is_station_updated(station, cached_data):
    if not cached_data or "datosEstaciones" not in cached_data:
        return True
    
    codigo_nacional = station.get("estacion", {}).get("codigoNacional")
    if not codigo_nacional or not station.get("datos"):
        return False
    
    current_momento = station["datos"][0].get("momento")
    if not current_momento:
        return True
    
    # 前回のデータから同じIDの観測所を探す
    for prev_station in cached_data.get("datosEstaciones", []):
        prev_codigo = prev_station.get("estacion", {}).get("codigoNacional")
        
        if codigo_nacional == prev_codigo and prev_station.get("datos"):
            # タイムスタンプの比較
            prev_momento = prev_station["datos"][0].get("momento")
            
            # タイムスタンプが同じであれば更新されていないとみなす
            if current_momento == prev_momento:
                return False
            
            return True
    
    # 前回のデータに観測所が見つからない場合は更新されているとみなす
    return True

def convert_to_required_format(data, cached_data=None):
    if not data or "datosEstaciones" not in data:
        log_message("有効な気象データがありません")
        return None
    
    updated_stations = []
    unchanged_count = 0
    
    # 現在時刻（アナウンス時間として使用）
    now = datetime.datetime.now(datetime.timezone.utc)
    observation_date = None
    
    # 各観測所のデータを処理
    for station in data.get("datosEstaciones", []):
        # 観測所が更新されているかチェック
        if cached_data and not is_station_updated(station, cached_data):
            unchanged_count += 1
            continue
        
        estacion_info = station.get("estacion", {})
        codigo_nacional = estacion_info.get("codigoNacional")
        
        # 観測所のデータがない場合はスキップ
        if not codigo_nacional or not station.get("datos"):
            continue
        
        # 最新のデータ（リストの先頭のデータ）を取得
        latest_data = station["datos"][0]
        momento = latest_data.get("momento")
        temperatura = latest_data.get("temperatura")
        
        # 観測日時を取得（最初の観測所データから）
        if observation_date is None and momento:
            try:
                obs_dt = datetime.datetime.strptime(momento, "%Y-%m-%d %H:%M:%S")
                
                observation_date = {
                    "year": obs_dt.year,
                    "month": obs_dt.month,
                    "day": obs_dt.day,
                    "hour": obs_dt.hour,
                    "min": obs_dt.minute,
                    "sec": obs_dt.second
                }
            except (ValueError, TypeError):
                # 日付解析に失敗した場合は現在時刻を使用
                observation_date = {
                    "year": now.year,
                    "month": now.month,
                    "day": now.day,
                    "hour": now.hour,
                    "min": now.minute,
                    "sec": now.second
                }
        
        airtmp_value = convert_value(latest_data.get("temperatura02Mts", temperatura), 10)
        dewtmp_value = convert_value(latest_data.get("puntoDeRocio"), 10)
        rhum_value = convert_value(latest_data.get("humedadRelativa"), 10)
        arprss_value = convert_value(latest_data.get("presionEstacion"), 10)
        ssprss_value = convert_value(latest_data.get("presionNivelDelMar"), 10)
        wnddir_value = convert_value(latest_data.get("direccionDelViento"))
        wnddir_10min_avg_value = convert_value(latest_data.get("direccionDelVientoPromedio10Minutos"))
        wndspd_value = convert_knots_to_ms(latest_data.get("fuerzaDelViento"))
        wndspd_10min_max_value = convert_knots_to_ms(latest_data.get("fuerzaDelViento10MinutosMax"))
        wnddir_10min_max_value = convert_value(latest_data.get("direccionDelViento10MinutosMax"))
        
        station_data = {
            "LCLID": codigo_nacional,
            "ID_GLOBAL_MNET": f"{Provider}_{codigo_nacional}",
            
            # 気温（temperatura02Mtsがなければtemperaturaを使用）
            "AIRTMP": airtmp_value if airtmp_value is not None else MISSING_INT16,
            "AIRTMP_AQC": MISSING_INT8,
            
            # 露点温度
            "DEWTMP": dewtmp_value if dewtmp_value is not None else MISSING_INT16,
            "DEWTMP_AQC": MISSING_INT8,
            
            # 相対湿度
            "RHUM": rhum_value if rhum_value is not None else MISSING_INT16,
            "RHUM_AQC": MISSING_INT8,
            
            # 気圧
            "ARPRSS": arprss_value if arprss_value is not None else MISSING_INT16,
            "ARPRSS_AQC": MISSING_INT8,
            
            # 海面気圧
            "SSPRSS": ssprss_value if ssprss_value is not None else MISSING_INT16,
            "SSPRSS_AQC": MISSING_INT8,
            
            # 風向
            "WNDDIR": wnddir_value if wnddir_value is not None else MISSING_INT16,
            "WNDDIR_AQC": MISSING_INT8,
            
            # 10分平均風向
            "WNDDIR_10MIN_AVG": wnddir_10min_avg_value if wnddir_10min_avg_value is not None else MISSING_INT16,
            "WNDDIR_10MIN_AVG_AQC": MISSING_INT8,
            
            # 風速（ノットからm/sに変換）
            "WNDSPD": wndspd_value if wndspd_value is not None else MISSING_INT16,
            "WNDSPD_AQC": MISSING_INT8,
            
            # 10分間最大風速（ノットからm/sに変換）
            "WNDSPD_10MIN_MAX": wndspd_10min_max_value if wndspd_10min_max_value is not None else MISSING_INT16,
            "WNDSPD_10MIN_MAX_AQC": MISSING_INT8,
            
            # 10分間最大風向
            "WNDDIR_10MIN_MAX": wnddir_10min_max_value if wnddir_10min_max_value is not None else MISSING_INT16,
            "WNDDIR_10MIN_MAX_AQC": MISSING_INT8
        }
        
        updated_stations.append(station_data)
    
    if not updated_stations:
        log_message(f"更新された観測所はありません")
        return None
    
    # 観測日時情報がない場合は現在時刻を使用
    if observation_date is None:
        observation_date = {
            "year": now.year,
            "month": now.month,
            "day": now.day,
            "hour": now.hour,
            "min": now.minute,
            "sec": now.second
        }
    
    announced_time = datetime.datetime.now(datetime.timezone.utc)
    result = {
        "tagid": tagid,
        "announced": announced_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "created": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "original": {
            "observation_date": observation_date,
            "point_count": len(updated_stations),
            "point_data": updated_stations
        }
    }
    
    log_message(f"更新した観測所数: {len(updated_stations)}")
    log_message(f"キャッシュ使用観測所数: {unchanged_count}")
    log_message(f"合計観測所数: {len(data.get('datosEstaciones', []))}")
    
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
    """S3ファイルを処理"""
    try:
        # 前回のキャッシュデータを取得
        cached_data = get_file_cache(bucket, key)
        
        # S3からデータを抽出
        data = extract_data_from_s3(bucket, key)
        if not data:
            log_message(f"データ抽出失敗: {bucket}/{key}")
            return None
        
        result = convert_to_required_format(data, cached_data)
        
        # データの更新がない場合
        if not result:
            log_message(f"変更なしでスキップ: {bucket}/{key}")
            return None
        
        set_file_cache(bucket, key, data)
        
        return result
    
    except Exception as e:
        log_message(f"処理エラー: {str(e)}")
        return None

def main(event, context):
    try:
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
            
            import uuid
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
        return {
            'statusCode': 500,
            'body': json.dumps(f'エラー: {str(e)}')
        }

if __name__ == "__main__":
    # ローカルテスト用
    main({"Records": []}, None)
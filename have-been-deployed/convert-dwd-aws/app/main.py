import asyncio
import urllib.request
import zipfile
import io
import csv
import json
from datetime import datetime, timezone
import re
import time
import ssl
import traceback
import boto3
import uuid
import os
import pickle
import hashlib

s3_client_eu = boto3.client("s3", region_name="eu-central-1")
s3_client_jp = boto3.client("s3", region_name="ap-northeast-1")
raw_data_bucket = os.getenv("RawDataBucket")   
converted_bucket = os.getenv("ConvertedBucket")    
tagid = os.getenv("tagid")
base_url = os.getenv("URL")

USE_SMART_CACHE = True 
CACHE_EXPIRY = 7200  

memory_cache = {}
url_timestamps = {}  

TMP_CACHE_DIR = "/tmp/dwdcache"
MAX_TMP_STORAGE = 2000 * 1024 * 1024  

MISSING_VALUES = {
    "INT8": -99,
    "INT16": -9999,
    "INT32": -999999,
    "STR": ""
}

INVALID_VALUES = {
    "INT8": -111,
    "INT16": -11111,
    "INT32": -1111111,
    "STR": ""
}

def get_missing_value(value_type):
    return MISSING_VALUES.get(value_type, None)

def get_invalid_value(value_type):
    return INVALID_VALUES.get(value_type, None)

def get_memory_cache(key):  
    if not USE_SMART_CACHE:
        return None
        
    try:
        if key in memory_cache:
            cache_data = memory_cache[key]
            if datetime.now(timezone.utc).timestamp() - cache_data['timestamp'] < CACHE_EXPIRY:
                return cache_data['data']
            del memory_cache[key]
    except Exception as e:
        print(f"Memory cache access error: {e}")
    return None

def set_memory_cache(key, data):
    if not USE_SMART_CACHE:
        return False
        
    try:
        memory_cache[key] = {
            'timestamp': datetime.now(timezone.utc).timestamp(),
            'data': data
        }
        return True
    except Exception as e:
        print(f"Memory cache save error: {e}")
        return False

def cleanup_memory_cache():
    if not USE_SMART_CACHE:
        memory_cache.clear()
        return
        
    current_time = datetime.now(timezone.utc).timestamp()
    expired_keys = [
        key for key, cache_data in memory_cache.items()
        if current_time - cache_data['timestamp'] > CACHE_EXPIRY
    ]
    for key in expired_keys:
        del memory_cache[key]

def init_file_cache():
    try:
        if not os.path.exists(TMP_CACHE_DIR):
            os.makedirs(TMP_CACHE_DIR)
        return True
    except Exception as e:
        print(f"Error initializing cache directory: {e}")
        return False

def get_file_cache_key(url):
    return hashlib.md5(url.encode('utf-8')).hexdigest()

def check_tmp_storage():
    if not os.path.exists(TMP_CACHE_DIR):
        return 0
        
    total_size = 0
    for dirpath, _, filenames in os.walk(TMP_CACHE_DIR):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.exists(fp):
                total_size += os.path.getsize(fp)
    return total_size

def cleanup_old_cache(required_space=50*1024*1024):
    if not os.path.exists(TMP_CACHE_DIR):
        return True
        
    cache_files = []
    try:
        for filename in os.listdir(TMP_CACHE_DIR):
            if filename.endswith('.data'):  
                filepath = os.path.join(TMP_CACHE_DIR, filename)
                if os.path.exists(filepath):
                    cache_files.append((filepath, os.path.getmtime(filepath)))
    
        cache_files.sort(key=lambda x: x[1])
        
        freed_space = 0
        for filepath, _ in cache_files:
            if freed_space >= required_space or len(cache_files) <= 5:  # 最低5つはキャッシュを残す
                break
                
            if os.path.exists(filepath):
                file_size = os.path.getsize(filepath)
                try:
                    os.remove(filepath)
                    meta_path = filepath.replace('.data', '.meta')
                    if os.path.exists(meta_path):
                        os.remove(meta_path)
                    freed_space += file_size
                    print(f"Removed old cache file: {filepath}, size: {file_size/1024:.2f}KB")
                except Exception as e:
                    print(f"Error removing cache file {filepath}: {e}")
                    
        return True
    except Exception as e:
        print(f"Error cleaning up cache: {e}")
        return False

def get_file_cache(url, cache_expiry=3600):
    if not USE_SMART_CACHE:
        return None
        
    try:
        cache_key = get_file_cache_key(url)
        cache_path = f"{TMP_CACHE_DIR}/cache_{cache_key}.data"
        meta_path = f"{TMP_CACHE_DIR}/cache_{cache_key}.meta"
        
        if os.path.exists(cache_path) and os.path.exists(meta_path):
            with open(meta_path, 'r') as f:
                metadata = json.load(f)
                
            # 更新時刻が記録されている場合、比較して使用するかどうか判断
            if url in url_timestamps:
                cached_update_time = metadata.get('update_time', 0)
                if cached_update_time < url_timestamps[url]:
                    print(f"Cache for {url} is outdated, will fetch new data")
                    try:
                        os.remove(meta_path)
                        os.remove(cache_path)
                    except:
                        pass
                    return None
            
            if time.time() - metadata['timestamp'] < cache_expiry:
                with open(cache_path, 'rb') as f:
                    return pickle.load(f)
            
            try:
                os.remove(meta_path)
                os.remove(cache_path)
            except:
                pass  
                
    except Exception as e:
        print(f"File cache read error for {url}: {e}")
    
    return None

def set_file_cache(url, data, update_time=None):
    if not USE_SMART_CACHE:
        return False
        
    if not init_file_cache():
        return False
        
    try:
        estimated_size = len(pickle.dumps(data))
        
        current_usage = check_tmp_storage()
        
        if current_usage + estimated_size > MAX_TMP_STORAGE:
            cleanup_old_cache(estimated_size)
        
        cache_key = get_file_cache_key(url)
        cache_path = f"{TMP_CACHE_DIR}/cache_{cache_key}.data"
        meta_path = f"{TMP_CACHE_DIR}/cache_{cache_key}.meta"
        
        with open(cache_path, 'wb') as f:
            pickle.dump(data, f)
        
        metadata = {
            'timestamp': time.time(),
            'url': url,
            'size': os.path.getsize(cache_path)
        }
        
        # 更新時刻情報がある場合は保存
        if update_time is not None:
            metadata['update_time'] = update_time
        
        with open(meta_path, 'w') as f:
            json.dump(metadata, f)
        
        return True
    except Exception as e:
        print(f"File cache write error for {url}: {e}")
        return False

def validate_env_vars():
    required_vars = [
        "RawDataBucket",   
        "ConvertedBucket",
        "tagid", 
        "URL"
    ]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

def fetch_first_file_modified(url):

    try:
        cache_key = f"url_modified_{url}"
        
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        
        with urllib.request.urlopen(url, context=ctx) as response:
            content = response.read().decode('utf-8')
            lines = [line.strip() for line in content.split('\n') if line.strip()]
            
            for i, line in enumerate(lines):
                if '10minutenwerte_TU_' in line and '_now.zip' in line:
                    date_parts = line.split()
                    for j in range(len(date_parts)):
                        if re.match(r'\d{2}-[A-Za-z]{3}-\d{4}', date_parts[j]):
                            try:
                                date_str = f"{date_parts[j]} {date_parts[j+1]}"
                                print(f"Found date string: {date_str}")
                                
                                timestamp = datetime.strptime(
                                    date_str, 
                                    '%d-%b-%Y %H:%M'
                                ).replace(tzinfo=timezone.utc)
                                
                                file_size = int(date_parts[j+2])
                                print(f"Successfully parsed - Date: {timestamp}, Size: {file_size}")
                                
                                update_timestamp = timestamp.timestamp()
                                url_timestamps[url] = update_timestamp
                                
                                category = url.split('/')[-3] 
                                category_url = f"{base_url}{category}/now/"
                                url_timestamps[category_url] = update_timestamp
                                
                                result = (timestamp, file_size)
                                
                                if USE_SMART_CACHE:
                                    set_memory_cache(cache_key, result)
                                    set_file_cache(cache_key, result, update_timestamp)
                                
                                return result
                            except (ValueError, IndexError) as e:
                                print(f"Error parsing date parts: {e}")
                                continue

            print("No valid file entries were found after processing all lines.")
            return None, None

    except Exception as e:
        print(f"Error fetching first file modified date for {url}: {e}")
        traceback.print_exc()
        return None, None

def generate_raw_s3_key(tagid, filename):
    return f"{tagid}/{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/{filename}"

def generate_json_s3_key(tagid, filename):
    return f"data/{tagid}/{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/{filename}"

class AsyncHTTPClient:
    def __init__(self, max_connections=150):  
        self.semaphore = asyncio.Semaphore(max_connections)
        self.context = ssl.create_default_context()
        self.context.check_hostname = False
        self.context.verify_mode = ssl.CERT_NONE
        self.retry_count = 5  
        self.retry_delay = 3  

    async def get(self, url, timeout=100):
        if USE_SMART_CACHE:

            cache_key = url
            cached_data = get_file_cache(cache_key)
            if cached_data:
                return cached_data

            cache_key = f"url_{url}"
            cached_data = get_memory_cache(cache_key)
            if cached_data:
                return cached_data

        async with self.semaphore:
            for attempt in range(self.retry_count):
                try:
                    data = await asyncio.wait_for(
                        asyncio.to_thread(
                            self._make_request, url
                        ),
                        timeout=timeout
                    )
                    if data:
                        if USE_SMART_CACHE:
                            update_time = url_timestamps.get(url)
                            set_memory_cache(f"url_{url}", data)
                            set_file_cache(url, data, update_time)
                        return data
                    
                    if attempt < self.retry_count - 1:
                        await asyncio.sleep(self.retry_delay * (attempt + 1))  
                    
                except asyncio.TimeoutError:
                    print(f"Timeout fetching {url} (attempt {attempt+1}/{self.retry_count})")
                    if attempt < self.retry_count - 1:
                        await asyncio.sleep(self.retry_delay * (attempt + 1))
                
                except Exception as e:
                    print(f"Error fetching {url}: {str(e)} (attempt {attempt+1}/{self.retry_count})")
                    if attempt < self.retry_count - 1:
                        await asyncio.sleep(self.retry_delay * (attempt + 1))
            
            return None  

    def _make_request(self, url):
        try:
            with urllib.request.urlopen(
                url, 
                context=self.context, 
                timeout=30
            ) as response:
                return response.read()
        except Exception as e:
            print(f"Request error for {url}: {str(e)}")
            return None

class ZipProcessor:
    def __init__(self):
        self.http_client = AsyncHTTPClient(max_connections=200) 
        self.base_url = base_url
        self.categories = [
            "air_temperature",
            "extreme_temperature",
            "extreme_wind",
            "precipitation",
            "solar",
            "wind"
        ]
        self.processed_count = 0
        self.total_files = 0
        self.station_data = {}
        
        self.element_mapping = {
            'air_temperature': {
                'PP_10': 'ARPRSS',
                'TT_10': 'AIRTMP',
                'RF_10': 'RHUM',
                'TD_10': 'DEWTMP'
            },
            'extreme_temperature': {
                'TX_10': 'AIRTMP_10MIN_MAX',
                'TN_10': 'AIRTMP_10MIN_MINI'
            },
            'wind': {
                'FF_10': 'WNDSPD',
                'DD_10': 'WNDDIR',
                'FX_10': 'GUSTS',
                'FMX_10': 'WNDSPD_10MIN_MAX',
                'DX_10': 'GUSTD'
            },
            'precipitation': {
                'RWS_10': 'PRCRIN_10MIN'
            },
            'solar': {
                'DS_10': 'SCTRAD_10MIN',
                'GS_10': 'GLBRAD_10MIN',
                'SD_10': 'SUNDUR_10MIN',
            }
        }

    def save_to_s3_raw(self, body, key):
        try:
            s3_client_eu.put_object(
                Body=body,
                Bucket=raw_data_bucket,
                Key=key,
                ContentType='text/csv'  
            )
            print(f"Successfully saved raw data to EU S3: {raw_data_bucket}/{key}")
            return True
        except Exception as error:
            print(f"Failed to save raw data to EU S3: {str(error)}")
            return False

    def save_to_s3_converted(self, body, key):
        try:
            s3_client_jp.put_object(
                Body=body,
                Bucket=converted_bucket,
                Key=key,
                ContentType='application/json'
            )
            print(f"Successfully saved converted data to JP S3: {converted_bucket}/{key}")
            return True
        except Exception as error:
            print(f"Failed to save converted data to JP S3: {str(error)}")
            return False
        
    async def check_category_update_time(self, category):
        url = f"{self.base_url}{category}/now/"
        timestamp, _ = fetch_first_file_modified(url)
        if timestamp:
            # 更新時刻をUNIXタイムスタンプで保存
            url_timestamps[url] = timestamp.timestamp()
            return timestamp
        return None
        
    async def get_zip_urls(self, category):
        url = f"{self.base_url}{category}/now/"
        
        if USE_SMART_CACHE:
            cache_key = f"zip_urls_{category}"
            
            # カテゴリの更新時刻がすでに取得されている場合、それを利用
            if url in url_timestamps:
                cached_urls = get_file_cache(cache_key)
                if cached_urls:
                    # メタデータでキャッシュの更新時刻をチェック
                    cache_path = f"{TMP_CACHE_DIR}/cache_{get_file_cache_key(cache_key)}.meta"
                    if os.path.exists(cache_path):
                        with open(cache_path, 'r') as f:
                            metadata = json.load(f)
                            cached_update_time = metadata.get('update_time', 0)
                            if cached_update_time >= url_timestamps[url]:
                                print(f"Using cached ZIP URLs for category {category} (still current)")
                                return cached_urls
                
                cached_urls = get_memory_cache(cache_key)
                if cached_urls:
                    return cached_urls
        
        content = await self.http_client.get(url)
        if not content:
            return []
        
        zip_files = re.findall(r'href="(10minutenwerte_\w+_\d+_now\.zip)"', content.decode('utf-8'))
        result = [(url + zip_file, category) for zip_file in zip_files]
        
        if result and USE_SMART_CACHE:
            update_time = url_timestamps.get(url)
            set_memory_cache(cache_key, result)
            set_file_cache(cache_key, result, update_time)
            
        return result

    def extract_station_id(self, filename):
        match = re.search(r'_(\d+)_now\.zip', filename)
        return match.group(1) if match else None

    def create_json_structure(self, observation_time=None):
        now = datetime.now(timezone.utc)
        if observation_time is None:
            observation_time = now
            
        return {
            "tagid": tagid,
            "announced": observation_time.strftime("%Y-%m-%dT%H:%M:00Z"),
            "created": observation_time.strftime("%Y-%m-%dT%H:%M:00Z"),
            "original": {
                "observation_date": {
                    "year": observation_time.year,
                    "month": observation_time.month,
                    "day": observation_time.day,
                    "hour": observation_time.hour,
                    "min": observation_time.minute,
                    "sec": 0
                },
                "point_count": 0,
                "point_data": []
            }
        }

    @staticmethod
    def create_station_json(station_id):
        return {
            "LCLID": str(station_id),
            "ID_GLOBAL_MNET": f"DWD_{station_id}",
            "ARPRSS": get_missing_value("INT16"),
            "ARPRSS_AQC": get_missing_value("INT8"),
            "AIRTMP": get_missing_value("INT16"),
            "AIRTMP_AQC": get_missing_value("INT8"),
            "AIRTMP_10MIN_MAX": get_missing_value("INT16"),
            "AIRTMP_10MIN_MAX_AQC": get_missing_value("INT8"),
            "AIRTMP_10MIN_MINI": get_missing_value("INT16"),
            "AIRTMP_10MIN_MINI_AQC": get_missing_value("INT8"),
            "RHUM": get_missing_value("INT16"),
            "RHUM_AQC": get_missing_value("INT8"),
            "DEWTMP": get_missing_value("INT16"),
            "DEWTMP_AQC": get_missing_value("INT8"),
            "WNDSPD_10MIN_MAX": get_missing_value("INT16"),
            "WNDSPD_10MIN_MAX_AQC": get_missing_value("INT8"),
            "WNDSPD": get_missing_value("INT16"),
            "WNDSPD_AQC": get_missing_value("INT8"),
            "WNDDIR": get_missing_value("INT16"),
            "WNDDIR_AQC": get_missing_value("INT8"),
            "GUSTS": get_missing_value("INT16"),
            "GUSTS_AQC": get_missing_value("INT8"),
            "GUSTD": get_missing_value("INT16"),
            "GUSTD_AQC": get_missing_value("INT8"),
            "PRCRIN_10MIN": get_missing_value("INT16"),
            "PRCRIN_10MIN_AQC": get_missing_value("INT8"),
            "SCTRAD_10MIN": get_missing_value("INT32"),
            "SCTRAD_10MIN_AQC": get_missing_value("INT8"),
            "GLBRAD_10MIN": get_missing_value("INT32"),
            "GLBRAD_10MIN_AQC": get_missing_value("INT8"),
            "SUNDUR_10MIN": get_missing_value("INT32"),
            "SUNDUR_10MIN_AQC": get_missing_value("INT8"),
        }

    @staticmethod
    def convert_to_float(value):
        if value in ['---', '', '-9999', '-999']:
            return get_invalid_value("INT16")  
        try:
            converted = float(value.replace(',', '.'))
            if converted == -999:  
                return get_invalid_value("INT16")
            return converted
        except ValueError:
            return get_invalid_value("INT16") 

    async def process_single_zip(self, url_info):
        url, category = url_info
        try:
            if USE_SMART_CACHE:
                category_url = f"{self.base_url}{category}/now/"
                zip_update_time = url_timestamps.get(category_url)
                
                if zip_update_time:
                    cache_key = f"processed_data_{url}"
                    processed_data = get_file_cache(cache_key)
                    
                    if processed_data:
                        # キャッシュが最新かどうかを確認
                        cache_path = f"{TMP_CACHE_DIR}/cache_{get_file_cache_key(cache_key)}.meta"
                        if os.path.exists(cache_path):
                            with open(cache_path, 'r') as f:
                                metadata = json.load(f)
                                cached_update_time = metadata.get('update_time', 0)
                                if cached_update_time >= zip_update_time:
                                    # キャッシュが最新
                                    station_id, station_data = processed_data
                                    if station_id:
                                        if station_id not in self.station_data:
                                            self.station_data[station_id] = {}
                                        self.station_data[station_id][category] = station_data
                                        self.processed_count += 1
                                        if self.processed_count % 100 == 0:
                                            print(f"Processed {self.processed_count}/{self.total_files} files (from cache)")
                                        return station_id
                    
                    processed_data = get_memory_cache(cache_key)
                    if processed_data:
                        station_id, station_data = processed_data
                        if station_id:
                            if station_id not in self.station_data:
                                self.station_data[station_id] = {}
                            self.station_data[station_id][category] = station_data
                            self.processed_count += 1
                            if self.processed_count % 100 == 0:
                                print(f"Processed {self.processed_count}/{self.total_files} files (from memory)")
                            return station_id

            # キャッシュになければ、または古ければ新しいデータを取得
            content = await self.http_client.get(url)
            
            if not content:
                return None

            station_id = self.extract_station_id(url)
            if not station_id:
                return None

            with zipfile.ZipFile(io.BytesIO(content)) as z:
                txt_file = [f for f in z.namelist() if f.endswith('.txt')][0]
                
                with z.open(txt_file) as f:
                    text_content = io.TextIOWrapper(f, encoding='utf-8')
                    csv_reader = csv.reader(text_content, delimiter=';')
                    rows = list(csv_reader)
                    
                    if len(rows) > 1:
                        headers = rows[0]
                        data_rows = rows[1:]
                        if data_rows:
                            latest_row = max(data_rows, key=lambda x: datetime.strptime(x[1], '%Y%m%d%H%M'))
                            
                            station_data = {
                                'headers': headers,
                                'data': latest_row
                            }
                            
                            if station_id not in self.station_data:
                                self.station_data[station_id] = {}
                            self.station_data[station_id][category] = station_data
                            
                            if USE_SMART_CACHE:
                                cache_key = f"processed_data_{url}"
                                category_url = f"{self.base_url}{category}/now/"
                                update_time = url_timestamps.get(category_url)
                                set_memory_cache(cache_key, (station_id, station_data))
                                set_file_cache(cache_key, (station_id, station_data), update_time)
                            
                            self.processed_count += 1
                            if self.processed_count % 100 == 0:
                                print(f"Processed {self.processed_count}/{self.total_files} files (new data)")
                                
                            return station_id

        except Exception as e:
            print(f"Error processing {url}: {str(e)}")
            return None

    async def process_batch(self, batch_urls):
        tasks = [self.process_single_zip(url_info) for url_info in batch_urls]
        return await asyncio.gather(*tasks)

    async def process_all_categories(self):
        try:
            validate_env_vars()
            cleanup_memory_cache()
            init_file_cache()

            # まず各カテゴリの更新時刻を確認
            update_tasks = [self.check_category_update_time(category) for category in self.categories]
            update_times = await asyncio.gather(*update_tasks)
            
            # 全カテゴリで最新の更新時刻を使用
            valid_times = [t for t in update_times if t is not None]
            if valid_times:
                latest_observation_time = max(valid_times)
                print(f"Latest observation time from all categories: {latest_observation_time}")
            else:
                print("Could not determine latest observation time from URLs")
                latest_observation_time = datetime.now(timezone.utc)

            # 並列でカテゴリごとのURLを取得
            category_tasks = [self.get_zip_urls(category) for category in self.categories]
            category_urls = await asyncio.gather(*category_tasks)
            
            all_urls = []
            for urls in category_urls:
                all_urls.extend(urls)

            self.total_files = len(all_urls)
            print(f"Found {self.total_files} total files to process")

            if not all_urls:
                print("No URLs found to process")
                return

            batch_size = 200  
            batch_tasks = []
            for i in range(0, len(all_urls), batch_size):
                batch_urls = all_urls[i:i + batch_size]
                batch_tasks.append(self.process_batch(batch_urls))
            
            # すべてのバッチを並列実行
            await asyncio.gather(*batch_tasks)

            if self.station_data:
                # S3への保存を準備
                text_content = ""
                for category in self.categories:
                    text_content += f"\n=== {category.upper()} ===\n"
                    category_stations = {
                        station_id: data[category]
                        for station_id, data in self.station_data.items()
                        if category in data
                    }
                    
                    if category_stations:
                        first_station = next(iter(category_stations.values()))
                        text_content += ';'.join(first_station['headers']) + '\n'
                        
                        # ソート処理を最適化
                        sorted_station_ids = sorted(category_stations.keys(), key=lambda x: int(x))
                        for station_id in sorted_station_ids:
                            text_content += ';'.join(category_stations[station_id]['data']) + '\n'
                    else:
                        text_content += "No data available for this category\n"

                output_file_name = datetime.now(timezone.utc).strftime('%Y%m%d%H%M')  
                raw_s3_key = generate_raw_s3_key(tagid, output_file_name)
                
                # JSON生成を高速化
                json_structure = self.create_json_structure(latest_observation_time)
                json_structure["original"]["point_data"] = []  
                
                # ステーションデータ処理を最適化
                for station_id, station_data in self.station_data.items():
                    station_json = ZipProcessor.create_station_json(station_id)
                    
                    # マッピング処理の最適化
                    for category, mappings in self.element_mapping.items():
                        if category in station_data:
                            headers = station_data[category]['headers']
                            values = station_data[category]['data']
                            
                            # ヘッダーインデックスをキャッシュして繰り返し検索を回避
                            header_indices = {}
                            for source_field in mappings.keys():
                                try:
                                    header_indices[source_field] = headers.index(source_field)
                                except ValueError:
                                    header_indices[source_field] = -1
                            
                            for source_field, target_field in mappings.items():
                                idx = header_indices.get(source_field, -1)
                                if idx >= 0:
                                    try:
                                        value = self.convert_to_float(values[idx])

                                        if value != get_missing_value("INT16") and value != get_invalid_value("INT16"):  
                                            if target_field in ['AIRTMP', 'SFCTMP', 'DEWTMP', 'AIRTMP_10MIN_MAX', 'AIRTMP_10MIN_MINI']:
                                                value = int(value * 10)
                                            elif target_field == 'PRCRIN_10MIN':
                                                value = int(round(value * 10))
                                            elif target_field in ['SCTRAD_10MIN', 'GLBRAD_10MIN']:
                                                value = int(round(value * 10000))
                                            elif target_field == 'SUNDUR_10MIN':
                                                value = int(round(value * 60))
                                            elif target_field in ['WNDSPD', 'ARPRSS', 'RHUM', 'GUSTS', 'WNDSPD_10MIN_MAX', 'GUSTD']:
                                                value = int(round(value * 10)) 
                                            elif target_field == 'WNDDIR':
                                                value = int(round(value))
                                                                        
                                            station_json[target_field] = value
                                        else:
                                            station_json[target_field] = get_invalid_value("INT16") 
                                    except (ValueError, IndexError):
                                        station_json[target_field] = get_invalid_value("INT16") 

                    json_structure["original"]["point_data"].append(station_json)

                json_structure["original"]["point_count"] = len(json_structure["original"]["point_data"])
                
                current_time = datetime.now(timezone.utc)
                random_suffix = str(uuid.uuid4())
                json_file_name = f"{current_time.strftime('%Y%m%d%H%M%S')}.{random_suffix}"
                conv_s3_key = generate_json_s3_key(tagid, json_file_name)
                
                raw_bytes = text_content.encode('utf-8')
                json_bytes = json.dumps(
                    json_structure, 
                    ensure_ascii=False, 
                    indent=2  
                ).encode('utf-8')                

                aws_tasks = [
                    asyncio.to_thread(self.save_to_s3_raw, raw_bytes, raw_s3_key),
                    asyncio.to_thread(self.save_to_s3_converted, json_bytes, conv_s3_key)
                ]
                await asyncio.gather(*aws_tasks)

                print("\nData Summary:")
                for category in self.categories:
                    category_count = sum(1 for data in self.station_data.values() if category in data)
                    print(f"{category.upper()}: {category_count} stations")
                
                if latest_observation_time:
                    print(f"Latest observation time: {latest_observation_time.strftime('%Y-%m-%d %H:%M UTC')}")

                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'raw_key': raw_s3_key,
                        'converted_key': conv_s3_key,
                        'observation_time': latest_observation_time.isoformat() if latest_observation_time else None,
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    })
                }

        except Exception as e:
            print(f"Error in process_all_categories: {str(e)}")
            traceback.print_exc()
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': str(e),
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })
            }

def main(event=None, context=None):
    try:
        validate_env_vars()
        processor = ZipProcessor()
        return asyncio.run(processor.process_all_categories())
    except Exception as e:
        print(f"Lambda handler error: {str(e)}")
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
        }

if __name__ == "__main__":
    main({}, {})
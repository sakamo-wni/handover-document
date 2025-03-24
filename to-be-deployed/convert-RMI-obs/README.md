# RMI 気象データ処理 Lambda

## 概要
このLambda関数は、ベルギー王立気象研究所（RMI: Royal Meteorological Institute of Belgium）の気象観測データを処理します。S3からSQSを通じて受信したGeoJSONデータを解析し、標準化されたJSONフォーマットに変換してS3に保存します。S3ベースのキャッシュシステムを使用して、変更のあったデータのみを処理する差分処理を実装しています。

## 技術仕様
- **ランタイム**: Python 3.12
- **実行環境**: AWS Lambda
- **入力ソース**: SQS (S3からのデータを含む)
- **出力先**: S3
- **入力フォーマット**: GeoJSON (RUヘッダー付き)
- **出力フォーマット**: 標準化JSON
- **キャッシュ**: S3バケット内の専用ディレクトリを使用

## データマッピング
プログラムは以下のようにGeoJSONのプロパティを標準化JSONフィールドにマッピングします：

| 標準化JSON要素 | GeoJSONプロパティ | 単位 | 変換処理 |
|--------------|----------------|----------------|----------------|
| LCLID | code | - | 文字列として使用 |
| ID_GLOBAL_MNET | `RMI_{code}` | - | プロバイダ名と観測局IDを連結 |
| WNDSPD | wind_speed_10m | メートル/秒 [m/s] | 10倍して整数化（精度向上のため） |
| WNDSPD_AQC | - | - | -99に設定 |
| GUSTS | wind_gusts_speed | メートル/秒 [m/s] | 10倍して整数化（精度向上のため） |
| GUSTS_AQC | - | - | -99に設定 |
| AIRTMP_1HOUR_AVG | temp_dry_shelter_avg | 摂氏 [°C] | 10倍して整数化（精度向上のため） |
| AIRTMP_1HOUR_AVG_AQC | - | - | -99に設定 |
| RHUM_1HOUR_AVG | humidity_rel_shelter_avg | パーセント [%] | 10倍して整数化（精度向上のため） |
| RHUM_1HOUR_AQC | - | - | -99に設定 |
| ARPRSS_1HOUR_AVG | pressure | ヘクトパスカル [hPa] | 10倍して整数化（精度向上のため） |
| ARPRSS_1HOUR_AVG_AQC | - | - | -99に設定 |
| PRCRIN_1HOUR | precip_quantity | ミリメートル [mm] | 10倍して整数化（精度向上のため） |
| PRCRIN_1HOUR_AQC | - | - | -99に設定 |

## 処理フロー
1. 環境変数の検証
2. SQSイベントからS3オブジェクトキーを抽出
3. S3からデータを取得し、RUヘッダーを削除
4. GeoJSONデータを解析
5. 各観測地点の差分検出：
   - 各地点のデータを前回のキャッシュと比較
   - 変更のないデータはスキップ
6. データを標準フォーマットに変換：
   - 数値の10倍処理と整数化
   - 観測日時情報の抽出
7. 変換されたデータをS3に保存
8. 各地点のキャッシュを更新（S3に保存）

## 特記事項
- タグIDは環境変数から設定（デフォルト: 460220001）
- S3ベースのキャッシュシステムを使用：
  - 各観測地点ごとに個別のキャッシュファイルを作成
  - キャッシュキーは `{CACHE_PREFIX}station_{station_id}.json` 形式
  - デフォルトキャッシュプレフィックスは `tmp_RMI/`
- 差分検出は、地点ごとのプロパティとジオメトリの完全一致で判定
- タイムスタンプは最初の特徴点から抽出（なければ現在時刻を使用）
- 詳細なログ出力により処理状況を追跡可能

## S3保存パス
### 標準化JSON
```
data/{tagid}/{YYYY}/{MM}/{DD}/{YYYYMMDDHHmmSS}.{uuid}
```

### キャッシュファイル
```
{cache_bucket}/{CACHE_PREFIX}station_{station_id}.json
```

## 入力GeoJSON形式の例
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [longitude, latitude]
      },
      "properties": {
        "code": "station_id",
        "timestamp": "2023-01-01T12:00:00Z",
        "wind_speed_10m": 5.2,
        "wind_gusts_speed": 8.4,
        "temp_dry_shelter_avg": 22.5,
        "humidity_rel_shelter_avg": 65.0,
        "pressure": 1013.2,
        "precip_quantity": 2.5
      }
    },
    ...
  ]
}
```

## 出力JSON形式の例
```json
{
  "tagid": "460220001",
  "announced": "2023-01-01T12:00:00Z",
  "created": "2023-01-01T12:05:00Z",
  "original": {
    "observation_date": {
      "year": 2023,
      "month": 1,
      "day": 1,
      "hour": 12,
      "min": 0,
      "sec": 0
    },
    "point_count": 1,
    "point_data": [
      {
        "LCLID": "station_id",
        "ID_GLOBAL_MNET": "RMI_station_id",
        "WNDSPD": 52,
        "WNDSPD_AQC": -99,
        "GUSTS": 84,
        "GUSTS_AQC": -99,
        "AIRTMP_1HOUR_AVG": 225,
        "AIRTMP_1HOUR_AVG_AQC": -99,
        "RHUM_1HOUR_AVG": 650,
        "RHUM_1HOUR_AQC": -99,
        "ARPRSS_1HOUR_AVG": 10132,
        "ARPRSS_1HOUR_AVG_AQC": -99,
        "PRCRIN_1HOUR": 25,
        "PRCRIN_1HOUR_AQC": -99
      }
    ]
  }
}
```

## ログと統計
プログラムは処理の各段階での詳細なログを出力します：
- 更新された観測地点数
- キャッシュを使って処理をスキップした地点数
- 合計地点数
- キャッシュのETagと保存サイズ
- 処理エラーの詳細

## 依存関係
- AWS SDK for Python (Boto3) - S3アクセス用
- botocore - AWS例外処理用
- json - JSONデータの解析と生成用
- datetime - 日時処理用
- logging - ログ出力用
- hashlib - キャッシュキー生成補助用
- pathlib - ファイルパス処理用
- uuid - ユニークID生成用

## 環境変数
- **tagid**: データの識別子（デフォルト: 460220001）
- **stock_s3**: 入力データが格納されているS3バケット
- **md_bucket**: 変換されたJSONデータを保存するS3バケット
- **cache_bucket**: キャッシュを保存するS3バケット（デフォルト: md_bucket）

## キャッシュ検証と信頼性
- キャッシュ書き込み後に即時検証を実施
- 詳細なエラーハンドリングとログ出力により問題を特定しやすい設計
- バケット間のデータ一貫性を確保するための工夫

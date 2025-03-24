# EMHI XMLデータ処理 Lambda

## 概要
このLambda関数は、エストニア気象局（EMHI）の観測生データ（XML形式）を処理します。指定URLからデータをダウンロードし、RUヘッダーを追加してS3に保存した後、JSONフォーマットに変換してS3に保存します。観測データと観測局データの両方を処理する機能を持っています。

## PSR 資料
このプログラムのPSR資料については以下のURLを参照してください：
[EMHI データ処理 PSR 資料](https://docs.google.com/spreadsheets/d/1nEc6D-uyFETIsW0lhUOOHOifWAX43brOT5hSyBWQ-ys/edit?gid=492387665#gid=492387665)


## 技術仕様
- **ランタイム**: Python 3.12
- **実行環境**: AWS Lambda
- **入力ソース**: エストニア気象局のWebサービス（URL）
- **出力先**: 
  - 生データ: EU リージョンのS3バケット
  - 変換データ: 日本リージョンのS3バケット
- **入力フォーマット**: XML
- **出力フォーマット**: 
  - 生データ: RUヘッダー付きXML
  - 変換データ: JSON

## データマッピング
プログラムは以下のようにXMLフィールドをJSON要素にマッピングします：

| JSON要素 | XML要素 | 単位 | 変換処理 |
|--------------|----------------|----------------|----------------|
| LCLID | name | - | そのまま使用 |
| ID_GLOBAL_MNET | `EMHI_{name}` | - | プロバイダ名とステーション名を連結 |
| HVIS | visibility | メートル [m] | km値を1000倍してメートルに変換 |
| HVIS_AQC | - | - | -99 (MISSING_INT8) に設定 |
| AIRTMP | airtemperature | 摂氏 [°C] | 10倍して整数化（精度向上のため） |
| AIRTMP_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDDIR | winddirection | 度 [°] | そのまま整数化 |
| WNDDIR_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDSPD | windspeed | メートル/秒 [m/s] | 10倍して整数化（精度向上のため） |
| WNDSPD_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDSPD_10MIN_MAX | windspeedmax | メートル/秒 [m/s] | 10倍して整数化（精度向上のため） |
| WNDSPD_10MIN_MAX_AQC | - | - | -99 (MISSING_INT8) に設定 |
| PRCRIN_10MIN | precipitations | ミリメートル [mm] | 10倍して整数化（精度向上のため） |
| PRCRIN_10MIN_AQC | - | - | -99 (MISSING_INT8) に設定 |
| SUNDUR_10MIN | sunshineduration | 分 [min] | そのまま整数化 |
| SUNDUR_10MIN_AQC | - | - | -99 (MISSING_INT8) に設定 |
| ARPRSS | airpressure | ヘクトパスカル [hPa] | 10倍して整数化（精度向上のため） |
| ARPRSS_AQC | - | - | -99 (MISSING_INT8) に設定 |
| RHUM | relativehumidity | パーセント [%] | 10倍して整数化（精度向上のため）。99%の場合は無効値に設定 |
| RHUM_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WX_original | phenomenon | テキスト | そのまま使用（エストニア語の天気表現） |
| WX_original_AQC | - | - | -99 (MISSING_INT8) に設定 |

## 処理フロー
1. 環境変数の検証
2. 指定URLからXMLデータをダウンロード
3. XMLデータからタイムスタンプを抽出
4. 観測データの処理:
   - RUヘッダーの生成と追加
   - 生データをEUリージョンのS3バケットに保存
   - XMLデータを解析してJSONに変換
   - JSONデータを日本リージョンのS3バケットに保存
5. 観測局データの処理（トリガーに応じて）:
   - XMLデータからGeoJSON形式に変換
   - 観測局データを日本リージョンのS3バケットに保存

## 特記事項
- タグIDは環境変数から設定
- 観測値の時刻は10分間隔に正規化（例：12:08 → 12:00）
- 欠損値は専用の定数で処理：
  - MISSING_INT8: -99（8ビット整数での欠損値）
  - MISSING_INT16: -9999（16ビット整数での欠損値）
  - MISSING_INT32: -999999（32ビット整数での欠損値）
- 無効値も定数で定義：
  - INVALID_INT16: -11111（16ビット整数での無効値）
  - INVALID_INT32: -1111111（32ビット整数での無効値）
- 浮動小数点の精度を保持するため、多くの数値データは10倍されて整数として格納
- 湿度（RHUM）が99%の場合、無効値として処理
- 視程（HVIS）はキロメートル単位からメートル単位に変換（1000倍）
- RUヘッダー情報:
  - データ名: EMHI_OBS_TABLE_AWS_raw
  - データID: 0200600041000140

## トリガー種別による処理の違い
- **StationRule**: 観測データと観測局データの両方を処理
- **ObservationRule**: 観測データのみを処理
- トリガー情報がない場合: 両方のデータを処理（デフォルト）

## S3保存パス
### 生データ（EUリージョン）
```
{tagid}/{YYYY}/{MM}/{DD}/{YYYYMMDDHHmm}
```

### 観測データJSON（日本リージョン）
```
data/{tagid}/{YYYY}/{MM}/{DD}/{YYYYMMDDHHmmSS}.{uuid}
```

### 観測局データJSON（日本リージョン）
```
metadata/spool/EMHI/metadata.json
```

## 依存関係
- AWS SDK for Python (Boto3) - S3アクセス用
- ElementTree - XMLデータ処理用
- urllib.request - ウェブリクエスト用
- json - JSONデータの解析と生成用
- os - 環境変数アクセス用
- datetime - 日時処理用
- uuid - ユニークIDの生成用

## 環境変数
- **RawDataBucket**: 生データを保存するS3バケット（EUリージョン）
- **ConvertedBucket**: 変換済みデータを保存するS3バケット（日本リージョン）
- **tagid**: データの識別子
- **URL**: データを取得するエストニア気象局のURL
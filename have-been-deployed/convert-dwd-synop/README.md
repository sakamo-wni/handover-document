# DWD BUFR データ処理 Lambda

## 概要
このLambda関数は、ドイツ気象局（DWD: Deutscher Wetterdienst）のBUFR形式の気象観測データを処理します。指定URLからbz2圧縮されたデータをダウンロードし、解凍後にS3に保存します。また、BUFRデータを解析して標準化されたJSONフォーマットに変換し、観測データと観測局データとしてS3に保存します。

## PSR 資料
このプログラムのPSR資料（問題特定書）については以下のURLを参照してください：
[DWD データ処理 PSR 資料](https://docs.google.com/spreadsheets/d/1IVaVNpurbhv4xuKcWMTeeL4phHvMaMcPyfBRbq16UDU/edit?gid=1246498904#gid=1246498904)


## 技術仕様
- **ランタイム**: Python 3.12
- **実行環境**: AWS Lambda
- **入力ソース**: DWD のbz2圧縮されたBUFRデータ（URL）
- **出力先**: S3バケット
- **入力フォーマット**: JSON（Binary Universal Form for the Representation of meteorological data）
- **出力フォーマット**: 
  - 生データ: JSON（解凍されたBUFRデータ）
  - 変換データ: 標準化JSON（観測データ）
  - 観測局データ: GeoJSON

## データマッピング
プログラムは以下のようにBUFRデータをJSON要素にマッピングします：

| JSON要素 | BUFR要素 | 単位 | 変換処理 |
|--------------|----------------|----------------|----------------|
| LCLID | stationOrSiteName | - | そのまま使用 |
| ID_GLOBAL_MNET | `DWD_{stationOrSiteName}` | - | プロバイダ名とステーション名を連結 |
| AIRTMP | airTemperature | 摂氏 [°C] | ケルビンから摂氏に変換して10倍 |
| AIRTMP_AQC | - | - | -99 (MISSING_INT8) に設定 |
| PRCRIN_10MIN | totalPrecipitationOrTotalWaterEquivalent | ミリメートル [mm] | 時間帯に応じて抽出（10分間） |
| PRCRIN_10MIN_AQC | - | - | -99 (MISSING_INT8) に設定 |
| PRCRIN_1HOUR | totalPrecipitationOrTotalWaterEquivalent | ミリメートル [mm] | 時間帯に応じて抽出（1時間） |
| PRCRIN_1HOUR_AQC | - | - | -99 (MISSING_INT8) に設定 |
| PRCRIN_6HOUR | totalPrecipitationOrTotalWaterEquivalent | ミリメートル [mm] | 時間帯に応じて抽出（6時間） |
| PRCRIN_6HOUR_AQC | - | - | -99 (MISSING_INT8) に設定 |
| PRCRIN_12HOUR | totalPrecipitationOrTotalWaterEquivalent | ミリメートル [mm] | 時間帯に応じて抽出（12時間） |
| PRCRIN_12HOUR_AQC | - | - | -99 (MISSING_INT8) に設定 |
| PRCRIN_24HOUR | totalPrecipitationOrTotalWaterEquivalent | ミリメートル [mm] | 時間帯に応じて抽出（24時間） |
| PRCRIN_24HOUR_AQC | - | - | -99 (MISSING_INT8) に設定 |
| SNWDPT | totalSnowDepth | センチメートル [cm] | メートルからセンチメートルに変換 |
| SNWDPT_AQC | - | - | -99 (MISSING_INT8) に設定 |
| AMTCLD | cloudCoverTotal | パーセント [%] | そのまま整数化 |
| AMTCLD_AQC | - | - | -99 (MISSING_INT8) に設定 |
| HVIS | horizontalVisibility | メートル [m] | そのまま整数化 |
| HVIS_AQC | - | - | -99 (MISSING_INT8) に設定 |
| GUSTD | maximumWindGustDirection | 度 [°] | そのまま整数化 |
| GUSTD_AQC | - | - | -99 (MISSING_INT8) に設定 |
| GUSTS | maximumWindGustSpeed | メートル/秒 [m/s] | 10倍して整数化 |
| GUSTS_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDSPD_10MIN_AVG | maximumWindSpeed10MinuteMeanWind | メートル/秒 [m/s] | 10倍して整数化 |
| WNDSPD_10MIN_AVG_AQC | - | - | -99 (MISSING_INT8) に設定 |
| AIRTMP_1HOUR_MINI | minimumTemperatureAt2M | 摂氏 [°C] | ケルビンから摂氏に変換して10倍 |
| AIRTMP_1HOUR_MINI_AQC | - | - | -99 (MISSING_INT8) に設定 |
| AIRTMP_1HOUR_MAX | maximumTemperatureAt2M | 摂氏 [°C] | ケルビンから摂氏に変換して10倍 |
| AIRTMP_1HOUR_MAX_AQC | - | - | -99 (MISSING_INT8) に設定 |
| DEWTMP | dewpointTemperature | 摂氏 [°C] | ケルビンから摂氏に変換して10倍 |
| DEWTMP_AQC | - | - | -99 (MISSING_INT8) に設定 |
| RHUM | relativeHumidity | パーセント [%] | 10倍して整数化 |
| RHUM_AQC | - | - | -99 (MISSING_INT8) に設定 |
| ARPRSS | nonCoordinatePressure | ヘクトパスカル [hPa] | パスカルからヘクトパスカルに変換して10倍 |
| ARPRSS_AQC | - | - | -99 (MISSING_INT8) に設定 |
| SSPRSS | pressureReducedToMeanSeaLevel | ヘクトパスカル [hPa] | パスカルからヘクトパスカルに変換して10倍 |
| SSPRSS_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDSPD | windSpeed | メートル/秒 [m/s] | 10倍して整数化 |
| WNDSPD_AQC | - | - | -99 (MISSING_INT8) に設定 |
| GLBRAD_1HOUR | globalSolarRadiationIntegratedOverPeriodSpecified | ジュール/平方メートル [J/m²] | そのまま整数化 |
| GLBRAD_1HOUR_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WX_original | presentWeather | テキスト | 気象コードから説明文に変換 |
| WX_original_AQC | - | - | -99 (MISSING_INT8) に設定 |

## 処理フロー
1. 環境変数の検証
2. 指定URLからbz2圧縮されたBUFRデータをダウンロード
3. データを解凍してメモリ上で処理
4. 生データ（解凍されたBUFRデータ）をS3に保存
5. BUFRデータの解析と変換：
   - 各観測所のデータを抽出
   - 各パラメータの値を標準形式に変換
   - 複数の時間帯の降水量データを適切に処理
   - 気温データを2mの高さのものを優先的に使用
   - データの重複がある場合は最新の値を使用
6. 変換されたデータをJSON形式でS3に保存
7. 観測局データをGeoJSON形式で抽出し、S3に保存
8. 処理結果の統計情報を返却

## 特記事項
- タグIDは環境変数から設定
- 欠損値は専用の定数で処理：
  - MISSING_INT8: -99（8ビット整数での欠損値）
  - MISSING_INT16: -9999（16ビット整数での欠損値）
  - MISSING_INT32: -999999（32ビット整数での欠損値）
- 無効値も定数で定義：
  - INVALID_INT16: -11111（16ビット整数での無効値）
  - INVALID_INT32: -1111111（32ビット整数での無効値）
- BUFRデータには複数の時間帯の降水量データが含まれる場合があり、それぞれ別々に処理
  - 10分間降水量: 直接値または1分値の集計から取得
  - 1時間降水量: 1時間の集計値
  - 6時間降水量: 6時間の集計値
  - 12時間降水量: 12時間の集計値
  - 24時間降水量: 24時間の集計値
- 気温データは2mの高さで測定されたものを優先的に使用（見つからない場合は一般的な気温データを使用）
- 現在天気（WX_original）はCSVファイル（DwdPresentWeather.csv）から読み込んだコード定義を使用
  - CSVファイルが存在しない場合は空の文字列を返す

## トリガー種別による処理の違い
- **StationRule**: 観測データと観測局データの両方を処理
- **ObservationRule**: 観測データのみを処理
- トリガー情報がない場合: 両方のデータを処理（デフォルト）

## S3保存パス
### 生データ
```
{tagid}/{YYYY}/{MM}/{DD}/{YYYYMMDDHHmmSS}_raw.json
```

### 観測データJSON
```
data/{tagid}/{YYYY}/{MM}/{DD}/{YYYYMMDDHHmmSS}.{uuid}
```

### 観測局データGeoJSON
```
metadata/spool/DWD_SYNOP/metadata.json
```

## 降水量データの集計方法
プログラムは降水量データを時間帯別に集計し、その結果をログに出力します：
```
[PRECIP LOG] 10分間降水量(直接): {n}件
[PRECIP LOG] 1分値*10データから合計した10分間降水量: {n}件
[PRECIP LOG] 1時間降水量: {n}件
[PRECIP LOG] 6時間降水量: {n}件
[PRECIP LOG] 12時間降水量: {n}件
[PRECIP LOG] 24時間降水量: {n}件
[PRECIP LOG] その他の時間降水量: {n}件
```

## 単位変換
- **気温**: ケルビン [K] → 摂氏 [°C] × 10
  - 例: 283.15K → 10.0°C → 100（整数値）
- **気圧**: パスカル [Pa] → ヘクトパスカル [hPa] × 10
  - 例: 101325Pa → 1013.25hPa → 10132（整数値）
- **雪の深さ**: メートル [m] → センチメートル [cm]
  - 例: 0.35m → 35cm

## 依存関係
- AWS SDK for Python (Boto3) - S3アクセス用
- bz2 - データ解凍用
- urllib.request - ウェブリクエスト用
- json - JSONデータの解析と生成用
- csv - 天気コード定義ファイル読み込み用
- os - 環境変数アクセス用
- datetime - 日時処理用
- uuid - ユニークIDの生成用

## 環境変数
- **RawDataBucket**: 生データを保存するS3バケット
- **ConvertedBucket**: 変換済みデータを保存するS3バケット
- **tagid**: データの識別子
- **URL**: データを取得するDWDのURL
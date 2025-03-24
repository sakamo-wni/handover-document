# DMI APIデータ処理 Lambda

## 概要
このLambda関数は、デンマーク気象局（DMI: Danish Meteorological Institute）のAPI経由で取得した観測データを処理します。APIからJSON形式で取得したデータにRUヘッダーを追加してS3に保存した後、標準化されたJSONフォーマットに変換して別のS3バケットに保存します。

## 技術仕様
- **ランタイム**: Python 3.12
- **実行環境**: AWS Lambda
- **入力ソース**: DMI の Web API
- **出力先**: 
  - 生データ: EU リージョンのS3バケット
  - 変換データ: 日本リージョンのS3バケット
- **入力フォーマット**: GeoJSON (API レスポンス)
- **出力フォーマット**: 
  - 生データ: RUヘッダー付き GeoJSON
  - 変換データ: 標準化JSON

## データマッピング
プログラムは以下のようにAPIパラメータをJSON要素にマッピングします：

| JSON要素 | APIパラメータID | 単位 | 変換処理 |
|--------------|----------------|----------------|----------------|
| LCLID | stationId | - | そのまま使用 |
| ID_GLOBAL_MNET | `DMI_{stationId}` | - | プロバイダ名とステーションIDを連結 |
| HVIS | visibility | メートル [m] | そのまま整数化 |
| HVIS_AQC | - | - | -99 (MISSING_INT8) に設定 |
| AMTCLD_8 | cloud_cover | オクタ [okta] | パーセント値をオクタスケールにマッピング |
| AMTCLD_8_AQC | - | - | -99 (MISSING_INT8) に設定 |
| AIRTMP | temp_dry | 摂氏 [°C] | 10倍して整数化（精度向上のため） |
| AIRTMP_AQC | - | - | -99 (MISSING_INT8) に設定 |
| AIRTMP_1HOUR_MAX | temp_max_past1h | 摂氏 [°C] | 10倍して整数化（精度向上のため） |
| AIRTMP_1HOUR_MAX_AQC | - | - | -99 (MISSING_INT8) に設定 |
| AIRTMP_1HOUR_AVG | temp_mean_past1h | 摂氏 [°C] | 10倍して整数化（精度向上のため） |
| AIRTMP_1HOUR_AVG_AQC | - | - | -99 (MISSING_INT8) に設定 |
| AIRTMP_1HOUR_MINI | temp_min_past1h | 摂氏 [°C] | 10倍して整数化（精度向上のため） |
| AIRTMP_1HOUR_MINI_AQC | - | - | -99 (MISSING_INT8) に設定 |
| RHUM | humidity | パーセント [%] | 10倍して整数化（精度向上のため） |
| RHUM_AQC | - | - | -99 (MISSING_INT8) に設定 |
| DEWTMP | temp_dew | 摂氏 [°C] | 10倍して整数化（精度向上のため） |
| DEWTMP_AQC | - | - | -99 (MISSING_INT8) に設定 |
| ARPRSS | pressure | ヘクトパスカル [hPa] | 10倍して整数化（精度向上のため） |
| ARPRSS_AQC | - | - | -99 (MISSING_INT8) に設定 |
| SSPRSS | pressure_at_sea | ヘクトパスカル [hPa] | 10倍して整数化（精度向上のため） |
| SSPRSS_AQC | - | - | -99 (MISSING_INT8) に設定 |
| PRCRIN_10MIN | precip_past10min | ミリメートル [mm] | 10倍して整数化（精度向上のため） |
| PRCRIN_10MIN_AQC | - | - | -99 (MISSING_INT8) に設定 |
| PRCRIN_1HOUR | precip_past1h | ミリメートル [mm] | 10倍して整数化（精度向上のため） |
| PRCRIN_1HOUR_AQC | - | - | -99 (MISSING_INT8) に設定 |
| GLBRAD_10MIN | radia_glob | ジュール/平方センチメートル [J/cm²] | W/m² から変換（× 600 / 10000） |
| GLBRAD_10MIN_AQC | - | - | -99 (MISSING_INT8) に設定 |
| GLBRAD_1HOUR | radia_glob_past1h | ジュール/平方センチメートル [J/cm²] | W/m² から変換（× 3600 / 10000） |
| GLBRAD_1HOUR_AQC | - | - | -99 (MISSING_INT8) に設定 |
| SUNDUR_10MIN | sun_last10min_glob | 分 [min] | そのまま整数化 |
| SUNDUR_10MIN_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDDIR | wind_dir | 度 [°] | そのまま整数化 |
| WNDDIR_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDSPD | wind_speed | メートル/秒 [m/s] | 10倍して整数化（精度向上のため） |
| WNDSPD_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDSPD_10MIN_MAX | wind_max | メートル/秒 [m/s] | 10倍して整数化（精度向上のため） |
| WNDSPD_10MIN_MAX_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDSPD_1HOUR_MAX | wind_max_per10min_past1h | メートル/秒 [m/s] | 10倍して整数化（精度向上のため） |
| WNDSPD_1HOUR_MAX_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WX_original | weather | コード | 国際的な天気コードから説明文に変換 |
| WX_original_AQC | - | - | -99 (MISSING_INT8) に設定 |

## 処理フロー
1. 環境変数の検証
2. メモリキャッシュのクリーンアップ
3. DMI APIからデータ取得：
   - 最新の1時間分のデータをリクエスト
   - ページネーションを使用して全データを取得
   - タイムアウトやエラー時のリトライ処理
4. 取得したデータにRUヘッダーを追加
5. 生データをEUリージョンのS3バケットに保存
6. データ処理と変換：
   - 観測所ごとにデータを集約
   - 各パラメータの値を標準形式に変換
   - 最新の観測値を使用（タイムスタンプ比較）
7. 変換済みデータを日本リージョンのS3バケットに保存
8. 処理結果の統計情報を返却

## 特記事項
- タグID: 441000125
- メモリキャッシュを使用して処理効率を向上（キャッシュ有効期限：3600秒）
- APIレスポンスと処理結果の両方をキャッシュ
- 欠損値は専用の定数で処理：
  - MISSING_INT8: -99（8ビット整数での欠損値）
  - MISSING_INT16: -9999（16ビット整数での欠損値）
  - MISSING_INT32: -999999999（32ビット整数での欠損値）
  - MISSING_STR: ""（文字列の欠損値）
- 無効値も定数で定義：
  - INVALID_INT8: -111（8ビット整数での無効値）
  - INVALID_INT16: -11111（16ビット整数での無効値）
  - INVALID_INT32: -1111111111（32ビット整数での無効値）
  - INVALID_STR: ""（文字列の無効値）
- 浮動小数点の精度を保持するため、多くの数値データは10倍されて整数として格納
- 雲量（cloud_cover）はパーセント値からオクタスケールに変換
- 日射量（radiation）はW/m²からJ/cm²に単位変換
- 複数の観測値がある場合、最新のタイムスタンプを持つデータを使用
- RUヘッダー情報:
  - データ名: DMI_OBS_AWS_raw
  - データID: 0200600041000125
- APIからのレスポンスはページネーションで複数回取得する場合があります

## 天気コード変換
プログラムは国際的な気象コード（WMO）を英語の説明文に変換します。一部の例：

| コード | 説明（英語） |
|--------|------------|
| 0 | Cloud development not observed or not observable |
| 1 | Clouds generally dissolving or becoming less developed |
| 2 | State of sky on the whole unchanged |
| ... | ... |
| 95 | Thunderstorm, slight or moderate, without hail but with rain and/or snow at time of observation |
| ... | ... |
| 199 | Tornado |

## S3保存パス
### 生データ（EUリージョン）
```
{tagid}/{YYYY}/{MM}/{DD}/{YYYYMMDDHHmm}
```

### 変換データJSON（日本リージョン）
```
data/{tagid}/{YYYY}/{MM}/{DD}/{YYYYMMDDHHmmSS}.{uuid}
```

## 依存関係
- AWS SDK for Python (Boto3) - S3アクセス用
- urllib.request - ウェブリクエスト用
- json - JSONデータの解析と生成用
- os - 環境変数アクセス用
- datetime - 日時処理用
- uuid - ユニークIDの生成用
- socket - ネットワークタイムアウト処理用
- time - リトライ処理の待機時間用

## 環境変数
- **RawDataBucket**: 生データを保存するS3バケット（EUリージョン）
- **ConvertedBucket**: 変換済みデータを保存するS3バケット（日本リージョン）
- **tagid**: データの識別子（441000125）
- **URL**: データを取得するDMI APIのURL
- **APIKey**: DMI APIにアクセスするためのAPIキー
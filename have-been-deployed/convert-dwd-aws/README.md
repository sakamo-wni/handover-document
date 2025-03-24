# DWD 10分値データ処理 Lambda

## 概要
このLambda関数は、ドイツ気象局（DWD: Deutscher Wetterdienst）の10分間隔観測データを処理します。複数の気象要素カテゴリ（気温、風、降水量など）のZIPファイルをダウンロードし、各観測所の最新データを抽出して統合し、標準化されたJSONフォーマットに変換してS3に保存します。

## PSR 資料
このプログラムのPSR資料（問題特定書）については以下のURLを参照してください：
[DWD データ処理 PSR 資料](https://docs.google.com/spreadsheets/d/1IVaVNpurbhv4xuKcWMTeeL4phHvMaMcPyfBRbq16UDU/edit?gid=1246498904#gid=1246498904)


## 技術仕様
- **ランタイム**: Python 3.12
- **実行環境**: AWS Lambda
- **入力ソース**: DWDの10分値データZIPファイル（複数カテゴリ）
- **出力先**: 
  - 生データ: EU リージョンのS3バケット
  - 変換データ: 日本リージョンのS3バケット
- **入力フォーマット**: ZIP圧縮されたCSV形式
- **出力フォーマット**: 
  - 生データ: 統合テキスト
  - 変換データ: 標準化JSON

## 対象カテゴリ
プログラムは以下のカテゴリの10分値データを処理します：
1. **air_temperature** - 気温データ
2. **extreme_temperature** - 極値気温データ
3. **extreme_wind** - 極値風データ
4. **precipitation** - 降水量データ
5. **solar** - 日射量データ
6. **wind** - 風データ

## データマッピング
プログラムは以下のようにDWDのフィールドをJSON要素にマッピングします：

| JSON要素 | DWDフィールド | カテゴリ | 単位 | 変換処理 |
|--------------|----------------|----------------|----------------|----------------|
| LCLID | station_id | - | - | そのまま使用 |
| ID_GLOBAL_MNET | `DWD_{station_id}` | - | - | プロバイダ名とステーションIDを連結 |
| ARPRSS | PP_10 | air_temperature | ヘクトパスカル [hPa] | 10倍して整数化 |
| AIRTMP | TT_10 | air_temperature | 摂氏 [°C] | 10倍して整数化 |
| RHUM | RF_10 | air_temperature | パーセント [%] | 10倍して整数化 |
| DEWTMP | TD_10 | air_temperature | 摂氏 [°C] | 10倍して整数化 |
| AIRTMP_10MIN_MAX | TX_10 | extreme_temperature | 摂氏 [°C] | 10倍して整数化 |
| AIRTMP_10MIN_MINI | TN_10 | extreme_temperature | 摂氏 [°C] | 10倍して整数化 |
| WNDSPD | FF_10 | wind | メートル/秒 [m/s] | 10倍して整数化 |
| WNDDIR | DD_10 | wind | 度 [°] | 整数に四捨五入 |
| GUSTS | FX_10 | wind | メートル/秒 [m/s] | 10倍して整数化 |
| WNDSPD_10MIN_MAX | FMX_10 | wind | メートル/秒 [m/s] | 10倍して整数化 |
| GUSTD | DX_10 | wind | 度 [°] | 整数に四捨五入 |
| PRCRIN_10MIN | RWS_10 | precipitation | ミリメートル [mm] | 10倍して整数化 |
| SCTRAD_10MIN | DS_10 | solar | ワット/平方メートル [W/m²] | 10000倍して整数化（J/cm²に変換） |
| GLBRAD_10MIN | GS_10 | solar | ワット/平方メートル [W/m²] | 10000倍して整数化（J/cm²に変換） |
| SUNDUR_10MIN | SD_10 | solar | 分 [min] | 60倍して整数化（秒に変換） |

## 処理フロー
1. 環境変数の検証
2. メモリキャッシュとファイルキャッシュの初期化
3. 各カテゴリの更新時刻を確認
4. 各カテゴリのZIPファイルURLを取得
5. 並列処理：
   - 複数のZIPファイルを同時にダウンロード（バッチ処理）
   - ZIPファイルからCSVを抽出
   - 各観測所の最新データを取得
6. 全カテゴリの観測所データを統合
7. 統合データをJSON形式に変換
8. 生データと変換データをS3に保存

## 特記事項
- 多段階キャッシュシステムを使用（メモリキャッシュとファイルキャッシュ）
  - 一時ファイルキャッシュを `/tmp/dwdcache` ディレクトリに保存
  - 最大キャッシュサイズ：2GB
  - キャッシュの有効期限：7200秒（2時間）
- 欠損値と無効値は専用の定数で処理：
  - MISSING_INT8: -99（8ビット整数での欠損値）
  - MISSING_INT16: -9999（16ビット整数での欠損値）
  - MISSING_INT32: -999999（32ビット整数での欠損値）
  - INVALID_INT8: -111（8ビット整数での無効値）
  - INVALID_INT16: -11111（16ビット整数での無効値）
  - INVALID_INT32: -1111111（32ビット整数での無効値）
- 非同期処理を使用して並列ダウンロードと処理を実装
  - 最大同時接続数：200
  - リトライ処理：5回（タイムアウトやエラー時）
- データの更新時刻はDWDのディレクトリリスティングから取得
- ZIPファイルごとに観測所の最新データのみを使用
- 全カテゴリの中から最新の観測時刻を特定して使用

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
- asyncio - 非同期処理用
- zipfile - ZIPファイル解凍用
- urllib.request - ウェブリクエスト用
- json - JSONデータの解析と生成用
- csv - CSVデータ処理用
- datetime - 日時処理用
- re - 正規表現処理用
- ssl - SSLコンテキスト設定用
- pickle - キャッシュデータ保存用
- hashlib - キャッシュキー生成用
- uuid - ユニークID生成用
- os - ファイルシステムとOS環境変数アクセス用

## 環境変数
- **RawDataBucket**: 生データを保存するS3バケット（EUリージョン）
- **ConvertedBucket**: 変換済みデータを保存するS3バケット（日本リージョン）
- **tagid**: データの識別子
- **URL**: DWDの10分値データベースURL

## 最適化とパフォーマンス
- 並列非同期処理による高速化（asyncio）
- 多段階キャッシング（メモリとファイル）によるリクエスト削減
- バッチ処理による効率的なダウンロード（200ファイルずつ）
- スマートキャッシュ：データの更新時刻に基づく選択的キャッシュ更新
- リソース使用量の監視と自動クリーンアップ
# DWD 観測局データ処理 Lambda

## 概要
このLambda関数は、ドイツ気象局（DWD: Deutscher Wetterdienst）の観測局リストデータを処理します。S3からSQSを通じてテキスト形式のデータを受信し、RUヘッダーを削除した後、GeoJSON形式に変換してS3に保存します。このデータは観測局の位置情報、名称、稼働期間などの基本情報を含みます。

## 技術仕様
- **ランタイム**: Python 3.12
- **実行環境**: AWS Lambda
- **入力ソース**: SQS (S3からのデータを含む)
- **出力先**: S3
- **入力フォーマット**: テキスト形式 (RUヘッダー付き)
- **出力フォーマット**: GeoJSON

## データマッピング
プログラムは以下のようにテキストデータをGeoJSON要素にマッピングします：

| GeoJSON要素 | テキストデータ列 | 説明 | 変換処理 |
|--------------|----------------|----------------|----------------|
| coordinates[0] | 6列目 | 経度 [度] | 浮動小数点に変換 |
| coordinates[1] | 5列目 | 緯度 [度] | 浮動小数点に変換 |
| coordinates[2] | 4列目 | 標高 [メートル] | 浮動小数点に変換 |
| LCLID | 1列目 | 観測局ID | そのまま使用 |
| LNAME | 7列目 | 観測局名 | そのまま使用 |
| CNTRY | - | 国コード | "DE"（ドイツ）固定値 |
| states | 8列目 | 州名 | そのまま使用 |
| OBS_BEGIND | 2列目 | 観測開始日 | yyyymmdd から ISO 8601形式に変換 |
| OBS_ENDD | 3列目 | 観測終了日 | yyyymmdd から ISO 8601形式に変換 |

## 処理フロー
1. 環境変数の検証
2. SQSイベントからS3オブジェクトキーを抽出
3. S3からデータを取得
4. データからRUヘッダーを削除
   - ヘッダー部分の終わりは `\x04\x1a` で識別
   - ヘッダーからannounced日時などのメタデータを抽出
5. データをGeoJSON形式に変換
   - テキストデータを行ごとに解析
   - 破線行 (`----`) を検出して実データの開始行を特定
   - 各行を解析して観測局情報を抽出
   - GeoJSON Feature形式にマッピング
6. 変換されたGeoJSONデータをS3に保存

## 特記事項
- このLambda関数は観測局のメタデータのみを処理し、観測データは処理しません
- 日付は `yyyymmdd` 形式から `yyyy-mm-ddT00:00:00Z` 形式に変換されます
- 無効な日付 (`99999999`) は `null` として処理されます
- テキストデータはLatin-1エンコーディングで読み込まれます
- 処理されたデータは固定のパス `metadata/spool/DWD_AWS/metadata.json` に保存されます

## S3保存パス
### GeoJSON
```
metadata/spool/DWD_AWS/metadata.json
```

## 入力データ形式
入力データは以下のような形式のテキストファイルです：

```
（RUヘッダー部分）
...
\x04\x1a

（説明部分）
...
--------------------
（実データ部分）
StationID Betrieb_Beginn Betrieb_Ende Stationshoehe Geogr.Breite Geogr.Laenge Stationsname Bundesland
...
```

## 依存関係
- AWS SDK for Python (Boto3) - S3アクセスとSTSクライアント用
- botocore - AWS例外処理用
- json - JSONデータの解析と生成用
- os - 環境変数アクセス用
- datetime - 日時処理用
- io - 入出力処理用

## 環境変数
- **stock_s3**: 入力データが格納されているS3バケット
- **md_bucket**: 変換されたGeoJSONデータを保存するS3バケット

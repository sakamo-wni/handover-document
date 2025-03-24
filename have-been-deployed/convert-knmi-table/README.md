# KNMI 観測局データ処理 Lambda

## 概要
このLambda関数は、オランダ王立気象研究所（KNMI: Koninklijk Nederlands Meteorologisch Instituut）の観測局メタデータを処理します。S3からSQSを通じて受信したNetCDFファイルを解析し、GeoJSON形式に変換してS3に保存します。観測局の位置情報や基本属性を標準化された形式で提供します。

## 技術仕様
- **ランタイム**: Python 3.12
- **実行環境**: AWS Lambda
- **入力ソース**: SQS (S3からのデータを含む)
- **出力先**: S3
- **入力フォーマット**: NetCDF (RUヘッダー付き)
- **出力フォーマット**: GeoJSON
- **依存ライブラリ**: netCDF4 (Lambda layerとして追加)

## データマッピング
プログラムは以下のようにNetCDF変数をGeoJSON要素にマッピングします：

| GeoJSON要素 | NetCDF変数 | 説明 | 変換処理 |
|--------------|----------------|----------------|----------------|
| coordinates[0] | lon | 経度 [度] | 浮動小数点に変換 |
| coordinates[1] | lat | 緯度 [度] | 浮動小数点に変換 |
| coordinates[2] | height | 標高 [メートル] | 浮動小数点に変換 |
| LCLID | WMO | 観測局ID | 文字列に変換 |
| LNAME | name | 観測局名 | 文字列として使用 |
| CNTRY | - | 国コード | "NL"（オランダ）固定値 |
| WMO_ID | WMO | 世界気象機関ID | 文字列に変換 |
| WIGOS_ID | WSI | WIGOS ID | 文字列として使用 |
| OBS_BEGIND | time | 観測開始日 | 日数を日付に変換（2009-02-10基準） |

## 処理フロー
1. 環境変数の検証
2. SQSイベントからS3オブジェクトキーを抽出
3. S3からデータを取得
4. データからRUヘッダーを削除
   - ヘッダー部分の終わりは `\x04\x1a` で識別
   - ヘッダーからannounced日時などのメタデータを抽出
5. 一時ファイルにNetCDFデータを保存
6. netCDF4ライブラリを使用してデータを読み込み
7. NetCDFデータをGeoJSON形式に変換
   - 日付の特殊変換処理（基準日からの日数）
   - 各フィールドの適切な型変換
8. 変換されたGeoJSONデータをS3に保存
9. 一時ファイルを削除

## 特記事項
- タグID: 441000163
- NetCDFファイルは `/tmp` ディレクトリに一時保存されます
- 日付情報は特殊な形式で保存されています：
  - 基準日（2009年2月10日）からの日数として格納
  - 例: 値が「365」の場合、「2010-02-10T00:00:00Z」に変換
- ISO 8601形式（YYYY-MM-DDThh:mm:ssZ）に変換して出力
- 非常に大きな日数値（|値| > 100000）は無効として処理
- 処理されたデータは固定のパス `metadata/spool/KNMI/metadata.json` に保存されます

## S3保存パス
### GeoJSON
```
metadata/spool/KNMI/metadata.json
```

## 入力NetCDF形式
NetCDFファイルには以下の変数が含まれています：
- **lat**: 緯度 (浮動小数点)
- **lon**: 経度 (浮動小数点)
- **height**: 高度 (浮動小数点、メートル単位)
- **time**: 観測開始日 (浮動小数点、基準日からの日数)
- **name**: 観測局名 (文字列)
- **WMO**: 世界気象機関ID (文字列)
- **WSI**: WIGOS ID (文字列)

## 出力GeoJSON形式
変換後のGeoJSONは以下のような形式です：

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [longitude, latitude, elevation]
      },
      "properties": {
        "LCLID": "WMO_ID",
        "LNAME": "Station Name",
        "CNTRY": "NL",
        "WMO_ID": "WMO_ID",
        "WIGOS_ID": "WIGOS_ID",
        "OBS_BEGIND": "YYYY-MM-DDThh:mm:ssZ"
      }
    },
    ...
  ]
}
```

## 依存関係
- AWS SDK for Python (Boto3) - S3アクセスとSTSクライアント用
- botocore - AWS例外処理用
- json - JSONデータの解析と生成用
- datetime - 日時処理と計算用
- urllib.parse - URL処理用
- netCDF4 - NetCDFファイル読み込み用 (Lambda layerとして提供)
- uuid - 一時ファイル名生成用
- os - ファイルシステム操作用

## 環境変数
- **stock_s3**: 入力データが格納されているS3バケット
- **md_bucket**: 変換されたGeoJSONデータを保存するS3バケット
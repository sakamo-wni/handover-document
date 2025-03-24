# DMI 観測局データ処理 Lambda

## 概要
このLambda関数は、デンマーク気象局（DMI: Danish Meteorological Institute）の観測局データを処理します。S3からSQSを通じてGeoJSON形式のデータを受信し、RUヘッダーを削除した後、重複を排除して最適化されたGeoJSON形式に変換してS3に保存します。このデータは観測局の位置情報、名称、稼働期間などの基本情報を含みます。

## PSR 資料
このプログラムのPSR資料（問題特定書）については以下のURLを参照してください：
[DMI データ処理 PSR 資料](https://docs.google.com/spreadsheets/d/1qa_T825A87Shf2dTG3P6dV02AHY9XGdskKw9jXNG9Mk/edit?gid=492387665#gid=492387665)


## 技術仕様
- **ランタイム**: Python 3.12
- **実行環境**: AWS Lambda
- **入力ソース**: SQS (S3からのデータを含む)
- **出力先**: S3
- **入力フォーマット**: GeoJSON形式 (RUヘッダー付き)
- **出力フォーマット**: 最適化されたGeoJSON

## データマッピング
プログラムは以下のように入力GeoJSONから出力GeoJSONへのマッピングを行います：

| 出力GeoJSON要素 | 入力データ要素 | 説明 | 処理 |
|--------------|----------------|----------------|----------------|
| coordinates[0] | geometry.coordinates[0] | 経度 [度] | 浮動小数点に変換 |
| coordinates[1] | geometry.coordinates[1] | 緯度 [度] | 浮動小数点に変換 |
| coordinates[2] | properties.stationHeight | 標高 [メートル] | 存在する場合のみ追加 |
| LCLID | properties.stationId | 観測局ID | 文字列として使用 |
| LNAME | properties.name | 観測局名 | 文字列として使用 |
| CNTRY | - | 国コード | "DK"（デンマーク）固定値 |
| OBS_BEGIND | properties.operationFrom | 観測開始日 | ISO形式の日時文字列 |
| OBS_ENDD | properties.operationTo | 観測終了日 | ISO形式の日時文字列 |

## 処理フロー
1. 環境変数の検証
2. SQSイベントからS3オブジェクトキーを抽出
3. S3からデータを取得
4. データからRUヘッダーを削除
   - ヘッダー部分の終わりは `\x04\x1a` で識別
   - ヘッダーからannounced日時などのメタデータを抽出
5. JSONデータを解析
6. 重複排除と最適化：
   - 観測局IDによる重複排除
   - 座標による重複排除
   - 観測局名による重複排除
   - 優先順位ルールの適用（詳細は下記）
7. 変換されたGeoJSONデータをS3に保存

## 特記事項
- この関数は観測局のメタデータのみを処理し、観測データは処理しません
- 重複排除の優先順位ルール：
  1. 稼働終了日が設定されていない観測局を優先
  2. 稼働終了日が新しい観測局を優先
  3. 高度情報がある観測局を優先
  4. 稼働開始日が新しい観測局を優先
- 日時のタイムゾーン情報は処理時に削除されます（ナイーブなdatetimeオブジェクトに変換）
- 処理されたデータは固定のパス `metadata/spool/DMI/metadata.json` に保存されます

## ログと統計
プログラムは処理の各段階での統計情報をログに出力します：
- 入力観測局数
- ID重複排除後の観測局数
- 座標重複排除後の観測局数
- 名前重複排除後の観測局数
- 最終出力観測局数
- 除去された重複数

## S3保存パス
### GeoJSON
```
metadata/spool/DMI/metadata.json
```

## 入力データ形式
入力データは以下のような形式のGeoJSONファイルです：

```
（RUヘッダー部分）
...
\x04\x1a
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
        "stationId": "...",
        "name": "...",
        "stationHeight": "...",
        "operationFrom": "...",
        "operationTo": "..."
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
- datetime - 日時処理用
- os - 環境変数アクセス用
- io - 入出力処理用

## 環境変数
- **stock_s3**: 入力データが格納されているS3バケット
- **md_bucket**: 変換されたGeoJSONデータを保存するS3バケット

# RMI 観測局データ処理 Lambda

## 概要
このLambda関数は、ベルギー王立気象研究所（RMI: Royal Meteorological Institute of Belgium）の観測局メタデータを処理します。S3からSQSを通じて受信したJSONデータを解析し、重複を排除しながらGeoJSON形式に変換してS3に保存します。観測局の位置情報や基本属性を標準化された形式で提供します。

## 技術仕様
- **ランタイム**: Python 3.x
- **実行環境**: AWS Lambda
- **入力ソース**: SQS (S3からのデータを含む)
- **出力先**: S3
- **入力フォーマット**: JSON (RUヘッダー付き)
- **出力フォーマット**: GeoJSON

## データマッピング
プログラムは以下のようにRMIのJSONデータフィールドをGeoJSON要素にマッピングします：

| GeoJSON要素 | RMIフィールド | 説明 | 変換処理 |
|--------------|----------------|----------------|----------------|
| coordinates[0] | geometry.coordinates[0] | 経度 [度] | 浮動小数点に変換 |
| coordinates[1] | geometry.coordinates[1] | 緯度 [度] | 浮動小数点に変換 |
| coordinates[2] | properties.altitude | 標高 [メートル] | 存在する場合のみ追加 |
| LCLID | properties.code | 観測局ID | 文字列に変換 |
| LNAME | properties.name | 観測局名 | 文字列として使用 |
| CNTRY | - | 国コード | "BE"（ベルギー）固定値 |
| OBS_BEGIND | properties.date_begin | 観測開始日 | そのまま使用（ISO形式） |
| OBS_ENDD | properties.date_end | 観測終了日 | そのまま使用（ISO形式）または空文字列 |

## 処理フロー
1. 環境変数の検証
2. SQSイベントからS3オブジェクトキーを抽出
3. S3からデータを取得
4. データからRUヘッダーを削除
   - ヘッダー部分の終わりは `\x04\x1a` で識別
5. JSONデータを解析
6. 重複排除と最適化：
   - 観測局IDによる重複排除
   - 座標による重複排除
   - 観測局名による重複排除
   - 優先順位ルールの適用（詳細は下記）
7. 変換されたGeoJSONデータをS3に保存

## 特記事項
- 重複排除の優先順位ルール：
  1. 稼働終了日が設定されていない観測局を優先
  2. 稼働終了日が新しい観測局を優先
  3. 高度情報がある観測局を優先
  4. 稼働開始日が新しい観測局を優先
- ISO形式の日付（例：`2023-01-01T00:00:00Z`）をdatetimeオブジェクトに変換する際、タイムゾーン情報を適切に処理
- 開始日が設定されていない場合は`datetime.min`、終了日が設定されていない場合は`datetime.max`を使用して優先順位を決定
- 処理されたデータは固定のパス `metadata/spool/RMI/metadata.json` に保存されます

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
metadata/spool/RMI/metadata.json
```

## 入力データ形式
RMIのJSONデータは以下のような形式です：

```json
{
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [longitude, latitude]
      },
      "properties": {
        "code": "station_id",
        "name": "Station Name",
        "altitude": elevation,
        "date_begin": "YYYY-MM-DDTHH:MM:SSZ",
        "date_end": "YYYY-MM-DDTHH:MM:SSZ"
      }
    },
    ...
  ]
}
```

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
        "LCLID": "station_id",
        "LNAME": "Station Name",
        "CNTRY": "BE",
        "OBS_BEGIND": "YYYY-MM-DDTHH:MM:SSZ",
        "OBS_ENDD": "YYYY-MM-DDTHH:MM:SSZ"
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
- io - 入出力処理用
- os - 環境変数アクセス用

## 環境変数
- **stock_s3**: 入力データが格納されているS3バケット
- **md_bucket**: 変換されたGeoJSONデータを保存するS3バケット
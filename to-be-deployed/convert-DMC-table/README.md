# DMC 観測局データ処理 Lambda

## 概要
このLambda関数は、チリ気象局DMC（Dirección Meteorológica de Chile）の観測局メタデータを処理します。S3からSQSを通じて受信したJSONデータを解析し、重複を排除しながらGeoJSON形式に変換してS3に保存します。観測局の位置情報や基本属性を標準化された形式で提供します。

## 技術仕様
- **ランタイム**: Python 3.12
- **実行環境**: AWS Lambda
- **入力ソース**: SQS (S3からのデータを含む)
- **出力先**: S3
- **入力フォーマット**: JSON (RUヘッダー付き)
- **出力フォーマット**: GeoJSON

## データマッピング
プログラムは以下のようにDMCの特殊なJSONフォーマットからGeoJSONに変換します：

| GeoJSON要素 | DMCフィールド | 説明 | 変換処理 |
|--------------|----------------|----------------|----------------|
| coordinates[0] | features.geometry.coordinates[0] | 経度 [度] | 浮動小数点に変換 |
| coordinates[1] | features.geometry.coordinates[1] | 緯度 [度] | 浮動小数点に変換 |
| coordinates[2] | features.properties.altitud | 標高 [メートル] | 存在する場合のみ追加 |
| LCLID | features.properties.CodigoNacional | 観測局ID | 文字列に変換 |
| LNAME | features.properties.nombreEstacion | 観測局名 | そのまま使用 |
| WIGOS_ID | features.properties.codigoWIGOS | WIGOS ID | そのまま使用 |
| WMO_ID | features.properties.CodigoOMM | 世界気象機関ID | 文字列に変換 |
| LATD | features.geometry.coordinates[1] | 緯度 [度] | 浮動小数点として保存 |
| LOND | features.geometry.coordinates[0] | 経度 [度] | 浮動小数点として保存 |
| ALT | features.properties.altitud | 標高 [メートル] | 浮動小数点または null |
| OBS_BEGIND | features.properties.fechaInstalacion | 観測開始日 | YYYY-MM-DDThh:mm:ssZ 形式に変換 |
| CNTRY | - | 国コード | "CL"（チリ）固定値 |

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
- DMCのJSONデータは特殊な入れ子構造を持っており、`features.features`のような形式でアクセスする
- 日付（fechaInstalacion）は「YYYY-MM-DD HH:MM:SS」形式から「YYYY-MM-DDThh:mm:ssZ」形式のISO8601に変換
- 重複排除の優先順位ルール：
  1. 高度情報がある観測局を優先
  2. インストール日が新しい観測局を優先
- 数値データ（経度、緯度、高度）は浮動小数点として変換
- 観測局IDと世界気象機関IDは文字列として統一
- 処理されたデータは固定のパス `metadata/spool/DMC/metadata.json` に保存されます

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
metadata/spool/DMC/metadata.json
```

## 入力データ形式
DMCのJSONデータは以下のような特殊な入れ子構造を持っています：

```json
{
  "features": [
    {
      "features": {
        "geometry": {
          "type": "Point",
          "coordinates": [longitude, latitude]
        },
        "properties": {
          "CodigoNacional": "...",
          "nombreEstacion": "...",
          "codigoWIGOS": "...",
          "CodigoOMM": "...",
          "altitud": "...",
          "fechaInstalacion": "YYYY-MM-DD HH:MM:SS"
        }
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

## 注意事項
- 同名のスタックを開発環境にテストデプロイしていますので、引き継がれる際はスタックを削除してからデプロイしてください。

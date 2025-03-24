# DMC 気象データ処理 Lambda

## 概要
このLambda関数は、チリ気象局DMC（Dirección Meteorológica de Chile）の気象観測データを処理します。S3からSQSを通じて受信したJSONデータを解析し、標準化されたフォーマットに変換してS3に保存します。データの効率的な処理のために、前回処理したデータとの差分比較を行い、変更があったデータのみを処理します。

## 技術仕様
- **ランタイム**: Python 3.x
- **実行環境**: AWS Lambda
- **入力ソース**: SQS (S3からのデータを含む)
- **出力先**: S3
- **入力フォーマット**: JSON (RUヘッダー付き)
- **出力フォーマット**: 標準化JSON

## データマッピング
プログラムは以下のようにDMCのJSONフィールドを標準化JSONフィールドにマッピングします：

| 標準化JSON要素 | DMCフィールド | 単位 | 変換処理 |
|--------------|----------------|----------------|----------------|
| LCLID | estacion.codigoNacional | - | そのまま使用 |
| ID_GLOBAL_MNET | `DMC_{codigoNacional}` | - | プロバイダ名と観測局IDを連結 |
| AIRTMP | temperatura02Mts または temperatura | 摂氏 [°C] | 数値抽出して10倍に整数化 |
| AIRTMP_AQC | - | - | -99 (MISSING_INT8) に設定 |
| DEWTMP | puntoDeRocio | 摂氏 [°C] | 数値抽出して10倍に整数化 |
| DEWTMP_AQC | - | - | -99 (MISSING_INT8) に設定 |
| RHUM | humedadRelativa | パーセント [%] | 数値抽出して10倍に整数化 |
| RHUM_AQC | - | - | -99 (MISSING_INT8) に設定 |
| ARPRSS | presionEstacion | ヘクトパスカル [hPa] | 数値抽出して10倍に整数化 |
| ARPRSS_AQC | - | - | -99 (MISSING_INT8) に設定 |
| SSPRSS | presionNivelDelMar | ヘクトパスカル [hPa] | 数値抽出して10倍に整数化 |
| SSPRSS_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDDIR | direccionDelViento | 度 [°] | 数値抽出 |
| WNDDIR_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDDIR_10MIN_AVG | direccionDelVientoPromedio10Minutos | 度 [°] | 数値抽出 |
| WNDDIR_10MIN_AVG_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDSPD | fuerzaDelViento | メートル/秒 [m/s] | ノットから変換して10倍に整数化 |
| WNDSPD_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDSPD_10MIN_MAX | fuerzaDelViento10MinutosMax | メートル/秒 [m/s] | ノットから変換して10倍に整数化 |
| WNDSPD_10MIN_MAX_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDDIR_10MIN_MAX | direccionDelViento10MinutosMax | 度 [°] | 数値抽出 |
| WNDDIR_10MIN_MAX_AQC | - | - | -99 (MISSING_INT8) に設定 |

## 処理フロー
1. 環境変数の検証
2. SQSイベントからS3オブジェクトキーを抽出
3. 前回処理したデータをキャッシュから取得
4. S3からデータを取得し、RUヘッダーを削除
5. JSONデータを解析
6. データの差分検出：
   - 各観測所データが更新されているかチェック
   - 前回と同じタイムスタンプのデータはスキップ
7. データを標準フォーマットに変換：
   - 単位変換（ノット→m/s）
   - 数値の抽出と整数化
   - 欠損値の適切な処理
8. 変換されたデータをS3に保存
9. 現在のデータをキャッシュに保存（次回の差分比較用）

## 特記事項
- タグIDは環境変数から設定（デフォルト: 460320021）
- キャッシュシステムを使用して以前処理したデータを記憶し、差分のみを処理
  - キャッシュは `/tmp/tmp_DMC/` ディレクトリに保存
  - キャッシュキーはSHA-256ハッシュを使用
- 欠損値は専用の定数で処理：
  - MISSING_INT8: -99（8ビット整数での欠損値）
  - MISSING_INT16: -9999（16ビット整数での欠損値）
  - MISSING_INT32: -999999999（32ビット整数での欠損値）
- 無効値も定数で定義：
  - INVALID_INT16: -11111（16ビット整数での無効値）
  - INVALID_INT32: -1111111111（32ビット整数での無効値）
- 値の変換処理：
  - 正規表現を使用して文字列から数値部分を抽出（例: "22.7 °C" → 22.7）
  - 風速はノットからメートル/秒に変換（変換係数: 0.51444）
  - ほとんどの数値は精度を保持するため10倍して整数化
- デフォルトでは観測所データの更新タイムスタンプ「momento」を比較して差分を検出

## S3保存パス
```
data/{tagid}/{YYYY}/{MM}/{DD}/{YYYYMMDDHHmmSS}.{uuid}
```

## 入力JSON形式の例
```json
{
  "datosEstaciones": [
    {
      "estacion": {
        "codigoNacional": "123456"
      },
      "datos": [
        {
          "momento": "2023-01-01 12:00:00",
          "temperatura": "22.7 °C",
          "temperatura02Mts": "22.5 °C",
          "puntoDeRocio": "15.3 °C",
          "humedadRelativa": "65 %",
          "presionEstacion": "1013.2 hPa",
          "presionNivelDelMar": "1013.5 hPa",
          "direccionDelViento": "180",
          "direccionDelVientoPromedio10Minutos": "175",
          "fuerzaDelViento": "5.2 kt",
          "fuerzaDelViento10MinutosMax": "7.1 kt",
          "direccionDelViento10MinutosMax": "185"
        }
      ]
    }
  ]
}
```

## 出力JSON形式の例
```json
{
  "tagid": "460320021",
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
        "LCLID": "123456",
        "ID_GLOBAL_MNET": "DMC_123456",
        "AIRTMP": 225,
        "AIRTMP_AQC": -99,
        "DEWTMP": 153,
        "DEWTMP_AQC": -99,
        "RHUM": 650,
        "RHUM_AQC": -99,
        "ARPRSS": 10132,
        "ARPRSS_AQC": -99,
        "SSPRSS": 10135,
        "SSPRSS_AQC": -99,
        "WNDDIR": 180,
        "WNDDIR_AQC": -99,
        "WNDDIR_10MIN_AVG": 175,
        "WNDDIR_10MIN_AVG_AQC": -99,
        "WNDSPD": 27,
        "WNDSPD_AQC": -99,
        "WNDSPD_10MIN_MAX": 37,
        "WNDSPD_10MIN_MAX_AQC": -99,
        "WNDDIR_10MIN_MAX": 185,
        "WNDDIR_10MIN_MAX_AQC": -99
      }
    ]
  }
}
```

## ログと統計
プログラムは処理の各段階での詳細なログを出力します：
- 更新された観測所数
- キャッシュを使って処理をスキップした観測所数
- 合計観測所数
- 処理エラーの詳細

## 依存関係
- AWS SDK for Python (Boto3) - S3アクセス用
- json - JSONデータの解析と生成用
- datetime - 日時処理用
- logging - ログ出力用
- hashlib - キャッシュキー生成用
- re - 正規表現を使った数値抽出用
- pathlib - ファイルパス処理用
- os - 環境変数とディレクトリ操作用
- uuid - ユニークID生成用

## 環境変数
- **tagid**: データの識別子（デフォルト: 460320021）
- **stock_s3**: 入力データが格納されているS3バケット
- **md_bucket**: 変換されたJSONデータを保存するS3バケット

## 最適化と処理効率
- 差分処理による不要な変換の省略
- ファイルキャッシュによるS3リクエストの削減
- 効率的な正規表現による数値抽出
## 注意事項
- 同名のスタックを開発環境にテストデプロイしていますので、引き継がれる際はスタックを削除してからデプロイしてください。

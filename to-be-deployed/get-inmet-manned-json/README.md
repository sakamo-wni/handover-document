# INMET データ処理 Lambda

## 概要

このLambda関数は、ブラジル気象局 (INMET: Instituto Nacional de Meteorologia) の観測データを処理します。S3にアップロードされたRUヘッダー付きJSONファイルを取得し、標準化されたJSON形式に変換してS3に保存します。
## 仕様書

INMET データ処理に関するPSR資料は下記URLを参照してください:
[INMET 仕様書](https://docs.google.com/spreadsheets/d/1n0rSwZMX6f6adxqsunAvs3xaltT_peExSeVSJVyo8zc/edit?gid=0#gid=0)

## 技術仕様

- **ランタイム**: Python 3.12
- **実行環境**: AWS Lambda
- **入力ソース**: SQS（S3イベントを含む）
- **出力先**: S3（出力バケット）
- **入力フォーマット**: RUヘッダー付きJSONテキスト
- **出力フォーマット**: 標準化JSON（UTF-8, 整形済）
- **ファイル保存パス**: `data/{tagid}/{YYYY}/{MM}/{DD}/{filename}`
- **依存ライブラリ**: boto3 / json / datetime / uuid / os / traceback

## データマッピング

| JSON要素              | 元データフィールド | 単位         | 変換処理                                      |
|-----------------------|--------------------|--------------|-----------------------------------------------|
| LCLID                | CD_ESTACAO         | -            | 文字列として使用                               |
| ID_GLOBAL_MNET       | -                  | -            | "INMET_" + CD_ESTACAO                          |
| AIRTMP_1HOUR_MAX     | TEMP_MAX           | °C           | 10倍して整数化、欠損時は -9999                 |
| AIRTMP_1HOUR_AVG     | TEMP_MED           | °C           | 同上                                          |
| AIRTMP_1HOUR_MIN     | TEMP_MIN           | °C           | 同上                                          |
| RHUM                 | UMID_MED           | %            | 10倍して整数化                                 |
| ARPRSS_1HOUR_AVG     | PRESS_EST          | hPa          | 10倍して整数化                                 |
| WNDDIR_1HOUR_AVG     | VENT_DIR           | °            | 整数のまま                                     |
| WNDSPD_1HOUR_AVG     | VENT_VEL           | m/s          | 10倍して整数化                                 |
| PRCRIN_1HOUR         | CHUVA              | mm           | 10倍して整数化                                 |
| *_AQC                | -                  | -            | 欠損値（MISSING_INT8 = -99）として固定        |

## 処理フロー

1. LambdaはSQSからイベントを受信し、bodyからS3のオブジェクトキーを抽出
2. S3からRUヘッダー付きのJSONファイルをダウンロード
3. RUヘッダー（\x04\x1aで区切り）を除去し、データ部分をJSONとしてロード
4. 観測日時（announced）を解析し、年・月・日・時刻を展開
5. 各観測点データを標準化されたJSON構造に変換
6. `data/{tagid}/{YYYY}/{MM}/{DD}/{filename}` に出力
7. 書き込み成功時にログ出力、失敗時はエラー内容を出力

## 特記事項

- すべての数値データは浮動小数点から10倍して整数化（精度維持のため）
- 欠損値には定数（MISSING_INT8〜32）を使用し、JSON構造を保つ
- 書き込み先のS3パスはUTC時刻とUUIDを用いて衝突を回避
- 出力は整形済みJSON（インデントあり、UTF-8）
- デバッグ用にtracebackも標準出力される

## 依存関係

- AWS SDK for Python (boto3)
- botocore
- json
- datetime
- uuid
- os
- traceback

## 問い合わせ事項

### 1. "RAD_GLO" の単位と負の値について

- **質問**: 仕様書では `RAD_GLO` の単位が「kJ/m²」、公式ドキュメントでは「W/m²」。さらに夜間データに負の値が含まれている点も気になる。
- **コメント**: 単位の食い違いおよび物理的に不自然な値（夜間の負の放射量）は要確認事項。処理上は単位換算またはフィルタ処理の検討が必要。

> 📌 INMET回答待ち(3/24時点)

---

### 2. "TEMP_MED" の平均時間幅が不明

- **質問**: 有人観測における `TEMP_MED`（気温中央値）がどの時間幅の平均なのか不明。
- **コメント**: 有人観測データは1時間ごとの定期観測ではなく、任意の時間で観測されている可能性がある。そのため、`TEMP_MED` の意味合いがあいまい。

> 📌 INMET回答待ち(3/24時点)


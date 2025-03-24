# INMET データ処理 Lambda

## 概要

このLambda関数は、ブラジル気象局 (INMET: Instituto Nacional de Meteorologia) の観測所データを処理します。S3からSQSを通じてデータを受信し、RUヘッダーを削除した後、GeoJSON 形式の標準化されたJSONフォーマットに変換し、S3に保存します。
INMETはawsとmannedの2種類の観測データがあるので、地点テーブルも2種類あります。
このプログラムはそれぞれの地点データを統合し、1つのデータセットにしています。

## 仕様書

INMET データ処理に関するPSR資料は下記URLを参照してください:
[INMET 仕様書](https://docs.google.com/spreadsheets/d/1n0rSwZMX6f6adxqsunAvs3xaltT_peExSeVSJVyo8zc/edit?gid=0#gid=0)

## 技術仕様

- **ランタイム**: Python 3.12
- **実行環境**: AWS Lambda
- **入力ソース**: SQS (S3からのデータを含む)
- **出力先**: S3
- **入力フォーマット**: 独自形式 + RUヘッダー付き
- **依存ライブラリ**: boto3 / json / datetime / uuid

## データマッピング

GeoJSON に変換する際、観測所の結果を次のようにマッピングします:

| JSON要素        | 元データフィールド            | 単位 | 変換処理                     |
| ------------- | -------------------- | -- | ------------------------ |
| LCLID         | CD\_ESTACAO          | -  | 文字列として使用                 |
| LNAME         | DC\_NOME             | -  | 文字列として使用                 |
| VL\_LATITUDE  | VL\_LATITUDE         | 線線 | floatに変換                 |
| VL\_LONGITUDE | VL\_LONGITUDE        | 線線 | floatに変換                 |
| VL\_ALTITUDE  | VL\_ALTITUDE         | m  | floatまたは MISSING\_VALUES |
| OBS\_BEGIND   | DT\_INICIO\_OPERACAO | 日付 | そのまま                     |
| OBS\_ENDD     | DT\_FIM\_OPERACAO    | 日付 | あればそのまま / なければ空文字        |

## 処理フロー

1. SQS から受信した S3 キーを一時保存
2. S3 から実データを取得
3. RUヘッダーを除去して JSON パース
4. 最新 2 件分のデータを結合
5. GeoJSON 形式に変換
6. S3 の metadata/spool/INMET/metadata.json に保存
7. 処理した一時メッセージを削除

## 特記事項

- 最新 2 件のメッセージのみを使用して結合
- RUヘッダーは `\x04\x1a` で区切られる
- 最新データが2件に満たない場合は保留のみ
- GeoJSON には altitude も含まれる
- CNTRY は "BR" で固定

## 依存関係

- AWS SDK for Python (boto3)
- botocore
- json
- datetime
- uuid
- os

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
# HUNMHS データ処理 Lambda

## 概要
このLambda関数は、ハンガリー気象局（HUNMHS）の10分ごとの観測生データを処理します。S3からSQSを通じてデータを受信し、RUヘッダーを削除した後、標準化されたJSONフォーマットに変換し、S3に保存します。

## PSR 資料
このプログラムのPSR資料については以下のURLを参照してください：
[HUNMHS データ処理 PSR 資料](https://docs.google.com/spreadsheets/d/11X06dqibNCYOstbVhfgnNBuZ2cbdI9NOr6g13mUYSG4/edit?gid=1848697441#gid=1848697441)

## 技術仕様
- **ランタイム**: Python 3.12
- **実行環境**: AWS Lambda
- **入力ソース**: SQS (S3からのデータを含む)
- **出力先**: S3
- **入力フォーマット**: CSVライクなフォーマット (セミコロン区切り、`;` を区切り文字として使用)

## データマッピング
プログラムは以下のように生データフィールドをJSON要素にマッピングします：

| JSON要素 | 生データフィールド (CSVカラム) |
|--------------|----------------|
| LCLID | StationNumber |
| ID_GLOBAL_MNET | `HUNMHS_{StationNumber}` として生成 |
| HVIS | v (スペース含む) |
| HVIS_AQC | -99 (MISSING_INT8) に設定 |
| WNDSPD | fs (スペース含む) × 10 |
| WNDSPD_AQC | -99 (MISSING_INT8) に設定 |
| GUSTS | fx (スペース含む) × 10 |
| GUSTS_AQC | -99 (MISSING_INT8) に設定 |
| GUSTD | fxd (スペース含む) |
| GUSTD_AQC | -99 (MISSING_INT8) に設定 |
| WNDDIR | fsd (スペース含む) |
| WNDDIR_AQC | -99 (MISSING_INT8) に設定 |
| AIRTMP | t (スペース含む) × 10 |
| AIRTMP_AQC | -99 (MISSING_INT8) に設定 |
| AIRTMP_10MIN_MAX | tx (スペース含む) × 10 |
| AIRTMP_10MIN_MAX_AQC | -99 (MISSING_INT8) に設定 |
| AIRTMP_10MIN_AVG | ta (スペース含む) × 10 |
| AIRTMP_10MIN_AVG_AQC | -99 (MISSING_INT8) に設定 |
| AIRTMP_10MIN_MINI | tn (スペース含む) × 10 |
| AIRTMP_10MIN_MINI_AQC | -99 (MISSING_INT8) に設定 |
| RHUM | u (スペース含む) × 10 |
| RHUM_AQC | -99 (MISSING_INT8) に設定 |
| ARPRSS | p (スペース含む) × 10 |
| ARPRSS_AQC | -99 (MISSING_INT8) に設定 |
| PRCRIN_10MIN | r (スペース含む) × 10 |
| PRCRIN_10MIN_AQC | -99 (MISSING_INT8) に設定 |

## 処理フロー
1. S3内の生データへの参照を含むSQSからメッセージを受信
2. 生データファイルをダウンロード
3. 生データからRUヘッダーを削除
4. セミコロン区切りのデータを解析
5. 上記のマッピングを使用して標準化されたJSONフォーマットに変換
6. 処理済みのJSONファイルをS3にアップロード

## 特記事項
- タグID: 441000144
- メモリキャッシュを使用して処理効率を向上（キャッシュ有効期限：3600秒）
- 欠損値は専用の定数で処理（MISSING_INT8: -99、MISSING_INT16: -9999、MISSING_INT32: -999999999）
- CSVのカラム名にスペースがあるため、特別な処理が必要
- 数値データは多くの場合10倍されて格納（精度向上のため）

## 依存関係
AWS SDK for Python (Boto3) - S3アクセスとSTSクライアント用
botocore - AWS例外処理用
json - JSONデータの解析と生成用
csv - CSVデータ処理用
io - 文字列IOストリーム処理用
datetime - 日時処理用
urllib.parse - URLエンコード/デコード用
uuid - ユニークIDの生成用
os - 環境変数アクセス用
sys - システム関連機能用

すべて標準ライブラリです。
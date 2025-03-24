# HUNMHS 1時間データ処理 Lambda

## 概要
このLambda関数は、ハンガリー気象局（HUNMHS）の1時間ごとの観測生データを処理します。S3からSQSを通じてデータを受信し、RUヘッダーを削除した後、標準化されたJSONフォーマットに変換し、S3に保存します。

## PSR 資料
このプログラムのPSR資料（問題特定書）については以下のURLを参照してください：
[HUNMHS データ処理 PSR 資料](https://docs.google.com/spreadsheets/d/11X06dqibNCYOstbVhfgnNBuZ2cbdI9NOr6g13mUYSG4/edit?gid=1848697441#gid=1848697441)

## 技術仕様
- **ランタイム**: Python 3.12
- **実行環境**: AWS Lambda
- **入力ソース**: SQS (S3からのデータを含む)
- **出力先**: S3
- **入力フォーマット**: CSVライクなフォーマット (セミコロン区切り、`;` を区切り文字として使用)

## データマッピング
プログラムは以下のように生データフィールドをJSON要素にマッピングします。各項目の単位と変換処理も記載しています：

| JSON要素 | 生データフィールド (CSVカラム) | 単位 | 変換処理 |
|--------------|----------------|----------------|----------------|
| LCLID | StationNumber | - | そのまま使用 |
| ID_GLOBAL_MNET | `HUNMHS_{StationNumber}` | - | プロバイダ名とステーション番号を連結 |
| HVIS | v (スペース含む) | メートル [m] | そのまま使用（整数値） |
| HVIS_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDSPD | fs (スペース含む) | メートル/秒 [m/s] | 10倍して整数化（精度向上のため） |
| WNDSPD_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDSPD_1HOUR_AVG | f (スペース含む) | メートル/秒 [m/s] | 10倍して整数化（精度向上のため） |
| WNDSPD_1HOUR_AVG_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDDIR_1HOUR_AVG | fd (スペース含む) | 度 [°] | そのまま使用（整数値） |
| WNDDIR_1HOUR_AVG_AQC | - | - | -99 (MISSING_INT8) に設定 |
| GUSTS_1HOUR | fx (スペース含む) | メートル/秒 [m/s] | 10倍して整数化（精度向上のため） |
| GUSTS_1HOUR_AQC | - | - | -99 (MISSING_INT8) に設定 |
| GUSTD_1HOUR | fxd (スペース含む) | 度 [°] | そのまま使用（整数値） |
| GUSTD_1HOUR_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDDIR | fsd (スペース含む) | 度 [°] | そのまま使用（整数値） |
| WNDDIR_AQC | - | - | -99 (MISSING_INT8) に設定 |
| AIRTMP | t (スペース含む) | 摂氏 [°C] | 10倍して整数化（精度向上のため） |
| AIRTMP_AQC | - | - | -99 (MISSING_INT8) に設定 |
| AIRTMP_1HOUR_MAX | tx (スペース含む) | 摂氏 [°C] | 10倍して整数化（精度向上のため） |
| AIRTMP_1HOUR_MAX_AQC | - | - | -99 (MISSING_INT8) に設定 |
| AIRTMP_1HOUR_AVG | ta (スペース含む) | 摂氏 [°C] | 10倍して整数化（精度向上のため） |
| AIRTMP_1HOUR_AVG_AQC | - | - | -99 (MISSING_INT8) に設定 |
| AIRTMP_1HOUR_MINI | tn (スペース含む) | 摂氏 [°C] | 10倍して整数化（精度向上のため） |
| AIRTMP_1HOUR_MINI_AQC | - | - | -99 (MISSING_INT8) に設定 |
| RHUM | u (スペース含む) | パーセント [%] | 10倍して整数化（精度向上のため） |
| RHUM_AQC | - | - | -99 (MISSING_INT8) に設定 |
| SSPRSS | p0 (スペース含む) | ヘクトパスカル [hPa] | 10倍して整数化（精度向上のため） |
| SSPRSS_AQC | - | - | -99 (MISSING_INT8) に設定 |
| ARPRSS | p (スペース含む) | ヘクトパスカル [hPa] | 10倍して整数化（精度向上のため） |
| ARPRSS_AQC | - | - | -99 (MISSING_INT8) に設定 |
| PRCRIN_1HOUR | r (スペース含む) | ミリメートル [mm] | 10倍して整数化（精度向上のため） |
| PRCRIN_1HOUR_AQC | - | - | -99 (MISSING_INT8) に設定 |
| GLBRAD_1HOUR | sr (スペース含む) | ワット/平方メートル [W/m²] | 10倍して整数化（精度向上のため） |
| GLBRAD_1HOUR_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WX_original | we (スペース含む) | コード | ハンガリー語の天気表現に変換 |
| WX_original_AQC | - | - | -99 (MISSING_INT8) に設定 |

## 処理フロー
1. S3内の生データへの参照を含むSQSからメッセージを受信
2. 生データファイルをダウンロード
3. 生データからRUヘッダーを削除
4. セミコロン区切りのデータを解析
5. 各観測局ごとに最新のデータを抽出
6. 上記のマッピングを使用して標準化されたJSONフォーマットに変換
7. 処理済みのJSONファイルをS3にアップロード

## 特記事項
- タグID: 441000143
- メモリキャッシュを使用して処理効率を向上（キャッシュ有効期限：3600秒）
- 欠損値は専用の定数で処理：
  - MISSING_INT8: -99（8ビット整数での欠損値）
  - MISSING_INT16: -9999（16ビット整数での欠損値）
  - MISSING_INT32: -999999999（32ビット整数での欠損値）
- 無効値も定数で定義：
  - INVALID_INT16: -11111（16ビット整数での無効値）
  - INVALID_INT32: -1111111111（32ビット整数での無効値）
- CSVのカラム名にスペースが含まれているため、列の抽出には特別な処理が必要
- 浮動小数点の精度を保持するため、多くの数値データは10倍されて整数として格納
- 天気コード（`we`列）はハンガリー語の天気表現に変換
- 同一観測局の複数レコードがある場合、最新のタイムスタンプを持つデータのみが処理される

## 天気コード変換表
コードは以下のようにハンガリー語に変換されます：
- 1: derült（晴れ）
- 2: kissé felhős（少し曇り）
- 3: közepesen felhős（やや曇り）
- 4: erősen felhős（かなり曇り）
- 5: borult（曇り）
- 6: fátyolfelhős（薄雲）
- 7: ködös（霧）
- 9: derült, párás（晴れ、霞）
- 10: közepesen felhős, párás（やや曇り、霞）
- 11: borult, párás（曇り、霞）
- 12: erősen fátyolfelhős（強い薄雲）
- 101: szitálás（霧雨）
- 102: eső（雨）
- 103: zápor（にわか雨）
- 104: zivatar esővel（雷雨）
- 105: ónos szitálás（凍霧雨）
- 106: ónos eső（凍雨）
- 107: hószállingózás（粉雪）
- 108: havazás（雪）
- 109: hózápor（にわか雪）
- 110: havaseső（みぞれ）
- 112: hózivatar（雷雪）
- 202: erős eső（強い雨）
- 203: erős zápor（強いにわか雨）
- 208: erős havazás（強い雪）
- 209: erős hózápor（強いにわか雪）
- 304: zivatar záporral（にわか雨を伴う雷雨）
- 310: havaseső zápor（にわかみぞれ）
- 500: hófúvás（地吹雪）
- 600: jégeső（雹）
- 601: dörgés（雷鳴）

## 依存関係
- AWS SDK for Python (Boto3) - S3アクセスとSTSクライアント用
- botocore - AWS例外処理用
- json - JSONデータの解析と生成用
- csv - CSVデータ処理用
- io - 文字列IOストリーム処理用
- datetime - 日時処理用
- uuid - ユニークIDの生成用
- os - 環境変数アクセス用
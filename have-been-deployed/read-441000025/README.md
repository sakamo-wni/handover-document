# KNMI NetCDFデータ処理 Lambda

## 概要
このLambda関数は、オランダ気象局（KNMI）の観測生データ（NetCDF形式）を処理します。S3からSQSを通じてデータを受信し、RUヘッダーを削除した後、標準化されたJSONフォーマットに変換し、S3に保存します。

## PSR 資料
このプログラムのPSR資料については以下のURLを参照してください：
[KNMI データ処理 PSR 資料](https://docs.google.com/spreadsheets/d/11X06dqibNCYOstbVhfgnNBuZ2cbdI9NOr6g13mUYSG4/edit?gid=1848697441#gid=1848697441)

## 技術仕様
- **ランタイム**: Python 3.12
- **実行環境**: AWS Lambda
- **入力ソース**: SQS (S3からのデータを含む)
- **出力先**: S3
- **入力フォーマット**: NetCDF (RUヘッダー付き)
- **依存ライブラリ**: netCDF4 (Lambda layerとして追加)

## データマッピング
プログラムは以下のように生データフィールドをJSON要素にマッピングします：

| JSON要素 | NetCDF変数 | 単位 | 変換処理 |
|--------------|----------------|----------------|----------------|
| LCLID | station | - | 文字列として使用 |
| ID_GLOBAL_MNET | `KNMI_{station}` | - | プロバイダ名とステーション番号を連結 |
| HVIS | vv | メートル [m] | そのまま使用（整数値） |
| HVIS_AQC | - | - | -99 (MISSING_INT8) に設定 |
| AMTCLD_8 | nc | オクタ [okta] | そのまま使用（整数値） |
| AMTCLD_8_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDSPD_MD | ffs | メートル/秒 [m/s] | 10倍して整数化（精度向上のため） |
| WNDSPD_MD_AQC | - | - | -99 (MISSING_INT8) に設定 |
| GUSTS | fxs | メートル/秒 [m/s] | 10倍して整数化（精度向上のため） |
| GUSTS_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDSPD_1HOUR_MAX | Sax1H | メートル/秒 [m/s] | 10倍して整数化（精度向上のため） |
| WNDSPD_1HOUR_MAX_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDSPD_1HOUR_AVG | Sav1H | メートル/秒 [m/s] | 10倍して整数化（精度向上のため） |
| WNDSPD_1HOUR_AVG_AQC | - | - | -99 (MISSING_INT8) に設定 |
| GUSTS_1HOUR | Sx1H | メートル/秒 [m/s] | 10倍して整数化（精度向上のため） |
| GUSTS_1HOUR_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDDIR_MD | dd | 度 [°] | そのまま使用（整数値） |
| WNDDIR_MD_AQC | - | - | -99 (MISSING_INT8) に設定 |
| AIRTMP_10MIN_MAX | tx | 摂氏 [°C] | 10倍して整数化（精度向上のため） |
| AIRTMP_10MIN_MAX_AQC | - | - | -99 (MISSING_INT8) に設定 |
| AIRTMP | ta | 摂氏 [°C] | 10倍して整数化（精度向上のため） |
| AIRTMP_AQC | - | - | -99 (MISSING_INT8) に設定 |
| AIRTMP_10MIN_MINI | tn | 摂氏 [°C] | 10倍して整数化（精度向上のため） |
| AIRTMP_10MIN_MINI_AQC | - | - | -99 (MISSING_INT8) に設定 |
| RHUM | rh | パーセント [%] | 10倍して整数化（精度向上のため） |
| RHUM_AQC | - | - | -99 (MISSING_INT8) に設定 |
| DEWTMP | td | 摂氏 [°C] | 10倍して整数化（精度向上のため） |
| DEWTMP_AQC | - | - | -99 (MISSING_INT8) に設定 |
| ARPRSS | p0 | ヘクトパスカル [hPa] | 10倍して整数化（精度向上のため） |
| ARPRSS_AQC | - | - | -99 (MISSING_INT8) に設定 |
| PRCINT | rg または pg | ミリメートル/時 [mm/h] | 10倍して整数化（精度向上のため） |
| PRCINT_AQC | - | - | -99 (MISSING_INT8) に設定 |
| PRCRIN_1HOUR | R1H | ミリメートル [mm] | 10倍して整数化（精度向上のため） |
| PRCRIN_1HOUR_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WX_original | ww | コード | オランダ語の天気表現に変換 |
| WX_original_AQC | - | - | -99 (MISSING_INT8) に設定 |

## 処理フロー
1. S3内の生データへの参照を含むSQSからメッセージを受信
2. S3から NetCDF ファイルをダウンロード
3. ファイルからRUヘッダーを削除
4. 一時ファイルにデータ部分を保存
5. netCDF4 ライブラリを使用してNetCDFデータを読み込み
6. 上記のマッピングを使用して標準化されたJSONフォーマットに変換
7. 処理済みのJSONファイルをS3にアップロード
8. 一時ファイルを削除

## 特記事項
- タグID: 441000025
- メモリキャッシュを使用して処理効率を向上（キャッシュ有効期限：3600秒）
- 欠損値は専用の定数で処理：
  - MISSING_INT8: -99（8ビット整数での欠損値）
  - MISSING_INT16: -9999（16ビット整数での欠損値）
  - MISSING_INT32: -999999999（32ビット整数での欠損値）
- 無効値も定数で定義：
  - INVALID_INT16: -11111（16ビット整数での無効値）
  - INVALID_INT32: -1111111111（32ビット整数での無効値）
- 浮動小数点の精度を保持するため、多くの数値データは10倍されて整数として格納
- 降水強度（PRCINT）は`rg`（Rain Gauge）を優先し、なければ`pg`（PWS）を使用
- NetCDFの処理のために`/tmp`ディレクトリに一時ファイルを作成
- 複数の観測局データが含まれていた場合、全ての観測局のデータを処理

## Lambda Layer
このLambda関数は `netCDF4` ライブラリを Lambda Layer として使用しています。このライブラリはNetCDFファイルの読み込みと解析に必要です。Layer には以下のライブラリを含める必要があります：
- netCDF4
- numpy
- HDF5関連の依存ライブラリ

## 現在天気コード変換表
`ww`コードは以下のようにオランダ語の天気表現に変換されます：

| コード | オランダ語 | 日本語訳 |
|--------|------------|----------|
| 0 | Helder | 晴れ |
| 1 | Bewolking afnemend over het afgelopen uur | 過去1時間で雲が減少 |
| 2 | Bewolking onveranderd over het afgelopen uur | 過去1時間で雲に変化なし |
| 3 | Bewolking toenemend over het afgelopen uur | 過去1時間で雲が増加 |
| 4 | Heiigheid of rook, of stof zwevend in de lucht | もやまたは煙、または空中に浮遊する塵 |
| 5 | Heiigheid of rook, of stof zwevend in de lucht | もやまたは煙、または空中に浮遊する塵 |
| 10 | Nevel | 霞 |
| 12 | Onweer op afstand | 遠雷 |
| 18 | Squalls | スコール |
| 20 | Mist | 霧 |
| 21 | Neerslag | 降水 |
| 22 | Motregen (niet onderkoeld) of Motsneeuw | 霧雨（非凍結）または霧雪 |
| 23 | Regen (niet onderkoeld) | 雨（非凍結） |
| 24 | Sneeuw | 雪 |
| 25 | Onderkoelde (mot)regen | 凍結（霧）雨 |
| 26 | Onweer met of zonder neerslag | 降水を伴うまたは伴わない雷雨 |
| 30 | Mist | 霧 |
| 32 | Mist of ijsmist, dunner geworden gedurende het afgelopen uur | 過去1時間で薄くなった霧または氷霧 |
| 33 | Mist of ijsmist, geen merkbare verandering gedurende het afgelopen uur | 過去1時間で変化のない霧または氷霧 |
| 34 | Mist of ijsmist, opgekomen of dikker geworden gedurende het afgelopen uur | 過去1時間で現れたまたは濃くなった霧または氷霧 |
| 35 | Mist met aanzetting van ruige rijp | 粗い霜を伴う霧 |
| 40 | NEERSLAG | 降水 |
| 41 | Neerslag, licht of middelmatig | 降水、弱いまたは中程度 |
| 42 | Neerslag, zwaar | 降水、強い |
| 50 | MOTREGEN | 霧雨 |
| 51 | Motregen niet onderkoeld, licht | 霧雨 非凍結、弱い |
| 52 | Motregen niet onderkoeld, matig | 霧雨 非凍結、中程度 |
| 53 | Motregen niet onderkoeld, dicht | 霧雨 非凍結、密な |
| 54 | Motregen onderkoeld, licht | 霧雨 凍結、弱い |
| 55 | Motregen onderkoeld, matig | 霧雨 凍結、中程度 |
| 56 | Motregen onderkoeld, dicht | 霧雨 凍結、密な |
| 57 | Motregen en regen, licht | 霧雨と雨、弱い |
| 58 | Motregen en regen, matig of zwaar | 霧雨と雨、中程度または強い |
| 60 | REGEN | 雨 |
| 61 | Regen niet onderkoeld, licht | 雨 非凍結、弱い |
| 62 | Regen niet onderkoeld, matig | 雨 非凍結、中程度 |
| 63 | Regen niet onderkoeld, zwaar | 雨 非凍結、強い |
| 64 | Regen onderkoeld, licht | 雨 凍結、弱い |
| 65 | Regen onderkoeld, matig | 雨 凍結、中程度 |
| 66 | Regen onderkoeld, zwaar | 雨 凍結、強い |
| 67 | Regen of motregen en sneeuw, licht | 雨または霧雨と雪、弱い |
| 68 | Regen of motregen en sneeuw, matig of zwaar | 雨または霧雨と雪、中程度または強い |
| 70 | SNEEUW | 雪 |
| 71 | Sneeuw, licht | 雪、弱い |
| 72 | Sneeuw, matig | 雪、中程度 |
| 73 | Sneeuw, zwaar | 雪、強い |
| 74 | IJsregen, licht | 氷雨、弱い |
| 75 | IJsregen, matig | 氷雨、中程度 |
| 76 | IJsregen, zwaar | 氷雨、強い |
| 77 | Motsneeuw | 霧雪 |
| 78 | IJskristallen | 氷晶 |
| 80 | Bui of neerslag onderbroken | にわか雨または断続的な降水 |
| 81 | Regen(bui) of regen onderbroken, licht | 雨（にわか）または断続的な雨、弱い |
| 82 | Regen(bui) of regen onderbroken, matig | 雨（にわか）または断続的な雨、中程度 |
| 83 | Regen(bui) of regen onderbroken, zwaar | 雨（にわか）または断続的な雨、強い |
| 84 | Regen(bui) of regen onderbroken, zeer zwaar | 雨（にわか）または断続的な雨、非常に強い |
| 85 | Sneeuw(bui) of sneeuw onderbroken, licht | 雪（にわか）または断続的な雪、弱い |
| 86 | Sneeuw(bui) of sneeuw onderbroken, matig | 雪（にわか）または断続的な雪、中程度 |
| 87 | Sneeuw(bui) of sneeuw onderbroken, zwaar | 雪（にわか）または断続的な雪、強い |
| 89 | Hagel(bui) of hagel onderbroken | 雹（にわか）または断続的な雹 |
| 90 | Onweer | 雷雨 |
| 91 | Onweer, licht of matig, zonder neerslag | 雷雨、弱いまたは中程度、降水なし |
| 92 | Onweer, licht of matig, met regen en/of sneeuw(buien) | 雷雨、弱いまたは中程度、雨および/または雪（にわか）を伴う |
| 93 | Onweer, licht of matig, met hagel | 雷雨、弱いまたは中程度、雹を伴う |
| 94 | Onweer, zwaar, zonder neerslag | 雷雨、強い、降水なし |
| 95 | Onweer, zwaar, met regen en/of sneeuw(buien) | 雷雨、強い、雨および/または雪（にわか）を伴う |
| 96 | Onweer, zwaar, met hagel | 雷雨、強い、雹を伴う |

## 依存関係
- AWS SDK for Python (Boto3) - S3アクセスとSTSクライアント用
- botocore - AWS例外処理用
- json - JSONデータの解析と生成用
- netCDF4 - NetCDFファイルの読み込みと処理用 (Lambda Layerとして提供)
- os - ファイルシステム操作用
- datetime - 日時処理用
- uuid - ユニークIDの生成用
- urllib.parse - URL解析用
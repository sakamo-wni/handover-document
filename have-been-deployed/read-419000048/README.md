# カナダ気象局データ処理 Lambda

## 概要
このLambda関数は、カナダ気象局（MSC: Meteorological Service of Canada）の観測データを処理します。すべての州・準州からXML形式の気象観測データを取得し、標準化されたJSONフォーマットに変換してS3に保存します。各州ごとの最新の気象データを統合し、統一された形式で提供します。

## PSR 資料
このプログラムのPSR資料（問題特定書）については以下のURLを参照してください：
[MSC データ処理 PSR 資料](https://docs.google.com/spreadsheets/d/1BPBsn6UYEDRWf-U4SOrl1dxWSmO_7AEdrgKPas9b-UE/edit?gid=1848697441#gid=1848697441)


## 技術仕様
- **ランタイム**: Python 3.12
- **実行環境**: AWS Lambda
- **入力ソース**: MSCのウェブサーバー上のXMLファイル
- **出力先**: S3
- **入力フォーマット**: XML
- **出力フォーマット**: 標準化JSON
- **対象地域**: カナダの全13州・準州（AB, BC, MB, NB, NL, NS, NT, NU, ON, PE, QC, SK, YT）

## データマッピング
プログラムは以下のようにXML要素をJSON要素にマッピングします：

| JSON要素 | XML要素 | 単位 | 変換処理 |
|--------------|----------------|----------------|----------------|
| LCLID | climate_station_number | - | 文字列として使用 |
| ID_GLOBAL_MNET | `MSC_{climate_station_number}` | - | プロバイダ名と観測局IDを連結 |
| HVIS | horizontal_visibility | メートル [m] | km → m に変換 (× 1000) |
| HVIS_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDSPD | wind_speed | メートル/秒 [m/s] | km/h → m/s に変換 (× 0.277778) 後、10倍して整数化 |
| WNDSPD_AQC | - | - | -99 (MISSING_INT8) に設定 |
| GUSTS | wind_gust_speed | メートル/秒 [m/s] | km/h → m/s に変換 (× 0.277778) 後、10倍して整数化 |
| GUSTS_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDDIR_16 | wind_direction | 16方位 | 方位記号(N,NNE等)を対応する数値(1-16)に変換 |
| WNDDIR_16_AQC | - | - | -99 (MISSING_INT8) に設定 |
| AIRTMP | air_temperature | 摂氏 [°C] | 10倍して整数化 |
| AIRTMP_AQC | - | - | -99 (MISSING_INT8) に設定 |
| DEWTMP | dew_point | 摂氏 [°C] | 10倍して整数化 |
| DEWTMP_AQC | - | - | -99 (MISSING_INT8) に設定 |
| RHUM | relative_humidity | パーセント [%] | 10倍して整数化 |
| RHUM_AQC | - | - | -99 (MISSING_INT8) に設定 |
| AMTCLD_8 | total_cloud_cover | オクタ [okta] | そのまま整数値として使用 ('/'の場合は無効値) |
| AMTCLD_8_AQC | - | - | -99 (MISSING_INT8) に設定 |
| SSPRSS | mean_sea_level | ヘクトパスカル [hPa] | kPa → hPa に変換 (× 10) 後、10倍して整数化 |
| SSPRSS_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WX_original | present_weather | テキスト | そのまま使用 |
| WX_original_AQC | - | - | -99 (MISSING_INT8) に設定 |

## 処理フロー
1. 各州・準州のディレクトリをスキャン
2. 各州の最新のXMLファイルを特定
3. XMLファイルをダウンロードして解析
4. データを標準フォーマットに変換
   - 単位変換（km/h→m/s、km→m、kPa→hPa）
   - 欠損値や無効値の処理
   - 16方位風向の変換
5. 全州のデータを統合
6. 最新の観測日時を決定
7. 統合データをJSONとしてS3に保存

## 特記事項
- JSONデータのタグIDは環境変数から設定
- メモリキャッシュを使用して処理効率を向上（キャッシュ有効期限：3600秒）
- 欠損値は専用の定数で処理：
  - MISSING_INT8: -99（8ビット整数での欠損値）
  - MISSING_INT16: -9999（16ビット整数での欠損値）
  - MISSING_INT32: -999999999（32ビット整数での欠損値）
- 無効値も定数で定義：
  - INVALID_INT8: -111（8ビット整数での無効値）
  - INVALID_INT16: -11111（16ビット整数での無効値）
  - INVALID_INT32: -1111111111（32ビット整数での無効値）
- 風向は16方位（N, NNE, NE, ...）から数値（1-16）に変換
- 専用の単位変換処理：
  - 風速と突風速度: km/h → m/s (× 0.277778) → 10倍整数化
  - 視程: km → m (× 1000)
  - 海面気圧: kPa → hPa (× 10) → 10倍整数化
- XMLのネスト構造とNamespaceを考慮した特殊な解析手法を使用

## S3保存パス
```
data/{tagid}/{YYYY}/{MM}/{DD}/{YYYYMMDDHHmmSS}.{uuid}
```

## 入力データソース
カナダ気象局のXMLディレクトリから各州のデータを取得：
```
https://dd.weather.gc.ca/observations/xml/{province}/hourly/
```
ここで、`{province}`は州コード（AB, BC, MB等）

## 依存関係
- AWS SDK for Python (Boto3) - S3アクセス用
- xml.etree.ElementTree - XMLパース用
- urllib.request - ウェブリクエスト用
- json - JSONデータの生成と解析用
- re - 正規表現処理用
- datetime - 日時処理用
- uuid - ユニークID生成用
- os - 環境変数アクセス用

## 環境変数
- **save_bucket**: 処理済みデータを保存するS3バケット
- **tagid**: データの識別子
- **WEATHER_BASE_URL**: カナダ気象局のベースURL（デフォルト: "https://dd.weather.gc.ca/observations/xml"）

## イベントブリッジ連携
- EventBridgeから受け取った実行時間を観測日時として使用可能
- イベント内に`time`フィールドが存在する場合、その時間を観測日時として利用

## エラーハンドリング
- 各州ごとに独立して処理し、1つの州でエラーが発生しても他の州の処理は続行
- ネットワークエラーや解析エラーの詳細なログ出力
- 有効なデータが取得できない場合のエラーレスポンス生成
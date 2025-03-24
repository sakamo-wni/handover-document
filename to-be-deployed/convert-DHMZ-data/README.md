# DHMZ XMLデータ処理 Lambda

## 概要
このLambda関数は、クロアチア気象水文局（DHMZ: Državni hidrometeorološki zavod）のXML形式の気象データを処理します。指定URLからデータをダウンロードし、S3に保存した後、観測データまたは観測局データとしてJSON形式に変換してS3に保存します。

## 技術仕様
- **ランタイム**: Python 3.x
- **実行環境**: AWS Lambda
- **入力ソース**: DHMZのウェブサービス（URL）
- **出力先**: S3バケット
- **入力フォーマット**: XML
- **出力フォーマット**: 
  - 生データ: XML
  - 観測局データ: GeoJSON
  - 観測データ: 標準化JSON

## データマッピング
プログラムは以下のようにXML要素をJSON要素にマッピングします：

### 観測局データ (GeoJSON)
| GeoJSON要素 | XML要素 | 説明 | 変換処理 |
|--------------|----------------|----------------|----------------|
| coordinates[0] | Lon | 経度 [度] | 浮動小数点に変換 |
| coordinates[1] | Lat | 緯度 [度] | 浮動小数点に変換 |
| LCLID | GradIme | 観測局名 | そのまま使用 |
| LNAME | GradIme | 観測局名 | そのまま使用 |
| CNTRY | - | 国コード | "HR"（クロアチア）固定値 |

### 観測データ (JSON)
| JSON要素 | XML要素 | 単位 | 変換処理 |
|--------------|----------------|----------------|----------------|
| LCLID | GradIme | - | そのまま使用 |
| ID_GLOBAL_MNET | `DHMZ_{GradIme}` | - | プロバイダ名と地点名を連結 |
| AIRTMP | Temp | 摂氏 [°C] | 10倍して整数化（精度向上のため） |
| AIRTMP_AQC | - | - | -99 (MISSING_INT8) に設定 |
| RHUM | Vlaga | パーセント [%] | 10倍して整数化（精度向上のため） |
| RHUM_AQC | - | - | -99 (MISSING_INT8) に設定 |
| ARPRSS | Tlak | ヘクトパスカル [hPa] | 10倍して整数化（精度向上のため） |
| ARPRSS_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDDIR_8 | VjetarSmjer | 8方位 | N->1, NE->2, E->3, SE->4, S->5, SW->6, W->7, NW->8 に変換 |
| WNDDIR_8_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WNDSPD | VjetarBrzina | メートル/秒 [m/s] | 10倍して整数化（精度向上のため） |
| WNDSPD_AQC | - | - | -99 (MISSING_INT8) に設定 |
| WX_original | Vrijeme | テキスト | そのまま使用（クロアチア語の天気表現） |
| WX_original_AQC | - | - | -99 (MISSING_INT8) に設定 |

## 処理フロー
1. 環境変数の検証
2. 指定URLからXMLデータをダウンロード
3. 生データをS3に保存
4. トリガー種別に応じた処理：
   - **StationRule**: 観測局データとして処理
     - XMLを解析し、観測局の位置情報を抽出
     - GeoJSON形式に変換してS3に保存
   - **ObservationRule**: 観測データとして処理
     - XMLを解析し、観測値と観測日時を抽出
     - 標準化JSONに変換してS3に保存

## 特記事項
- タグIDは環境変数から設定
- 観測値の時刻は10分間隔に正規化（例：12:08 → 12:00）
- 欠損値は専用の定数で処理：
  - MISSING_INT8: -99（8ビット整数での欠損値）
  - MISSING_INT16: -9999（16ビット整数での欠損値）
  - MISSING_INT32: -999999（32ビット整数での欠損値）
- 無効値も定数で定義：
  - INVALID_INT16: -11111（16ビット整数での無効値）
  - INVALID_INT32: -1111111（32ビット整数での無効値）
- 気圧データは「*」記号がある場合、それを削除して処理
- 風向は文字列表現（N, NE, E, SEなど）から8方位の数値（1-8）に変換

## トリガー種別による処理の違い
- **StationRule**: 観測局データのみを処理
- **ObservationRule**: 観測データのみを処理
- トリガー情報がない場合: 観測データのみを処理（デフォルト）

## S3保存パス
### 生データ
```
{tagid}/{YYYY}/{MM}/{DD}/{YYYYMMDDHHmmSS}_raw.xml
```

### 観測局データGeoJSON
```
metadata/spool/DHMZ/metadata.json
```

### 観測データJSON
```
data/{tagid}/{YYYY}/{MM}/{DD}/{YYYYMMDDHHmmSS}.{uuid}
```

## 入力XML形式
XMLデータには以下の主要要素が含まれています：

- **DatumTermin**: 観測日時の情報
  - **Datum**: 日付（DD.MM.YYYY形式）
  - **Termin**: 時間（HH形式）

- **Grad**: 都市（観測地点）情報
  - **GradIme**: 都市名
  - **Lat**: 緯度
  - **Lon**: 経度
  - **Podatci**: 観測データ
    - **Temp**: 気温
    - **Vlaga**: 湿度
    - **Tlak**: 気圧
    - **VjetarSmjer**: 風向
    - **VjetarBrzina**: 風速
    - **Vrijeme**: 天気状況

## 依存関係
- AWS SDK for Python (Boto3) - S3アクセス用
- urllib.request - ウェブリクエスト用
- xml.etree.ElementTree - XMLデータ解析用
- json - JSONデータの生成用
- datetime - 日時処理用
- uuid - ユニークID生成用
- os - 環境変数アクセス用

## 環境変数
- **RawDataBucket**: 生データを保存するS3バケット
- **ConvertedBucket**: 変換済みデータを保存するS3バケット
- **tagid**: データの識別子
- **URL**: データを取得するDHMZのURL
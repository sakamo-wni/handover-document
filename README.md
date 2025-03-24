# 引き継ぎ資料

このリポジトリは、坂本の引き継ぎ資料です。

## ディレクトリ構造

- `have-been-deployed`: これまでにリリースしたプログラム
- `to-be-deployed`: 現在取り掛かっているプログラム

すべてのプログラムには、それぞれのコードの説明用のREADME.mdが追加されています。

## have-been-deployed（リリース済みプログラム）

| プログラム名 | 機能 | 対象機関 |
|------------|------|---------|
| convert-dmi-table | GeoJSON作成 | デンマーク気象局(DMI) |
| get-and-read-DMI-OBS | JSON作成 | デンマーク気象局(DMI) |
| convert-dwd-aws | JSON作成 | ドイツ気象局(DWD) AWS |
| convert-dwd-aws-table | GeoJSON作成 | ドイツ気象局(DWD) AWS |
| convert-dwd-synop | JSONとGeoJSON作成 | ドイツ気象局(DWD) SYNOP |
| get-and-read-EMHI | JSONとGeoJSON作成 | エストニア気象局(EMHI) |
| read-441000025 | JSON作成 | オランダ気象局(KNMI) |
| convert-knmi-table | GeoJSON作成 | オランダ気象局(KNMI) |
| read-441000143 | 10分データのJSON作成 | ハンガリー気象局(HUNMHS) |
| read-441000144 | 1時間データのJSON作成 | ハンガリー気象局(HUNMHS) |
| read-integrated-canada | GeoJSON作成 | カナダ気象局(MSC) |

## to-be-deployed（開発中プログラム）

| プログラム名 | 機能 | 対象機関 |
|------------|------|---------|
| convert-dhmz-data | JSONとGeoJSON作成 | クロアチア気象局(DHMZ) |
| convert-dmc-obs | JSON作成 | チリ気象局(DMC) |
| convert-dmc-table | GeoJSON作成 | チリ気象局(DMC) |
| convert-rmi-obs | JSON作成 | ベルギー気象局(RMI) |
| convert-rmi-table | GeoJSON作成 | ベルギー気象局(RMI) |
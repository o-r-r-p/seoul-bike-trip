# seoul-bike-trip
This repository is to analysis the Bike Trip Duration while considering atmospheric factors

## Resource of Dataset
[https://www.kaggle.com/datasets/tagg27/seoul-bike-trip?select=cleaned_seoul_bike_data.csv](https://www.kaggle.com/datasets/tagg27/seoul-bike-trip?select=cleaned_seoul_bike_data.csv)

## About Dataset
- 高度道路交通システム（ITS）と旅行者情報システムの進歩のためには、旅行時間を正確に予測することが極めて重要.
- 旅行時間を予測するために、ソウルの自転車シェアリングシステムにおけるレンタル自転車の旅行時間（Duration）を予測する.
- Durationの予測には、ソウルバイクのデータと気象データが組み合わせてあるtrain.csv, test.csvを用いて行った.
- over 9M records, 26 columns

## Target Variable
**Duration** 自転車の走行時間

## Models
GBDT
- LightGBM
- XGBoost
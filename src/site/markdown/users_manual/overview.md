
# DataCell User's Manual : Overview

## Installation

### 動作条件

- Java8
- maven3
- Linux または Mac OS X
  - 動作確認環境はUbuntu Linux 16.04LTS, OS X Sierra

### 事前準備

プログラムの呼び出しは以下の通りである。

これだとプログラムの呼び出しが面倒なので、aliasを設定して短い名前で呼び出せるようにする。
(aliasを設定するのは、標準入力からのデータの読み取りを可能とするため。）


## DcLoad

タブ区切りテキストファイルをDataCellデータベースに入れる。

### 例

1. まず２カラムのタブ区切りテキストファイルを用意する。1カラム目がIDで2カラム目がvalueである。
2. 以下のようにしてデータを読み取る。DBが存在しなければ作成する。


    cat datafile.txt | ¥
    dc_load -db db_name -ds dataset_name -p predicate_name -op=s


例えば以下のようなデータがあった時、
<code>-op</code>オプションの値によって、DB内のデータは以下に示すとおりになる。

    key    value
    A      1
    A      1
    A      2
    B      3


#### s : 単純なデータロード

DB内のデータは以下の通り。DB内のデータは入力データと同一となる。

    key    value
    A      1
    A      1
    A      2
    B      3

#### kv : put row if the key-value pair is absent.

DB内のデータは以下の通り。key-valueの組みの重複はとり除かれる。

    key    value
    A      1
    A      2
    B      3

#### k : put row if the key is absent.

DB内のデータは以下の通り。keyの重複はとり除かれる。最初に入ったデータがDB中に残る。

    key    value
    A      1
    B      3

#### r : put row with replacing values.

DB内のデータは以下の通り。keyの重複はとり除かれる。最後に入ったデータがDB中に残る。

    key    value
    A      2
    B      3


## DcList

データベースの中身を表示する。

- <code>dc_list -db db_name</code>
  - data_setのリストを表示する。
- <code>dc_list -db db_name -ds data_set_name</code>
  - predicateのリストを表示する。
- <code>dc_list -db db_name -ds dataset_name -p pred</code>
  - データを表示する。

## DcJoin


  cat datafile.txt | ¥
  dc_join -db db_name -ds dataset_name -p predicate_name ¥
          -t target_column_number [-c collapse_flg]



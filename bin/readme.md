# SQLのサブクエリのユニークキー、Nullabilityを計算する

対応しているSQL構文はごく一部で bin/parser.mly の通り。

## ユニークキーの計算

* option値
  * None: ユニークキーが存在しない
  * Some: ユニークキーが存在する
    * 「ユニークになるキーの組み合わせ」の配列
      * 空配列のときは、結果は単一レコードということ
        * group by指定なしの集約関数
        * TOP 1 (未実装)

1. fromやjoinテーブルのユニークキーのデカルト積を作成
2. 「ユニークキー -> そのカラムの値に依存するカラム」のリストを作る (dependencies)
  * 同一テーブルであれば、「主キー -> それ以外のカラム」を追加
  * on a = b というJOINであれば、「a -> b」と「b -> a」を追加
    * OUTER JOINの場合は、「nullにならないほうのカラム -> nullになりうるほうのカラム」を追加
  * whereによる事実上のjoin条件の場合も同様に考えられる（未実装）
  * CROSS JOINの場合はdependenciesは作らない（CROSS JOINの意味通り、キーはデカルト積になる）
3. dependenciesによって、1で作ったユニークキーの冗長なものを置換して削除する
4. group byが指定されていたら、そこで指定されたカラムの集合をユニークキーとして追加する

## nullabilityの計算
* 外部結合によって結合先のカラムがnullableになることの計算
* inner joinやcross joinはnullableになることはない
* left outer joinやright outer joinは、join先のテーブルがnullableになる
  * ただし、外部キー制約がついていて、参照カラムがnot nullであれば、nullableにはならない


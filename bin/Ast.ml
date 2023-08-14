type id = string

type logical_operator = And
type value_operator = Eq

type join_type =
  | InnerJoinType
  | LeftOuterJoinType
  | RightOuterJoinType
  | CrossJoinType

type agg_func =
  | Count of id
  | Sum of id
  | Avg of id
  | Min of id
  | Max of id

type column_like =
  | Column of id
  | ColumnAlias of id * id
  | AggFunc of agg_func * id
  (* | SubSelect of select * id *)
and table_like = Table of id | SubQuery of select * id
and join =
  InnerJoin of table_like * logical_expr
  | LeftOuterJoin of table_like * logical_expr
  | RightOuterJoin of table_like * logical_expr
  | CrossJoin of table_like
and select = {
  columns : column_like list;
  from : table_like;
  joins : join list;
  group_by : id list;
}
and logical_expr =
  LogicalOperator of logical_operator * logical_expr * logical_expr
| ValueOperator of value_operator * value_expr * value_expr
and value_expr =
  ValueColumn of id

let build_join join_type table_like expr =
  match join_type with
  | InnerJoinType -> InnerJoin (table_like, expr)
  | LeftOuterJoinType -> LeftOuterJoin (table_like, expr)
  | RightOuterJoinType -> RightOuterJoin (table_like, expr)
  | CrossJoinType -> CrossJoin (table_like)

let rec show_table_like table_like =
  match table_like with
  | Table id -> Printf.sprintf "Table(%s)" id
  | SubQuery (select, alias) ->
      Printf.sprintf "SubQuery(%s, %s)" (show_select select) alias
and show_logical_expr expr =
  match expr with
  | LogicalOperator (operator, left, right) ->
      Printf.sprintf "LogicalOperator(%s, %s, %s)"
        (show_logical_operator operator)
        (show_logical_expr left)
        (show_logical_expr right)
  | ValueOperator (operator, left, right) ->
      Printf.sprintf "ValueOperator(%s, %s, %s)"
        (show_value_operator operator)
        (show_value_expr left)
        (show_value_expr right)
and show_logical_operator operator =
  match operator with
  | And -> "And"
and show_value_operator operator =
  match operator with
  | Eq -> "Eq"
and show_value_expr expr =
  match expr with
  | ValueColumn id -> Printf.sprintf "Column(%s)" id
and show_join join =
  match join with
  | InnerJoin (table_like, expr) ->
      Printf.sprintf "InnerJoin(%s, %s)"
        (show_table_like table_like)
        (show_logical_expr expr)
  | LeftOuterJoin (table_like, expr) ->
      Printf.sprintf "LeftOuterJoin(%s, %s)"
        (show_table_like table_like)
        (show_logical_expr expr)
  | RightOuterJoin (table_like, expr) ->
      Printf.sprintf "RightOuterJoin(%s, %s)"
        (show_table_like table_like)
        (show_logical_expr expr)
  | CrossJoin table_like ->
      Printf.sprintf "CrossJoin(%s)"
        (show_table_like table_like)
and show_joins joins =
  Printf.sprintf "[%s]"
    (String.concat ", " (List.map show_join joins))
and show_select select =
  Printf.sprintf "Select(columns=%s, from=%s, joins=%s, group_bys=%s)"
    (show_column_likes select.columns)
    (show_table_like select.from)
    (show_joins select.joins)
    (show_columns select.group_by)
and show_columns columns =
  Printf.sprintf "[%s]"
    (String.concat ", " columns)
and show_column_likes columns =
  Printf.sprintf "[%s]"
    (String.concat ", " (List.map show_column_like columns))
and show_column_like c =
  match c with
  | Column id -> Printf.sprintf "Column(%s)" id
  | AggFunc (func, id) ->
      Printf.sprintf "AggFunc(%s, %s)"
        (show_agg_func func)
        id
  | ColumnAlias (id, alias) ->
      Printf.sprintf "ColumnAlias(%s, %s)" id alias
and show_agg_func func =
  match func with
  | Count c -> "Count (" ^ c ^ ")" 
  | Sum c -> "Sum (" ^ c ^ ")"
  | Avg c -> "Avg (" ^ c ^ ")"
  | Min c -> "Min (" ^ c ^ ")"
  | Max c -> "Max (" ^ c ^ ")"


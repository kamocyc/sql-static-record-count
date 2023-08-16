type id = string

let list_equals list1 list2 =
  let list1 = List.sort_uniq String.compare list1 in
  let list2 = List.sort_uniq String.compare list2 in
  list1 = list2

module NameMap = Map.Make (String)

module List = struct
  include List

  let find p ls msg =
    match filter p ls with
    | x::_ -> x
    | _ -> failwith ("find: " ^ msg)

  let find_one p ls msg =
    match filter p ls with
    | [x] -> x
    | [] -> failwith ("find_one (NO MATCH): " ^ msg)
    | _ -> failwith ("find_one (MULTIPLE MATCH): " ^ msg)
end

module Schema = struct
  type column_type = CInt | CNVarchar

  type column = {
    column_name : string;
    column_type : column_type;
    is_nullable : bool;
  }

  type foreign_key = {
    base_column_name : string;
    reference_table_name: string;
    reference_column_name: string;
  }

  type table = {
    table_name : string;
    columns : column list;
    unique_keys : id list list option;
    foreign_keys: foreign_key list;
  }

  type t = { tables : table list }

  let show_column column =
    let open Printf in
    sprintf "%s: %s(%s)" column.column_name
      (match column.column_type with CInt -> "int" | CNVarchar -> "varchar")
      (match column.is_nullable with false -> "not_null" | true -> "nullable")
  
  let show_table table =
    let open Printf in
    let columns =
      List.map (fun c -> show_column c) table.columns |> String.concat ", "
    in
    let unique_keys =
      match table.unique_keys with
      | None -> "None"
      | Some unique_keys ->
        List.map
          (fun uk -> "(" ^ String.concat ", " (uk) ^ ")")
          unique_keys
        |> String.concat ", "
    in
    sprintf "%scolumns: (%s), unique_keys: (%s)" (if table.table_name = "" then "" else table.table_name ^ ": ") columns unique_keys

  let show_schema schema =
    let tables = List.map (fun t -> show_table t) schema.tables in
    let tables = String.concat "\n" tables in
    Printf.sprintf "%s" tables
end

module AstChecker = struct
  let remove_superset_keys unique_keys_ =
    (* ks2 >= ks1 *)
    let is_superset ks1 ks2 =
      List.for_all (fun c -> List.mem c ks2) ks1
    in
    let rec sub unique_keys result =
      match unique_keys with
      | [] -> result
      | x :: xs ->
        let exists_superset =
          List.exists (fun uk -> List.length uk <> List.length x && is_superset uk x) unique_keys_ in
        if exists_superset then sub xs result else sub xs (x :: result)
    in
    sub unique_keys_ []

  let rec to_column_list (expr : Ast.logical_expr) =
    match expr with
    | ValueOperator (Eq, ValueColumn c1, ValueColumn c2) ->
      [(c1, c2)]
    | LogicalOperator (And, e1, e2) ->
      to_column_list e1 @ to_column_list e2

  let calc_unique_keys schema_table join_schema_tables schema_column_names joins =
    if schema_table.Schema.unique_keys = None || List.exists (fun (_, t) -> t.Schema.unique_keys = None) join_schema_tables then
      None
    else begin
      let unique_keys =
        (* joinテーブルのukを結合して、最大の集合を作る *)
        let unique_keys =
          let rec sub acc (unique_keys : id list list list) =
            match unique_keys with
            | [] -> acc
            | x :: xs ->
                let acc =
                  List.map
                    (fun uk -> List.map (fun uk' -> uk @ uk') x)
                    acc
                  |> List.flatten
                in
                sub acc xs
              in
          sub (Option.get schema_table.Schema.unique_keys) (List.map (fun (_, t) -> (Option.get t.Schema.unique_keys)) join_schema_tables) in
        let dependencies =
          (List.map (fun uk -> List.map (fun k -> (uk, k)) schema_column_names) (Option.get schema_table.unique_keys)
          @
          (List.map (fun (_, t) -> List.map (fun uk -> List.map (fun k -> (uk, k)) (List.map (fun c -> c.Schema.column_name) t.Schema.columns)) (Option.get t.Schema.unique_keys)) join_schema_tables
          |> List.flatten)
          @
          (List.map (fun join ->
            match join with
            | Ast.InnerJoin (_, cs) -> cs |> to_column_list |> List.map (fun (c1, c2) -> [([c1], c2); ([c2], c1)]) |> List.flatten
            | Ast.LeftOuterJoin (_, cs) -> cs |> to_column_list |> List.map (fun (c1, c2) ->
              if String.starts_with ~prefix:(schema_table.Schema.table_name ^ ".") c1 then
                [([c1], c2)]
              else
                [([c2], c1)]
            )|> List.flatten
            | Ast.RightOuterJoin (_, cs) -> cs |> to_column_list |> List.map (fun (c1, c2) -> 
              if String.starts_with ~prefix:(schema_table.Schema.table_name ^ ".") c1 then
                [([c2], c1)]
              else
                [([c1], c2)]) |> List.flatten
            | Ast.CrossJoin _ -> []
            ) joins))
          |> List.flatten
        in
        let dependencies =
          List.filter (fun (fs, t) -> not @@ List.exists (fun f -> f = t) fs) dependencies
        in
        print_endline @@ "dependencies: " ^ (dependencies |> List.map (fun (fs, t) -> "(" ^ (String.concat ", " fs) ^ ") -> " ^ t) |> String.concat ", ");
        let unique_keys =
          List.map
            (fun uk ->
              let rec sub uk =
                let r =
                  List.fold_left
                    (fun uk (fs, t) ->
                      List.map (fun k -> if k = t then fs else [k]) uk
                      |> List.flatten
                    )
                    uk
                    dependencies
                  |> List.sort_uniq compare in
                if list_equals r uk then r else sub r
              in
              sub uk
            )
            unique_keys in
        let unique_keys =
          remove_superset_keys unique_keys in
        unique_keys
      in
      Some unique_keys
    end
    
  let get_alias_name c =
    match c with
    | Ast.ColumnAlias (_, alias) -> alias
    | Ast.Column c -> c
    | Ast.AggFunc (_, alias) -> alias
    
  let rec get_from (from : Ast.table_like) (schema : Schema.t) =
    match from with
    | Ast.Table table ->
        let schema_table =
          List.find
            (fun (t : Schema.table) -> t.table_name = table)
            schema.tables
            (Printf.sprintf "Table not found: %s" table) in
        schema_table
    | Ast.SubQuery (query, name) ->
        let sub_table = check_select query schema in
        let columns = List.map (fun column ->
          let column_name =
            match String.split_on_char '.' column.Schema.column_name with
            | [table; column] -> name ^ "." ^ column
            | _ -> name ^ "." ^ column.Schema.column_name in
          { column with Schema.column_name = column_name }
        ) sub_table.columns in
        { sub_table with table_name = name; columns = columns }
  and check_select (select : Ast.select) (schema : Schema.t) : Schema.table =
    let ({ from; columns; joins; group_by } : Ast.select) = select in
    let schema_table = get_from from schema in
    let schema_column_names = schema_table.columns |> List.map (fun c -> c.Schema.column_name) in
    let join_schema_tables =
      List.map
        (fun (j : Ast.join) ->
          let t =
            match j with
            | InnerJoin (t, _) -> t
            | LeftOuterJoin (t, _) -> t
            | RightOuterJoin (t, _) -> t
            | CrossJoin t -> t
          in
          (j, get_from t schema))
        joins
    in
    let get_schema_column c =
      match List.filter
        (fun (sc : Schema.column) -> sc.column_name = c (* || (String.starts_with ~prefix:"." c && String.ends_with ~suffix:c sc.column_name) *))
        (schema_table.columns
        @ (List.map
              (fun (t : 'a * Schema.table) -> (snd t).columns)
              join_schema_tables
          |> List.flatten)) with
      | [sc] -> sc
      | [] -> failwith ("(get_schema_column) not found " ^ c)
      | _ -> failwith ("multiple found " ^ c) in
    let get_occuring_columns column_like =
      match column_like with
      | Ast.Column c -> [c]
      | Ast.AggFunc (aggFunc, _) -> begin
        match aggFunc with
        | Count c -> [c]
        | Sum c -> [c]
        | Avg c -> [c]
        | Min c -> [c]
        | Max c -> [c]
      end
      | Ast.ColumnAlias (c, _) -> [c]
    in
    let columns = columns |> List.map get_occuring_columns |> List.flatten |> List.map get_schema_column in
    let group_by = List.map get_schema_column group_by |> List.map (fun {Schema.column_name} -> column_name) in
    
    let foreign_keys =
      let all_foreign_keys = schema_table.foreign_keys @ (List.map (fun (_, t) -> t.Schema.foreign_keys) join_schema_tables |> List.flatten) in
      List.filter (fun { Schema.base_column_name} -> List.exists (fun {Schema.column_name} -> base_column_name = column_name) columns) all_foreign_keys in
    let can_be_considerred_as_inner_join (base_table_schema: Schema.table) (join_table: Schema.table) (join_columns: (id * id) list) =
      let foreign_keys = base_table_schema.foreign_keys in
      let base_table_columns = base_table_schema.columns in
      List.for_all (fun join_column ->
        List.exists (fun { Schema.reference_table_name; reference_column_name; base_column_name } ->
          let (column1, column2) = join_column in
          let base_table_column = List.find (fun c -> c.Schema.column_name = base_column_name) base_table_columns "" in
          not base_table_column.is_nullable &&
            join_table.Schema.table_name = reference_table_name &&
            ((column1 = reference_column_name && column2 = base_column_name) || (column2 = reference_column_name && column1 = base_column_name))
          )
          foreign_keys
      )
      join_columns
    in
    let unique_keys = calc_unique_keys schema_table join_schema_tables schema_column_names joins in
    let unique_keys =
      match unique_keys with
      | Some unique_keys ->
        let unique_keys =
          if List.length group_by = 0 then begin
            if List.exists (fun c -> match c with Ast.AggFunc _ -> true | _ -> false) select.columns then
              (* group by 指定なしの集約関数により、単一の値にまとめられた *)
              []
            else
              unique_keys
          end else
          List.concat
            [
              unique_keys;
              [ group_by ];
            ] in
        let unique_keys = unique_keys |> remove_superset_keys in
        if List.length unique_keys = 0 then failwith "no unique key";
        Some unique_keys
      | None ->
        if List.length group_by = 0 then begin
          if List.exists (fun c -> match c with Ast.AggFunc _ -> true | _ -> false) select.columns then
            (* group by 指定なしの集約関数により、単一の値にまとめられた *)
            Some []
          else
            None
        end else
          Some [ group_by ]
    in
    let unique_keys =
      match unique_keys with
      | Some unique_keys ->
        (* selectで指定されたカラムのみを含むkeysのみ残す => このときに消えた場合はどうするか => それ以降の外側のテーブルはキーが存在しない *)
        let unique_keys = List.filter (fun keys ->
          List.for_all (fun k ->
            List.exists (fun c -> get_alias_name c = k) select.columns
          ) keys
        ) unique_keys in
        List.iter (fun keys ->
          print_endline @@ "unique_keys: " ^ (String.concat ", " keys)
        ) unique_keys;
        if List.length unique_keys = 0 then None else Some unique_keys
      | None -> None
    in
    let columns =
      (* nullability *)
      let rec sub nullables (joins: (Ast.join * Schema.table) list) =
        match joins with
        | [] -> nullables
        | (join, s)::xs -> begin
          match join with
          | Ast.InnerJoin (t, columns) ->
            (* nullの追加はなし *)
            sub ((s, (Some join, false))::nullables) xs
          | CrossJoin t ->
            (* nullの追加は無し *)
            sub ((s, (Some join, false))::nullables) xs
          | LeftOuterJoin (t, columns) ->
            let b = can_be_considerred_as_inner_join schema_table s (to_column_list columns) in
            (* 基本は左にnullを追加 *)
            sub ((s, (Some join, not b))::nullables) xs
          | RightOuterJoin (t, columns) ->
            let b = can_be_considerred_as_inner_join schema_table s (to_column_list columns) in
            (* 基本は右にnullを追加 *)
            let nullables = List.map (fun (s, (j, _)) -> (s, (j, not b))) nullables in
            sub ((s, (Some join, false))::nullables) xs
        end
      in
      let nullables = sub [(schema_table, (None, false))] join_schema_tables in
      List.map
        (fun column ->
          let table_name =
            match String.split_on_char '.' column.Schema.column_name with
            | [t; _] -> t
            | _ -> failwith (column.Schema.column_name) in
          let nullable = List.find_one (fun (s, _) -> s.Schema.table_name = table_name) nullables table_name in
          { column with is_nullable = column.is_nullable || (snd @@ snd nullable) }
        )
        columns
    in
    let columns =
      List.mapi (fun i c ->
        let original_column = List.nth select.columns i in
        match original_column with
        | Ast.Column (_) -> c
        | Ast.ColumnAlias (_, alias) -> { c with Schema.column_name = alias }
        | Ast.AggFunc (_, alias) -> { c with column_name = alias }
        )
        columns in
    {
      table_name = "";
      columns;
      unique_keys = unique_keys;
      foreign_keys = foreign_keys
    }
end

let get_schema () =
  let schema : Schema.t =
    {
      tables =
        [
          {
            table_name = "order";
            columns =
              [
                {
                  column_name = "order_id";
                  column_type = CInt;
                  is_nullable = false;
                };
                {
                  column_name = "customer_id";
                  column_type = CInt;
                  is_nullable = false;
                };
                {
                  column_name = "order_name";
                  column_type = CNVarchar;
                  is_nullable = true;
                };
              ];
            unique_keys = Some [ [ "order_id" ] ];
            foreign_keys = [ { base_column_name = "customer_id"; reference_table_name = "customer"; reference_column_name = "customer_id" } ]
          };
          {
            table_name = "customer";
            columns = [
              {
                column_name = "customer_id";
                column_type = CInt;
                is_nullable = false;
              };
              {
                column_name = "customer_name";
                column_type = CNVarchar;
                is_nullable = false;
              }
            ];
            unique_keys = Some [ [ "customer_id" ] ];
            foreign_keys = []
          }
        ];
    } in
  let preprocess_schema schema =
    let { Schema.tables } = schema in
    let tables =
      tables
      |> List.map (fun ({ Schema.table_name; columns } as ts) ->
        let columns = List.map (fun ( {Schema.column_name} as cs ) -> {cs with column_name = table_name ^ "." ^ column_name}) columns in
        let unique_keys =
          match ts.unique_keys with
          | Some unique_keys -> begin
            let unique_keys = List.map (fun uk -> List.map (fun column_name -> table_name ^ "." ^ column_name) uk) unique_keys in
            if List.length unique_keys = 0 then failwith "no unique key";
            Some unique_keys
          end
          | None -> None in
        let foreign_keys = List.map (fun {Schema.base_column_name; reference_table_name; reference_column_name} ->
          {Schema.base_column_name = table_name ^ "." ^ base_column_name; reference_table_name = reference_table_name; reference_column_name = reference_table_name ^ "." ^ reference_column_name}
        ) ts.foreign_keys in
        {ts with columns; unique_keys; foreign_keys}
      ) in
    { Schema.tables } in
  let schema = preprocess_schema schema in
  schema

type simple_table_like = {
  columns: id list;
  name: string;
}

let rec get_table_like_schema (from : Ast.table_like) =
  match from with
  | Table id ->
    let schema = get_schema () in
    let table = List.find (fun t -> t.Schema.table_name = id) schema.Schema.tables "not found table" in
    (* print_endline @@ "columns = " ^ (table.Schema.columns |> List.map (fun c -> c.Schema.column_name) |> String.concat ", "); *)
    {
      columns = table.Schema.columns |> List.map (fun c -> c.Schema.column_name);
      name = id;
    }
  | SubQuery (ast, id) ->
    let schema = get_query ast in
    {
      columns = schema.columns |> List.map (fun c ->
        match String.split_on_char '.' c with
        | [t; c] -> id ^ "." ^ c
        | _ -> id ^ "." ^ c)
      ;
      name = id;
    }

and get_query (ast : Ast.select) =
  let { Ast.columns; from; joins; group_by } = ast in
  let get_alias_name c =
    match c with
    | Ast.ColumnAlias (_, alias) -> alias
    | Ast.Column c -> c
    | Ast.AggFunc (_, alias) -> alias in
  {
    columns = columns |> List.map get_alias_name;
    name = "";
  }

let get_table_like_name (table_like : Ast.table_like) =
  match table_like with
  | Table id -> id
  | SubQuery (_, id) -> id

let has_duplicates xs =
  let rec sub xs =
    match xs with
    | [] -> false
    | x::xs ->
      if List.exists (fun y -> x = y) xs then true
      else sub xs
  in
  sub xs

let rec to_fqn_table_like table_like = 
  match table_like with
  | Ast.Table id -> Ast.Table id
  | Ast.SubQuery (ast, id) -> Ast.SubQuery (to_fqn ast, id)
and to_fqn (ast : Ast.select) =
  let { Ast.columns; from; joins; group_by } = ast in
  let from = to_fqn_table_like from in
  let joins = List.map (fun (join) ->
    match join with
    | Ast.InnerJoin (table_like, expr) -> Ast.InnerJoin (to_fqn_table_like table_like, expr)
    | Ast.LeftOuterJoin (table_like, expr) -> Ast.LeftOuterJoin (to_fqn_table_like table_like, expr)
    | Ast.RightOuterJoin (table_like, expr) -> Ast.RightOuterJoin (to_fqn_table_like table_like, expr)
    | Ast.CrossJoin (table_like) -> Ast.CrossJoin (to_fqn_table_like table_like)
  ) joins in
  let from_schema = get_table_like_schema from in
  let join_schemas = List.map (fun (join) ->
    match join with
    | Ast.InnerJoin (table_like, _) -> get_table_like_schema table_like
    | Ast.LeftOuterJoin (table_like, _) -> get_table_like_schema table_like
    | Ast.RightOuterJoin (table_like, _) -> get_table_like_schema table_like
    | Ast.CrossJoin (table_like) -> get_table_like_schema table_like
  ) joins in
  let schemas = from_schema::join_schemas in
  List.iter (fun { columns } ->
    if columns |> has_duplicates then failwith "duplicate column name"
  ) schemas;
  if schemas |> List.map (fun {name} -> name) |> has_duplicates then failwith "duplicate table name";
  (* print_endline @@ "columns " ^ (columns |> List.map (fun column ->
    match column with
    | Ast.Column c -> c
    | Ast.ColumnAlias (_, alias) -> alias
    | Ast.AggFunc (_, alias) -> alias
  ) |> String.concat ", ");
  print_endline @@ "schemas: " ^ (schemas |> List.map (fun {name} -> name) |> String.concat ", "); *)
  let get_fqn target_name =
    if String.index_opt target_name '.' <> None then target_name
    else begin
      let x = List.find_one (fun { columns; name } ->
        (* print_endline @@ "columns: " ^ (columns |> String.concat ", "); *)
        List.exists (fun column_name -> (List.nth (String.split_on_char '.' column_name) 1) = target_name) columns
      ) schemas ("get_fqn (" ^ target_name ^ ")") in
      x.name ^ "." ^ target_name
    end in
  let columns = List.map (fun column ->
    match column with
    | Ast.Column c -> Ast.Column (get_fqn c)
    | Ast.ColumnAlias (c, alias) -> Ast.ColumnAlias (get_fqn c, alias)
    | Ast.AggFunc (agg_func, alias) -> begin
      match agg_func with
      | Ast.Count c -> Ast.AggFunc (Ast.Count (get_fqn c), alias)
      | Ast.Sum c -> Ast.AggFunc (Ast.Sum (get_fqn c), alias)
      | Ast.Avg c -> Ast.AggFunc (Ast.Avg (get_fqn c), alias)
      | Ast.Max c -> Ast.AggFunc (Ast.Max (get_fqn c), alias)
      | Ast.Min c -> Ast.AggFunc (Ast.Min (get_fqn c), alias)
    end
  ) columns in
  let group_by = List.map get_fqn group_by in
  (* TODO: joinの対応 *)
  { Ast.columns; from; joins; group_by }

let process_sql sql =
  let lexbuf = Lexing.from_string sql in
  let ast = Parser.prog Lexer.read lexbuf in
  let ast = to_fqn ast in
  print_endline @@ Ast.show_select ast;
  
  let schema = get_schema () in
  let result = AstChecker.check_select ast schema in
  print_endline @@ "result table: " ^ Schema.show_schema { tables = [ result ] };
  Schema.show_schema { tables = [ result ] }

let () =
  let sql = "
    SELECT
    order_id,
    order.customer_id,
    customer.customer_id,
    customer_name
    FROM
    order
    LEFT OUTER JOIN
    customer
      ON order.customer_id = customer.customer_id
    GROUP BY customer_name" in
  (* let sql = "
    SELECT
    customer_id,
    ct
    FROM (
    SELECT
      customer_id,
      COUNT(order_id) AS ct
    FROM
      order
    GROUP BY
      customer_id
    ) AS t
  " in *)
  ignore @@ process_sql sql

(* open Js_of_ocaml

let _ =
  Js.export "myLib"
    (object%js
        method process_sql sql = process_sql sql
      end) *)
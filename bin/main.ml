type id = string

let list_equals list1 list2 =
  let list1 = List.sort_uniq String.compare list1 in
  let list2 = List.sort_uniq String.compare list2 in
  list1 = list2

  module NameMap = Map.Make (String)

  (* module Ast = struct
    type table_like = Table of id | SubQuery of select * string
  
    and join =
      | InnerJoin of table_like * (id * id) list
      | LeftOuterJoin of table_like * (id * id) list
      | CrossJoin of table_like
  
    and select = {
      columns : id list;
      from : table_like;
      joins : join list;
      group_by : id list (* where: where; *);
    }
  
    type t = Select of select
  
    let show_column column name_map =
      let open Printf in
      sprintf "%s" (NameMap.find column name_map)
  
    let rec show_from from name_map =
      let open Printf in
      match from with
      | Table table -> sprintf "%s" (NameMap.find table name_map)
      | SubQuery (select, alias) ->
          sprintf "(%s) as %s" (show_ast (Select select) name_map) alias
  
    and show_group_by group_by name_map =
      let open Printf in
      if List.length group_by > 0 then
        let group_by = List.map (fun c -> show_column c name_map) group_by in
        String.concat ", " group_by
      else ""
  
    and show_ast ast name_map =
      let open Printf in
      match ast with
      | Select { columns; from; group_by } ->
          let columns = List.map (fun c -> show_column c name_map) columns in
          let columns = String.concat ", " columns in
          sprintf "SELECT %s FROM %s %s" columns (show_from from name_map)
            (show_group_by group_by name_map)
  end *)

module Ast = Parser.RawAst

module Schema = struct
  type column_type = CInt | CNVarchar

  type column = {
    column_name : string;
    column_type : column_type;
    is_not_null : bool;
  }

  type foreign_key = {
    base_column_name : string;
    reference_table_name: string;
    reference_column_name: string;
  }

  type table = {
    table_name : string;
    columns : column list;
    unique_keys : id list list;
    foreign_keys: foreign_key list;
  }

  type t = { tables : table list }

  let show_column column =
    let open Printf in
    sprintf "%s: %s" column.column_name
      (match column.column_type with CInt -> "int" | CNVarchar -> "varchar")
  
  let show_table table =
    let open Printf in
    let columns =
      List.map (fun c -> show_column c) table.columns |> String.concat ", "
    in
    let unique_keys =
      List.map
        (fun uk -> "(" ^ String.concat ", " (uk) ^ ")")
        table.unique_keys
      |> String.concat ", "
    in
    sprintf "%s: (%s), unique_keys: (%s)" table.table_name columns unique_keys

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

  let rec get_from (from : Ast.table_like) (schema : Schema.t) =
    match from with
    | Ast.Table table ->
        let schema_table : Schema.table option =
          List.find_opt
            (fun (t : Schema.table) -> t.table_name = table)
            schema.tables
        in
        let schema_table =
          match schema_table with
          | None ->
              failwith
              @@ Printf.sprintf "Table not found: %s"
              @@ table
          | Some schema_table -> schema_table
        in
        schema_table
    | Ast.SubQuery (query, name) ->
        let sub_table = check_select query schema in
        { sub_table with table_name = name }

  and calculate_unique_keys (schema_table : Schema.table)
      (join_schema_tables : (Ast.join * Schema.table) list) =
    let rec sub unique_keys join_schema_tables =
      match join_schema_tables with
      | (join, table_schema) :: xs -> (
          match join with
          | Ast.CrossJoin _ ->
              let schema_unique_keys = table_schema.Schema.unique_keys in
              let result =
                List.map
                  (fun uk -> List.map (fun uk' -> uk @ uk') schema_unique_keys)
                  unique_keys
                |> List.flatten
              in
              sub result xs
          | Ast.InnerJoin (_, join_columns) ->
              let schema_unique_keys = table_schema.Schema.unique_keys in
              let left_columns, right_columns = List.split join_columns in
              (* left_columnsが左のテーブルのunique_keysのいずれかと一致するか *)
              let is_unique_key_in_left =
                List.exists
                  (fun (uk : id list) -> list_equals uk left_columns)
                  unique_keys
              in
              (* right_columnsが右のテーブルのunique_keysのいずれかと一致するか *)
              let is_unique_key_in_right =
                List.exists
                  (fun (uk : id list) -> list_equals uk right_columns)
                  schema_unique_keys
              in
              let new_unique_keys =
                if is_unique_key_in_left && is_unique_key_in_right then
                  (* 1:1結合なので、右側のunique_keysもuniqueになる *)
                  unique_keys @ schema_unique_keys
                else if is_unique_key_in_left then
                  (* 右側のテーブル以上はレコードは増えないので、右側のunique_keysを採用 *)
                  schema_unique_keys
                else if is_unique_key_in_right then
                  (* 左側のテーブル以上はレコードは増えないので、左側のunique_keysを採用 *)
                  unique_keys
                else
                  (* どちらもunique_keysではないので、組み合わせ *)
                  List.map
                    (fun uk ->
                      List.map (fun uk' -> uk @ uk') schema_unique_keys)
                    unique_keys
                  |> List.flatten
              in
              sub new_unique_keys xs
          (* left outer joinの判定は、外部キー制約の情報が必要 *)
          | Ast.LeftOuterJoin (_, join_columns) -> failwith "not implemented")
      | [] -> unique_keys
    in
    sub schema_table.unique_keys join_schema_tables |> remove_superset_keys

  and check_select (select : Ast.select) (schema : Schema.t) : Schema.table =
    let ({ from; columns; joins; group_by } : Ast.select) = select in
    let schema_table = get_from from schema in
    let join_schema_tables =
      List.map
        (fun (j : Ast.join) ->
          let t =
            match j with
            | InnerJoin (t, _) -> t
            | LeftOuterJoin (t, _) -> t
            | CrossJoin t -> t
          in
          (j, get_from t schema))
        joins
    in
    (* let ast_columns =
         List.map (fun (c : Ast.column) -> c.column_id) columns
         |> List.sort_uniq Int.compare
       in
       let schema_columns =
         List.sort_uniq Int.compare
         @@ List.map (fun (c : Schema.column) -> c.column_id) schema_table.columns
       in
       (* columnsの中にschema_columnsにないものがある *)
       (* TODO: joinの分 *)
       let diff =
         List.filter (fun c -> not @@ List.mem c schema_columns) ast_columns
       in
       if diff <> [] then
         failwith
         @@ Printf.sprintf "Column not found: %s"
         @@ String.concat ", " (List.map string_of_int diff);
       (* group_byカラムの中にschema_columnsにないものがある *)
       let ast_group_by =
         List.map (fun (c : Ast.column) -> c.column_id) group_by
       in
       let diff =
         List.filter (fun c -> not @@ List.mem c schema_columns) ast_group_by
       in
       if diff <> [] then
         failwith
         @@ Printf.sprintf "Column not found: %s"
         @@ String.concat ", " (List.map string_of_int diff); *)
    (* let result_table_id = generate_id () in *)
    let get_schema_column c =
      match List.filter
        (fun (sc : Schema.column) -> sc.column_name = c || (String.starts_with ~prefix:"." c && String.ends_with ~suffix:c sc.column_name))
        (schema_table.columns
        @ (List.map
              (fun (t : 'a * Schema.table) -> (snd t).columns)
              join_schema_tables
          |> List.flatten)) with
      | [sc] -> sc
      | [] -> failwith ("not found " ^ c)
      | _ -> failwith ("multiple found " ^ c) in
    let columns = List.map get_schema_column columns in
    let group_by = List.map get_schema_column group_by |> List.map (fun {Schema.column_name} -> column_name) in
    let unique_keys =
      let unique_keys = calculate_unique_keys schema_table join_schema_tables in
      let unique_keys =
        List.concat
          [
            unique_keys;
            [ group_by ];
          ] in
      let unique_keys = unique_keys |> remove_superset_keys in
      if List.length unique_keys = 0 then failwith "no unique key";
      unique_keys
    in
    let foreign_keys =
      let all_foreign_keys = schema_table.foreign_keys @ (List.map (fun (_, t) -> t.Schema.foreign_keys) join_schema_tables |> List.flatten) in
      List.filter (fun { Schema.base_column_name} -> List.exists (fun {Schema.column_name} -> base_column_name = column_name) columns) all_foreign_keys in
    {
      table_name = "";
      columns;
      unique_keys = unique_keys;
      foreign_keys = foreign_keys
    }
end

let get_ast () =
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
                  is_not_null = true;
                };
                {
                  column_name = "customer_id";
                  column_type = CInt;
                  is_not_null = true;
                };
                {
                  column_name = "order_name";
                  column_type = CNVarchar;
                  is_not_null = false;
                };
              ];
            unique_keys = [ [ "order_id" ] ];
            foreign_keys = [ { base_column_name = "customer_id"; reference_table_name = "customer"; reference_column_name = "customer_id" } ]
          };
          {
            table_name = "customer";
            columns = [
              {
                column_name = "customer_id";
                column_type = CInt;
                is_not_null = true;
              };
              {
                column_name = "customer_name";
                column_type = CNVarchar;
                is_not_null = true;
              }
            ];
            unique_keys = [ [ "customer_id" ] ];
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
        let unique_keys = List.map (fun uk -> List.map (fun column_name -> table_name ^ "." ^ column_name) uk) ts.unique_keys in
        if List.length unique_keys = 0 then failwith "no unique key";
        let foreign_keys = List.map (fun {Schema.base_column_name; reference_table_name; reference_column_name} ->
          {Schema.base_column_name = table_name ^ "." ^ base_column_name; reference_table_name = reference_table_name; reference_column_name = reference_table_name ^ "." ^ reference_column_name}
        ) ts.foreign_keys in
        {ts with columns; unique_keys; foreign_keys}
      ) in
    { Schema.tables } in
  let schema = preprocess_schema schema in
  schema
  (* let ast =
    Ast.Select
      {
        columns = [ { column_id = 1; column_name = "order_id" }; { column_id = 2; column_name = "customer_id" }; { column_id = 6; column_name = "customer_name"} ];
        from = Table { table_id = 10; table_name = "order" };
        joins = [ Ast.InnerJoin (Table { table_id = 11; table_name = "customer" }, [ { column_id = 2; column_name = "customer_id" }, { column_id = 5; column_name = "customer_id" } ]) ];
        group_by = [ { column_id = 2; column_name = "customer_id" } ];
      }
  in
  print_endline @@ Ast.show_ast ast;
  print_endline @@ Schema.show_schema schema;
  let result = AstCheker.check_ast ast schema in
  print_endline @@ Schema.show_schema { tables = [ result ] }
*)
let () =
  (* let lexer = Parser.Lexer.init "SELECT A, piyo FROM users INNER JOIN (SELECT neko FROM fuga) AS sections ON sections.section_id = users.section_id2 GROUP BY hoge, fuga" in *)
  let lexer = Parser.Lexer.init "SELECT order_id, order.customer_id, customer_name FROM order CROSS JOIN customer GROUP BY order_id" in
  let (select, lexer) = Parser.Parser.query_specification lexer in
  print_endline @@ Parser.RawAst.show_select select;
  if lexer.pos >= lexer.len then begin
    let schema = get_ast () in
    let result = AstChecker.check_select select schema in
    print_endline @@ Schema.show_schema { tables = [ result ] };
    (* let ast, name_map = to_ast_from_select select schema in *)
    (* print_endline @@ Ast.show_ast (Ast.Select ast) name_map; *)
    print_endline "EOF"
  end else
    begin
      print_endline @@ "pos: " ^ (string_of_int lexer.pos);
      print_endline @@ "len: " ^ (string_of_int lexer.len);
      print_endline @@ "not EOF: " ^ (String.sub lexer.str lexer.pos (lexer.len - lexer.pos))
    end

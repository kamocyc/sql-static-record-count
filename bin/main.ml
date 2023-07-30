type id = int

let generate_id =
  let id = ref 0 in
  fun () ->
    id := !id + 1;
    !id

let list_equals list1 list2 =
  let list1 = List.sort_uniq Int.compare list1 in
  let list2 = List.sort_uniq Int.compare list2 in
  list1 = list2

  module NameMap = Map.Make (Int)

  module Ast = struct
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
  end
module Schema = struct
  type column_type = CInt | CNVarchar

  type column = {
    column_id : id;
    column_name : string;
    column_type : column_type;
    is_not_null : bool;
  }

  type table = {
    table_id : id;
    table_name : string;
    columns : column list;
    unique_keys : id list list;
  }

  type t = { tables : table list }

  let show_schema schema =
    let open Printf in
    let show_column column =
      let open Printf in
      sprintf "%s: %s" column.column_name
        (match column.column_type with CInt -> "int" | CNVarchar -> "varchar")
    in
    let show_table table =
      let open Printf in
      let columns =
        List.map (fun c -> show_column c) table.columns |> String.concat ", "
      in
      let unique_keys =
        List.map
          (fun uk -> "(" ^ String.concat ", " (List.map string_of_int uk) ^ ")")
          table.unique_keys
        |> String.concat ", "
      in
      sprintf "%s: (%s) (%s)" table.table_name columns unique_keys
    in
    let tables = List.map (fun t -> show_table t) schema.tables in
    let tables = String.concat "\n" tables in
    sprintf "%s" tables
end

module AstCheker = struct
  let rec get_from (from : Ast.table_like) (schema : Schema.t) name_map =
    match from with
    | Ast.Table table ->
        let schema_table : Schema.table option =
          List.find_opt
            (fun (t : Schema.table) -> t.table_id = table)
            schema.tables
        in
        let schema_table =
          match schema_table with
          | None ->
              failwith
              @@ Printf.sprintf "Table not found: %s"
              @@ (NameMap.find table name_map)
          | Some schema_table -> schema_table
        in
        schema_table
    | Ast.SubQuery (query, name) ->
        let sub_table = check_select query schema name_map in
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
    sub schema_table.unique_keys join_schema_tables

  and check_select (select : Ast.select) (schema : Schema.t) name_map : Schema.table =
    let ({ from; columns; joins; group_by } : Ast.select) = select in
    let schema_table = get_from from schema name_map in
    let join_schema_tables =
      List.map
        (fun (j : Ast.join) ->
          let t =
            match j with
            | InnerJoin (t, _) -> t
            | LeftOuterJoin (t, _) -> t
            | CrossJoin t -> t
          in
          (j, get_from t schema name_map))
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
    let result_table_id = generate_id () in
    let columns =
      List.map
        (fun c ->
          List.find
            (fun (sc : Schema.column) -> sc.column_id = c)
            (schema_table.columns
            @ (List.map
                 (fun (t : 'a * Schema.table) -> (snd t).columns)
                 join_schema_tables
              |> List.flatten)))
        columns
    in
    let unique_keys =
      calculate_unique_keys schema_table join_schema_tables
    in
    {
      table_id = result_table_id;
      table_name = "sub" ^ string_of_int result_table_id;
      columns;
      unique_keys =
        List.concat
          [
            unique_keys;
            [ group_by ];
          ];
    }

  let check_ast (ast : Ast.t) (schema : Schema.t) =
    match ast with Select ast -> check_select ast schema
end

module IdentifierMap = struct
  include Map.Make (String)

  let find key map =
    try find key map with Not_found -> failwith ("not found " ^ key)

  let of_list ls =
    List.fold_left
      (fun map (k, v) -> add k v map)
      empty
      ls
  
  let to_list map =
    fold (fun k v acc -> (k, v) :: acc) map []
    |> List.rev
  
  (* keyは必ず.を含むとする *)
  let find_one key map =
    filter (fun k _ ->
      k = key ||
      String.ends_with ~suffix:key k
    ) map
    |> to_list
    |> (function
      | [] -> Either.Left "not found"
      | [ (_, v) ] -> Right v
      | _ -> Left ("not unique " ^ key))
  
  let find_one_exn key map =
    match find_one key map with
    | Right v -> v
    | Left msg -> failwith msg
end

let to_ast_from_select (select: Parser.RawAst.select) (schema: Schema.t) =
  let gen_id = 
    let id = ref 0 in
    fun () -> 
      id := !id + 1;
      !id in
  let table_id_map =
    schema.tables
    |> List.map (fun ({ Schema.table_id; table_name }) -> ("." ^ table_name, gen_id ()))
    |> IdentifierMap.of_list in
  (* let column_id_map =
    schema.tables
    |> List.map (fun { Schema.columns; table_name } -> List.map (fun { Schema.column_name } -> (table_name ^ "." ^ column_name, gen_id ())) columns)
    |> List.flatten
    |> IdentifierMap.of_list
  in
  let name_id_map =
    IdentifierMap.of_list
      (IdentifierMap.to_list table_id_map @ IdentifierMap.to_list column_id_map) in
  (* show name_id_map *)
  name_id_map |> IdentifierMap.to_list |> List.iter (fun (k, v) -> Printf.printf "%s: %d\n" k v); *)

  let rec to_ast_from_table_like (name_id_map : id IdentifierMap.t) table_like =
    match table_like with
    | Parser.RawAst.Table name ->
      let column_id_map =
        let table = List.find (fun { Schema.table_name } -> table_name = name) schema.tables in
        List.map (fun { Schema.column_name } -> (table.table_name ^ "." ^ column_name, gen_id ())) table.columns
        |> IdentifierMap.of_list in
      column_id_map, Ast.Table (IdentifierMap.find_one_exn ("." ^ name) table_id_map)
    | SubQuery (select, alias) ->
      let column_id_map, select = to_ast_from_select name_id_map select in
      column_id_map, Ast.SubQuery (select, alias)
  and to_ast_from_join name_id_map join =
    let to_column_ids (columns: ((string * string) * (string * string)) list) =
      List.map (fun ((table_name1, column_name1), (table_name2, column_name2)) -> IdentifierMap.find_one_exn (table_name1 ^ "." ^ column_name1) column_id_map, IdentifierMap.find_one_exn (table_name2 ^ "." ^ column_name2) column_id_map) columns
    in
    match join with
    | Parser.RawAst.InnerJoin (table_like, columns) ->
      let column_id_map, table_like = to_ast_from_table_like name_id_map table_like in
      column_id_map, Ast.InnerJoin (table_like, to_column_ids columns)
    | LeftOuterJoin (table_like, columns) ->
      let column_id_map, table_like = to_ast_from_table_like name_id_map table_like in
      column_id_map, LeftOuterJoin (table_like, to_column_ids columns)
    | CrossJoin table_like ->
      let column_id_map, table_like = to_ast_from_table_like name_id_map table_like in
      column_id_map, CrossJoin (table_like)
  and to_ast_from_select name_id_map select =
    let (name_id_map', from) = to_ast_from_table_like name_id_map select.from in
    let (name_id_maps, joins) = List.map (to_ast_from_join name_id_map) select.joins |> List.split in
    let name_id_map = List.fold_left (fun acc map -> IdentifierMap.union (fun _ _ -> failwith "not unique") acc map) name_id_map (name_id_map'::name_id_maps) in
    let columns = select.columns |> List.map (fun (c1, c2) -> IdentifierMap.find_one_exn (c1 ^ "." ^ c2) name_id_map) in
    let group_by = select.group_by |> List.map (fun (c1, c2) -> IdentifierMap.find_one_exn (c1 ^ "." ^ c2) name_id_map) in
    let returning_name_id_map =
      select.columns |> List.map (fun (c1, c2) -> IdentifierMap.)
    {
      Ast.columns = columns;
      from = from;
      joins = joins;
      group_by = group_by;
    } in
  let ast = to_ast_from_select table_id_map select in
  let id_name_map =
    IdentifierMap.to_list name_id_map
    |> List.map (fun (k, v) -> (v, k))
    |> List.to_seq
    |> NameMap.of_seq in
  ast, id_name_map


let get_ast () =
  let schema : Schema.t =
    {
      tables =
        [
          {
            table_id = 10;
            table_name = "order";
            columns =
              [
                {
                  column_id = 1;
                  column_name = "order_id";
                  column_type = CInt;
                  is_not_null = true;
                };
                {
                  column_id = 2;
                  column_name = "customer_id";
                  column_type = CInt;
                  is_not_null = true;
                };
                {
                  column_id = 3;
                  column_name = "order_name";
                  column_type = CNVarchar;
                  is_not_null = false;
                };
              ];
            unique_keys = [ [ 1 ] ];
          };
          {
            table_id = 11;
            table_name = "customer";
            columns = [
              {
                column_id = 5;
                column_name = "customer_id";
                column_type = CInt;
                is_not_null = true;
              };
              {
                column_id = 6;
                column_name = "customer_name";
                column_type = CNVarchar;
                is_not_null = true;
              }
            ];
            unique_keys = [ [ 5 ] ];
          }
        ];
    } in
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
  let lexer = Parser.Lexer.init "SELECT order_id, customer_id, customer_name FROM order" in
  let (select, lexer) = Parser.Parser.query_specification lexer in
  print_endline @@ Parser.RawAst.show_select select;
  if lexer.pos >= lexer.len then begin
    let schema = get_ast () in
    let ast, name_map = to_ast_from_select select schema in
    print_endline @@ Ast.show_ast (Ast.Select ast) name_map;
    print_endline "EOF"
  end else
    begin
      print_endline @@ "pos: " ^ (string_of_int lexer.pos);
      print_endline @@ "len: " ^ (string_of_int lexer.len);
      print_endline @@ "not EOF: " ^ (String.sub lexer.str lexer.pos (lexer.len - lexer.pos))
    end

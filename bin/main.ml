type id = int
type t = A | B
type r = { f1 : string }

module Schema = struct
  type column_type = CInt | CNVarchar

  type column = {
    column_id : id;
    column_name : string;
    column_type : column_type;
  }

  type table = { table_id : id; column_name : string; fields : column list }
end

module Ast = struct
  type column = { column_id : id; column_name : string }
  type table = { table_id : id; table_name : string }
  type select = { columns : column list; table : table (* where: where; *) }
  type ast = Select of select

  let show_column column =
    let open Printf in
    sprintf "%s" column.column_name

  let show_table table =
    let open Printf in
    sprintf "%s" table.table_name

  let show_ast ast =
    let open Printf in
    match ast with
    | Select { columns; table } ->
        let columns = List.map (fun c -> show_column c) columns in
        let columns = String.concat ", " columns in
        sprintf "SELECT %s FROM %s" columns (show_table table)
end

let get_ast () =
  let open Ast in
  let ast =
    Select { columns = []; table = { table_id = 1; table_name = "test" } }
  in
  print_endline @@ show_ast ast

let () = get_ast ()

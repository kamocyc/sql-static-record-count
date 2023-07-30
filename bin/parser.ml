module Lexer = struct
  type t = {
    str: string;
    pos: int;
    len: int;
  }

  type symbol =
    | Select
    | From
    | Where
    | Group
    | By
    | Join
    | Inner
    | Left
    | Right
    | Outer
    | Cross
    | On
    | Full
    | RPAREN
    | LPAREN
    | Comma
    | As
    | Eq
    | Dot
    | EOF
    | Number of int
    | Identifier of string

  let show_symbol symbol =
    match symbol with
    | Select -> "SELECT"
    | From -> "FROM"
    | Where -> "WHERE"
    | Group -> "GROUP"
    | By -> "BY"
    | Join -> "JOIN"
    | Inner -> "INNER"
    | Left -> "LEFT"
    | Right -> "RIGHT"
    | Outer -> "OUTER"
    | RPAREN -> "RPAREN"
    | LPAREN -> "LPAREN"
    | Cross -> "CROSS"
    | Comma -> "COMMA"
    | Eq -> "EQ"
    | As -> "AS"
    | On -> "ON"
    | Full -> "FULL"
    | Dot -> "DOT"
    | EOF -> "EOF"
    | Number n -> Printf.sprintf "NUMBER(%d)" n
    | Identifier str -> Printf.sprintf "IDENTIFIER(%s)" str

  let eof = ""

  let init str =
    { str; pos = 0; len = String.length str }
  
  let return_string sub from until =
    let len = until - from in
    if len = 0 then
      None
    else
      Some (String.sub sub from len)
    
  let take_alphatnumeric lexer =
    let {pos = initial_pos; len; str} = lexer in
    let rec loop pos =
      if pos
         >= len then
        (return_string str initial_pos pos, { lexer with pos })
      else
        let c = String.get str pos in
        match c with
        | 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' -> loop (pos + 1)
        | _ -> (return_string str initial_pos pos, { lexer with pos })
    in
    loop initial_pos
  
  let take_number lexer =
    let {pos = initial_pos; len; str} = lexer in
    let rec loop pos =
      if pos >= len then
        (return_string str initial_pos pos, { lexer with pos })
      else
        let c = String.get str pos in
        match c with
        | '0' .. '9' -> loop (pos + 1)
        | _ -> (return_string str initial_pos pos, { lexer with pos })
    in
    loop initial_pos

  let next lexer =
    let {pos; len; str} = lexer in
    let rec loop pos =
      if pos >= len then
        (EOF, { lexer with pos })
      else
        let c = String.get str pos in
        match c with
        | ' ' | '\t' | '\n' | '\r' -> loop (pos + 1)
        | ',' -> (Comma, { lexer with pos = pos + 1 })
        | '.' -> (Dot, { lexer with pos = pos + 1 })
        | '(' -> (LPAREN, { lexer with pos = pos + 1 })
        | ')' -> (RPAREN, { lexer with pos = pos + 1 })
        | '=' -> (Eq, { lexer with pos = pos + 1 })
        | _ -> begin
          match take_alphatnumeric { lexer with pos } with
          | Some str, lexer -> begin
            match String.lowercase_ascii str with
            | "select" -> (Select, lexer)
            | "from" -> (From, lexer)
            | "where" -> (Where, lexer)
            | "group" -> (Group, lexer)
            | "by" -> (By, lexer)
            | "join" -> (Join, lexer)
            | "inner" -> (Inner, lexer)
            | "left" -> (Left, lexer)
            | "right" -> (Right, lexer)
            | "outer" -> (Outer, lexer)
            | "on" -> (On, lexer)
            | "full" -> (Full, lexer)
            | "as" -> (As, lexer)
            | str ->
              (Identifier str, lexer)
          end
          | None, _ -> begin
            match take_number { lexer with pos } with
            | Some str, lexer ->
              (Number (int_of_string str), lexer)
            | None, _ ->
              failwith @@ "invalid character (" ^ String.make 1 c ^ ") at " ^ string_of_int pos ^ " in " ^ str
          end
        end
    in
    loop pos
end

type id = int

module RawAst = struct
  type logical_operator = And | Or
  type value_operator = Eq

  type table_like = Table of string | SubQuery of select * string
  and join =
    InnerJoin of table_like * ((string * string) * (string * string)) list
    | LeftOuterJoin of table_like * ((string * string) * (string * string)) list
    | CrossJoin of table_like
  and select = {
    columns : (string * string) list;
    from : table_like;
    joins : join list;
    group_by : (string * string) list;
  }
  and logical_expr =
    LogicalOperator of logical_operator * logical_expr * logical_expr
  | ValueOperator of value_operator * value_expr * value_expr
  and value_expr =
    Column of (string * string)

  let rec show_table_like table_like =
    match table_like with
    | Table id -> Printf.sprintf "Table(%s)" id
    | SubQuery (select, alias) ->
        Printf.sprintf "SubQuery(%s, %s)" (show_select select) alias
  and show_join join =
    match join with
    | InnerJoin (table_like, columns) ->
        Printf.sprintf "InnerJoin(%s, %s)"
          (show_table_like table_like)
          (List.split columns |> (fun (columns1, columns2) -> show_columns columns1, show_columns columns2) |> (fun (s1, s2) -> Printf.sprintf "(%s, %s)" s1 s2))
    | LeftOuterJoin (table_like, columns) ->
        Printf.sprintf "LeftOuterJoin(%s, %s)"
          (show_table_like table_like)
          (List.split columns |> (fun (columns1, columns2) -> show_columns columns1, show_columns columns2) |> (fun (s1, s2) -> Printf.sprintf "(%s, %s)" s1 s2))
    | CrossJoin table_like ->
        Printf.sprintf "CrossJoin(%s)"
          (show_table_like table_like)
  and show_joins joins =
    Printf.sprintf "[%s]"
      (String.concat ", " (List.map show_join joins))
  and show_select select =
    Printf.sprintf "Select(%s, %s, %s, %s)"
      (show_columns select.columns)
      (show_table_like select.from)
      (show_joins select.joins)
      (show_columns select.group_by)
  and show_columns columns =
    Printf.sprintf "[%s]"
      (String.concat ", " (List.map (fun (column, alias) -> Printf.sprintf "(%s, %s)" column alias) columns))

end

module Parser = struct
  let equals symbol1 symbol2 =
    match symbol1, symbol2 with
    | Lexer.Select, Lexer.Select
    | Lexer.From, Lexer.From
    | Lexer.Where, Lexer.Where
    | Lexer.Group, Lexer.Group
    | Lexer.By, Lexer.By
    | Lexer.Join, Lexer.Join
    | Lexer.Inner, Lexer.Inner
    | Lexer.Left, Lexer.Left
    | Lexer.Right, Lexer.Right
    | Lexer.Outer, Lexer.Outer
    | Lexer.RPAREN, Lexer.RPAREN  
    | Lexer.LPAREN, Lexer.LPAREN
    | Lexer.Comma, Lexer.Comma
    | Lexer.Dot, Lexer.Dot
    | Lexer.As, Lexer.As
    | Lexer.On, Lexer.On
    | Lexer.Full, Lexer.Full
    | Lexer.Identifier _, Lexer.Identifier _
    | Lexer.Number _, Lexer.Number _
    | Lexer.EOF, Lexer.EOF -> ()
    | _ -> raise (Invalid_argument "not equals")
  let prefixed_identifier lexer =
    let identifier, lexer = Lexer.next lexer in
    match identifier with
    | Lexer.Identifier identifier -> begin
      let dot, lexer2 = Lexer.next lexer in
      try
        equals dot Lexer.Dot;
        let identifier2, lexer = Lexer.next lexer2 in
        match identifier2 with
        | Lexer.Identifier identifier2 ->
          ((identifier, identifier2), lexer)
        | _ ->
          raise (Invalid_argument "invalid prefixed_identifier")
      with
        | Invalid_argument _ ->
          (("", identifier), lexer)
      end
    | _ -> raise (Invalid_argument "invalid prefixed_identifier")
  
  let column_list lexer =
    let column, lexer = prefixed_identifier lexer in
    let rec loop columns lexer =
      match Lexer.next lexer with
      | Lexer.Comma, lexer -> begin
        let column, lexer = prefixed_identifier lexer in
        loop (column :: columns) lexer
      end
      | _ ->
        (List.rev columns, lexer)
    in
    loop [column] lexer
  
  type join_type = Inner | LeftOuter | RightOuter | FullOuter | Cross

  let rec table_like lexer =
    let s, lexer = Lexer.next lexer in
    match s with
    | Lexer.Identifier s ->
      (RawAst.Table s, lexer)
    | Lexer.LPAREN -> begin
      let sub_query, lexer = query_specification lexer in
      let rparen, lexer = Lexer.next lexer in
      equals rparen Lexer.RPAREN;
      let as_, lexer = Lexer.next lexer in
      equals as_ Lexer.As;
      let alias, lexer = Lexer.next lexer in
      match alias with
      | Lexer.Identifier alias ->
        (RawAst.SubQuery (sub_query, alias), lexer) 
      | _ ->
        failwith "invalid alias"
      end
    | _ ->
      raise (Invalid_argument "invalid table_like")
  and boolean_expression lexer =
    let ident, lexer = prefixed_identifier lexer in
    let op, lexer = Lexer.next lexer in
    match op with
    | Lexer.Eq ->
      let ident2, lexer = prefixed_identifier lexer in
      (RawAst.ValueOperator (RawAst.Eq, Column ident, Column ident2), lexer)
    | _ ->
      raise (Invalid_argument "invalid boolean expression")
  and join_clause lexer =
    let join_type1, lexer = Lexer.next lexer in
    let join_type2, lexer2 = Lexer.next lexer in
    let join_type, lexer =
      match join_type1, join_type2 with
      | Lexer.Inner, _ -> Inner, lexer
      | Lexer.Cross, _ -> Cross, lexer
      | Lexer.Left, Lexer.Outer -> LeftOuter, lexer2
      | Lexer.Right, Lexer.Outer -> RightOuter, lexer2
      | Lexer.Full, Lexer.Outer -> FullOuter, lexer2
      | _ -> raise (Invalid_argument "invalid join") in
    let join, lexer = Lexer.next lexer in
    equals join Lexer.Join;
    let table, lexer = table_like lexer in
    let on, lexer = Lexer.next lexer in
    equals on Lexer.On;
    let expression, lexer = boolean_expression lexer in
    let join_keys = match expression with
      | RawAst.ValueOperator (RawAst.Eq, Column id1, Column id2) ->
        [(id1, id2)]
      | _ ->
        failwith "invalid join keys" in
    match join_type with
    | Inner ->
      (RawAst.InnerJoin (table, join_keys), lexer)
    | LeftOuter ->
      (RawAst.LeftOuterJoin (table, join_keys), lexer)
    | Cross ->
      (RawAst.CrossJoin table, lexer)
    | _ -> 
      failwith "not implemented"
  and query_specification lexer =
    let select, lexer = Lexer.next lexer in
    equals select Lexer.Select;
    let select_columns, lexer = column_list lexer in
    let from, lexer = Lexer.next lexer in
    equals from Lexer.From;
    let table, lexer = table_like lexer in
    let joins, lexer =
      let rec loop joins lexer =
        try
          let join, lexer = join_clause lexer in
          loop (join::joins) lexer
        with
        | Invalid_argument _ ->
          List.rev joins, lexer in
      loop [] lexer in
    let group_by_columns, lexer =
      try
        let group, lexer = Lexer.next lexer in
        equals group Lexer.Group;
        let by, lexer = Lexer.next lexer in
        equals by Lexer.By;
        let group_by_columns, lexer = column_list lexer in
        group_by_columns, lexer
      with
      | Invalid_argument _ ->
        [], lexer in
    {
      RawAst.columns = select_columns;
      RawAst.from = table;
      RawAst.joins = joins;
      RawAst.group_by = group_by_columns;
    }, lexer
end

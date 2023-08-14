{
open Parser
}

let white = [' ' '\t' '\r' '\n']+
let digit = ['0'-'9']
let int = '-'? digit+
let letter = ['a'-'z' 'A'-'Z' '_']
let id = letter+

rule read =
  parse
  | white { read lexbuf }
  | "SELECT" { SELECT }
  | "FROM" { FROM }
  | "WHERE" { WHERE }
  | "GROUP" { GROUP }
  | "BY" { BY }
  | "JOIN" { JOIN }
  | "INNER" { INNER }
  | "LEFT" { LEFT }
  | "RIGHT" { RIGHT }
  | "OUTER" { OUTER }
  | "CROSS" { CROSS }
  | "ON" { ON }
  | "FULL" { FULL }
  | "AS" { AS }
  | "AND" { AND }
  | "COUNT" { COUNT }
  | "SUM" { SUM }
  | "MAX" { MAX }
  | "MIN" { MIN }
  | "AVG" { AVG }
  (* | "OR" { OR }
  | "@" { AT } *)
  | "(" { LPAREN }
  | ")" { RPAREN }
  | "," { COMMA }
  | "." { DOT }
  | "=" { EQ }
  | id { IDENTIFIER (Lexing.lexeme lexbuf) }
  | int { INT (int_of_string (Lexing.lexeme lexbuf)) }
  | eof { EOF }

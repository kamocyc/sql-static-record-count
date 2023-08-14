%{
open Ast
%}

%token <int> INT
%token <string> IDENTIFIER
%token SELECT
%token FROM
%token WHERE
%token GROUP
%token BY
%token JOIN
%token INNER
%token LEFT
%token RIGHT
%token OUTER
%token CROSS
%token ON
%token FULL
%token RPAREN
%token LPAREN
%token COMMA
%token AS
%token EQ
%token DOT
%token AND
%token COUNT
%token SUM
%token AVG
%token MIN
%token MAX
%token EOF
%token AT

%start <Ast.select> prog

%%

prog:
  | s = select; EOF { s }
  ;

select:
  | SELECT; s = select_list; FROM; t = table_like; j = join_clauses; g = group_clause
    { {columns = s; from = t; joins = j; group_by = g} }
  ;

select_list:
  | l = column_like; COMMA; ls = select_list { l :: ls }
  | l = column_like { [l] }
  ;

column_like:
  // | RPAREN; s = select; RPAREN; AS; alias = ident { SubSelect (s, alias) }
  | f = agg_func; AS; alias = ident { AggFunc (f, alias) }
  | c = column; AS; alias = ident { ColumnAlias (c, alias) }
  | c = column; { Column c }
  ;

agg_func:
  | COUNT; LPAREN; c = column; RPAREN { Count c }
  | SUM; LPAREN; c = column; RPAREN { Sum c }
  | AVG; LPAREN; c = column; RPAREN { Avg c }
  | MIN; LPAREN; c = column; RPAREN { Min c }
  | MAX; LPAREN; c = column; RPAREN { Max c }
  ;

column:
  | i1 = ident; DOT; i2 = ident { i1 ^ "." ^ i2 }
  | i = ident { i }
  ;

ident:
  | i = IDENTIFIER { i }
  ;

table_like:
  | i = ident { Table i }
  | LPAREN; s = select; RPAREN; AS; alias = ident { SubQuery (s, alias) }
  ;

join_clauses:
  | j = join_clause; js = join_clauses { j :: js }
  | j = join_clause { [j] }
  | { [] }
  ;

join_clause:
  | j = join_type; t = table_like; ON; e = logical_expr { build_join j t e }
  ;

join_type:
  | INNER; JOIN { InnerJoinType }
  | LEFT; OUTER; JOIN { LeftOuterJoinType }
  | RIGHT; OUTER; JOIN { RightOuterJoinType }
  | CROSS; JOIN { CrossJoinType }
  ;

group_clause:
  | GROUP; BY; list = simple_select_list { list }
  | { [] }
  ;

simple_select_list:
  | l = column; COMMA; ls = simple_select_list { l :: ls }
  | l = column { [l] }
  ;

logical_expr:
  | e1 = logical_expr; AND; e2 = logical_expr { LogicalOperator (And, e1, e2) }
  // | e1 = logical_expr; OR; e2 = logical_expr { LogicalOperator (Or, e1, e2) }
  | e1 = value_expr; EQ; e2 = value_expr { ValueOperator (Eq, e1, e2) }
  | LPAREN; e = logical_expr; RPAREN { e }
  ;

value_expr:
  | i = column { ValueColumn i }
  // | AT; i = ident { Parameter i }
  // | i = integer { Integer i }
  ;

integer:
  | i = INT { i }
  ;

%%

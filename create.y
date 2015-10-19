%{
#include <string.h>
#include <stdio.h>

int yydebug=0;

void yyerror(const char *str)
{
	fprintf(stderr,"error: %s\n",str);
}

int yywrap()
{
	return 1;
}

main()
{
	yyparse();
}

%}

%token CREATE TABLE TYPE DROP NAME OB CB COMMA

%%

statement:CREATE TABLE NAME OB attribs CB
		{
			printf("Correct create Statment");
		}
	|
	 DROP TABLE NAME
		{
			printf("Correct drop Statement");
		}
	 ;

attribs:attrib
	|
	attrib COMMA attribs
	;

attrib:NAME TYPE
       ;



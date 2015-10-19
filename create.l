%{
#include "y.tab.h"
%}

%%
CREATE|create 		return CREATE;
TABLE|table 		return TABLE;
INT|STR20|int|str20 	return TYPE;
DROP|drop		return DROP;
SELECT|select		return SELECT;
WHERE|where		return WHERE;
INSERT|insert		return INSERT;
ORDER\ BY|order\ by	return ORDERBY;
OR|or			return OR;
AND|and			return AND;
NOT|not			return NOT;
VALUE|values		return VALUES;
INTO|into		return INTO;
NULL|null		return NULL;
[a-zA-Z0-9]* 	        return NAME;
\(			return OB;
\)			return CB;
\,			return COMMA;
%%

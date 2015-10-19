#include<iostream>
#include<string>
#include<vector>
#include "Block.h"
#include "Config.h"
#include "Disk.h"
#include "Field.h"
#include "MainMemory.h"
#include "Relation.h"
#include "Schema.h"
#include "SchemaManager.h"
#include "Tuple.h"

using namespace std;

int main(){

cout<<"---------------------------------Initialization-----------------------------\n";

cout<<"Enter the sql query\n";

MainMemory mainMemory;
Disk disk;

SchemaManager schemaManager(&mainMemory, &disk);

vector<string> field_names;
vector<enum FIELD_TYPE> field_types;

//get query from the user

//validate query

//classify query

//get attributes of query

//switch to appropriate action


/* using the query create table contacts (id int , first_name str20, last_name str20, email str20)*/ 

field_names.push_back("id");
field_names.push_back("first_name");
field_names.push_back("last_name");
field_names.push_back("email");

field_types.push_back(INT);
field_types.push_back(STR20);
field_types.push_back(STR20);
field_types.push_back(STR20);

Schema schema(field_names, field_types);

cout<<schema<<endl;







string ex;
cin>> ex;

return 0;

}

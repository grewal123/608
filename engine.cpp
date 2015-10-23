#include<iostream>
#include<regex>
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

bool isNumber(string str) {

	regex exp("^-?\\d+");
	if(regex_match(str,exp))return true;
	else return false;
}

MainMemory mainMemory;
Disk disk;
SchemaManager schemaManager(&mainMemory, &disk);

void createTable(string tableName, vector<string> fieldNames, vector<string> fieldTypes) {

	vector<enum FIELD_TYPE> enumTypes;
	vector<string>::iterator it;
	for(it = fieldTypes.begin();it!=fieldTypes.end();it++){
		string str = *it;
		if(str=="INT"||str=="int") 
			enumTypes.push_back(INT);
		else
			enumTypes.push_back(STR20);
	}

	Schema schema(fieldNames,enumTypes);
	Relation *relation = schemaManager.createRelation(tableName,schema);
	cout<<schemaManager<<endl;
}


void dropTable(string tableName) {
	
	if(schemaManager.relationExists(tableName)) {
		schemaManager.deleteRelation(tableName);
	}
	else
		cout<<"Table "<<tableName<<" doesn't exist"<<endl;
}

void insertIntoTable(string tableName, vector<string> fieldNames, vector<string> fieldValues) {
        if(!schemaManager.relationExists(tableName)) { 
		cout<<"Illegal Tablename"<<endl;
		return;
	}	
	Relation *relation = schemaManager.getRelation(tableName);
	Tuple tuple = relation->createTuple();
	Block *block = mainMemory.getBlock(0);
	Schema schema = relation->getSchema();
	block->clear();
 	vector<string>::iterator it,it1;	
	for(it = fieldNames.begin(),it1 = fieldValues.begin();it!=fieldNames.end();it++, it1++) {
		string str=*it,str1=*it1;
		int type = schema.getFieldType(str);
		if(!type) {
			if(isNumber(str1)) {
				tuple.setField(str,stoi(str1));
			}
			else {
				cout<<"Data type is not supported\n";
				return;
			}
		} 
		else {
			if(str1.length()>20) {
				cout<<"Data type is not supported\n";
				return;
			}
			else tuple.setField(str,str1);
		}
	}
	block->appendTuple(tuple);
	relation->setBlock(relation->getNumOfBlocks(),0);
	cout<<*relation<<endl;
}


void deleteFromTable() {
}

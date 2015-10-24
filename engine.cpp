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

string removeSpaces(string str) {
	string::iterator end_pos = std::remove(str.begin(), str.end(), ' ');
        str.erase(end_pos, str.end());
	return str;
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
	Schema schema = relation->getSchema();
 	vector<string>::iterator it,it1;	
	for(it = fieldNames.begin(),it1 = fieldValues.begin();it!=fieldNames.end();it++, it1++) {
		string str=*it,str1=*it1;
		str = removeSpaces(str);
		int type = schema.getFieldType(str);
		if(!type) {
			str1 = removeSpaces(str1);
			if(isNumber(str1)) {
				tuple.setField(str,stoi(str1));
			}
			else {
				cout<<"Data type is not supported\n";
				return;
			}
		} 
		else {
			regex exp("\\ *\"(.*)\"");
			cmatch match;
			if(regex_match(str1.c_str(),match,exp)) {
				str1 = match[1];
				if(str1.length()>20) {
					cout<<"Data type is not supported\n";
					return;
				}
				else tuple.setField(str,str1);
			}
			else {
				cout<<"Data type is not supported\n";
				return;
			}
		}
	}
	if(relation->getNumOfBlocks()>0){
		relation->getBlock(relation->getNumOfBlocks()-1,0);
		Block *block = mainMemory.getBlock(0);
		if(block->isFull()){ 
			block->clear();
			block->appendTuple(tuple);
			relation->setBlock(relation->getNumOfBlocks(),0);
		}
		else {
			block->appendTuple(tuple);
			relation->setBlock(relation->getNumOfBlocks()-1,0);
		}
	}
	else {
		Block *block = mainMemory.getBlock(0);
		block->clear();
		block->appendTuple(tuple);
		relation->setBlock(0,0);
	}
	cout<<*relation<<endl;
}


void deleteFromTable(string tableName) {

	if(!schemaManager.relationExists(tableName)) {
		cout<<"Illegal Table Name"<<endl;
		return;
	}
	Relation *relation = schemaManager.getRelation(tableName);
	Schema schmea = relation->getSchema();
	int tableSize = relation->getNumOfBlocks();
	if(tableSize>NUM_OF_BLOCKS_IN_MEMORY) {
			
	}
	else {
	
	}
}

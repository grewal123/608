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

extern vector<string> split(string str, char delimiter);

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
//tokenize the condition such as id=3
// into tokens of fieldName and fieldValue
vector<string>  tokenize(string condition)
{
	vector<string> tokens;
	char * dup = strdup(condition.c_str());
    char * token = strtok(dup,"=");
    while(token != NULL)
    {
    	tokens.push_back(string(token));
        // the call is treated as a subsequent calls to strtok:
        // the function continues from where it left in previous invocation
        token = strtok(NULL,"=");
    }
    return tokens;

}
//Inputs : Tuple, vector of strings , schema of tuple
// return boolean for match of condition in token

bool conditionMatches(Tuple tuple,vector<string> tokens,Schema schema) {
	tokens[0] = removeSpaces(tokens[0]);
	tokens[1] = removeSpaces(tokens[1]);
	//if field has int type
	if(schema.getFieldType(tokens[0]) == INT)
	{
		int fieldValue = tuple.getField(tokens[0]).integer;
		//checking the condition
		if(fieldValue == stoi(tokens[1]))
		return true;
	}
	//if field type is string
	if(schema.getFieldType(tokens[0]) == STR20)
	{
		regex exp("\\ *\"(.*)\"");
		cmatch match;
		if(regex_match(tokens[1].c_str(),match,exp))
		{
			string* fieldValue = tuple.getField(tokens[0]).str;
			if(*fieldValue == match[1])
			return true;
		}
	}
	return false;	
}




MainMemory mainMemory;
Disk disk;
SchemaManager schemaManager(&mainMemory, &disk);

vector<Tuple> getAllTuplesOfRelation(string tableName);


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
		block->setTuple(0,tuple);
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
	while(relation->getNumOfBlocks())
	relation->deleteBlocks(relation->getNumOfBlocks()-1);
	cout<<*relation<<endl;

}

//new Api to delete specific tuples from table 
void deleteTuplesFromTable(string tableName,vector<string> tokens) {

	if(!schemaManager.relationExists(tableName)) {
		cout<<"Illegal Table Name"<<endl;
		return;
	}
	Relation *relation = schemaManager.getRelation(tableName);
	Schema schema = relation->getSchema();
	int n = relation->getNumOfBlocks();
	vector<Tuple> tuples;
	//iterating over block in relation
	for(int i=0;i<n;i++)
	{
		relation->getBlock(i,0);
		Block *block = mainMemory.getBlock(0);
		//fetching all tuples in block i
		tuples = block->getTuples();
	    vector<Tuple>::iterator it;
	    int index=0;
		for(it = tuples.begin();it!=tuples.end();it++) 
		{
			//to check if tuple isn't already nullified
			if(!(it->isNull()))
			if(conditionMatches(*it,tokens,schema))
			{	
				block->nullTuple(index);
			}
			index++;
		}
		//resetting block to disk
		relation->setBlock(i,0);
	}
}
//delete statement with two conditions stored in tokens1 and tokens2
void deleteTuplesFromTable(string tableName,vector<string> tokens1,vector<string> tokens2)
{
	if(!schemaManager.relationExists(tableName)) {
		cout<<"Illegal Table Name"<<endl;
		return;
	}
	Relation *relation = schemaManager.getRelation(tableName);
	Schema schema = relation->getSchema();
	int n = relation->getNumOfBlocks();
	vector<Tuple> tuples;
	for(int i=0;i<n;i++)
	{
		relation->getBlock(i,0);
		Block *block = mainMemory.getBlock(0);
		//fetching all tuples in block i
		tuples = block->getTuples();
	    vector<Tuple>::iterator it;
	    int index=0;
		for(it = tuples.begin();it!=tuples.end();it++) 
		{
			//to check if tuple isn't already nullified
			if(!(it->isNull()))
			if(conditionMatches(*it,tokens1,schema) && conditionMatches(*it,tokens2,schema))
			{	
				block->nullTuple(index);
			}
			index++;
		}
		//resetting block to disk
		relation->setBlock(i,0);
	}
}


void projection(vector<string> attributes) {
}

void selectFromTable(bool distinct, string attributes, string tables) {
	vector<string> tableNames = split(tables, ',');
	string tableName;
	vector<string>::iterator it;
	for(it=tableNames.begin();it!=tableNames.end();it++){
		tableName = *it;
		if(!schemaManager.relationExists(tableName)) {
			cout<<"Illegal Table Name "<<tableName<<endl;
			return;
		}
	}
	vector<Tuple> tuples;
	tuples = getAllTuplesOfRelation(tableName);
	if(distinct) 
	cout<<"the distinct flag is set to true"<<endl;	
	vector<string> attributeNames = split(attributes, ',');
	projection(attributeNames);
	
	//cout<<*relation<<endl;
}

vector<Tuple> getAllTuplesOfRelation(string tableName) {
	 Relation *relation = schemaManager.getRelation(tableName);
         Block *block = mainMemory.getBlock(0);
         int size = relation->getNumOfBlocks();
         int index=0,rem=10;
         vector<Tuple> tuples;
         while(size>0) {
                 if(size<10)
                 rem = size;
                 relation->getBlocks(index,0,rem);
                 cout<<mainMemory<<endl;
                 for(int i=0;i<rem;i++) {
                         block = mainMemory.getBlock(i);
                         for(int j=0;j<block->getNumTuples();j++){
                                 tuples.push_back(block->getTuple(j));
                         }
                         block->clear();
                 }
                 size = size-10;
                 index = index+10;          
         }
         vector<Tuple>::iterator it1;
         for(it1=tuples.begin();it1!=tuples.end();it1++){
                 cout<<"the tuple are "<<*it1<<endl;
         }
	return tuples;
}

void getDistinctTuplesOfRelation(string tableName, vector<string> attributes) {


}

void whereCondition(string condition) {
	cout<<"no idea how to evaluate this where statement"<<endl;
	return;
}

void setDistinct(string tableName) {
	Relation *relation = schemaManager.getRelation(tableName);
	int size = relation->getNumOfBlocks();
	//for(int i=0
	//vector<Tuple> allTuples = 

}


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

class Table {
string tableName;
vector<Tuple> tuples;
public:
	Table(string tableName, vector<Tuple> tuples) {
		this->tableName = tableName;
		this->tuples = tuples;
	}
	string getTableName() {
		return tableName;
	}
	vector<Tuple> getTuples() {
		return tuples;
	}
};

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

string getColumn(string str, string tableName) {
	regex exp("([a-zA-Z0-9]*)\\.([a-zA-Z0-9]*)");
	cmatch field;
	if(regex_match(str.c_str(),field,exp)) { 
		if(tableName==field[1])	
		return field[2];
		else
		return "*$";
	}	
	else
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
vector<Tuple> getDistinctTuples(vector<Tuple> tuples);
string distinct(string tableName);

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

string projection(vector<string> attributes, string tableName) {
	Relation *relation = schemaManager.getRelation(tableName);
	Schema tableSchema = relation->getSchema();
	vector<string> fieldNames;
	vector<enum FIELD_TYPE> fieldTypes;
	vector<string>::iterator it;
	int flag=-1;
	bool print=true;
	for(it=attributes.begin();it!=attributes.end();it++) {
		for(int i=0;i<tableSchema.getNumOfFields();i++) {
			string temp = *it;
			if(tableSchema.getFieldName(i)==getColumn(temp, tableName)) 
			flag=i;
		}
		if(flag!=-1) {
			fieldNames.push_back(tableSchema.getFieldName(flag));
			fieldTypes.push_back(tableSchema.getFieldType(flag));
			flag = -1;
		}	
	}
	Schema dupSchema(fieldNames,fieldTypes);
	Relation *relationDup = schemaManager.createRelation(tableName.append("_dup"), dupSchema);	
	Tuple tuple = relationDup->createTuple();
	vector<Tuple>::iterator it1;
	Block *block = mainMemory.getBlock(9);
	block->clear();
	int index=0;
	for(int i=0;i<relation->getNumOfBlocks();i++) {
		relation->getBlock(i,0);//assuming 0 is free
		vector<Tuple> t = mainMemory.getBlock(0)->getTuples();
		for(it1=t.begin();it1!=t.end();it1++) {
			for(int j=0;j<fieldNames.size();j++) {
				if(fieldTypes[j]==INT)
				tuple.setField(fieldNames[j],it1->getField(fieldNames[j]).integer);
				else
				tuple.setField(fieldNames[j],*(it1->getField(fieldNames[j]).str));
			}
			if(!block->isFull())	
			block->appendTuple(tuple);
			else {
				relationDup->setBlock(index,9);
				index++;
				block->clear();
				block->appendTuple(tuple);
			}
		}
	}
	if(index!=relationDup->getNumOfBlocks()-1)
		relationDup->setBlock(index, 9);
	return tableName;
}

void temp(vector<string> attributes, vector<Table> tables) {

	vector<Table>::iterator tableIter;
	vector<Tuple>::iterator tupleIter;
	vector<string>::iterator stringIter;
	for(tableIter=tables.begin();tableIter!=tables.end();tableIter++) {
		vector<Tuple> tuple=tableIter->getTuples();
		for(tupleIter=tuple.begin();tupleIter!=tuple.end();tupleIter++) {
			cout<<*tupleIter<<endl;
		}
		projection(attributes, tableIter->getTableName());
	}
}

void selectFromTable(bool dis, string attributes, string tabs) {
	vector<string> tableNames = split(tabs, ',');
	vector<Table> tables;
	vector<string> attributeNames = split(attributes, ',');
	vector<string> temp2, temp1;
	for(int i=0;i<tableNames.size();i++) {
		temp2.push_back(removeSpaces(tableNames[i]));
	}
	tableNames = temp2;
	for(int i=0;i<attributeNames.size();i++) {
		temp1.push_back(removeSpaces(attributeNames[i]));
	}
	attributeNames = temp1;
	string tableName;
	if(tableNames.size()==1) {
		Relation *relation = schemaManager.getRelation(tableNames[0]);
		if(attributeNames.size()==1 && attributeNames[0]=="*") { 
			if(dis) {
				distinct(tableNames[0]);
				relation = schemaManager.getRelation(tableNames[0].append("_distinct"));
			}
		cout<<*relation<<endl;
		schemaManager.deleteRelation(tableNames[0]);
		}
		else {
			string tempName = projection(attributeNames, tableNames[0]);
			string d;
			relation = schemaManager.getRelation(tempName);
			if(dis) {
				d = distinct(tempName);
				relation = schemaManager.getRelation(d);	
			}
			cout<<*relation<<endl;
			schemaManager.deleteRelation(tempName);
			if(dis)
			schemaManager.deleteRelation(d);
		}
	}
	else {
	vector<string>::iterator it;
	for(it=tableNames.begin();it!=tableNames.end();it++){
		tableName = *it;
		if(!schemaManager.relationExists(tableName)) {
			cout<<"Illegal Table Name "<<tableName<<endl;
			return;
		}
	
		vector<Tuple> tuples;
		if(dis) 
		tuples = getDistinctTuples(tuples);	
		Table table = Table(tableName, tuples);
		tables.push_back(table);
		}	
	temp(attributeNames, tables);
	}
}

void writeTuples(string tableName, vector<Tuple> tuples) {
	vector<Tuple>::iterator it;
	Relation *relation = schemaManager.getRelation(tableName);
	Tuple tuple = relation->createTuple();
	Block *block = mainMemory.getBlock(0);
	block->clear();
	int index = 0;
	for(it=tuples.begin();it!=tuples.end();it++) {
		tuple = *it;
		if(!block->isFull()) 
		block->appendTuple(tuple);
		else {
			relation->setBlock(index,0);
			index++;
			block->clear();
			block->appendTuple(tuple);
		}
	}
	if(index!=relation->getNumOfBlocks()-1)
		relation->setBlock(index,0);
}

string distinct(string tableName) {
	Relation *relation = schemaManager.getRelation(tableName);
	Schema schema = relation->getSchema();
	int size = relation->getNumOfBlocks();
	vector<Tuple> tuples;
	bool flag = true;
	if(size<=10) {
		relation->getBlocks(0,0,size);
		for(int i=0;i<size;i++) {
			Block *block = mainMemory.getBlock(i);
			for(int j=0;j<block->getNumTuples();j++) {
				tuples.push_back(block->getTuple(j));
			}
		}
	tuples = getDistinctTuples(tuples);
	Relation *relation1 = schemaManager.createRelation(tableName+"_distinct",schema); 
	writeTuples(tableName+"_distinct",tuples);
	}
	else {
		int index = 0, loadSize=10;
		while(size>0) {
			relation->getBlocks(index,0,loadSize);
			for(int i=0;i<loadSize;i++) {
				Block *block = mainMemory.getBlock(i);
				for(int j=0;j<block->getNumTuples();j++) {
					tuples.push_back(block->getTuple(j));
				}
				//tuples = sortTuples(tuples, tableName);
				if(flag) {	
					Relation *relation2= schemaManager.createRelation(tableName+"_distinct", schema);
					flag = false;
				}
				writeTuples(tableName+"_distinct", tuples);
			}
			index = index+10;
			size = size-10;
			if(size<10)
			loadSize = size;
		}
	}
	return tableName+"_distinct";
}

bool compare(Tuple tuple1, Tuple tuple2) {
	Schema tupleSchema = tuple1.getSchema();
	for(int i=0;i<tuple1.getNumOfFields();i++) {
		if(tupleSchema.getFieldType(i) == INT) {
			if(tuple1.getField(i).integer != tuple2.getField(i).integer)
			return false;
		}
		else {
			if(*(tuple1.getField(i).str) != *(tuple2.getField(i).str)) 
			return false;
		}
	}
	return true;
}

vector<Tuple> getDistinctTuples(vector<Tuple> tuples) {
	vector<Tuple>::iterator it,it1;
	vector<Tuple> temp;
	bool flag = true;
	for(it=tuples.begin();it!=tuples.end();it++) {
		for(it1=it+1;it1!=tuples.end();it1++) {
			if(compare(*it,*it1))
			flag = false;
		}
		if(flag)
		temp.push_back(*it);
		flag = true;
	}
	return temp;
}

void whereCondition(string condition) {
	cout<<"no idea how to evaluate this where statement"<<endl;
	return;
}

/*
void selection(string tableName, condition) {
	Relation *relation = SchemaManager.getRelation(tableName);
	int numOfBlocks = relation->getNumOfBlocks();
	if(numOfBlocks<9) {
		//onepass
		relation->getBlocks(0,0,num);
		for(int i=0;i<num;i++) {
			
		}
	}
	else {
		//twopass	
	}
}
*/

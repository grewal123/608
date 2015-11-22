#include<iostream>
#include<regex>
#include<string>
#include<vector>
#include<algorithm>
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
extern vector<string> splitWord(string str, string splitter);

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

int conditionMatches(Tuple tuple, vector<string> tokens) {
	Schema schema = tuple.getSchema();
	bool flag=false;
	for(int i=0;i<schema.getNumOfFields();i++) {
		if(schema.getFieldName(i) == tokens[0])
		flag=true;
	}
	if(flag)
	{
		if(tuple.getSchema().getFieldType(tokens[0]) == INT) {
			int fieldValue = tuple.getField(tokens[0]).integer;
			if(isNumber(tokens[1])){
				if(fieldValue == stoi(tokens[1]))
				return 0; 
				else 
				return 1;
			}	
		}
		else {
			regex exp("\\ *\"(.*)\"");
			cmatch match;
			if(regex_match(tokens[1].c_str(),match,exp))
			{
				string* fieldValue = tuple.getField(tokens[0]).str;
				if(*fieldValue == match[1])
				return 0;
				else 
				return 1;
			}
		}
	}
	return 0;	
}

MainMemory mainMemory;
Disk disk;
SchemaManager schemaManager(&mainMemory, &disk);
vector<Tuple> getDistinctTuples(vector<Tuple> tuples);
string distinct(string tableName);
bool whereConditionEvaluator(string whereCondition, Tuple tuple);
int compareNotEqual(Tuple tuple1, Tuple tuple2);
string crossJoin(string tableName1, string tableName2);
void mCrossJoin(string tableName);
void schemaBuilder(vector<string> tableNames);
string xyz(string tableName1, string tableName2);

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
	cout<<disk.getDiskIOs()<<endl;
}

void deleteFromTable(string tableName, string whereCondition) {
	if(whereCondition.empty()){
		if(!schemaManager.relationExists(tableName)) {
			cout<<"Illegal Table Name"<<endl;
			return;
		}
		Relation *relation = schemaManager.getRelation(tableName);
		while(relation->getNumOfBlocks())
		relation->deleteBlocks(relation->getNumOfBlocks()-1);
	}
	else {
		Relation *relation = schemaManager.getRelation(tableName);
		for(int i=0;i<relation->getNumOfBlocks();i++) {
			relation->getBlock(i,0);
			Block *block = mainMemory.getBlock(0);
			vector<Tuple> tuples = block->getTuples();
			for(int j=0;j<tuples.size();j++) {
				if(whereConditionEvaluator(whereCondition, tuples[j])) {
					block->nullTuple(j);
				}
			}	
			relation->setBlock(i,0);
		}
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
		relation->getBlock(i,0);
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

bool validate(vector<string> tableNames) {
	for(int i=0;i<tableNames.size();i++) {
		if(!schemaManager.relationExists(tableNames[i])) {
			cout<<"Invalid Table Name "<< tableNames[i]<<endl;
			return true;
		}
	}
	return false;
}

void selectFromTable(bool dis, string attributes, string tabs, string whereCondition, string orderBy) {
	int disk0 = disk.getDiskIOs();
	vector<string> tableNames = split(tabs, ',');
	vector<Table> tables;
	vector<string> attributeNames = split(attributes, ',');
	string tableName;
	if(validate(tableNames)) return;
	if(tableNames.size()==1) {
		Relation *relation = schemaManager.getRelation(tableNames[0]);
		if(attributeNames.size()==1 && attributeNames[0]=="*") { 
			string d;
			if(dis) {
				d = distinct(tableNames[0]);
				relation = schemaManager.getRelation(d);
			}
			if(!whereCondition.empty()) {
				for(int i=0; i<relation->getNumOfBlocks();i++) {
					relation->getBlock(i,0);
					Block *block = mainMemory.getBlock(0);
					vector<Tuple> tuples = block->getTuples();
					for(int j=0;j<tuples.size();j++) {
						if(whereConditionEvaluator(whereCondition, tuples[j])) {
							cout<<tuples[j]<<endl;
						}
					}
				}
			}
			if(whereCondition.empty())
			cout<<*relation<<endl;
			if(dis)
			schemaManager.deleteRelation(d);
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
		vector<string> projections;
		if(tableNames.size()==2) {
			string temp = crossJoin(tableNames[0], tableNames[1]);
			if(attributeNames.size()==1 && attributeNames[0] =="*") {
				Relation *relation = schemaManager.getRelation(temp);
				cout<<*relation<<endl;
				schemaManager.deleteRelation(temp);
			}
		}
		else {
			//schemaBuilder(tableNames);
			bool flag =true;
			string str = crossJoin(tableNames[0],tableNames[1]);
			for(int i=2;i<tableNames.size();i++) {
				//mCrossJoin(tableNames[i]);
				str = xyz(str, tableNames[i]);
			}
			Relation *relation = schemaManager.getRelation(str);
			cout<<*relation<<endl;
			schemaManager.deleteRelation(str);
		}
	}
	cout<<"No. of disk IO's used for this opertaion are "<<disk.getDiskIOs()-disk0<<endl;
}

void writeTuples(string tableName, vector<Tuple> tuples) {
	vector<Tuple>::iterator it;
	Relation *relation = schemaManager.getRelation(tableName);
	Tuple tuple = relation->createTuple();
	Block *block = mainMemory.getBlock(0);
	block->clear();
	int index = 0;
	for(it=tuples.begin();it!=tuples.end();it++) {
		if(relation->getNumOfBlocks()>0) {
			relation->getBlock(relation->getNumOfBlocks()-1,0); 
			Block *block = mainMemory.getBlock(0);
			if(block->isFull()) {
				block->clear();
				block->appendTuple(*it);
				relation->setBlock(relation->getNumOfBlocks(),0);
			}
			else {
				block->appendTuple(*it);
				relation->setBlock(relation->getNumOfBlocks()-1,0);
			}
		}
		else {
			Block *block = mainMemory.getBlock(0);
			block->clear();
			block->setTuple(0,*it);
			relation->setBlock(0,0);
		}	
	}
}

vector<Tuple> sortTuples(vector<Tuple> tuples, string tableName) {
	return tuples;
}

void partition(vector<Tuple> &tuples, int left, int right) {
	int mid = left+(right-left)/2;
	Tuple pivot = tuples[mid];
	swap(tuples[mid], tuples[left]);
	int i = left;
	int j = right;
	while(i <= j) {
		while(compareNotEqual(tuples[left+1],pivot)==-1) i++;
		while(compareNotEqual(pivot, tuples[right])==1) j--;
		if(i<=j) {
			swap(tuples[i],tuples[j]);
			i++;
			j--;
		}
	}
	if(left<j) partition(tuples,left, j);
	if(right<i) partition(tuples,i,right);
	//swap(tuples[i-1], tuples[left]);
	//return i-1;
}
/*
void quickSort(vector<Tuple> &tuples, int left, int right) {
	if(left>=right) return;
	int p = partition(tuples,left,right);
	quickSort(tuples,left,p-1);
	quickSort(tuples,p+1,right);
}*/

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
			}	
			tuples = getDistinctTuples(tuples);
			//partition(tuples, 0, tuples.size()-1);
			if(flag) {	
				Relation *relation2= schemaManager.createRelation(tableName+"_dis", schema);
				flag = false;
			}
			writeTuples(tableName+"_dis", tuples);
			Relation *relation2 = schemaManager.getRelation(tableName+"_dis");
			tuples.clear();
			index = index+10;
			size = size-10;
			if(size<10)
			loadSize = size;
		}
		if(size<=100) {
			Relation *relation2 = schemaManager.createRelation(tableName+"_distinct", schema);
			relation = schemaManager.getRelation(tableName+"_dis");
			int buckets = relation->getNumOfBlocks()/10;
			vector<Tuple> tuples;
			for(int i=0;i<10;i++) {
				for(int j=0;j<buckets;j++) {
					if(j*10+i > relation->getNumOfBlocks()) break;
					relation->getBlock(i+10*j,j);
					Block *block = mainMemory.getBlock(j);
					for(int k=0;k<block->getNumTuples();k++) {
						tuples.push_back(block->getTuple(k));
					}
				}
			}
			tuples = getDistinctTuples(tuples);
			writeTuples(tableName+"_distinct", tuples);
			tuples.clear();
			schemaManager.deleteRelation(tableName+"_dis");
		}
		else 
		cerr<<"Table size exceeds the limit size(mainMemory)^2"<<endl;
	}
	return tableName+"_distinct";
}



int compareNotEqual(Tuple tuple1, Tuple tuple2) {
	Schema tupleSchema = tuple1.getSchema();
	for(int i=0;i<tuple1.getNumOfFields();i++) {
		if(tupleSchema.getFieldType(i) == INT) {
			if(tuple1.getField(i).integer > tuple2.getField(i).integer) return 1;
			if(tuple1.getField(i).integer < tuple2.getField(i).integer) return -1;
		}
		else {
			string str1,str2;	
			str1 = *(tuple1.getField(i).str);
			str2 = *(tuple2.getField(i).str);
			int diff = str1.compare(str2);
			if(diff!=0) return diff;
		}
	}
	return 0;
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

int evaluate(string str, Tuple tuple, bool flag) {
	vector<string> temp = split(str,'=');//should be able to evaluate < and >
	if(flag) {
		if(conditionMatches(tuple,temp)==0) return 1;
		else return 0;
	}
	return conditionMatches(tuple,temp); 
}

bool whereConditionEvaluator(string str, Tuple tuple) {
	vector<string> orConditions = splitWord(str, "OR");
	vector<map<int, bool> > values;
	map<int, bool> subMap;
	bool flag=false;
	regex exp("not\\ *(.*)", regex_constants::icase);
	cmatch field;
	for(int i=0;i<orConditions.size();i++) {
		vector<string> andConditions = splitWord(orConditions[i], "AND");
		for(int j=0;j<andConditions.size();j++) {
			if(regex_match(andConditions[j].c_str(), field, exp)) {
				flag=true;
				andConditions[j] = field[1];
			}
			subMap[j]  = evaluate(andConditions[j], tuple, flag);
			flag = false;
		}
		values.push_back(subMap);
		subMap.clear();
	}
	int multiplier = 1;
	for(int i=0;i<values.size();i++) {
		bool flag = false;
		int c=0;
		for(int j=0;j<values[i].size();j++) {
			c = c+values[i][j];
			
		}
		if(c>1) c=1;
		multiplier = multiplier*c;
	}
	if(multiplier==0) return true;
	return false;
}

void insertIntoRelation(string tableName, Tuple tuple) {
	Relation *relation = schemaManager.getRelation(tableName);
	if(relation->getNumOfBlocks()>0){
                relation->getBlock(relation->getNumOfBlocks()-1,9);
                Block *block = mainMemory.getBlock(9);
                if(block->isFull()){
                        block->clear();
                        block->appendTuple(tuple);
                        relation->setBlock(relation->getNumOfBlocks(),9);
                }
                else {
                        block->appendTuple(tuple);
                        relation->setBlock(relation->getNumOfBlocks()-1,9);
                }
        }
        else {
                Block *block = mainMemory.getBlock(9);
                block->clear();
                block->setTuple(0,tuple);
                relation->setBlock(0,9);
        }

}

void join(Tuple tuple1, Tuple tuple2, string tableName1, string tableName2) {
	Relation *relation = schemaManager.getRelation(tableName2+"_join");
	Tuple tuple =relation->createTuple();
	for(int i=0;i<tuple1.getNumOfFields();i++) {
		if(tuple1.getSchema().getFieldType(i) == INT)
		tuple.setField(tableName1+"."+tuple1.getSchema().getFieldName(i), tuple1.getField(i).integer);
		else
		tuple.setField(tableName1+"."+tuple1.getSchema().getFieldName(i), *(tuple1.getField(i).str) );
	}
	for(int i=0;i<tuple2.getNumOfFields();i++) {
	        if(tuple2.getSchema().getFieldType(i) == INT)
                tuple.setField(tableName2+"."+tuple2.getSchema().getFieldName(i), tuple2.getField(i).integer);
                else                
		tuple.setField(tableName2+"."+tuple2.getSchema().getFieldName(i), *(tuple2.getField(i).str) );
        }
	insertIntoRelation(tableName2+"_join", tuple);
}

string crossJoin(string tableName1, string tableName2) {
	string small,big;
	if(schemaManager.getRelation(tableName1)->getNumOfBlocks()<=schemaManager.getRelation(tableName2)->getNumOfBlocks()) {
		small = tableName1;
		big = tableName2;
	}
	else {
		small=tableName2;
		big=tableName1;
	}
	Schema schema1 = schemaManager.getSchema(small);
        Schema schema2 = schemaManager.getSchema(big);
        vector<string> fieldNames;
        vector<enum FIELD_TYPE> fieldTypes;
        for(int i=0;i<schema1.getNumOfFields();i++) {
                fieldNames.push_back(small+"."+schema1.getFieldName(i));
                fieldTypes.push_back(schema1.getFieldType(i));
        }
        for(int i=0;i<schema2.getNumOfFields();i++) {
                fieldNames.push_back(big+"."+schema2.getFieldName(i));
                fieldTypes.push_back(schema2.getFieldType(i));
        }
        Schema schema(fieldNames,fieldTypes);
        Relation *relation = schemaManager.createRelation(big+"_join",schema);
	Relation *relation1 = schemaManager.getRelation(small);
	Relation *relation2 = schemaManager.getRelation(big);
	int size1 = relation1->getNumOfBlocks(), size2 = relation2->getNumOfBlocks();
	for(int x=0;x<size1;x++) {
		relation1->getBlock(x,0);
		Block *block0 = mainMemory.getBlock(0);
		for(int y=0;y<block0->getNumTuples();y++) {
			Tuple tuple1 = block0->getTuple(y);	
			for(int i=0;i<size2;i++) {
				relation2->getBlock(i,1);
				Block *block = mainMemory.getBlock(1);
				for(int j=0;j<block->getNumTuples();j++) {
					Tuple tuple2 = block->getTuple(j);
					join(tuple1, tuple2, small, big);
				}
			}
		}
	}
	string rt = big+"_join";
	return rt;
}

void joinxyz(Tuple tuple1, Tuple tuple2, string tableName2) {
        Relation *relation = schemaManager.getRelation(tableName2+"_join");
        Tuple tuple =relation->createTuple();
        for(int i=0;i<tuple1.getNumOfFields();i++) {
                if(tuple1.getSchema().getFieldType(i) == INT)
                tuple.setField(tuple1.getSchema().getFieldName(i), tuple1.getField(i).integer);
                else
                tuple.setField(tuple1.getSchema().getFieldName(i), *(tuple1.getField(i).str) );
        }
        for(int i=0;i<tuple2.getNumOfFields();i++) {
                if(tuple2.getSchema().getFieldType(i) == INT)
                tuple.setField(tableName2+"."+tuple2.getSchema().getFieldName(i), tuple2.getField(i).integer);
                else
                tuple.setField(tableName2+"."+tuple2.getSchema().getFieldName(i), *(tuple2.getField(i).str) );
        }
        insertIntoRelation(tableName2+"_join", tuple);
}

string xyz(string tableName1, string tableName2) {
	Schema schema1 = schemaManager.getSchema(tableName1);
        Schema schema2 = schemaManager.getSchema(tableName2);
        vector<string> fieldNames;
        vector<enum FIELD_TYPE> fieldTypes;
        for(int i=0;i<schema1.getNumOfFields();i++) {
                fieldNames.push_back(schema1.getFieldName(i));
                fieldTypes.push_back(schema1.getFieldType(i));
        }
        for(int i=0;i<schema2.getNumOfFields();i++) {
                fieldNames.push_back(tableName2+"."+schema2.getFieldName(i));
                fieldTypes.push_back(schema2.getFieldType(i));
        }
	Schema schema(fieldNames,fieldTypes);
        Relation *relation = schemaManager.createRelation(tableName2+"_join",schema);
        Relation *relation1 = schemaManager.getRelation(tableName1);
        Relation *relation2 = schemaManager.getRelation(tableName2);
	cout<<*relation1<<endl;
        int size1 = relation1->getNumOfBlocks(), size2 = relation2->getNumOfBlocks();
        for(int x=0;x<size1;x++) {
                relation1->getBlock(x,0);
                Block *block0 = mainMemory.getBlock(0);
                for(int y=0;y<block0->getNumTuples();y++) {
                        Tuple tuple1 = block0->getTuple(y);
                        for(int i=0;i<size2;i++) {
                                relation2->getBlock(i,1);
                                Block *block = mainMemory.getBlock(1);
                                for(int j=0;j<block->getNumTuples();j++) {
                                        Tuple tuple2 = block->getTuple(j);
                                        joinxyz(tuple1, tuple2, tableName2);
                                }
                        }
                }
        }
	schemaManager.deleteRelation(tableName1);
        string rt = tableName2+"_join";
        return rt;
}


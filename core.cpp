#include<iostream>
#include<regex>
#include<string>
#include<vector>
#include<algorithm>
#include<stack>
#include<sstream>
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

void insertTuple(string tableName, Tuple tuple) {
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

string getColumnValues(string expression, Tuple tuple) {
	stringstream ss(expression);
        char c;
        string internal;
        string str;
        while(ss.get(c)) {
                if(c=='(' || c==')' || c=='*' || c=='/' || c=='+' || c=='-' || isdigit(c) || c==' ') {
                        if(!str.empty()) 
                        internal.append(to_string(tuple.getField(str).integer));
                        internal.push_back(c);
                        str = "";
                }
                else
                str.push_back(c);
        }
	if(!str.empty()) internal.append(to_string(tuple.getField(str).integer));
        return internal;

}

int calculate(char op, int value2, int value1) {
        switch(op) {
                case '+': return value1+value2;
                case '-': return value1-value2;
                case '*': return value1*value2;
                case '/':
                        if(value2 != 0)
                        return value1/value2;
        }
        return 0;

}

bool precedence(char op1, char op2) {
        if(op2 == '(' || op2 == ')') return false;
        if((op1 == '*' || op1 == '/')&&(op2=='+' || op2 == '-')) return false;
        else return true;
}

int mathParser(string expression) {
        stack<int> values;
        stack<char> operators;
        const char *c = expression.c_str();
        for(int i=0;i<expression.length();i++) {
                if(c[i] == ' ') continue;
                if(c[i]>= '0' && c[i]<='9') {
                        string str;
                        while(i<expression.length() && c[i]>='0' && c[i]<='9')
                        str.push_back(c[i++]);
                        values.push(stoi(str));
		}
                if(c[i] == '(') operators.push(c[i]);
                if(c[i] == ')') {
                        while(operators.top() != '(') {
                                char x = operators.top();
                                operators.pop();
                                int y = values.top();
                                values.pop();
                                int z = values.top();
                                values.pop();
                                values.push(calculate(x,y,z));
                        }
                        operators.pop();

                }
                if(c[i] == '+' || c[i] == '-' || c[i] == '*' || c[i] == '/') {
                        while(!operators.empty() && precedence(c[i], operators.top())) {
                                char x = operators.top();
                                operators.pop();
                                int y = values.top();
                                values.pop();
                                int z = values.top();
                                values.pop();
                                values.push(calculate(x,y,z));
                        }
                        operators.push(c[i]);
                }
        }
        while(!operators.empty()) {
                char x = operators.top();
                operators.pop();
                int y = values.top();
                values.pop();
                int z = values.top();
                values.pop();
                values.push(calculate(x,y,z));
        }      
        return values.top();
}

int eval(Tuple tuple, vector<string> tokens, int op) {
	string token0 = getColumnValues(tokens[0], tuple);
	string token1 = getColumnValues(tokens[1], tuple);
	int t1 = mathParser(token0);
	int t2 = mathParser(token1);
	switch(op) {
		case 0:
			if(t1==t2) return 0;
			return 1;
		case 1:
			if(t1>t2) return 0;
			return 1;
		case 2:
			if(t1<t2) return 0;
			return 1;
	}
	return 0;
}

bool ifExists(string str, string symbol) {
	regex exp(symbol, regex_constants::icase);
	if(regex_search(str,exp)) return true;
	return false;

}

int conditionMatches(Tuple tuple, vector<string> tokens, int op) {
	Schema schema = tuple.getSchema();
	bool flag1=false, flag2=false;
	if(ifExists(tokens[1], "\\+|\\/|\\-|\\/|\\(|\\)|\\*") || ifExists(tokens[0], "\\+|\\/|\\-|\\/|\\(|\\)|\\*")) {
		return eval(tuple, tokens, op);
	}
	for(int i=0;i<schema.getNumOfFields();i++) {
		if(schema.getFieldName(i) == tokens[0])
		flag1=true;
		if(schema.getFieldName(i) == tokens[1])
		flag2=true; 
	}
	if(flag1 && flag2) {
		if(tuple.getSchema().getFieldType(tokens[0]) == INT) {
			switch(op) {
			case 0:
				if(tuple.getField(tokens[0]).integer == tuple.getField(tokens[1]).integer)	
				return 0;
				else return 1;
			case 1:
				if(tuple.getField(tokens[0]).integer > tuple.getField(tokens[1]).integer)      
                                return 0;
                                else return 1;
			case 2:
				if(tuple.getField(tokens[0]).integer < tuple.getField(tokens[1]).integer)      
                                return 0;
                                else return 1;
			}
		}
		else {
			if(*(tuple.getField(tokens[1]).str) == *(tuple.getField(tokens[1]).str))	return 0;
			else return 1;
		}
	}
	if(flag1)
	{
		if(tuple.getSchema().getFieldType(tokens[0]) == INT) {
			int fieldValue = tuple.getField(tokens[0]).integer;
			if(isNumber(tokens[1])){
				switch (op) {
				case 0:
					if(fieldValue == stoi(tokens[1]))
					return 0; 
					else 
					return 1;
				case 1:
					if(fieldValue > stoi(tokens[1]))
					return 0;
					else
					return 1;
				case 2:
					if(fieldValue < stoi(tokens[1]))
					return 0;
					else
					return 1;
				}
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

int evaluate(string str, Tuple tuple, bool flag) {
	if(str=="0" || str=="1") {
		if(flag) {
			if(str=="0") return 1;
			else return 0;
		}
		else 
		return stoi(str);
		
	}
	vector<string> temp = split(str,'=');
	int op = 0;
	if(ifExists(str, ">")) {
		temp.clear();
		temp = split(str,'>');
                op = 1;
	}
	if(ifExists(str, "<")) {
		temp.clear();
		temp = split(str,'<');
                op = 2;
	}
	if(flag) {
		if(conditionMatches(tuple,temp,op)==0) return 1;
		else return 0;
	}
	return conditionMatches(tuple,temp,op); 
}

bool whereConditionEvaluator(string whereCondition, Tuple tuple) {
	if(whereCondition.empty()) 
		return true;
	int begin = whereCondition.find("[");
	int end = whereCondition.find("]");
	while(begin != -1 && end!=-1) {
		whereCondition.replace(begin, end-begin+1, ( whereConditionEvaluator(whereCondition.substr(begin+1, end-begin-1), tuple) == true)?"0":"1");
		begin = whereCondition.find("[");
		end = whereCondition.find("]");
	}
	vector<string> orConditions = splitWord(whereCondition, "OR");
	vector<map<int, bool> > values;
	map<int, bool> subMap;
	bool flag=false;
	regex exp("not\\ *(.*)", regex_constants::icase);
	cmatch field;
	for(int i=0;i<orConditions.size();i++) {
		vector<string> andConditions = splitWord(orConditions[i], "AND");
		for(int j=0;j<andConditions.size();j++) {
			/*if(str.compare("0") == 0 || str == "1") {
				cout<<"andCondition is already evaluated: "<<andConditions[j]<<endl;
				if(isNumber(andConditions[j]))
				subMap[j] = stoi(andConditions[j]);
			}
			else {*/
				if(regex_match(andConditions[j].c_str(), field, exp)) {
					flag=true;
					andConditions[j] = field[1];
				}
				subMap[j]  = evaluate(andConditions[j], tuple, flag);
				flag = false;
		//	}
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

string projection(vector<string> attributes, string tableName, string whereCondition) {
	
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
			if(tableSchema.getFieldName(i)==temp || tableName+"."+tableSchema.getFieldName(i) == temp) 
			flag=i;
		}
		if(flag!=-1) {
			fieldNames.push_back(tableSchema.getFieldName(flag));
			fieldTypes.push_back(tableSchema.getFieldType(flag));
			flag = -1;
		}	
	}
	if(attributes.size()==1 && attributes[0] == "*") {
		if(whereCondition.empty()) return tableName;
		fieldNames = tableSchema.getFieldNames();
		fieldTypes = tableSchema.getFieldTypes();
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
			if(!it1->isNull()){
			for(int j=0;j<fieldNames.size();j++) {
				if(fieldTypes[j]==INT)
				tuple.setField(fieldNames[j],it1->getField(fieldNames[j]).integer);
				else
				tuple.setField(fieldNames[j],*(it1->getField(fieldNames[j]).str));
			}
			bool ttp = whereConditionEvaluator(whereCondition, *it1);
			if(ttp) {
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

void insertTuples(string tableName, vector<Tuple> tuples) {
	
	for(int i=0;i<tuples.size();i++) {
		insertTuple(tableName, tuples[i]);
	}
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

string distinct(string tableName) {

	Relation *relation = schemaManager.getRelation(tableName);
	Schema schema = relation->getSchema();
	int size = relation->getNumOfBlocks();
	vector<Tuple> tuples;
	bool flag = true;
	//one-pass
	if(size<=10) {
		relation->getBlocks(0,0,size);
		tuples = mainMemory.getTuples(0,size);
		tuples = getDistinctTuples(tuples);
		Relation *relation1 = schemaManager.createRelation(tableName+"_distinct",schema); 
		insertTuples(tableName+"_distinct",tuples);
	}
	//two pass
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
			insertTuples(tableName+"_dis", tuples);
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
			insertTuples(tableName+"_distinct", tuples);
			tuples.clear();
			schemaManager.deleteRelation(tableName+"_dis");
		}
		else 
		cerr<<"Table size exceeds the limit size(mainMemory)^2"<<endl;
	}
	return tableName+"_distinct";
}

void join(Tuple tuple1, Tuple tuple2, string tableName1, string tableName2, string whereCondition, bool multi, vector<string> attributes) {
	Relation *relation = schemaManager.getRelation(tableName2+"_join");
	Tuple tuple =relation->createTuple();
	if(!multi) {
		for(int i=0;i<tuple1.getNumOfFields();i++) {
			if(tuple1.getSchema().getFieldType(i) == INT)
			tuple.setField(tableName1+"."+tuple1.getSchema().getFieldName(i), tuple1.getField(i).integer);
			else
			tuple.setField(tableName1+"."+tuple1.getSchema().getFieldName(i), *(tuple1.getField(i).str) );
		}
	}
	else {
		 for(int i=0;i<tuple1.getNumOfFields();i++) {
                        if(tuple1.getSchema().getFieldType(i) == INT)
                        tuple.setField(tuple1.getSchema().getFieldName(i), tuple1.getField(i).integer);
                        else
                        tuple.setField(tuple1.getSchema().getFieldName(i), *(tuple1.getField(i).str) );
                }
	}
	for(int i=0;i<tuple2.getNumOfFields();i++) {
	        if(tuple2.getSchema().getFieldType(i) == INT)
                tuple.setField(tableName2+"."+tuple2.getSchema().getFieldName(i), tuple2.getField(i).integer);
                else                
		tuple.setField(tableName2+"."+tuple2.getSchema().getFieldName(i), *(tuple2.getField(i).str) );
        }
	if((attributes.size()==1 && attributes[0]=="*") || multi) {
		if(whereConditionEvaluator(whereCondition, tuple)) 
		insertTuple(tableName2+"_join", tuple);
	}
	else {
		Relation *relation1 = schemaManager.getRelation(tableName2+"_joinp");
		Tuple tuplep = relation1->createTuple();
		for(int i=0;i<attributes.size();i++) {
			if(tuplep.getSchema().getFieldType(attributes[i]) == INT)
			tuplep.setField(attributes[i], tuple.getField(attributes[i]).integer);
			else
			tuplep.setField(attributes[i], *(tuple.getField(attributes[i]).str));
		}	
		if(whereConditionEvaluator(whereCondition, tuple))
		insertTuple(tableName2+"_joinp", tuplep);
	}
}

string crossJoin(vector<string> attributes, string tableName1, string tableName2, string whereCondition, bool multi) {

	string small,big;
	bool proj = false;
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
	if(!multi) {
        	for(int i=0;i<schema1.getNumOfFields();i++) {
                	fieldNames.push_back(small+"."+schema1.getFieldName(i));
                	fieldTypes.push_back(schema1.getFieldType(i));
        	}
	}
	else {
		for(int i=0;i<schema1.getNumOfFields();i++) {
                        fieldNames.push_back(schema1.getFieldName(i));
                        fieldTypes.push_back(schema1.getFieldType(i));
                }
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
	if(!((attributes.size()==1 && attributes[0]=="*") || multi)) {
		vector<string> fieldNames1;
		vector<enum FIELD_TYPE> fieldTypes1;
		for(int i=0;i<attributes.size();i++) {
			int temp = schema.getFieldOffset(attributes[i]);
			fieldNames1.push_back(schema.getFieldName(temp));
			fieldTypes1.push_back(schema.getFieldType(attributes[i]));
		}
		Schema schema1(fieldNames1, fieldTypes1);
		proj = true;
		Relation *relationp = schemaManager.createRelation(big+"_joinp", schema1);
	}
	if(size1<=10) {
		relation1->getBlocks(0,0,size1);
		vector<Tuple> tuples = mainMemory.getTuples(0,size1);
		for(int x=0;x<tuples.size();x++) {
			for(int i=0;i<size2;i++) {
                                 relation2->getBlock(i,1);
                                 Block *block = mainMemory.getBlock(1);
                                 for(int j=0;j<block->getNumTuples();j++) {
                                         Tuple tuple2 = block->getTuple(j);
                                         join(tuples[x], tuple2, small, big, whereCondition, multi, attributes);
                                 }
                         }

		}
	}	
	else {
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
					join(tuple1, tuple2, small, big, whereCondition, multi, attributes);
				}
			}
		}
	}
	}
	string rt = big+"_join";
	if(proj) {
		schemaManager.deleteRelation(rt);
		rt = big+"_joinp";
	}
	return rt;
}

/* 
 * database access functions
 * create, drop, insert, delete, select statements
 */
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
	insertTuple(tableName, tuple);
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
				if(!tuples[j].isNull() && whereConditionEvaluator(whereCondition, tuples[j])) {
					block->nullTuple(j);
				}
			}	
			relation->setBlock(i,0);
		}
	}
}

void selectFromTable(bool dis, string attributes, string tabs, string whereCondition, string orderBy) {
	
	int disk0 = disk.getDiskIOs();
	vector<string> tableNames = split(tabs, ',');
	vector<string> attributeNames = split(attributes, ',');
	string tableName;
	if(validate(tableNames)) return;
	if(tableNames.size()==1) {
		Relation *relation = schemaManager.getRelation(tableNames[0]);
		string pName = projection(attributeNames, tableNames[0], whereCondition);
		string d;
		relation = schemaManager.getRelation(pName);
		if(dis) {
			d = distinct(pName);
			relation = schemaManager.getRelation(d);	
		}
		cout<<*relation<<endl;
		if(!(attributeNames.size()==1 && attributeNames[0]=="*" && whereCondition.empty()))
		schemaManager.deleteRelation(pName);
		if(dis)
		schemaManager.deleteRelation(d);
	}
	else {
		vector<string>::iterator it;
		vector<string> projections;
		if(tableNames.size()==2) {
			string ptemp = crossJoin(attributeNames, tableNames[0], tableNames[1], whereCondition, false);
			string d;
			Relation *relation = schemaManager.getRelation(ptemp);
			if(dis) {
				d = distinct(ptemp);
				relation = schemaManager.getRelation(d);
			} 
			cout<<*relation<<endl;
			schemaManager.deleteRelation(ptemp);
			if(dis)
			schemaManager.deleteRelation(d);
		}
		else {
			bool flag =true;
			vector<string> blah;
			string str = crossJoin(blah, tableNames[0],tableNames[1], whereCondition, false);
			for(int i=2;i<tableNames.size();i++) {
				str = crossJoin(blah, str, tableNames[i], whereCondition, true);
			}
			Relation *relation = schemaManager.getRelation(str);
			cout<<*relation<<endl;
			schemaManager.deleteRelation(str);
		}
	}
	cout<<"No. of disk IO's used for this opertaion are "<<disk.getDiskIOs()-disk0<<endl;
}

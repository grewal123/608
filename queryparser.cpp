#include<iostream>
#include<string>
#include<regex>
#include<vector>
#include<algorithm>
#include<sstream>
#include <string.h>
using namespace std;

extern void createTable(string tableName, vector<string> fieldNames, vector<string> fieldTypes);

extern void dropTable(string tableName);

extern void insertIntoTable(string tableName, vector<string> fieldNames, vector<string> fieldValues);

extern void deleteFromTable(string tableName);

extern void selectFromTable(bool distinct, string columnNames, string tableName);

extern void deleteTuplesFromTable(string tableName,vector<string> tokens);

extern void deleteTuplesFromTable(string tableName,vector<string> tokens1,vector<string> token2);

extern vector<string>  tokenize(string condition);



vector<string> split(string str, char delimiter) {
  vector<string> internal;
  stringstream ss(str); 
  string token;
  
  while(getline(ss, token, delimiter)) {
    internal.push_back(token);
  }
  
  return internal;
}
void runQuery(string query);

int main() {
bool flag=true;
string query, exit="exit";
while(flag){
cout<<"sql>  ";
getline(cin,query);
flag = exit.compare(query);
if(flag){
runQuery(query);
}

}
return 0;
}
void runQuery(string query) {
string createQuery,fieldQuery,dropQuery,insertQuery,deleteQuery, selectQuery, selectQuery0, selectQuery1, selectQuery2, selectQuery3, distinctQuery;
cmatch attributes,fields;
string tableName;
bool distinct=false;

createQuery ="^create table ([a-zA-z0-9]*)\\ ?[(](.*)[)]";
regex createPattern(createQuery, regex_constants::icase);
fieldQuery = "\\ *([a-zA-Z0-9]*)\\ *(int|str20)\\ *"; 
regex fieldPattern(fieldQuery, regex_constants::icase);
dropQuery = "^drop table ([a-zA-Z0-9]*)";
regex dropPattern(dropQuery, regex_constants::icase);
insertQuery = "^insert into ([a-zA-Z0-9]*)\\ *[(](([a-zA-Z0-9]*\\,*\\ *)*)[)]\\ *values\\ *[(]((.*)|select .*)[)]";
//insertQuery = "^insert into ([a-zA-Z0-9]*)\\ *[(](([a-zA-Z0-9]*\\,*\\ *)*)[)]\\ *values\\ *[(](([[:alnum:], \"]*\\,*\\ *)*|select .*)[)]";
regex insertPattern(insertQuery, regex_constants::icase);
deleteQuery = "^delete from ([a-zA-Z0-9]*)\\ *((where) (.*))*";
regex deletePattern(deleteQuery, regex_constants::icase);
//conditionQuery for conditions in delete query
string conditionQuery = "((.*))\\ *(and)\\ *(.*)";
regex conditionPattern(conditionQuery, regex_constants::icase);
//select queries
selectQuery = "^select (.*)";
selectQuery0 = "^select (.*) from (.*) where (.*) order by (.*)";
selectQuery1 = "^select (.*) from (.*) order by (.*)";
selectQuery2 = "^select (.*) from (.*) where (.*)";
selectQuery3 = "^select (.*) from (.*)";
regex selectPattern(selectQuery, regex_constants::icase);
regex selectPattern0(selectQuery0, regex_constants::icase);
regex selectPattern1(selectQuery1, regex_constants::icase);
regex selectPattern2(selectQuery2, regex_constants::icase);
regex selectPattern3(selectQuery3, regex_constants::icase);

distinctQuery = "\\ *distinct (.*)";
regex distinctPattern(distinctQuery, regex_constants::icase);


//create table statement
if(regex_match(query.c_str(),attributes,createPattern)) {
	tableName = attributes[1];
	vector<string> data = split(attributes[2],',');
	vector<string> fieldNames;
	vector<string> fieldTypes;
	vector<string>::iterator it, it1, it2;
	for(it= data.begin();it!=data.end();it++) {
		string str = *it;
		if(regex_match(str.c_str(),fields,fieldPattern)) {
			fieldNames.push_back(fields[1]);
			fieldTypes.push_back(fields[2]);

		}
		else {
			cout<<"Invalid query1 "<<query<<endl;
			return;
		}
	}
	createTable(tableName, fieldNames, fieldTypes);
	return;
}

//drop table statement
else if(regex_match(query.c_str(),attributes,dropPattern)) {
	tableName = attributes[1];
	dropTable(tableName);
	return;
}

//insert into statement
else if(regex_match(query.c_str(), attributes, insertPattern)) {
	cout<<"insert statment"<<endl;
	tableName = attributes[1];
	string attributeList = attributes[2];
	string valueList = attributes[4];
	vector<string>::iterator it;
	if(regex_match(valueList,selectPattern)) {
		cout<<"2 match"<<endl;
		runQuery(valueList);
		cout<<"implement a way to get the values from select and pass to vector<string> fieldValues"<<endl;
		return;
	}	//process select and the values to the fieldValues
      	vector<string> fieldNames = split(attributeList, ',');
	vector<string> fieldValues = split(valueList, ',');
	if(fieldNames.size()!=fieldValues.size()) {
		cout<<"invalid statement"<<query<<endl;
		return;
	}
	insertIntoTable(tableName,fieldNames,fieldValues);
	return;
}

//delete from statment
else if(regex_match(query.c_str(),attributes,deletePattern)) {
	tableName = attributes[1];
	
	if(attributes[3]=="WHERE")
	{
		//dividing the string after "where" in tokens
		//seperated by = and push them to vector of strings
		//tokens[0] is field name 
		//token[1]  is field value
		string str = attributes[4];
		if(regex_match(str.c_str(),attributes,conditionPattern))
		{
			string condition1 = attributes[2];
			string condition2 = attributes[4];
			vector<string> tokens1,tokens2;
			tokens1 = tokenize(condition1);
			tokens2 = tokenize(condition2);
			deleteTuplesFromTable(tableName,tokens1,tokens2);
		
		}
		else
		{
	     	vector<string> tokens = tokenize(str);
	     	deleteTuplesFromTable(tableName,tokens);
	    }
		return;
	}
	deleteFromTable(tableName);
	return;
}

//select statement
else if(regex_match(query.c_str(),selectPattern)) {

	if(regex_match(query.c_str(), attributes, selectPattern0)) {
	}
	else if(regex_match(query.c_str(), attributes, selectPattern1)) {
        }
	else if(regex_match(query.c_str(), attributes, selectPattern2)) {
        }
	else if(regex_match(query.c_str(), attributes, selectPattern3)) {
		string temp = attributes[1];
		if(regex_match(temp.c_str(), fields, distinctPattern)) {
			distinct = true;
			temp = fields[1];	
		}		
        	selectFromTable(distinct, temp, attributes[2]);
		return;
	}
	else {
		cout<<"invalid statement "<<query<<endl;	
		return;
	}
}
else {
	cout<<"Invalid query "<<query<<endl;
	return;
}
return;
}

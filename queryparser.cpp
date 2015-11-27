#include<iostream>
#include<string>
#include<regex>
#include<vector>
#include<algorithm>
#include<sstream>
#include<cstddef>

using namespace std;

extern void createTable(string tableName, vector<string> fieldNames, vector<string> fieldTypes);
extern void dropTable(string tableName);
extern void insertIntoTable(string tableName, vector<string> fieldNames, vector<string> fieldValues);
extern void deleteFromTable(string tableName, string whereCondition);
extern void selectFromTable(bool distinct, string columnNames, string tableNames, string whereCondition, string orderBy);
extern string removeSpaces(string str);

vector<string> split(string str, char delimiter) {
  vector<string> internal;
  stringstream ss(str); 
  string token;
  while(getline(ss, token, delimiter)) {
    internal.push_back(removeSpaces(token));
  }
  return internal;
}

string trimSpaces(string input) {
	string str = input;
	string whiteSpaces = (" \t\f\n\v\r");
	str.erase(str.find_last_not_of(whiteSpaces)+1);
	str.erase(0, str.find_first_not_of(whiteSpaces));
	return str;
}

vector<string> splitWord(string str, string splitter) {
	vector<string> internal;
	size_t index;
	while((index=str.find(splitter))!=string::npos) {
		internal.push_back(trimSpaces(str.substr(0,index)));
		str = str.substr(index+splitter.size(), str.size());
	}
	internal.push_back(trimSpaces(str));
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
regex insertPattern(insertQuery, regex_constants::icase);
deleteQuery = "^delete from ([a-zA-Z0-9]*)\\ *((where) (.*))*";
regex deletePattern(deleteQuery, regex_constants::icase);
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
	string whereCondition = attributes[4];
	deleteFromTable(tableName, whereCondition);
	return;
}

//select statement
else if(regex_match(query.c_str(),selectPattern)) {

	if(regex_match(query.c_str(), attributes, selectPattern0)) {
		string temp = attributes[1];
                if(regex_match(temp.c_str(), fields, distinctPattern)) {
                        distinct = true;
                        temp = fields[1];
                }
		selectFromTable(distinct, temp, attributes[2], attributes[3], attributes[4]);
		return;
	}
	else if(regex_match(query.c_str(), attributes, selectPattern1)) {
        	string temp = attributes[1];
                if(regex_match(temp.c_str(), fields, distinctPattern)) {
                        distinct = true;
                        temp = fields[1];
                }
		selectFromTable(distinct, temp, attributes[2], "", attributes[3]);
		return;
	}
	else if(regex_match(query.c_str(), attributes, selectPattern2)) {
		string temp = attributes[1];
                if(regex_match(temp.c_str(), fields, distinctPattern)) {
                        distinct = true;
                        temp = fields[1];
                }
		selectFromTable(distinct, temp, attributes[2], attributes[3], "");
       		return;
	 }
	else if(regex_match(query.c_str(), attributes, selectPattern3)) {
		string temp = attributes[1];
		if(regex_match(temp.c_str(), fields, distinctPattern)) {
			distinct = true;
			temp = fields[1];	
		}		
        	selectFromTable(distinct, temp, attributes[2], "", "");
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

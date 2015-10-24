#include<iostream>
#include<string>
#include<regex>
#include<vector>
#include<algorithm>
#include<sstream>
using namespace std;

extern void createTable(string tableName, vector<string> fieldNames, vector<string> fieldTypes);

extern void dropTable(string tableName);

extern void insertIntoTable(string tableName, vector<string> fieldNames, vector<string> fieldValues);

vector<string> split(string str, char delimiter) {
  vector<string> internal;
  stringstream ss(str); // Turn the string into a stream.
  string tok;
  
  while(getline(ss, tok, delimiter)) {
    internal.push_back(tok);
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
string createQuery,fieldQuery,dropQuery,insertQuery,deleteQuery;
cmatch attributes,fields;
string tableName;

createQuery ="^create table ([a-zA-z0-9]*)\\ ?[(](.*)[)]";
regex createPattern(createQuery, regex_constants::icase);
fieldQuery = "\\ *([a-zA-Z0-9]*)\\ *(int|str20)\\ *"; 
regex fieldPattern(fieldQuery, regex_constants::icase);
dropQuery = "^drop table ([a-zA-Z0-9]*)";
regex dropPattern(dropQuery, regex_constants::icase);
insertQuery = "^insert into ([a-zA-Z0-9]*)\\ *[(](([a-zA-Z0-9]*\\,*\\ *)*)[)]\\ *values\\ *[(]((.*)|select .*)[)]";
//insertQuery = "^insert into ([a-zA-Z0-9]*)\\ *[(](([a-zA-Z0-9]*\\,*\\ *)*)[)]\\ *values\\ *[(](([[:alnum:], \"]*\\,*\\ *)*|select .*)[)]";
regex insertPattern(insertQuery, regex_constants::icase);
deleteQuery = "^delete from ([a-zA-Z0-9]*)\\ *(where (.*))";
regex deletePattern(deleteQuery, regex_constants::icase);


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
	//if(false) valueList == Select statement
		//process select and the values to the fieldValues
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
	cout<<attributes[1]<<endl<<attributes[2]<<endl<<attributes[3]<<endl<<attributes[4]<<endl<<attributes[5]<<endl<<attributes[6]<<endl;
}
else {
	cout<<"Invalid query "<<query<<endl;
	return;
}
return;
}

#include <vector>
#include <string>
#include <iostream>
#include <cstdlib>
#include <cstdio>

#include "Schema.h"
#include "Catalog.h"


using namespace std;


int main (int argc, char* argv[]) {

	if (argc != 4) {
		cout << "Usage: main [sqlite_file] [no_tables] [no_atts]" << endl;
		return -1;
	}


	string dbFile = argv[1];

	int tNo = atoi(argv[2]);

	int aNo = atoi(argv[3]);


	Catalog catalog(dbFile);



	//catalog.Test();
	




	cout << catalog << endl; cout.flush();
	//cout<<"tNo: "<<tNo<<" aNo: "<<aNo<<endl;


	////////////////////////////////
	for (int i = 0; i < tNo; i++) {
	//for (int i = 0; i < 20; i++) {
		char tN[1000]; sprintf(tN, "T_%d", i);
		
		string tName = tN;



		int taNo = i * aNo;
		vector<string> atts;
		vector<string> types;
		for (int j = 0; j < taNo; j++) {
			char aN[20]; sprintf(aN, "A_%d_%d", i, j);
			string aName = aN;
			atts.push_back(aN);

			string aType;
			int at = j % 3;
			//if (0 == at) aType = "Integer";
			//else if (1 == at) aType = "Float";
			//else if (2 == at) aType = "String";

			if (0 == at) aType = "INTEGER";
			else if (1 == at) aType = "FLOAT";
			else if (2 == at) aType = "STRING";
			types.push_back(aType);
		}
		bool ret = catalog.CreateTable(tName, atts, types);
		if (true == ret) {
			cout << "CREATE TABLE " << tName << " OK" << endl;

			for (int j = 0; j < taNo; j++) {
				unsigned int dist = i * 10 + j;
				string aN = atts[j];
				catalog.SetNoDistinct(tName, atts[j], dist);
			}

			unsigned int tuples = i * 1000;
			catalog.SetNoTuples(tName, tuples);

			string path = "/home/user/DATA/" + tName + ".dat";
			catalog.SetDataFile(tName, path);
		}
		else {
			cout << "CREATE TABLE " << tName << " FAIL" << endl;
		}
	}


	////////////////////////////////
	catalog.Save();
	cout << catalog << endl; cout.flush();


	////////////////////////////////
	vector<string> tables;
	catalog.GetTables(tables);
	for (vector<string>::iterator it = tables.begin();
		 it != tables.end(); it++) {
		cout << *it << endl;

	}
	cout << endl;


	////////////////////////////////
	for (int i = 0; i < 2; i++) {
		//int r = rand() % tNo + 1;
		//cout<<"CONTENTS OF R:				"<<r<<endl;
		char tN[20]; sprintf(tN, "T_%d", i);
		string tName = tN;

		unsigned int tuples;
		catalog.GetNoTuples(tName, tuples);
		cout << tName << " tuples = " << tuples << endl;

		string path;
		catalog.GetDataFile(tName, path);
		cout << tName << " path = " << path << endl;

		vector<string> atts;
		catalog.GetAttributes(tName, atts);
		for (vector<string>::iterator it = atts.begin();
			 it != atts.end(); it++) {
			cout << *it << " ";
		}
		cout << endl;

		Schema schema;
		catalog.GetSchema(tName, schema);
		cout << schema << endl;

		////////////////////////////////
		for (int j = 0; j < 2; j++) {
			//int s = rand() % (r * aNo) + 1;
			//cout<<"CONTENTS OF S:				"<<s<<endl;
			char aN[20]; sprintf(aN, "A_%d_%d", i, j);
			string aName = aN;

			unsigned int distinct;
			catalog.GetNoDistinct(tName, aName, distinct);
			cout << tName << "." << aName << " distinct = " << distinct << endl;
		}
	}


	////////////////////////////////
	for (int i = 0; i < 5; i++) {
		cout<<"IN DROP LOOP"<<endl;
		char tN[20]; sprintf(tN, "T_%d", i);
		string tName = tN;

		bool ret = catalog.DropTable(tName);
		if (true == ret) {
			cerr << "DROP TABLE " << tName << " OK" << endl;
		}
		else {
			cerr << "DROP TABLE " << tName << " FAIL" << endl;
		}
	}

	return 0;

}

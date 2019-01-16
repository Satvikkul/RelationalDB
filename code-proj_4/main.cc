#include <iostream>
#include <string>
#include <stdio.h>

#include "Catalog.h"
extern "C"{
#include "QueryParser.h"
}
#include "QueryOptimizer.h"
#include "QueryCompiler.h"
#include "RelOp.h"

using namespace std;
//byson flex, tyson

// these data structures hold the result of the parsing
extern struct FuncOperator* finalFunction; // the aggregate function
extern struct TableList* tables; // the list of tables in the query
extern struct AndList* predicate; // the predicate in WHERE
extern struct NameList* groupingAtts; // grouping attributes
extern struct NameList* attsToSelect; // the attributes in SELECT
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query

extern "C" int yyparse();
extern "C" int yylex_destroy();


int main () {
	
	// this is the catalog
	string dbFile = "catalog.sqlite";
	Catalog catalog(dbFile);

	// this is the query optimizer
	// it is not invoked directly but rather passed to the query compiler
	QueryOptimizer optimizer(catalog);
	//QueryOptimizer Optimize();
	//TableList* _tables; 
	//_tables.tableName = "T_1";
	//AndList* _predicate;
	//OptimizationTree* _root;
	//optimizer.Optimize(tables predicate, _root);

	// this is the query compiler
	// it includes the catalog and the query optimizer
	QueryCompiler compiler(catalog, optimizer);

	// the query parser is accessed directly through yyparse
	// this populates the extern data structures
	int parse = -1;
	if (yyparse () == 0) {
		cout << "OK!" << endl;
		parse = 0;
	}
	else {
		cout << "Error: Query is not correct!" << endl;
		parse = -1;
	}


	yylex_destroy();


	if (parse != 0) return -1;
	// at this point we have the parse tree in the ParseTree data structures
	// we are ready to invoke the query compiler with the given query
	// the result is the execution tree built from the parse tree and optimized

	QueryExecutionTree queryTree;
	compiler.Compile(tables, attsToSelect, finalFunction, predicate,
		groupingAtts, distinctAtts, queryTree);
	cout << queryTree << endl;

	////////////////////////////////////////PART3///////////////////////////////////////////////////
	DBFile DB1;
	DBFile DB2;
	DBFile DB3;
	DBFile DB4;
	DBFile DB5;
	DBFile DB6;
	DBFile DB7;
	DBFile DB8;



	vector<string> tabs;
	vector<string> paths;
	catalog.GetTables(tabs);
	for(int i = 0; i < tabs.size(); i++)
	{
		paths.push_back("/home/satvikkul/Desktop/EECS277/Project-3/" + tabs[i] + ".heap");
	}
	
	for(int i = 0; i < paths.size(); i++)
	{
	
		catalog.SetDataFile(tabs[i], paths[i]);
	}

	
	const char* file1 = paths[0].c_str();
	const char* file2 = paths[1].c_str();
	const char* file3 = paths[2].c_str();
	const char* file4 = paths[3].c_str();
	const char* file5 = paths[4].c_str();
	const char* file6 = paths[5].c_str();
	const char* file7 = paths[6].c_str();
	const char* file8 = paths[7].c_str();

	// DB1.Create(file1, Heap);
	// DB2.Create(file2, Heap);
	// DB3.Create(file3, Heap);
	// DB4.Create(file4, Heap);
	// DB5.Create(file5, Heap);
	// DB6.Create(file6, Heap);
	// DB7.Create(file7, Heap);
	// DB8.Create(file8, Heap);

   
	Schema s1;
	Schema s2;
	Schema s3;
	Schema s4;
	Schema s5;
	Schema s6;
	Schema s7;
	Schema s8;

	DB1.Open(paths[0].c_str());
	DB2.Open(paths[1].c_str());
	DB3.Open(paths[2].c_str());
	DB4.Open(paths[3].c_str());
	DB5.Open(paths[4].c_str());
	DB6.Open(paths[5].c_str());
	DB7.Open(paths[6].c_str());
	DB8.Open(paths[7].c_str());


	// catalog.GetSchema(tabs[0], s1);
	// //cout<<"TABS[0]: "<<tabs[0]<<endl;
	// catalog.GetSchema(tabs[1], s2);
	// //cout<<"TABS[0]: "<<tabs[1]<<endl;
	// catalog.GetSchema(tabs[2], s3);
	// //cout<<"TABS[0]: "<<tabs[2]<<endl;
	// catalog.GetSchema(tabs[3], s4);
	// //cout<<"TABS[0]: "<<tabs[3]<<endl;
	// catalog.GetSchema(tabs[4], s5);
	// //cout<<"TABS[0]: "<<tabs[4]<<endl;
	// catalog.GetSchema(tabs[5], s6);
	// //cout<<"TABS[0]: "<<tabs[5]<<endl;
	// catalog.GetSchema(tabs[6], s7);
	// //cout<<"TABS[0]: "<<tabs[6]<<endl;
	// catalog.GetSchema(tabs[7], s8);
	// //cout<<"TABS[0]: "<<tabs[7]<<endl;


	// string f1 = "/home/satvikkul/Desktop/tpch_2_17_0/dbgen/customer.tbl";
	// DB1.Load(s1,f1.c_str());

	// string f2 = "/home/satvikkul/Desktop/tpch_2_17_0/dbgen/lineitem.tbl";
	// DB2.Load(s2,f2.c_str());

	// string f3 = "/home/satvikkul/Desktop/tpch_2_17_0/dbgen/nation.tbl";
	// DB3.Load(s3,f3.c_str());

	// string f4 = "/home/satvikkul/Desktop/tpch_2_17_0/dbgen/orders.tbl";
	// DB4.Load(s4,f4.c_str());

	// string f5 = "/home/satvikkul/Desktop/tpch_2_17_0/dbgen/part.tbl";
	// DB5.Load(s5,f5.c_str());

	// string f6 = "/home/satvikkul/Desktop/tpch_2_17_0/dbgen/partsupp.tbl";
	// DB6.Load(s6,f6.c_str());

	// string f7 = "/home/satvikkul/Desktop/tpch_2_17_0/dbgen/region.tbl";
	// DB7.Load(s7,f7.c_str());

	// string f8 = "/home/satvikkul/Desktop/tpch_2_17_0/dbgen/supplier.tbl";
	// DB8.Load(s8,f8.c_str());


	DB1.Close();
	DB2.Close();
	DB3.Close();
	DB4.Close();
	DB5.Close();
	DB6.Close();
	DB7.Close();
	DB8.Close();
	
	// DB3.testDBFile(s3,paths[2]);

	queryTree.ExecuteQuery();

	return 0;
}

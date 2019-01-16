#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"
#include "Catalog.h"

using namespace std;


QueryCompiler::QueryCompiler(Catalog& _catalog, QueryOptimizer& _optimizer) :
	catalog(&_catalog), optimizer(&_optimizer) {
}

QueryCompiler::~QueryCompiler() {
}


TableList* Copy(TableList* table) 
{
    if (table == NULL) 
    {
    	return NULL;
    }

    TableList* NewTable = new TableList;
    NewTable->tableName = table->tableName;
    NewTable->next = Copy(table->next);
    return NewTable;
}

NameList* Copy3(NameList* table) 
{
    if (table == NULL) 
    {
    	return NULL;
    }

    NameList* NewTable = new NameList;
    NewTable->name = table->name;
    NewTable->next = Copy3(table->next);
    return NewTable;
}

void QueryCompiler::Compile(TableList* _tables, NameList* _attsToSelect,
	FuncOperator* _finalFunction, AndList* _predicate,
	NameList* _groupingAtts, int& _distinctAtts,
	QueryExecutionTree& _queryTree) {


TableList * _tables2;
_tables2 = Copy(_tables);
	// create a SCAN operator for each table in the query

/*	vector<Scan*> Op;
	vector<string> Tabs;

	while(_tables != NULL)
	{
		cout<<"COMPILER1"<<endl;
		Schema SC;
		DBFile DB;
		Op.push_back(new Scan(SC, DB));
		Tabs.push_back(_tables->tableName);
cout<<"TABLES: "<<_tables->tableName<<endl;
		_tables = _tables->next;
	}

	// push-down selections: create a SELECT operator wherever necessary
	
	//cout<<"TABLES: "<<_tables->tableName<<endl;
	vector<Select*> Op2;
	for(int i = 0; i < Tabs.size(); i++)
	{
		cout<<"OUTER LOOP"<<endl;
		do
		{
			cout<<"INNER LOOP"<<endl;
			Schema schema; 
			CNF predicate;
			Record constants;
			RelationalOp* producer;
			cout<<"INNER LOOP2"<<endl;
			if((_predicate->left->left->code = NAME && _predicate->left->right->code != NAME) || (_predicate->left->right->code == NAME && _predicate->left->left->code != NAME))
			{
				cout<<"INNER LOOP3"<<endl;
				if((strcmp(_tables->tableName,_predicate->left->left->value) == 0 || (strcmp(_tables->tableName,_predicate->left->right->value) == 0)))
				{
					cout<<"INNER LOOP4"<<endl;
					Op2.push_back(new Select(schema, predicate, constants, producer));
				}
			}
			cout<<"INNER LOOP5"<<endl;
		}while(_predicate = _predicate->rightAnd);
	}
*/



		// create a SCAN operator for each table in the query
	map<string, Scan*> Op;
	vector<string> Tabs;
	map<string, Select*> Op2;


	while(_tables != NULL)
	{
		//cout<<"HERE"<<endl;
		Schema SC;
		DBFile DB;
		string p;

		string TName = _tables->tableName;
		catalog->GetDataFile(TName, p);
		cout<<p<<endl;
		catalog->GetSchema(TName, SC);

		DB.Open(p.c_str());
		DB.MoveFirst();

		Op[TName] = new Scan(SC, DB);
		Tabs.push_back(_tables->tableName);

		CNF c;
		Record r;
		RelationalOp* producer;

		

		c.ExtractCNF(*_predicate, SC, r);

		

		if(c.numAnds !=0)
		{
			Op2[TName] = new Select(SC, c, r, Op[TName]);	
		}

		//_tables2 = _tables;
		_tables = _tables->next;

	}



	cout<<"OP SIZE: 		"<<Op.size()<<endl;
	cout<<"OP2 SIZE: 		"<<Op2.size()<<endl;

	if(_tables2 == NULL)
	{
		cout<<"TABLES2 IS NULL"<<endl;
	}




	// call the optimizer to compute the join order
	//cout<<"HERE"<<endl;
	OptimizationTree* root;
	optimizer->Optimize(_tables2, _predicate, root);
	

	 vector<string> FM;
	 cout<<"HERE"<<endl;
	 FM = optimizer->GetMap();
	 cout<<"					SIZE: 		"<<FM.size()<<endl;

	// /*for(int i = 0; i < FM.size(); i++)
	// {
	// 	cout<<"IN COMPILER TABLES ORDERD LIST: "<<FM[i]<<endl;
	// }*/
	 
	// // create join operators based on the optimal order computed by the optimizer


	RelationalOp* now = NULL;
	RelationalOp* left = NULL;
	RelationalOp* right = NULL;
	string na;
 	na = FM[0];

 	Schema lSchema;
 	//Schema rSchema;
 	Schema oSchema;

 	CNF cnf;

 	catalog->GetSchema(na, lSchema);

 	oSchema.Append(lSchema);

cout<<"oSChema: "<<oSchema<<endl;
bool check = false;

 	//if(FM.size() > 1)
 	//{
 		if(Op.find(FM[0]) == Op.end() && Op2.find(FM[0]) == Op2.end()){
			cerr << "This is really bad" << endl;
			while(1);
		}
		if(Op2.find(na) == Op2.end())
		{
			now = Op[na];	
			cout<<"RIGHT HERE 11111111111111"<<endl;
		}
		else
		{
			cout<<"RIGHT HERE asdfasdfasdf"<<endl;
			now = Op2[na];
		}

		for(int i = 1; i < FM.size(); i++)
		{
			Schema rSchema, dummySchema;
			cout<<"HERE2"<<endl;
			if(Op.find(FM[i]) == Op.end() && Op2.find(FM[i]) == Op2.end()){
				cerr << "This is really bad" << endl;
				while(1);
			}
			if(Op2.find(FM[i]) == Op2.end())
			{
				cout<<"HERE3"<<endl;
				right = Op[FM[i]];
			}
			else
			{
				cout<<"HERE4"<<endl;
				right = Op2[FM[i]];
			}
			//cnf.ExtractCNF(*_predicate, lSchema, rSchema);

			catalog->GetSchema(FM[i], rSchema);
			oSchema.Append(rSchema);
			cnf.ExtractCNF(*_predicate, lSchema, rSchema);
			now = new Join(lSchema, rSchema, oSchema, cnf, now, right);
			lSchema.Swap(dummySchema);
			lSchema =oSchema;

			cout<<"HERE5"<<endl;
		}
	//}

			cout<<"RIGHT HERE"<<endl;

			//cout<<"NOW: "<<*now<<endl;

	int numAttsInput2;
	int numAttsOutput2;
	//int* keepMe2;
	Schema out;

cout<<"DISTINCT ATTS: "<<_distinctAtts<<endl;

	if(_distinctAtts == 1)
	{
		cout<<"IN DISTINCT IF: "<<endl;
		Schema dupSch;	
		now->GiveSchema(dupSch);
		DuplicateRemoval* dupRemoval = new DuplicateRemoval(dupSch, now);
		now = (RelationalOp*) dupRemoval;
		check = true;
		string outFile = "output.txt";
		cout<<"oschema: "<<oSchema<<endl;
		WriteOut* writeOut = new WriteOut(dupSch, outFile, now);
		now = (RelationalOp*) writeOut;
			//now = (RelationalOp*) dis;
	}

cout<<"FINALFUNCTION: "<<_finalFunction<<endl;

if(_finalFunction != NULL)
{
	Function Func;
	FuncOperator* finalFunc = _finalFunction;

	Func.GrowFromParseTree(finalFunc, oSchema);

	vector<string> atts;
	vector<string> attsType;
	vector<unsigned int> dists;

	atts.push_back("Sum");
	attsType.push_back("FLOAT");
	dists.push_back(1);

	Schema SumSchema(atts, attsType, dists);

	cout<<"SUM SCHEMA!!!!!!!!!!!!!!!!!!!!!!!"<<endl;
	cout<<SumSchema<<endl;


	Sum* s = new Sum(oSchema, SumSchema, Func, now);
	now = (RelationalOp*) s;
	check = true;

	string outFile = "output.txt";
	cout<<"oschema: "<<oSchema<<endl;
	WriteOut* writeOut = new WriteOut(SumSchema, outFile, now);
	now = (RelationalOp*) writeOut;


				cout<<"RIGHT HERE2"<<endl;
}
	// create the remaining operators based on the query


	int counter = 0;
		int counter2 = 0;
	cout<<"COUNTER2: "<<counter2<<endl;

	NameList * _names2;
	_names2 = Copy3(_groupingAtts);
	NameList * _names3;
	_names3 = Copy3(_groupingAtts);

	vector<string> att2;

	while(_names3 != NULL)
	{
		att2.push_back(_names3->name);
		_names3 = _names3->next;
	}

	cout<<"COUNTER2: "<<counter2<<endl;
	while(_names2 != NULL)
	{
		counter2++;
		_names2 = _names2->next;
	}

	cout<<"COUNTER2: "<<counter2<<endl;
	//int i2 = 0; 
	int j2 = 0;
	int Sindex[counter2];

	for(int i = 0; i < att2.size(); i++)
	{

		if(oSchema.Index(att2[i]) != -1)	
		{
			Sindex[j2] = i;
			j2++;
		}
		//i2++;
	}
	OrderMaker ord(oSchema, Sindex, j2);


Schema out3;
	vector<string> Gatts;
	vector<unsigned int> distincts;
	vector<string> types;

	 while(_groupingAtts != NULL)
	 {
	 	counter++;
	 	//Gatts.push_back(_groupingAtts->name);
	 	string attName = string(_groupingAtts->name);
	 	Gatts.push_back(attName);
	 	int dummy;
	 	dummy = oSchema.GetDistincts(attName);
	 	distincts.push_back(dummy);
	 	string dummy2;
	 	dummy2 = oSchema.FindType(attName);
	 	//cout<<"dummy2:		"<<dummy2.GetData()<<endl;
	 	types.push_back(dummy2);

	 	_groupingAtts = _groupingAtts->next;
	 }
	cout<<"COUNTER: "<<counter<<endl;
	//counter = 0;
	 if(counter != 0)
	 {
	 	Function Func;
	 	Schema schemaOut(Gatts, types, distincts);
	 	//string attrName = string(_groupingAtts->name);
	 	cout<<"GROUPBY ATTRIBUTE: "<<Gatts[0]<<endl;
	 	cout<<"SCHEMA OUT: "<<schemaOut<<endl;
		Func.GrowFromParseTree(_finalFunction, oSchema);

	 	GroupBy* GB = new GroupBy(oSchema, schemaOut, ord, Func, now);
	 	now = (RelationalOp*) GB;
	 	check = true;
	 }
	 vector<int> keepMe2;

	 numAttsInput2 = oSchema.GetNumAtts();
	 numAttsOutput2 = 0;
	 out = oSchema;

	 while(_attsToSelect !=NULL)
	 {
	 	string st(_attsToSelect->name);
	 	keepMe2.push_back(oSchema.Index(st));
	 	_attsToSelect = _attsToSelect->next;
	 	numAttsOutput2++;
	 }



	 out.Project(keepMe2);

	 cout<<"out: "<<out<<endl;

	 for(int i = 0; i < keepMe2.size(); i++)
	 {
	 	cout<<"KEEEPME2: "<<keepMe2[i]<<endl;
	 }

	 int* keepMe = new int[keepMe2.size()];
	 copy(keepMe2.begin(), keepMe2.end(), keepMe);

if(check == false)
{
cout<<"RIGHT HERE2"<<endl;

	Project* proj = new Project(oSchema, out, numAttsInput2, numAttsOutput2, keepMe, now);
	now = (RelationalOp*) proj;
cout<<"RIGHT HERE3"<<endl;

	string outFile = "output.txt";
	cout<<"oschema: "<<oSchema<<endl;
	WriteOut* writeOut = new WriteOut(out, outFile, now);
	now = (RelationalOp*) writeOut;
}



		// Connecting everything in the query execution tree

		_queryTree.SetRoot(*now);	



///////////////////////////////////////////////*************/////////////////////////////////////////////////////////////////////

	 // Creating join operators based on the optimal order computed by the optimizer

		

////////////////////////////////////////////////****************////////////////////////////////////////////////////////////////////	 

	// connect everything in the query execution tree and return

	// free the memory occupied by the parse tree since it is not necessary anymore
}

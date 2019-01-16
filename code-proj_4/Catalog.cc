#include <iostream>
#include "sqlite3.h"

#include "Schema.h"
#include "Catalog.h"

#include <vector> 
#include "Swapify.h"
#include "Swapify.cc"
#include <stdio.h>

using namespace std;

/////NAMES
static int callback(void *data, int argc, char** argv, char **azColName)
{
	Catalog* inst = (Catalog*) data;
	Swapify<int> y(inst->count);
	Swapify<string> x(argv[0]);
	inst->tabNames.Insert(y, x);
	//cout << "Inserted " <<(*((int*)data))  << " " << argv[0] << endl;

	//count++;
	//inst->count++;
	inst->count++;
	return 0;
}



/////Number of Tuples
static int callback1(void *data, int argc, char** argv, char **azColName)
{
	vector<string> name;
	((const char*)data);

	//Swapify<int> y(*((int*)data));
	Catalog* inst = (Catalog*) data;
	Swapify<int> y(inst -> count1);
	Swapify<int> x(atoi(argv[0]));
	inst->tabNumTups.Insert(y, x);
	//cout << "Inserted " << count1 << " " << atoi(argv[0]) << endl;

	inst->count1++;
	//(*((int*)data))++;
	return 0;
}


/////PATH
static int callback2(void *data, int argc, char** argv, char **azColName)
{
	vector<string> name;
	((const char*)data);
	Catalog* inst = (Catalog*) data;
	//Swapify<int> y(*((int*)data));
	Swapify<int> y(inst->count2);
	Swapify<string> x(argv[0]);
	inst->tabPath.Insert(y, x);
	//cout << "Inserted " << count2 << " " << argv[0] << endl;

	inst->count2++;
	//(*((int*)data))++;
	return 0;
}



/////Table ID
static int callback3(void *data, int argc, char** argv, char **azColName)
{
	vector<string> name;
	((const char*)data);
	Catalog* inst = (Catalog*) data;
	//Swapify<int> y(*((int*)data));
	Swapify<int> y(inst->count3);
	Swapify<int> x(atoi(argv[0]));
	inst->tabTID.Insert(y, x);
	//cout << "Inserted " << count3 << " " << atoi(argv[0]) << endl;

	inst->count3++;
	//(*((int*)data))++;
	return 0;
}




////////////////////////ATTRIBUTES///////////////////////////////////////



/////Attribute Names
static int Acallback(void *data, int argc, char** argv, char **azColName)
{
	vector<string> name;
	((const char*)data);
	Catalog* inst = (Catalog*) data;
	//Swapify<int> y(*((int*)data));
	Swapify<int> y(inst->acount);
	Swapify<string> x(argv[0]);
	inst->attNames.Insert(y, x);
	//cout << "Inserted " << acount << " " << argv[0] << endl;

	inst->acount++;
	//(*((int*)data))++;
	return 0;
}



/////Attribute Table ID
static int Acallback1(void *data, int argc, char** argv, char **azColName)
{
	vector<string> name;
	((const char*)data);
	Catalog* inst = (Catalog*) data;
	//Swapify<int> y(*((int*)data));
	Swapify<int> y(inst->acount1);
	Swapify<int> x(atoi(argv[0]));
	inst->attTID.Insert(y, x);
	//cout << "Inserted " << acount1 << " " << atoi(argv[0]) << endl;

	inst->acount1++;
	//(*((int*)data))++;
	return 0;
}



/////Number of Distinct Attributes
static int Acallback2(void *data, int argc, char** argv, char **azColName)
{
	vector<string> name;
	((const char*)data);
	Catalog* inst = (Catalog*) data;
	//Swapify<int> y(*((int*)data));
	Swapify<int> y(inst->acount2);
	Swapify<int> x(atoi(argv[0]));
	inst->attNumDis.Insert(y, x);
	//cout << "Inserted " << acount2 << " " << atoi(argv[0]) << endl;

	inst->acount2++;
	//(*((int*)data))++;
	return 0;
}


/////Attribute Type
static int Acallback3(void *data, int argc, char** argv, char **azColName)
{
	vector<string> name;
	((const char*)data);
	Catalog* inst = (Catalog*) data;
	//Swapify<int> y(*((int*)data));
	Swapify<int> y(inst->acount3);
	Swapify<string> x(argv[0]);
	inst->attType.Insert(y, x);
	//cout << "Inserted " << acount3 << " " << argv[0] << endl;

	inst->acount3++;
	//(*((int*)data))++;
	return 0;
}




/////Attribute ID
static int Acallback4(void *data, int argc, char** argv, char **azColName)
{
	vector<string> name;
	((const char*)data);
	Catalog* inst = (Catalog*) data;
	//Swapify<int> y(*((int*)data));
	Swapify<int> y(inst->acount4);
	Swapify<int> x(atoi(argv[0]));
	inst->attID.Insert(y, x);
	//cout << "Inserted " << acount4 << " " << atoi(argv[0]) << endl;

	inst->acount4++;
	//(*((int*)data))++;
	return 0;
}





/////Delete ALL
static int Dcallback(void *data, int argc, char** argv, char **azColName)
{
	return 0;
}


/////Set Number of Tuples
static int Tcallback(void *data, int argc, char** argv, char **azColName)
{

	return 0;
}

static int Icallback(void *NotUsed, int argc, char **argv, char **azColName){

   return 0;
}
static int Icallback2(void *NotUsed, int argc, char **argv, char **azColName){

   return 0;
}


static int DropCallback(void *data, int argc, char **argv, char **azColName){
return 0;
}


Catalog::Catalog(string& _fileName) {

	//cerr<<"INSIDE THE CONSTRUCTOR"<<endl;


	///////Connecting to the Database
   	sqlite3 *db;
   	char *zErrMsg = 0;
   	int rc;

   	rc = sqlite3_open(_fileName.c_str(), &db);

   	if( rc ){
      		//cerr<<"Can't open database: "<< sqlite3_errmsg(db)<<endl;
      		//return 0;
  	 }else{
      		//cerr<<"Opened database successfully"<<endl;
   	}

	///////

	int rc1;
	int rc2;
	int rc3;
	int rc4;

	char* sql;
	char* sql1;
	char* sql2;
	char* sql3;

	const char* data = "Callback function called";

	sql = "SELECT name FROM Tables";
	sql1 = "SELECT noTuples FROM Tables";
	sql2 = "SELECT Path FROM Tables";
	sql3 = "SELECT tID FROM Tables";

	/////////////////NAMES
	rc1 = sqlite3_exec(db, sql, callback, this, &zErrMsg);
	//cerr<<"AFTER FIRST QUERY"<<endl;
	//cerr<<"FIRST QUERY: "<<sql<<endl;
	
	if( rc1 != SQLITE_OK )
	{
      		//cerr<<"SQL error: "<<zErrMsg<<endl;
	}
   	else
	{
      		//cerr<<"Operation done successfully"<<endl;
   	}

	/*for(int i = 0; i < count; i++)
	{
		Swapify<int> y(i);
		cout << tabNames.Find(y).GetData() << endl;
	}*/


	//////////////////Number of Tuples
	rc2 = sqlite3_exec(db, sql1, callback1, this, &zErrMsg);

	if (rc2 != SQLITE_OK)
	{
		//cout << "SQL error: " << zErrMsg << endl;
	}
	else
	{
		//cout << "Operation done successfully" << endl;
	}


	/*for (int i = 0; i < count1; i++)
	{
		Swapify<int> y(i);
	}*/

	/////////////////PATH
	rc3 = sqlite3_exec(db, sql2, callback2, this, &zErrMsg);

	if (rc3 != SQLITE_OK)
	{
		//cout << "SQL error: " << zErrMsg << endl;
	}
	else
	{
		//cout << "Operation done successfully" << endl;
	}


	/*for (int i = 0; i < count2; i++)
	{
		Swapify<int> y(i);
	}*/


	///////////////////Table ID
	rc4 = sqlite3_exec(db, sql3, callback3, this, &zErrMsg);

	if (rc4 != SQLITE_OK)
	{
		//cout << "SQL error: " << zErrMsg << endl;
	}
	else
	{
		//cout << "Operation done successfully" << endl;
	}


	/*for (int i = 0; i < count3; i++)
	{
		Swapify<int> y(i);
		//cout << tabTID.Find(y).GetData() << endl;
	}*/


	////////////////////////////////FOR ATTRIBUTES TABLE//////////////////////////////////////

	int arc1;
	int arc2;
	int arc3;
	int arc4;
	int arc5;

	char* asql;
	char* asql1;
	char* asql2;
	char* asql3;
	char* asql4;

	asql = "SELECT name FROM Attributes";
	asql1 = "SELECT tID FROM Attributes";
	asql2 = "SELECT noDistinct FROM Attributes";
	asql3 = "SELECT type FROM Attributes";
	asql4 = "SELECT attID FROM Attributes";


	/////////////////Attribute NAMES

	arc1 = sqlite3_exec(db, asql, Acallback, this, &zErrMsg);

	if (arc1 != SQLITE_OK)
	{
		//cout << "SQL error: " << zErrMsg << endl;
	}
	else
	{
		//cout << "Operation done successfully" << endl;
	}


	/*for (int i = 0; i < acount; i++)
	{
		Swapify<int> y(i);
	}*/

	///////////////////Table ID for Attribuite Table to join with Tables table
	arc2 = sqlite3_exec(db, asql1, Acallback1, this, &zErrMsg);

	if (arc2 != SQLITE_OK)
	{
		//cout << "SQL error: " << zErrMsg << endl;
	}
	else
	{
		//cout << "Operation done successfully" << endl;
	}

	//cout<<"ATTRIBUTE TIDs: "<<endl;

	/*for (int i = 0; i < acount1; i++)
	{
		Swapify<int> y(i);
	}*/


	//////////////////Attribute Number of Distinct Elements
	arc3 = sqlite3_exec(db, asql2, Acallback2, this, &zErrMsg);

	if (arc3 != SQLITE_OK)
	{
		//cout << "SQL error: " << zErrMsg << endl;
	}
	else
	{
		//cout << "Operation done successfully" << endl;
	}


	/*for (int i = 0; i < acount2; i++)
	{
		Swapify<int> y(i);
	}*/

	/////////////////Attribute Type
	arc4 = sqlite3_exec(db, asql3, Acallback3, this, &zErrMsg);

	if (arc4 != SQLITE_OK)
	{
		//cout << "SQL error: " << zErrMsg << endl;
	}
	else
	{
		//cout << "Operation done successfully" << endl;
	}


	/*for (int i = 0; i < acount3; i++)
	{
		Swapify<int> y(i);
	}*/



	///////////////////Attribute ID
	arc5 = sqlite3_exec(db, asql4, Acallback4, this, &zErrMsg);

	if (arc5 != SQLITE_OK)
	{
		//cout << "SQL error: " << zErrMsg << endl;
	}
	else
	{
		//cout << "Operation done successfully" << endl;
	}


	/*for (int i = 0; i < acount4; i++)
	{
		Swapify<int> y(i);
	}*/

	//sqlite3_close(db)

}

Catalog::~Catalog() {
	TableLength2 = tabNames.Length();
	//cerr<<"LENGTH				"<<TableLength2<<endl;
Save();
	

}

bool Catalog::Save() {

/*tabNames.MoveToStart();
int cnt = 0;
while(tabNames.AtEnd() == false)
{
	cout<<"TABLES IN MAP: "<<tabNames.CurrentData()<<endl;
	tabNames.Advance();
	cnt++;
}*/
	///////Connecting to the Database
   	sqlite3 *db;
   	char *zErrMsg = 0;
   	int rc;

   	rc = sqlite3_open("catalog.sqlite", &db);

   	if( rc ){
      	//cout<<"Can't open database: "<< sqlite3_errmsg(db)<<endl;
      	//return 0;
  	 }else{
      	//cout<<"Opened database successfully"<<endl;
   	}

	int rc1;
	int rc2;
	int rc3;
	int rc4;

	const char* data = "Callback function called";

	char* sql1;
	sql1 = 	"DELETE from Tables;";
	char* sql3;
	sql3 = "DELETE from Attributes;";

	rc1 = sqlite3_exec(db, sql1, Dcallback, (void*)data, &zErrMsg);

	if (rc1 != SQLITE_OK)
	{
		//cout << "SQL error: " << zErrMsg << endl;
	}
	else
	{
		//cout << "Deleted successfully" << endl;
	}

	rc4 = sqlite3_exec(db, sql3, Dcallback, (void*)data, &zErrMsg);

	if (rc4 != SQLITE_OK)
	{
		//cout << "SQL error: " << zErrMsg << endl;
	}
	else
	{
		//cout << "Deleted successfully" << endl;
	}
	//cerr<<"TABLES: "<<endl;

	/*for(int r = 0; r < tabNames.Length();r++)
	{
		Swapify<int> e(r);
		cerr<<tabNames.Find(e).GetData()<<endl;
		cerr<<tabNumTups.Find(e).GetData()<<endl;
		cerr<<tabPath.Find(e).GetData()<<endl;
		cerr<<tabTID.Find(e).GetData()<<endl;	
		
	}*/
	//cerr<<"tabNames.Length(): "<<tabNames.Length()<<endl;



	//cerr<<"NEW LENGTH OF TABLES: "<<tabNames.Length()<<endl;

/*{
	Swapify<int> u(5);

	cerr<<"NAMES: "<<tabNames.Find(u).GetData()<<endl;
}*/
	for(int i = 0; i < tabNames.Length(); i++)
	{
		char sql[1000];	

		Swapify<int> y(i);
		//cout<<"IN LOOP AT ITERATION: "<<i<<endl;
		sprintf(sql,"INSERT INTO Tables (name, noTuples, Path, tID) VALUES ('%s',%d,'%s',%d);", tabNames.Find(y).GetData().c_str(), tabNumTups.Find(y).GetData(), tabPath.Find(y).GetData().c_str(), tabTID.Find(y).GetData());

		rc2 = sqlite3_exec(db, sql, Dcallback, (void*)data, &zErrMsg);

		if(rc2 != SQLITE_OK)
		{
			//cout<<"SQL ERROR: "<<zErrMsg<<endl;
			sqlite3_free(zErrMsg);
		}
		else
		{
			//cout<<"Inserted Correctly"<<endl;
		}
	}
	/*tabNames.MoveToStart();
	tabNumTups.MoveToStart();
	tabPath.MoveToStart();
	tabTID.MoveToStart();
	//tabNames.MoveToStart;
	for(int i = 0; i < cnt; i++)
	{
		char sql[1000];	

		Swapify<int> y(i);
		cout<<"IN LOOP AT ITERATION: "<<i<<endl;
		sprintf(sql,"INSERT INTO Tables (name, noTuples, Path, tID) VALUES ('%s',%d,'%s',%d);", tabNames.CurrentData(), tabNumTups.CurrentData(), tabPath.CurrentData(), tabTID.CurrentData());

		rc2 = sqlite3_exec(db, sql, Dcallback, (void*)data, &zErrMsg);

		if(rc2 != SQLITE_OK)
		{
			//cout<<"SQL ERROR: "<<zErrMsg<<endl;
			sqlite3_free(zErrMsg);
		}
		else
		{
			//cout<<"Inserted Correctly"<<endl;
		}
		tabNames.Advance();
		tabNumTups.Advance();
		tabPath.Advance();
		tabTID.Advance();
	}*/


	for(int r = 0; r < attNames.Length();r++)
	{
		Swapify<int> e(r);
		//cerr<<attNames.Find(e).GetData()<<endl;
		//cerr<<attTID.Find(e).GetData()<<endl;
		//cerr<<attNumDis.Find(e).GetData()<<endl;
		//cerr<<attType.Find(e).GetData()<<endl;	
		//cerr<<attID.Find(e).GetData()<<endl;	
	}


	//cerr<<"Attributes Length: "<<attNames.Length()<<endl;
	for(int j = 0; j < attNames.Length(); j++)
	{
		Swapify<int> x(j);
		char sql2[1000];

sprintf(sql2, "INSERT INTO Attributes (name, tID, noDistinct, Type, attID) VALUES ('%s', %d, %d, '%s', %d);", attNames.Find(x).GetData().c_str(), attTID.Find(x).GetData(), attNumDis.Find(x).GetData(), attType.Find(x).GetData().c_str(), attID.Find(x).GetData());

		rc3 = sqlite3_exec(db, sql2, Dcallback, (void*)data, &zErrMsg);

		if(rc3 != SQLITE_OK)
		{
			//cout<<"SQL ERROR: "<<zErrMsg<<endl;
			sqlite3_free(zErrMsg);
		}
		else
		{
			//cout<<"Inserted Correctly"<<endl;
		}
	}
sqlite3_close(db);


}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {

	//cout<<"COUNT: "<<count<<endl;
	for(int i = 0; i < tabNames.Length(); i++)
	{
		//cout<<"HERE0"<<endl;
		Swapify<int> y(i);
		if(tabNames.Find(y).GetData() == _table)
		{
			//cout<<"HERE"<<endl;
			_noTuples = tabNumTups.Find(y).GetData();
			return true;
		}
	}

	return false;
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {

	for(int i = count; i < tabNames.Length(); i++)
	{
		Swapify<int> y(i);
		if(tabNames.Find(y).GetData() == _table)
		{
				Swapify<int> y(i);
				Swapify<int> x(_noTuples);
				tabNumTups.Insert(y, x);
				inTabCount2++;
		}
	}

}

bool Catalog::GetDataFile(string& _table, string& _path) {

	for(int i = 0; i < tabNames.Length(); i++)
	{
		Swapify<int> y(i);
		if(tabNames.Find(y).GetData() == _table)
		{
			_path = tabPath.Find(y).GetData();
			return true;
		}
	}

	return false;
}

void Catalog::SetDataFile(string& _table, string& _path) {

	//cout<<"tabNames.Length(): "<<_table<<endl;
	for(int i = 0; i < tabNames.Length(); i++)
	{
		Swapify<int> y(i);
		
		if(tabNames.Find(y).GetData() == _table)
		{
			//cout<<"NAME: "<<tabNames.Find(y).GetData()<<"AT I: "<<i<<endl;
			//cout<<"PATH: "<<_path.c_str()<<endl;
			Swapify<int> g(i);
			//cout<<"G: "<<g<<endl;
			Swapify<string> x(_path.c_str());
			//cout<<"X: "<<x<<endl;
			tabPath.Find(g) = x;
			//tabPath.Insert(g, x);
			Swapify<int> f(i);
			//cout<<"PATHS: "<<tabPath.Find(f).GetData()<<endl;
			inTabCount3++;	
		}
	}

}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {

//cout<<"TABLE: "<<_table<<endl;
//cout<<"ATTRIBUTE: "<<_attribute<<endl;


	int TID = 0;

	for(int i = 0; i < tabNames.Length(); i++)
	{
		Swapify<int> y(i);
		if(tabNames.Find(y).GetData() == _table)
		{
			TID = tabTID.Find(y).GetData();
			for(int j = 0; j < attNames.Length(); j++)
			{
				Swapify<int> x(j);
				if(TID == attTID.Find(x).GetData())///////////////ask about garbage value
				{
					if(_attribute == attNames.Find(x).GetData())
					{
						_noDistinct = attNumDis.Find(x).GetData();
					}

					else
					{
						//cout<<"No attriutes Found"<<endl;
					}
				}
			}
			return true;
		}
	}
	return false;
}
void Catalog::SetNoDistinct(string& _table, string& _attribute, unsigned int& _noDistinct) 
{
	int TID = 0;

	for(int i = count; i < tabNames.Length(); i++)
	{
		Swapify<int> y(i);
		//cerr<<"ITERATION: "<<i<<endl;
		if(tabNames.Find(y).GetData() == _table)
		{
			//cerr<<"Attributes Distinct first if: "<<endl;
			//cerr<<"Table: 		"<<tabNames.Find(y).GetData()<<endl;
			TID = tabTID.Find(y).GetData();
			//cerr<<"TID:		"<<TID<<endl;
							//cerr<<"						"<<endl;
				//cerr<<"						"<<endl;
			for(int j = acount; j < attNames.Length(); j++)
			{
				Swapify<int> k(j);
				//cerr<<"Attribute: "<<_attribute<<endl;
				//cerr<<"MAP ATTRIBUTE: "<<attNames.Find(k).GetData()<<endl;
				//cerr<<"ATTRIBUTE TID: "<<attTID.Find(k).GetData()<<endl;
				//cerr<<"TABLE TID: "<<TID<<endl;
				Swapify<int> x(j);
				if(TID == attTID.Find(x).GetData())
				{
					//cerr<<"IN SECOND LOOPS FIRST IF"<<endl;
					if(attNames.Find(x).GetData() == _attribute)
					{
						//cerr<<"IN SECOND LOOPS SECOND IF"<<endl;
						Swapify<int> g(j);
						Swapify<int> h(_noDistinct);
						attNumDis.Insert(g, h);
						//cerr << "Inserted: " << h << " at Position: " << g << endl;

						inAttCount2++;
					}
				}
			}
		}
	}
}


void Catalog::GetTables(vector<string>& _tables) {

	for(int i = 0; i < tabNames.Length(); i++)
	{
		Swapify<int> y(i);
		_tables.push_back(tabNames.Find(y).GetData());
	}

}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {

	int TID = 0;

	for(int i = 0; i < tabNames.Length(); i++)
	{
		Swapify<int> y(i);
		if(tabNames.Find(y).GetData() == _table)
		{
			TID = tabTID.Find(y).GetData();
			for(int j = 0; j < attNames.Length(); j++)
			{
				Swapify<int> x(j);
				if(TID == attTID.Find(x).GetData())
				{
					_attributes.push_back(attNames.Find(x).GetData());
					//cout<<"ATTRIBUTES BEING INSERTED: "<<attNames.Find(x).GetData()<<endl;
				}
			}
			return true;
		}
	}
	return false;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {

	int TID = 0;
	vector<string> atts, types;
	vector<unsigned int> distincts;


	for(int i = 0; i < tabNames.Length(); i++)
	{
		Swapify<int> y(i);
		if(tabNames.Find(y).GetData() == _table)
		{
			TID = tabTID.Find(y).GetData();
			for(int j = 0; j < attNames.Length(); j++)
			{
				Swapify<int> x(j);
				if(TID == attTID.Find(x).GetData())
				{
					atts.push_back(attNames.Find(x).GetData().c_str());
					types.push_back(attType.Find(x).GetData().c_str());
					distincts.push_back(attNumDis.Find(x).GetData());
				}
				
			}

			/*for(int k = 0; k < types.size(); k++)
			{
				cout<<"TYPES VECTOR: "<<types[k]<<endl;
			}*/

			Schema schema(atts, types, distincts);
			_schema = schema;
			return true;
		}
	}



	//cout<<"FALSE"<<endl;
	return false;
}

bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {

	int TID = 0;

	for(int i = 0; i < tabNames.Length(); i++)
	{ 
		Swapify<int> y(i);
		if(tabNames.Find(y).GetData() == _table)
		{
			cerr<<"Table Already Exists: "<<tabNames.Find(y).GetData()<<endl;
			dummy2++;
			cerr<<"DUMMY2:  "<<dummy2<<endl;
			return false;
		}
	}

	Swapify<int> t(5);
	
	Swapify<int> a(tabNames.Length());
	Swapify<int> f(tabNames.Length());
	Swapify<int> l(tabNames.Length());

	Swapify<string> b(_table.c_str());
 
	tabNames.Insert(a, b);
	tabTID.Insert(f, l);	

	int dum;
	dum = attNames.Length();

	for(int j = 0; j < _attributes.size(); j++)
	{
		Swapify<int> s(dum);
		Swapify<string> t(_attributes[j].c_str());
		attNames.Insert(s, t);
		Swapify<int> p(dum);
		Swapify<int> u(tabNames.Length() - 1);
		attTID.Insert(p, u);
		Swapify<int> q(dum);	
		Swapify<string> v(_attributeTypes[j].c_str());
		attType.Insert(q, v);
		Swapify<int> e(dum);
		Swapify<int> w(dum);
		attID.Insert(e,w);
		attcount++;	
		inAttCount++;
		dum++;
	}
	CountTID++;
	incount++;
	inTabCount++;
	TableLength = tabNames.Length();
	return true;
}











bool Catalog::DropTable(string& _table) {

	//cerr<<"IN DROP TABLE"<<endl;
	//int TID = 0;
	//int i;
		
	//cerr<<"			"<<tabNames.Length()<<endl;



//cerr<<"I Previous: "<<i<<endl;
//cerr<<"inTabCount: "<<inTabCount<<endl;
		//for(int k = 0; k < tabNames.Length(); k++)
		//{
		//	Swapify<int> h(k);
			//cerr<<"IN LOOP"<<endl;
		//	cerr<<"TABLE NAMES: 				"<<tabNames.Find(h).GetData()<<endl;
	//	}

		//tabNames.MoveToStart();
	//	cerr<<"					"<<newCount<<endl;
		//cerr<<"TABLE LEN: "<<tabNames.Length()<<endl;

	/*for(int k = 0; k < tabNames.Length(); k++)
	{
		Swapify<int> dumm(k);
		cerr<<"TABLES REMAINING: "<<tabNames.Find(dumm).GetData()<<endl;
	}*/

	/*cerr<<"NEW LENGTHS:		"<<TableLength<<endl;
	for(int i = dummy5; i < TableLength; i++)
	{

		cerr<<"DUMMY5:		"<<dummy5<<endl;
		Swapify<int> f(i);
		if(tabNames.Find(f).GetData() == _table)
		{

			TID = tabTID.Find(f).GetData();
			cerr<<"TID: "<<TID<<endl;
			cout<<"HERE0"<<endl;
			Swapify<int> q(i);
			Swapify<int> removed_key;
			Swapify<string> removed_string;
			Swapify<int> q2(i);
			Swapify<int> removed_key2;
			Swapify<string> removed_string2;
			Swapify<int> q3(i);
			Swapify<int> removed_key3;
			Swapify<int> removed_int;
			Swapify<int> q4(i);
			Swapify<int> removed_key4;
			Swapify<int> removed_int2;
			tabNames.Remove(q,removed_key,removed_string);
			tabNumTups.Remove(q3, removed_key3, removed_int);
			tabPath.Remove(q2, removed_key2, removed_string2);
			tabTID.Remove(q4, removed_key4, removed_int2);



			cout<<"HERE"<<endl;

			Swapify<int> o(i);
			dummy5++;
			//tabNames.Advance();
			return true;
		}
	}	

/*
	for(int k = dummy5; k < TableLength; k++)
	{	
		Swapify<int> m(dummy6);
		Swapify<int> h(k);
		Swapify<string> n(tabNames.Find(h).GetData());
		tabNames2.Insert(m, n);
	}
*/
	/*for(int i = 0; i < tabNames2.Length(); i++)
	{
		Swapify<int> g(i);
		cerr<<"NEW DUMMY TABLE:			"<<tabNames2.Find(g).GetData()<<endl;
	}*/
/*
			for(int j = 0; j < tabTID.Find(o).GetData(); j++)
			{
					Swapify<int> w(j);
					Swapify<int> w2(j);
					Swapify<int> w3(j);
					Swapify<int> w4(j);
					Swapify<int> w5(j);
					cerr<<"newCount2: "<<newCount2<<endl;

					cerr<<"dummy.Length(): "<< dummy<<endl;
					//cerr<<"ATT: "<<attNames.Find(w).GetData()<<endl;
					cerr<<"j: "<< w<<endl;
					//cerr<<"inAttCount: c"<<inAttCount<<endl;
					if(TID == attTID.Find(w).GetData())
					{
						Swapify<int>R_key;
						Swapify<string>R_string;
						attNames.Remove(w, R_key, R_string);
						Swapify<int>R_key2;
						Swapify<int>R_int;
						attTID.Remove(w2, R_key2, R_int);
						Swapify<int>R_key3;
						Swapify<int>R_int2;
						attNumDis.Remove(w3, R_key3, R_int2);
						Swapify<int>R_key4;
						Swapify<string>R_string2;
						attType.Remove(w4, R_key4, R_string2);
						Swapify<int>R_key5;
						Swapify<int>R_int3;
						attID.Remove(w5, R_key5, R_int3);
						cout<<"HERE2"<<endl;
						
					}
				inAttCount--;
				dummy--;

				//}
			//}

					newCount2++;
					inTabCount--;
a++;
						return true;

		}

	newCount++;

	//}
//sqlite3_close(db);
*/

	return false;

}


string Catalog::FindTable(int ID)
{
	int index;
	string tab;

	Swapify<int> y(ID);

	index = attTID.Find(y).GetData();

	Swapify<int> x(index);
	tab = tabNames.Find(x).GetData();

	return tab;
}


ostream& operator<<(ostream& _os, Catalog& _c) {

	int TID = 0;

	for(int i = _c.count; i < _c.inTabCount; i++)
	{
		Swapify<int> y(i);
		cout<<_c.tabNames.Find(y).GetData()<<" \ "<< _c.tabNumTups.Find(y).GetData()<< " \ "<<_c.tabPath.Find(y).GetData()<<endl;
		TID = _c.tabTID.Find(y).GetData();

		for(int j = _c.acount; j < _c.inAttCount; j++)
		{
			Swapify<int> x(j);
			if(TID == _c.attTID.Find(x).GetData())
			{
				cout<<_c.attNames.Find(x).GetData()<<" \ "<<_c.attType.Find(x).GetData()<<" \ "<<_c.attNumDis.Find(x).GetData()<<endl;
			}
		}
	}

	return _os;
}




















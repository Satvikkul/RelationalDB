#include <string>
#include <cstring>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"


using namespace std;


DBFile::DBFile () : fileName("") {
	Pointer = 0;
}

DBFile::~DBFile () {
}

DBFile::DBFile(const DBFile& _copyMe) :
	file(_copyMe.file),	fileName(_copyMe.fileName), Pointer(0) {}

DBFile& DBFile::operator=(const DBFile& _copyMe) {
	// handle self-assignment first
	if (this == &_copyMe) return *this;

	file = _copyMe.file;
	fileName = _copyMe.fileName;

	return *this;
}

int DBFile::Create (const char* f_path, FileType f_type) {
	fileName = f_path;
	type = f_type;
	return file.Open(0, f_path);
}

int DBFile::Open (const char* f_path) {
	fileName = f_path;
	return file.Open(1, f_path);
}

void DBFile::Load (Schema& schema, const char* textFile) {
	MoveFirst();
	FILE * f;
	string s = textFile;
	f = fopen(&s[0],"r");
	int cnt = 0;
	while(1)
	{
		// cout << cnt++ << endl;
		Record* _rec = new Record();
		if(_rec->ExtractNextRecord(schema, *f) == 1)
		{
			AppendRecord(*_rec);
		}
		else 
		{
			break;
		}

	}		
	file.AddPage(pg, file.GetLength());
	pg.EmptyItOut();
	Pointer++;
	fclose(f);
	
}

int DBFile::Close () {
	cout<<file.Close()<<endl;
	return file.Close();
}

void DBFile::MoveFirst () {
	Pointer = 0;

}

void DBFile::AppendRecord (Record& rec) {
	if(pg.Append(rec) == 0)
	{
		file.AddPage(pg, file.GetLength());
		pg.EmptyItOut();
		pg.Append(rec);
		Pointer++;
	}
}

int DBFile::GetNext (Record& rec) {

	if(pg.GetFirst(rec) == 0)
	{
		//cout<<"POINTER: "<<Pointer<<endl;
		if(file.GetLength() == Pointer)
	 	{
		  	return 0;
		}
		if(file.GetPage(pg, Pointer)  == -1)
		{
			
			return 0;
		}
		pg.GetFirst(rec);
		Pointer++;
		
	}
	return 1;

	
}

bool DBFile::testDBFile(Schema _schema, string _heapPath) {
	// open DBFile
	char* heapPath = new char[_heapPath.length()+1];
	strcpy(heapPath, _heapPath.c_str());
	DBFile dbFile;
	if(dbFile.Open(heapPath) == -1) {
		cout << "ERROR: DBFile::Open" << endl << endl;
		return false;
	}

	// retrieve data from DBFile
	dbFile.MoveFirst();
	while(true) {
		Record record;
		if(dbFile.GetNext(record) == 1) {
		
			record.print(cout, _schema); cout << endl;
		} else {
			break;
		}
	}


	if(dbFile.Close() == -1) {
		cout << "ERROR: DBFile::Close" << endl << endl;
		return false;
	}

	return true;
}

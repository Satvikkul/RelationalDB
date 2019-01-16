#ifndef DBFILE_H
#define DBFILE_H

#include <string>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"

using namespace std;


class DBFile {
private:
	File file;
	string fileName;
	FileType type;
	Page pg;
	Page pg2;
	int Pointer;

public:
	DBFile ();
	virtual ~DBFile ();
	DBFile(const DBFile& _copyMe);
	DBFile& operator=(const DBFile& _copyMe);

	int Create (const char* fpath, FileType file_type);
	int Open (const char* fpath);
	int Close ();

	void Load (Schema& _schema, const char* textFile);

	void MoveFirst ();
	void AppendRecord (Record& _addMe);
	int GetNext (Record& _fetchMe);
	bool testDBFile(Schema _schema, string _heapPath);
};

#endif //DBFILE_H

#include <iostream>
#include "RelOp.h"
#include "Catalog.h"
#include <algorithm>
#include <fstream>

using namespace std;


ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}


Scan::Scan(Schema& _schema, DBFile& _file) {

	schema = _schema;
	file = _file;

}

bool Scan::GetNext(Record & rec)
{
	
	return file.GetNext(rec);
}

Scan::~Scan() {

}

ostream& Scan::print(ostream& _os) {
	
	string name = "catalog.sqlite";
	Catalog c(name);
	vector<string> tabs;
	c.GetTables(tabs);
	vector<string> atts2;
	int in;

	for(int i = 0; i < tabs.size(); i++)
	{
		c.GetAttributes(tabs[i],atts2);
	}


	for(int i = 0; i < atts2.size(); i++)
	{
		if(schema.Index(atts2[i]) != -1)
		{
			in = i;
			break;
		}
	}

	string name2;

	name2 = c.FindTable(in);
	_os<<name2<<"  ";


	return _os;

}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer) {

	schema = _schema;
	predicate = _predicate;
	constants = _constants;
	producer = _producer;
}

bool Select::GetNext(Record & _rec)
{
	if (!producer->GetNext(_rec))
	{
		return false;
	}
	while(!predicate.Run(_rec, constants))
	{
		if(!producer->GetNext(_rec))
		{
			return false;
		}
	}
	return true;
}

Select::~Select() {

}

ostream& Select::print(ostream& _os) {
	//return _os << " SELECT "<< *producer;

		_os << "Select [";
	for(int i = 0; i < predicate.numAnds; i++) {
		if(i > 0) {
			_os << " AND ";
		}

		Comparison comp = predicate.andList[i];
		vector<Attribute> atts = schema.GetAtts();
		if(comp.operand1 != Literal) {
			_os << atts[comp.whichAtt1].name;
		} else { // see Record::print for more info
			int pointer = ((int *) constants.GetBits())[comp.whichAtt1 + 1];
			if (atts[comp.whichAtt1].type == Integer) {
				int *myInt = (int *) &(constants.GetBits()[pointer]);
				_os << *myInt;
			} else if (atts[comp.whichAtt1].type == Float) {
				double *myDouble = (double *) &(constants.GetBits()[pointer]);
				_os << *myDouble;
			} else if (atts[comp.whichAtt1].type == String) {
				char *myString = (char *) &(constants.GetBits()[pointer]);
				_os << myString;
			}
		}

		if (comp.op == LessThan) {
			_os << " < ";
		} else if (comp.op == GreaterThan) {
			_os << " > ";
		} else if (comp.op == Equals) {
			_os << " = ";
		} else {
			_os << " ? ";
		}

		if(comp.operand2 != Literal) {
			_os << atts[comp.whichAtt2].name;
		} else { // see Record::print for more info
			int pointer = ((int *) constants.GetBits())[comp.whichAtt2 + 1];
			if (atts[comp.whichAtt1].type == Integer) {
				int *myInt = (int *) &(constants.GetBits()[pointer]);
				_os << *myInt;
			} else if (atts[comp.whichAtt1].type == Float) {
				double *myDouble = (double *) &(constants.GetBits()[pointer]);
				_os << *myDouble;
			} else if (atts[comp.whichAtt1].type == String) {
				char *myString = (char *) &(constants.GetBits()[pointer]);
				_os << "\'" << myString << "\'";
			}
		}
	}
	_os << "]" << *producer;
	return _os;
}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {

	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	numAttsInput = _numAttsInput;
	numAttsOutput = _numAttsOutput;
	keepMe = _keepMe;
	producer = _producer;
}

bool Project::GetNext(Record& rec) {
	if(producer->GetNext(rec))
	{
		rec.Project(keepMe, numAttsOutput, numAttsInput);		
		return true;
	}
	return false;
	cout<<"IN PROJECT"<<endl;

}


Project::~Project() {

}

ostream& Project::print(ostream& _os) {

		_os << "Project [";
	vector<Attribute> atts = schemaOut.GetAtts();
	for(vector<Attribute>::iterator it = atts.begin(); it != atts.end(); it++) {
		if(it != atts.begin())
			_os << ", ";
		_os << it->name;
	}
	_os << "]\n\t \n\t" << *producer;
	return _os;
}


Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) {

	/*schemaLeft = _schemaLeft;
	schemaRight = _schemaRight;
	schemaOut = _schemaOut;
	predicate = _predicate;
	left = _left;
	right = _right;*/

	/*schemaLeft = _schemaLeft;
	schemaRight = _schemaRight;
	schemaOut = _schemaOut;
	predicate = _predicate;
	left = _left;
	right = _right;*/

	schemaLeft = _schemaLeft; 	
	schemaRight = _schemaRight;
	left = _left;
	right = _right;	
	schemaOut = _schemaOut;
	predicate = _predicate;
	countLeft = 0; 
	countRight = 0;
	vectorIndex = 0; 
	D=1;
	phase = 4;
	fcount = 0;
			

	leftsize = schemaLeft.GetNumAtts();
	rightsize = schemaRight.GetNumAtts();		
	total = schemaOut.GetNumAtts();						

	vector<Attribute> attsLeft = _schemaLeft.GetAtts();

	/*for(int i = 0; i < attsLeft.size(); i++)
	{
		cout<<"attributes:::: "<<attsLeft[i].name<<endl;
	}*/

	vector<Attribute> attsRight = _schemaRight.GetAtts();

	/*for(int i = 0; i < attsRight.size(); i++)
	{
		cout<<"attributes:::: "<<attsRight[i].name<<endl;
	}*/

	for (auto a:attsLeft)
	{	
		if (_schemaLeft.GetDistincts(a.name) > countLeft)	
		{
			countLeft = _schemaLeft.GetDistincts(a.name);
			//cout<<"countLeft: "<<countLeft<<endl;
		}
	}
	for (auto a:attsRight)	{
		if (_schemaRight.GetDistincts(a.name) > countRight)	
		{
			countRight = _schemaRight.GetDistincts(a.name);
			//cout<<"countRight: "<<countRight<<endl;
		}
	}



	/*if (predicate.andList[predicate.numAnds-1].operand1 == Left)
	{
		for (int i = 0; i < predicate.numAnds; i++)
		{		
			watt1.push_back(predicate.andList[i].whichAtt1);
			watt2.push_back(predicate.andList[i].whichAtt2);
		}		
	}
		 else
		 {

		 	for (int i = 0; i < predicate.numAnds; i++)
		 	{		
		 		watt1.push_back(predicate.andList[i].whichAtt2);
		 		watt2.push_back(predicate.andList[i].whichAtt1);
		 	}
		 }*/

	//*ats[total];
	
	int k = 0;
	rightstart = 0;
	bool check2 = false;
	/*for(int i = 0; i < leftsize; i++)
	{
		ats[i] = i;
	}
	for(int j = leftsize; j < total; j++)
	{
		ats[j] = k;
		cout<<"J: "<<k<<endl;
		k++;
	}*/
		/*else{
			cout<<" ";
		}*/



	cnt = 0;
	is_join_finished = false;
	
	

	//cout<<"END OF JOIN"<<endl;
}

bool Join::GetNext(Record& record)
 	{	

    //cout<<"INSIDE JOIN"<<endl;

	if(!is_join_finished)
	{	
		Record rec;
		 int *ats = new int[schemaOut.GetNumAtts()];

		 while(left->GetNext(rec))
		 {
		 	lef.Insert(rec);
		 	//cout<<"INSERTED"<<endl;
		 }

		 while(right->GetNext(rec))
		 {
		 	rit.Insert(rec);
		 }
		cout<<"LEFT: "<<lef.Length()<<endl;
		cout<<"RIGHT: "<<rit.Length()<<endl;

		 if(lef.Length() > rit.Length())
		 {
		 	while(rit.RightLength() != 0)
		 	{
		 		while(lef.RightLength()!=0)
		 		{
		 			if(predicate.Run(lef.Current(), rit.Current()))
		 			{
		 				int j = 0;
		 				for(int i = 0; i < schemaLeft.GetNumAtts(); i++)
		 				{
		 					ats[i] = i;
		 				}
		 				for(int i = schemaLeft.GetNumAtts(); i < schemaOut.GetNumAtts(); i++)
		 				{
		 					ats[i] = j;
		 					j++;
		 				}
		 				record.MergeRecords(lef.Current(), rit.Current(), leftsize, rightsize, ats, total, leftsize);
						//cout<<"RECORD WAS MERGED"<<endl;
	                     R.push_back(record);
		 			}
		 			lef.Advance();
		 		}
		 		lef.MoveToStart();
		 		rit.Advance();
		 	}
		 }

	     else
	     {
		cout<<"LEFT RIGHT LENGTH: "<<lef.RightLength()<<endl;
	         while(lef.RightLength() != 0)
	         {
	             while(rit.RightLength()!=0)
	             {
	                 if(predicate.Run(lef.Current(), rit.Current()))
	                 {
	                 	int j = 0;
	                 	for(int i = 0; i < schemaLeft.GetNumAtts(); i++)
		 				{
		 					ats[i] = i;
		 				}
		 				for(int i = schemaLeft.GetNumAtts(); i < schemaOut.GetNumAtts(); i++)
		 				{
		 					ats[i] = j;
		 					j++;
		 				}
	                     record.MergeRecords(lef.Current(), rit.Current(), leftsize, rightsize, ats, total, leftsize);
	                     //cout<<"RECORD WAS MERGED"<<endl;
	                     R.push_back(record);
//cout<<"RECORD WAS MERGED"<<endl;
	                 }
	                 rit.Advance();
	             }
	             rit.MoveToStart();
	             lef.Advance();
	         }
	     }
	     is_join_finished = true;
	     
}
	    cout<<"RSIZE: "<<R.size()<<endl;

	    
			if(cnt < R.size())
			{
				record = R[cnt];
				cnt++;
				//cout<<"RETURN TRUE"<<endl;
				return true;
			}
			else
			{
				//cout<<"RETURN FALSE"<<endl;
				return false;
			}


	     /*for(int i = 0; i < R.size(); i++)
	     {
	         if(predicate.Run(R[i],record))
	         {
	         	cout<<"RETURN TRUE"<<endl;
	        	record = R[i];
			count7++;
			//return true;
	         }
	         else
	        {
	         	cout<<"RETURN FALSE"<<endl;
	             return false;
	        }
     	     }*/

     }	



// 	Record rec;

// 	while(left->GetNext(rec))
// 	{
// 		lef.Insert(rec);
// 	}

// 	while(right->GetNext(rec))
// 	{
// 		rit.Insert(rec);
// 	}

// 	if(lef.Length() > rit.Length())
// 	{
// 		while(rit.RightLength() != 0)
// 		{
// 			while(lef.RightLength()!=0)
// 			{
// 				if(predicate.Run(lef.currentRecord, rit.currentRecord) == true)
// 				{
// 					record.MergeRecords(lef.currentRecord, rit.currentRecord, leftsize, rightsize, ats, total, );
// 				}
// 			}
// 		}
// 	}



// 		while (1)
// 		{
// 			if (right -> GetNext(rerecord)){

// 				Record copy = rerecord;
// 				int* attsToKeep = &watt2[0];
// 				int numAttsToKeep = predicate.numAnds;
// 				int numAttsNow = schemaRight.GetNumAtts();
// 				copy.Project (attsToKeep, numAttsToKeep, numAttsNow);
// 				Schema sCopy = schemaRight;
// 				sCopy.Project(watt2);
// 				vector <Attribute> attsToErase = sCopy.GetAtts();


/*	if(D==1){
		right->SetNoPages(noPages);
		left->SetNoPages(noPages);
	
		Record rerecord;

		if (countLeft > countRight)
		{
			int recSize = 0;
			int count = 0;	
			while (1)
			{
				if (right -> GetNext(rerecord)){

					Record copy = rerecord;
					int* attsToKeep = &watt2[0];
					int numAttsToKeep = predicate.numAnds;
					int numAttsNow = schemaRight.GetNumAtts();
					copy.Project (attsToKeep, numAttsToKeep, numAttsNow);
					Schema sCopy = schemaRight;
					sCopy.Project(watt2);
					vector <Attribute> attsToErase = sCopy.GetAtts();
					
								
					recSize += rerecord.GetSize();

					int c=0;
			
					stringstream s;
					copy.print(s, sCopy);
					string sc = "", ss = s.str();

					for (int i = 0; i < attsToErase.size(); i++) 
					{
						string insideLoop = attsToErase[i].name;
						ss.erase (ss.find(insideLoop), insideLoop.size()+2);
						sc+= ss;
					}

					auto it = List.find(s.str());
					if(it != List.end())
					{	
						List[sc].push_back(rerecord);
					}
					else
					{
						vector <Record> v;
						v.push_back(rerecord);
						List.insert (make_pair(sc, v));
					}
				
			
				}else
				{
					break;
				}

			}
		}
		else
		{
			int recSize = 0;
			int count = 0;
			
			while (1)
			{
				if (left -> GetNext(rerecord))
				{
					Record copy;
					copy = rerecord;
					int* attsToKeep = &watt1[0];
					int numAttsToKeep = predicate.numAnds;
					int numAttsNow = schemaLeft.GetNumAtts();
					copy.Project (attsToKeep, numAttsToKeep, numAttsNow);
					Schema sCopy = schemaLeft;
					sCopy.Project(watt1);
					vector <Attribute> attsToErase = sCopy.GetAtts();

					
					
					recSize += rerecord.GetSize();

					int c=0;

					stringstream s;
					copy.print(s, sCopy);
					string sc = "", ss = s.str();

					for (int i = 0; i < attsToErase.size(); i++) 
					{
						string insideLoop = attsToErase[i].name;
						ss.erase (ss.find(insideLoop), insideLoop.size()+2);
						sc+= ss;
					}

					auto it = List.find(s.str());
					if(it != List.end())	
					{
						List[sc].push_back(rerecord);
					}
					else
					{
						vector <Record> v;
						Record r;
						v.push_back(rerecord);
						List.insert (make_pair(sc, v));
					}
			
				}
			else
			{
				break;
			}

			}
		}
		D=0;
	}

	Record rLeft;

	if (countLeft > countRight) 
	{
		while (1)
		{ 
			if (lastrec.GetSize() == 0) 
				if(!left -> GetNext(lastrec)) return false;
	
			Record copy = lastrec;
			int* attsToKeep = &watt1[0];
			int numAttsToKeep = predicate.numAnds;
			int numAttsNow = schemaLeft.GetNumAtts();
			copy.Project (attsToKeep, numAttsToKeep, numAttsNow);
			Schema sCopy = schemaLeft;
			sCopy.Project(watt1);
		
			vector <Attribute> attsToErase = sCopy.GetAtts();
			stringstream s;
			copy.print(s, sCopy);
			string sc = "", ss = s.str();

			for (int i = 0; i < attsToErase.size(); i++) 
			{
				string insideLoop = attsToErase[i].name;
				ss.erase (ss.find(insideLoop), insideLoop.size()+2);
				sc+= ss;
			}

			auto it = List.find(sc);
			if (it == List.end()) lastrec.Nullify();
			else
			{
				if (it->second.size() == vectorIndex)
				{
					vectorIndex = 0;
					if(!left -> GetNext(lastrec)) return false;
				}

				else
				{
					rLeft = it->second[vectorIndex];
					vectorIndex++;
					if (predicate.Run (lastrec, rLeft))
					{
						record.AppendRecords( lastrec, rLeft, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
						return true;
					}
				}
			}
		}
	}

	else
	{
		while (1)
		{
			if (lastrec.GetSize() == 0) 
				if(!right -> GetNext(lastrec)) return false;

			Record copy = lastrec;
			int* attsToKeep = &watt2[0];
			int numAttsToKeep = predicate.numAnds;
			int numAttsNow = schemaRight.GetNumAtts();
			copy.Project (attsToKeep, numAttsToKeep, numAttsNow);
			Schema sCopy = schemaRight;
			sCopy.Project(watt2);
		
			vector <Attribute> attsToErase = sCopy.GetAtts();
			stringstream s;
			copy.print(s, sCopy);
			string sc = "", ss = s.str();

			for (int i = 0; i < attsToErase.size(); i++) 
			{
				string insideLoop = attsToErase[i].name;
				ss.erase (ss.find(insideLoop), insideLoop.size()+2);
				sc+= ss;
			}

			auto it = List.find(sc);
			if (it == List.end()) lastrec.Nullify();
			else
			{
				if (it->second.size() == vectorIndex)
				{
					vectorIndex = 0;
					if(!right -> GetNext(lastrec)) return false;
				}

				else
				{
					rLeft = it->second[vectorIndex];
					vectorIndex++;
					if (predicate.Run (rLeft, lastrec))
					{
						record.AppendRecords( rLeft, lastrec, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
						return true;
					}
				}
			}
		}
	}*/

//}

Join::~Join() {

}

ostream& Join::print(ostream& _os) {
	return _os<<"("<<*left<<","<<*right<<")";


	return _os;

}


DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) {

	schema = _schema;
	producer = _producer;
}

bool DuplicateRemoval::GetNext(Record& rec)
{
	while(true)
	{
		if(producer->GetNext(rec) == false)
		{
			return false;
		}
		stringstream dummy;
		rec.print(dummy, schema);
		auto it = Set.find(dummy.str());
		if(it == Set.end())
		{
			Set[dummy.str()] = rec;
			return true;
		}
	}
}

DuplicateRemoval::~DuplicateRemoval() {

}


ostream& DuplicateRemoval::print(ostream& _os) {
	//return _os << " DISTINCT " << *producer<<endl;
	return _os << "Distinct \n\t \n\t" << *producer;
}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
	RelationalOp* _producer) {

	schemaIn = _schemaIn;
	schemaOut = _schemaOut; 
	compute = _compute;
	producer = _producer;
	check = false;
}

bool Sum::GetNext(Record& rec)
{
	if(check == true)
	{
		return false;
	}

	int IntegerSum = 0;
	double DoubleSum = 0;
	while(producer->GetNext(rec) == true)
	{
		int intResult = 0;
		double doubleResult = 0;
		Type typ = compute.Apply(rec, intResult, doubleResult);
		if(typ == Integer)
		{
			IntegerSum += intResult;
		}
		if(typ == Float)
		{	
			DoubleSum += doubleResult;
		}
	}
	double answer;
	answer = DoubleSum + (double)IntegerSum;
	cout << "answer: " << answer << endl;
	char* RecordMem = new char[PAGE_SIZE];
    int currentRecord = sizeof (int) * (2);
	((int *) RecordMem)[1] = currentRecord;
   // *((double*)(RecordMem + currentRecord)) = answer;
    *((double *) &(RecordMem[currentRecord])) = answer;
    currentRecord += sizeof (double);
	((int *) RecordMem)[0] = 1;
  //  Record RecordSum;
    rec.CopyBits( RecordMem, currentRecord );
    delete [] RecordMem;
	//rec = RecordSum;
	check = true;
	return true;

}


Sum::~Sum() {

}


ostream& Sum::print(ostream& _os) {
	return _os << " SUM " <<*producer<<endl;
	
	//_os << "SUM(";
	
	//_os << ")\n\t \n\t" << *producer;;
	//return _os;
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) {

	schemaIn =  _schemaIn;
	schemaOut = _schemaOut;
	compute = _compute;	
	producer = _producer;
	check1 = 0;
}


bool GroupBy::GetNext(Record& rec)
{


	vector<int> attsToKeep1;
	vector<int> attsToKeep2;

	for(int i = 0; i < schemaOut.GetNumAtts(); i++)
	{
		attsToKeep1.push_back(i);
	}

	sch1 = schemaOut;
	sch1.Project(attsToKeep1);


	attsToKeep2.push_back(0);

	sch2 = schemaOut;
	sch2.Project(attsToKeep2);

	if(check1 == 0)
	{	
		while(producer->GetNext(rec))
		{
			int IntResult = 0;
			double DoubleResult = 0;

			compute.Apply(rec, IntResult, DoubleResult);
			double Result;
			Result = (double)IntResult + DoubleResult;

			rec.Project(&groupingAtts.whichAtts[0], groupingAtts.numAtts, sch1.GetNumAtts());

			stringstream str;

			rec.print(str , sch1);

			auto it = Map.find(str.str());

			if(it != Map.end())
			{
				Map[str.str()] += Result;
			}
			else
			{
				Map[str.str()] = Result;
				rMap[str.str()] = rec;
			}
		}

		check1 = 1;
	}
	
	if(check1 == 1)
	{
		if(Map.empty())
		{
			return false;
		}

		Record dummy = rMap.begin()->second;
		string s;
		s = Map.begin()->first;

		char* recordSpace = new char[PAGE_SIZE];
		int currentRecord = sizeof (int)*2;
		((int*)recordSpace)[1] = currentRecord;
		*((double*) & (recordSpace[currentRecord])) = Map.begin()->second;
		currentRecord += sizeof(double);

		((int*)recordSpace)[0] = currentRecord;
		Record sRec;
		sRec.CopyBits(recordSpace, currentRecord);
		delete [] recordSpace;

		Record Record2;
		Record2.AppendRecords(sRec, dummy, 1, schemaOut.GetNumAtts()-1);
		rMap.erase(s);
		Map.erase(s);
		rec = Record2;
		return true;
	}



}

GroupBy::~GroupBy() {

}

ostream& GroupBy::print(ostream& _os) {
	
		_os << "GroupBy [";
	vector<Attribute> atts = schemaOut.GetAtts();
	for(vector<Attribute>::iterator it = atts.begin(); it != atts.end(); it++) {
		if(it != atts.begin())
			_os << ", ";

		string attrName = it->name;
		if(attrName == "sum") {
			_os << "SUM(";

			_os << ")";
		} else {
			_os << attrName;
		}
	}
	_os << "]\n\t \n\t" << *producer;
	return _os;
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {

	schema = _schema;
	outFile = _outFile;
	producer = _producer;
	f.open (&outFile[0]);
}

bool WriteOut::GetNext(Record& rec)
{

	if(producer->GetNext(rec) == true)
	{
		rec.print(f,schema);
		f<<endl;
		return true;
	}
	return false;


}

WriteOut::~WriteOut() {

}

ostream& WriteOut::print(ostream& _os) {
	_os << " WRITEOUT " << *producer<<endl;
	_os<<endl;
	return _os;
}




ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
    
    Record r;
	unsigned long recs = 0;

	return _os <<*_op.root<<endl;
    

}

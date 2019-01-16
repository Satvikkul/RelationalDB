#include <string>
#include <vector>
#include <iostream>
#include <cmath>
#include <algorithm>

#include "Schema.h"
#include "Comparison.h"
#include "QueryOptimizer.h"


using namespace std;


QueryOptimizer::QueryOptimizer(Catalog& _catalog) : catalog(&_catalog) {
	//_catalog = &Catalog::GetCatalog();
}

QueryOptimizer::~QueryOptimizer() {
}

TableList* Copy2(TableList* table) 
{
    if (table == NULL) 
    {
    	return NULL;
    }

    TableList* NewTable = new TableList;
    NewTable->tableName = table->tableName;
    NewTable->next = Copy2(table->next);
    return NewTable;
}

void QueryOptimizer::Optimize(TableList* _tables, AndList* _predicate,
	OptimizationTree* _root) {
	// compute the optimal join order
	//Catalog catalog;
	int counter = 0;//GetNumTup
	//string _table = "T_1";
	unsigned int _noTuples;
	/*string _path;
	string _attribute = "A_1_0";*/
	unsigned int _noDistinct;
	//catalog->GetNoTuples(_table, _noTuples);
	//catalog->GetDataFile(_table, _path);
	//catalog->GetNoDistinct(_table, _attribute, _noDistinct);
	/*cout<<"NUMBER OF TUPLES for T_1: "<<_noTuples<<endl;
	cout<<"Path for T_1: "<<_path<<endl;
	cout<<"NUMBER OF Distinct Values for T_1: "<<_noDistinct<<endl;*/
	//

//cout<<"TABLES IN OPTIMIZER FILE: "<<_tables->tableName<<endl;

	TableList* _tables3 = Copy2(_tables);
	TableList* _tables4 = Copy2(_tables);
	TableList* _tables5 = Copy2(_tables);
	TableList* _tables6 = Copy2(_tables);

	while(_tables != NULL)
	{
		//cout<<"HERE0"<<endl;
		counter++;
		_tables = _tables->next;
	}

	//cout<<"COUNTER: "<<counter<<endl;


	vector<Cost> c;
	for(int i = 0; i < counter; i++)
	{
		string s = _tables3->tableName;
		catalog->GetNoTuples(s, _noTuples);
		Cost temp;
		temp.size = _noTuples;
		temp.cost = 0;
		temp.Tname = _tables3->tableName;
		c.push_back(temp);
		CostMap[i] = c[i];
		_tables3 = _tables3->next;
	}

for(int i = counter - 1; i >= 0; i--)
{
	cout<<"Table: " <<CostMap[i].Tname<<" Cost: "<<CostMap[i].cost<<" Size: "<<CostMap[i].size<<endl;
}

cout<<"/////////////////////////////////////AFTER PUSH DOWN SELECTION////////////////////////////////////////////"<<endl;
/*vector<string> atts3;
for(int i = 0; i < counter; i++)
{
	string Tname3;
	Tname3 = _tables5->tableName;
	catalog->GetAttributes(Tname3, atts3);
	_tables5 = _tables5->next;

}

vector<string> preds;*/

/*for(int i = 0; i < atts.size(); i++)
{
	cout<<"ATTRIBUTES: "<<atts[i]<<endl;
}
*/

/*AndList * predicate = _predicate;

while(predicate != NULL)
{
	preds.push_back(predicate->left->left->value);
	//cout<<"PREDICATES: "<<_predicate->left->left->value<<endl;
	predicate = predicate->rightAnd;
}*/

CNF c2;

vector<Attribute>atts2;

for(int i = 0; i < counter; i++)
{
	Schema SC3;
	Record r2;
	int diviser = 1;
	unsigned int distinct;

	string TName2 = _tables4->tableName;
	//cout<<"TNAME2: "<<TName2<<endl;

	catalog->GetSchema(TName2, SC3);
	//cout<<"SCHEMA: "<<SC3<<endl;
	atts2 = SC3.GetAtts();


	/*cout<<"ATTRIBUTES PER ITERATION:"<<endl;
	for(int i = 0; i < atts2.size(); i++)
	{
		cout<<atts2[i].name<<endl;
	}*/

	c2.ExtractCNF(*_predicate, SC3, r2);

	//cout<<"TABLE NAME: "<<TName2<<endl;

	if(c2.numAnds > 0)
	{
		for(int j = 0; j < c2.numAnds; j++)
		{
			if((c2.andList[j].operand1 == Left) || (c2.andList[j].operand1 == Right))
			{
				if((c2.andList[j].op == LessThan) || (c2.andList[j].op == GreaterThan))
				{
					diviser = diviser * 3;
					//cout<<"DIVISOR: "<<diviser<<endl;
				}
				else
				{
					//cout<<"TABLE: "<<TName2<<endl;
					int index1 = c2.andList[j].whichAtt1;
					//cout<<"INDEX1: "<<atts2[index1].name<<endl;
					catalog->GetNoDistinct(TName2, atts2[index1].name, distinct);		
					//cout<<"DISTINCT: "<<distinct<<endl;	
					diviser = diviser * distinct;		
					//cout<<"DIVISER: "<<diviser<<endl;
				}
			}
			
			if((c2.andList[j].operand2 == Left) || (c2.andList[j].operand2 == Right))
			{
				if((c2.andList[j].op == LessThan) || (c2.andList[j].op == GreaterThan))
				{
					diviser = diviser * 3;
				}
				else
				{
					int index2 = c2.andList[j].whichAtt2;
					//cout<<"INDEX: "<<atts2[index2].name<<endl;

					catalog->GetNoDistinct(TName2, atts2[index2].name, distinct);		
					//cout<<"DISTINCT: "<<distinct<<endl;	
					diviser = diviser * distinct;		
				}
			}
		}
	}
	CostMap[i].size = CostMap[i].size / diviser;
	//atts.clear();
	_tables4 = _tables4->next;
}

for(int i = counter - 1; i >= 0; i--)
{
	cout<<"Table: " <<CostMap[i].Tname<<" Cost: "<<CostMap[i].cost<<" Size: "<<CostMap[i].size<<endl;
}

/*for(int i = 0; i < preds.size(); i++)
{
	cout<<"PREDS: "<<preds[i]<<endl;
}*/

vector<string> tabs;
for(int i = 0; i < counter; i++)
{
	string Tname5;
	Tname5 = _tables6->tableName;
	tabs.push_back(Tname5);
	_tables6 = _tables6->next;
}

vector<Cost> mp;
for(int i = 0; i < counter; i++)
{
	Cost tmp;
	tmp.size = CostMap[i].size;
	tmp.cost = CostMap[i].cost;
	tmp.Tname = CostMap[i].Tname;
	mp.push_back(tmp);
	/*mp[i].Tname = CostMap[i].Tname;
	mp[i].cost = CostMap[i].cost;
	mp[i].size = CostMap[i].size;*/
}


/*for(int i = 0; i < counter; i++)
{
	cout<<"TABLES VECTOR: "<<tabs[i]<<endl;
}*/


CNF c3;

vector<Cost> dum;


vector<Attribute> atts3;
vector<Attribute> atts4;
int dummy = 0;
for(int i = 0; i < tabs.size(); i++)
{
	string tab1 = tabs[i];
	Schema SC4;
	catalog->GetSchema(tab1, SC4);
	//cout<<"SCHEMA: "<<SC4<<endl;
	atts3 = SC4.GetAtts();
	int k = 1;
	unsigned long long int diviser2 = 1;
	unsigned int dis1;
	unsigned int dis2;
	unsigned long long int x;
	unsigned long long int y;
	unsigned long long int tup1;
	unsigned long long int tup2;

	//catalog->GetNoTuples(tab1, tup1);
	tup1 = CostMap[i].size;
	//cout<<"tup1: "<<tup1<<endl;
	//cout<<"TABS SIZE: "<<tabs.size()<<endl;;

	for(int j = i + 1; j < tabs.size(); j++)
	{
		diviser2 = 1;
		//cout<<"IN LOOP 2"<<endl;
		string tab2 = tabs[j];
		string name = tab1 + "," + tab2;
		Schema SC5;
		catalog->GetSchema(tab2, SC5);
		atts4 = SC5.GetAtts();


		//catalog->GetNoTuples(tab2, tup2);
		tup2 = CostMap[j].size;
		//cout<<"tup2: "<<tup2<<endl;

		//cout<<"SC4: "<<SC4<<endl;
		//cout<<"SC5: "<<SC5<<endl;

		c3.ExtractCNF(*_predicate, SC4, SC5);

		for(int l = 0; l < c3.numAnds; l++)
		{
			if(c3.andList[l].operand1 == Left)
			{
				int Index3 = c3.andList[l].whichAtt2;
				//cout<<"ATT NAME1: "<<atts3[Index3].name<<endl;
				catalog->GetNoDistinct(tab1, atts3[Index3].name, dis1);
				x = dis1;
				//cout<<"dis1:"<<y<<endl;

				Index3 = c3.andList[l].whichAtt1;
				//cout<<"ATT NAME2: "<<atts3[Index3].name<<endl;
				catalog->GetNoDistinct(tab2, atts4[Index3].name, dis2);
				y = dis2;
				//cout<<"dis2:"<<x<<endl;				

			}

			if(c3.andList[l].operand1 == Right)
			{
				int Index4 = c3.andList[l].whichAtt2;
				//cout<<"ATT NAME1 (Second if): "<<atts3[Index4].name<<endl;
				catalog->GetNoDistinct(tab1, atts3[Index4].name, dis1);
				x = dis1;
				//cout<<"dis1:"<<x<<endl;

				Index4 = c3.andList[l].whichAtt1;
				//cout<<"ATT NAME2 (Second if): "<<atts3[Index4].name<<endl;
				catalog->GetNoDistinct(tab2, atts4[Index4].name, dis2);
				y = dis2;	
				//cout<<"dis2:"<<y<<endl;			

			}

			if(x > y)
			{
				//cout<<"DIVISER2: "<<diviser2<<endl;
				diviser2 = diviser2 * x;
				//cout<<"TUP1:	"<<tup1<<"	TUP2:	"<<tup2<<endl;
				//cout<<"DIVISER2: "<<diviser2<<endl;
			}
			else
			{
				//cout<<"DIVISER2: "<<diviser2<<endl;
				//cout<<"DIVISER2: "<<diviser2<<endl;
				diviser2 = diviser2 * y;
				//cout<<"DIVISER2: "<<diviser2<<endl;
			}
		}

		double mult;
		Cost temp2;
		//mult = (tup1 * tup2);
		//cout<<"MULT: "<<mult<<endl;
		//cout<<"EQUATION: "<<"( "<<tup1<<" * "<<tup2<<" )"<<" / "<<diviser2<<endl;
		temp2.size = (tup1 * tup2) / (diviser2);

		temp2.cost = 0;
		temp2.Tname = name;
		Schema SC6(SC4);
		Schema SC7(SC5);
		SC6.Append(SC7);
		temp2.schema = SC6;
		//cout<<"SCHEMA6: "<<SC6<<endl;
		dum.push_back(temp2);
		CostMap2[dummy] = dum[dummy];
		dummy++;

	}
	k++;
}

cout<<"//////////////////////////////////AFTER JOINING TWO TABLES//////////////////////////////////////////"<<endl;
for(int i = 0; i < dum.size(); i++)
{
	cout<<"Table: " <<CostMap2[i].Tname<<" Cost: "<<CostMap2[i].cost<<" Size: "<<CostMap2[i].size<<" 	"<<endl;
}

for(int i = 0; i< tabs.size(); i++)
{
	cout<<"TABLES:		"<<tabs[i]<<endl;
}

map<int, struct Cost> OptimalMap;

if(tabs.size() > 1)
{
	Greedy(CostMap2, mp, _predicate);
}
else
{
	vector<string> removed2;
	removed2.push_back(tabs[0]);
	Result = removed2;
}

}


void QueryOptimizer::Greedy(map<int, struct Cost> m, vector<Cost>tabs, AndList* _predicate)
{

	//reverse(tabs.begin(),tabs.end());
	map<int, struct Cost> Opt;
	map<int, struct Cost> CostMap3;
	vector<Cost> dum2;
	vector<string> removed;
	int sz = m.size();
	cout<<"SIZE: "<<m.size()<<endl;
	int dummy = 1000000000;
	int k = 0;

	int im = tabs.size();

	for(int i = 0; i < sz; i++)
	{

		cout<<"NAME: "<<m[2].Tname<<endl;
		if(abs(m[i].size) < abs(dummy))
		{
			dummy = m[i].size;
			cout<<"DUMMY: "<<dummy<<endl;
			Cost temp3;
			temp3.size = dummy;
			temp3.cost = m[i].cost;
			temp3.Tname = m[i].Tname;
			temp3.schema = m[i].schema;
			dum2.push_back(temp3);
			Opt[0] = dum2[k];
			k++;
		}

	}

	//for(int i = 0; i < Opt.size(); i++)
	//{
		cout<<"OPTIMAL SOLUTION: "<<Opt[0].Tname<<"		"<<Opt[0].size<<"		"<<Opt[0].cost<<"		"<<endl;
	//}

		string test = Opt[0].Tname;
		cout<<"TEST: "<<test<<endl;
		char n[test.length()];
		char n2[test.length()];
		string nam;
		string nam2;
		int len = 0;
		int len2 = 0;

		for(int i = 0; i < test.length(); i++)
		{
			if(test[i] == ',')
			{
				break;
			}
			n[i] = test[i];
			len++;

		}
		for(int i = 0; i < len; i++)
		{
			nam += n[i];
		}
		cout<<"FIRST NAME: "<<nam<<endl;
		removed.push_back(nam);
		cout<<"len: "<<len<<endl;
		cout<<"test size: "<<test.length()<<endl;

		for(int i = len + 1; i < test.length(); i++)
		{
			n2[len2] = test[i];
			len2++;
		}

		for(int i = 0; i < len2; i++)
		{
			nam2 += n2[i];
		}
		cout<<"SECOND NAME: "<<nam2<<endl;
		removed.push_back(nam2);

		int save;
		int save2;

		map<int, struct Cost>::iterator it;
		map<int, struct Cost>::iterator it2;
		


		for(int i = 0; i < tabs.size(); i++)
		{
			if(nam.compare(tabs[i].Tname) == 0)
			{
				save = i;
				cout<<"INDEX1: "<<save<<endl;
			}
		}

		tabs.erase(tabs.begin() + save);
		//it = m2.find(save);
		//m2.erase (it); 


		for(int i = 0; i < tabs.size(); i++)
		{
			if(nam2.compare(tabs[i].Tname) == 0)
			{
				save2 = i;
				cout<<"INDEX2: "<<save2<<endl;
			}
		}
		
		tabs.erase(tabs.begin() + save2);
		/*cout<<"HERE"<<endl;
		it2 = m2.find(save2);
		cout<<"HERE1"<<it2<<endl;
		m2.erase (it2); 
		cout<<"HERE2"<<endl;*/

		for(int i = 0; i < tabs.size(); i++)
		{
			cout<<"NEW TABLES LIST: "<<tabs[i].Tname<<"		"<<tabs[i].size<<"		"<<tabs[i].cost<<endl;
		}
		
		/*for(int i = 0; i < m2.size(); i++)
		{
			cout<<"Table: " <<m2[i].Tname<<" Cost: "<<m2[i].cost<<" Size: "<<m2[i].size<<endl;
		}*/


		CNF c4;
		vector<Attribute> atts5;
		vector<Attribute> atts6;
		vector<Cost> dum;
		int dummy1 = 0;
		long long int dummy2 = 10000000000000;
		int o = 0;	
		vector<Cost> dum3;
		string g;
		int save3;
		

		while(tabs.size() > 0)
		{
			int i = 0;

			//for(int i = 0; i < tabs.size(); i++)
			//{
			cout<<"I: "<<i<<endl;
				dummy2 = 10000000000000;
				string tab3 = Opt[0].Tname;
				string tab4 = tabs[i].Tname;
				cout<<"tab4: "<<tab4<<endl;
				cout<<"tab3: "<<tab3<<endl; 

				string nms;
				nms = tab3 + "," + tab4;

				Schema SC8;
				catalog->GetSchema(tab4, SC8);
				//cout<<"SCHEMA: "<<SC4<<endl;
				//Schema SC9 = Opt[0].schema;
				atts5 = Opt[0].schema.GetAtts();
				atts6 = SC8.GetAtts();
				int k = 1;
				unsigned long long int diviser2 = 1;
				unsigned int dis1;
				unsigned int dis2;
				unsigned long long int x[1000];
				unsigned long long int y;
				unsigned long long int tup1;
				unsigned long long int tup2;

				tup1 = Opt[0].size;
				cout<<"tup1: "<<tup1<<endl;
				tup2 = tabs[i].size;
				cout<<"tup2: "<<tup2<<endl;

				//cout<<"schema1: "<<Opt[0].schema<<endl;
				//cout<<"schema2: "<<SC8<<endl;

				c4.ExtractCNF(*_predicate, Opt[0].schema, SC8);
				cout<<"NUMANDS: "<<c4.numAnds<<endl;

				for(int l = 0; l < c4.numAnds; l++)
				{
					if(c4.andList[l].operand1 == Left)
					{
						int Index3 = c4.andList[l].whichAtt2;
						cout<<"ATT NAME1: "<<atts5[Index3].name<<endl;
						for(int r = 0; r < removed.size(); r++)
						{
							catalog->GetNoDistinct(removed[r], atts5[Index3].name, dis1);
							cout<<"REMOVED: "<<removed[r]<<endl;
							x[r] = dis1;
							cout<<"dis1:"<<x[r]<<endl;
						}

						Index3 = c4.andList[l].whichAtt1;
						cout<<"ATT NAME2: "<<atts6[Index3].name<<endl;
						catalog->GetNoDistinct(tab4, atts6[Index3].name, dis2);
						y = dis2;
						cout<<"dis2:"<<y<<endl;				

					}

					if(c4.andList[l].operand1 == Right)
					{
						int Index4 = c4.andList[l].whichAtt2;
						cout<<"ATT NAME1 (Second if): "<<atts5[Index4].name<<endl;
						//catalog->GetNoDistinct(tab3, atts5[Index4].name, dis1);
						//cout<<"Table1: "<<tab3<<endl;
						for(int r = 0; r < removed.size(); r++)
						{
							cout<<"IN LOOP S"<<endl;
							catalog->GetNoDistinct(removed[r], atts5[Index4].name, dis1);
							cout<<"REMOVED: "<<removed[r]<<endl;
							cout<<"IN LOOP S2"<<endl;
							x[r] = dis1;
							cout<<"IN LOOP S3"<<endl;
							cout<<"dis1:"<<x[r]<<endl;
						}
						//x = dis1;
						//cout<<"dis1:"<<x<<endl;

						Index4 = c4.andList[l].whichAtt1;
						cout<<"ATT NAME2 (Second if): "<<atts6[Index4].name<<endl;
						catalog->GetNoDistinct(tab4, atts6[Index4].name, dis2);
						cout<<"Table2: "<<tab4<<endl;
						y = dis2;	
						cout<<"dis2:"<<y<<endl;			

					}

					cout<<"REMOVED SIZE: "<<removed.size()<<endl;
					for(int e = 0; e < removed.size(); e++)
					{
						if(x[e] > y)
						{
							cout<<"X[e]: "<<x[e]<<endl;
							cout<<"Y: "<<y<<endl;
							cout<<"DIVISER2: "<<diviser2<<endl;
							diviser2 = x[e];
							cout<<"TUP1:	"<<tup1<<"	TUP2:	"<<tup2<<endl;
							cout<<"DIVISER2: "<<diviser2<<endl;
							//break;
						}

						else
						{
							cout<<"DIVISER2: "<<diviser2<<endl;
							//cout<<"DIVISER2: "<<diviser2<<endl;
							diviser2 = y;
							cout<<"DIVISER2: "<<diviser2<<endl;
							//break;
						}
					}

					Cost temp2;
					//mult = (tup1 * tup2);
					//cout<<"MULT: "<<mult<<endl;
					cout<<"EQUATION: "<<"( "<<tup1<<" * "<<tup2<<" )"<<" / "<<diviser2<<endl;
					temp2.size = (tup1 * tup2) / (diviser2);
					cout<<"NEW SIZE: "<<temp2.size<<endl;
					cout<<"DUMMY1: "<<dummy1<<endl;
					temp2.cost = 0;
					temp2.Tname = nms;
					Schema SC9(Opt[0].schema);
					Schema SC10(SC8);
					SC9.Append(SC10);
					temp2.schema = SC9;
					dum.push_back(temp2);
					cout<<"asdfasdfasdfasdfsdf: "<<dum[dummy1].size<<endl;
					CostMap3[0] = dum[dummy1];
					cout<<"DUMMY1: "<<dummy1<<endl;
				}
				cout<<"COST MAP 3 SIZE: "<<CostMap3.size()<<endl;
				for(int d = 0; d < tabs.size(); d++)
				{
					cout<<"SIZE: "<<abs(CostMap3[0].size)<<endl;
					cout<<"DUMMY2: "<<dummy2<<endl;
					if(abs(CostMap3[0].size) < abs(dummy2))
					{
						dummy2 = CostMap3[0].size;
						cout<<"DUMMY: "<<dummy2<<endl;
						Cost temp4;
						temp4.size = dummy2;
						temp4.cost = tabs[d].size + Opt[0].size;
						g = CostMap3[0].Tname;
						temp4.Tname = CostMap3[0].Tname;
						temp4.schema = CostMap3[0].schema;
						dum3.push_back(temp4);
						Opt[0] = dum3[o];
						o++;
					}
				}
			cout<<"OPTIMAL SOLUTION *: "<<Opt[0].Tname<<"		"<<Opt[0].size<<"		"<<Opt[0].cost<<endl;
			//}

			for(int h = 0; h < tabs.size(); h++)
			{
				if(g.compare(tabs[h].Tname) == 0)
				{
					save3 = h;
				}
			}

			removed.push_back(tabs[save3].Tname);
			tabs.erase(tabs.begin() + save2);
			for(int w = 0; w < tabs.size(); w++)
			{
				cout<<"TABLES LEFT TO EVALUATE: "<<tabs[w].Tname<<endl;
			}
			dummy1++;
			i++;
		}

	cout<<"FINAL SOLUTION: "<<Opt[0].Tname<<"		"<<Opt[0].size<<"		"<<Opt[0].cost<<endl;
	vector<string> FinalMap;
	//FinalMap.push_back(Opt[0].Tname);
	//cout<<"AAAAAAAAAAAAAAAAAAAAAAAA: "<<FinalMap[0]<<endl;

	for(int v = 0; v < removed.size(); v++)
	{
		cout<<"REMOVED: "<<removed[v]<<endl;
	}


	Result = removed;


}



/*void Partition(int size, Cost m)
{
	for(int i = 0; i < m.size(); i++)
	{

	}
}*/

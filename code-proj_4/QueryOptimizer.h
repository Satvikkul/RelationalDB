#ifndef _QUERY_OPTIMIZER_H
#define _QUERY_OPTIMIZER_H

#include "Schema.h"
#include "Catalog.h"
#include "ParseTree.h"
#include "RelOp.h"

#include <string>
#include <vector>
#include <map>

using namespace std;


// data structure used by the optimizer to compute join ordering
struct OptimizationTree {
	// list of tables joined up to this node

	vector<string> tables;
	// number of tuples in each of the tables (after selection predicates)
	vector<int> tuples;
	// number of tuples at this node
	int noTuples;

	// connections to children and parent
	OptimizationTree* parent;
	OptimizationTree* leftChild;
	OptimizationTree* rightChild;
};

class QueryOptimizer {
private:
	Catalog* catalog;
struct Node{
	//*node;
	Node *left;
	Node *right;	
};
struct Cost{
	int size;
	int cost;	
	Node *tree;
	string Tname;
	Schema schema;
};

map<int, struct Cost> CostMap;
map<int, struct Cost> CostMap2;
vector<string> Result;

public:
	QueryOptimizer(Catalog& _catalog);
	virtual ~QueryOptimizer();

	void Optimize(TableList* _tables, AndList* _predicate, OptimizationTree* _root);

	void Greedy(map<int, struct Cost> m, vector<Cost>tabs, AndList* _predicate);

	vector<string> GetMap()
	{
		return Result;
	}
};

#endif // _QUERY_OPTIMIZER_H

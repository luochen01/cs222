#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <map>
#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum
{
	MIN = 0, MAX, COUNT, SUM, AVG
} AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value
{
	AttrType type;          // type of value
	void *data;         // value

	Value()
	{
		type = TypeInt;
		data = NULL;
	}

	Value(AttrType type, void * data)
	{
		this->type = type;
		this->data = data;
	}

	bool operator<(const Value& rhs) const;

	//make a deep copy of value
	Value copy() const;

	void free();
};

struct Condition
{
	string lhsAttr;        // left-hand side attribute
	CompOp op;             // comparison operator
	bool bRhsIsAttr;   // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
	string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
	Value rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};

class Iterator
{
protected:
	bool end;

	// All the relational operators and access methods are iterators.
public:
	Iterator()
	{
		end = false;
	}

	virtual RC getNextTuple(void *data) = 0;

	virtual void getAttributes(vector<Attribute> &attrs) const = 0;

	virtual ~Iterator()
	{
	}
};

class TableScan: public Iterator
{
	// A wrapper inheriting Iterator over RM_ScanIterator
public:
	RelationManager &rm;
	RM_ScanIterator *iter;
	string tableName;
	vector<Attribute> attrs;
	vector<string> attrNames;
	RID rid;

	TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL) :
			rm(rm)
	{
		//Set members
		this->tableName = tableName;

		// Get Attributes from RM
		rm.getAttributes(tableName, attrs);

		// Get Attribute Names from RM
		unsigned i;
		for (i = 0; i < attrs.size(); ++i)
		{
			// convert to char *
			attrNames.push_back(attrs.at(i).name);
		}

		// Call RM scan to get an iterator
		iter = new RM_ScanIterator();
		rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

		// Set alias
		if (alias)
			this->tableName = alias;
	}
	;

	// Start a new iterator given the new compOp and value
	void setIterator()
	{
		iter->close();
		delete iter;
		iter = new RM_ScanIterator();
		rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
	}
	;

	RC getNextTuple(void *data)
	{
		return iter->getNextTuple(rid, data);
	}
	;

	void getAttributes(vector<Attribute> &attrs) const
	{
		attrs.clear();
		attrs = this->attrs;
		unsigned i;

		// For attribute in vector<Attribute>, name it as rel.attr
		for (i = 0; i < attrs.size(); ++i)
		{
			string tmp = tableName;
			tmp += ".";
			tmp += attrs.at(i).name;
			attrs.at(i).name = tmp;
		}
	}
	;

	~TableScan()
	{
		iter->close();
	}
	;
};

class IndexScan: public Iterator
{
	// A wrapper inheriting Iterator over IX_IndexScan
public:
	RelationManager &rm;
	RM_IndexScanIterator *iter;
	string tableName;
	string attrName;
	vector<Attribute> attrs;
	char key[PAGE_SIZE];
	RID rid;

	IndexScan(RelationManager &rm, const string &tableName, const string &attrName,
			const char *alias = NULL) :
			rm(rm)
	{
		// Set members
		this->tableName = tableName;
		this->attrName = attrName;

		// Get Attributes from RM
		rm.getAttributes(tableName, attrs);

		// Call rm indexScan to get iterator
		iter = new RM_IndexScanIterator();
		rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

		// Set alias
		if (alias)
			this->tableName = alias;
	}
	;

	// Start a new iterator given the new key range
	void setIterator(void* lowKey, void* highKey, bool lowKeyInclusive, bool highKeyInclusive)
	{
		iter->close();
		delete iter;
		iter = new RM_IndexScanIterator();
		rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive,
				*iter);
	}
	;

	RC getNextTuple(void *data)
	{
		int rc = iter->getNextEntry(rid, key);
		if (rc == 0)
		{
			rc = rm.readTuple(tableName.c_str(), rid, data);
		}
		return rc;
	}
	;

	void getAttributes(vector<Attribute> &attrs) const
	{
		attrs.clear();
		attrs = this->attrs;
		unsigned i;

		// For attribute in vector<Attribute>, name it as rel.attr
		for (i = 0; i < attrs.size(); ++i)
		{
			string tmp = tableName;
			tmp += ".";
			tmp += attrs.at(i).name;
			attrs.at(i).name = tmp;
		}
	}
	;

	~IndexScan()
	{
		iter->close();
	}
	;
};

class UnaryOperator: public Iterator
{
protected:
	Iterator* input;
	byte buffer[PAGE_SIZE];
	vector<Attribute> inputAttrs;
	vector<Attribute> outputAttrs;

public:
	UnaryOperator(Iterator* input);

	virtual ~UnaryOperator()
	{

	}

	virtual void getAttributes(vector<Attribute> &attrs) const;
};

class Filter: public UnaryOperator
{
protected:
	Condition condition;

	int leftIndex;
	int rightIndex;

	// Filter operator
public:
	Filter(Iterator *input,               // Iterator of input R
			const Condition &condition     // Selection condition
			);
	~Filter();

	RC getNextTuple(void *data);

};

class Project: public UnaryOperator
{
private:
	vector<int> attrIndexes;

	// Projection operator
public:
	Project(Iterator *input,                    // Iterator of input R
			const vector<string> &attrNames);

	// vector containing attribute names
	~Project();

	RC getNextTuple(void *data);

};

class Join: public Iterator
{
protected:
	byte leftBuffer[PAGE_SIZE];
	byte rightBuffer[PAGE_SIZE];

	Iterator* leftIn;
	Iterator* rightIn;
	Condition condition;

	vector<Attribute> leftAttrs;
	vector<Attribute> rightAttrs;
	vector<Attribute> outputAttrs;

	Join(Iterator* leftIn, Iterator* rightIn, const Condition& condition);

	virtual ~Join()
	{

	}

	void getAttributes(vector<Attribute> &attrs) const;

};

class BNLJoin: public Join
{
protected:
	unsigned numPages;
	// Block nested-loop join operator

	int leftIndex;
	int rightIndex;

	map<Value, vector<byte *>> leftTuples;
	int nextJoinPos;
	Value rightValue;
	vector<byte*>* matchedLeftTuples;

	RC loadLeftTuples();
	RC loadNextRightTuple();
	void clearLeftTuples();

public:
	BNLJoin(Iterator *leftIn,            // Iterator of input R
			TableScan *rightIn,           // TableScan Iterator of input S
			const Condition &condition,   // Join condition
			const unsigned numPages       // # of pages that can be loaded into memory,
										  //   i.e., memory block size (decided by the optimizer)
			);

	~BNLJoin()
	{

	}

	RC getNextTuple(void *data);

};

class INLJoin: public Join
{

	// Index nested-loop join operator
public:
	INLJoin(Iterator *leftIn,           // Iterator of input R
			IndexScan *rightIn,          // IndexScan Iterator of input S
			const Condition &condition   // Join condition
			);

	~INLJoin()
	{
	}

	RC getNextTuple(void *data);

};

// Optional for everyone. 10 extra-credit points
class GHJoin: public Join
{
protected:
	unsigned numPartitions;
	// Grace hash join operator
public:
	GHJoin(Iterator *leftIn,               // Iterator of input R
			Iterator *rightIn,               // Iterator of input S
			const Condition &condition,      // Join condition (CompOp is always EQ)
			const unsigned numPartitions // # of partitions for each relation (decided by the optimizer)
			);

	~GHJoin()
	{
	}

	RC getNextTuple(void *data);

};

class Aggregate: public UnaryOperator
{
private:
	bool groupBy;
	Attribute aggAttr;
	int aggAttrIndex;

	AggregateOp op;
	Attribute groupAttr;
	int groupAttrIndex;

	map<Value, float> aggResults;
	map<Value, float>::iterator aggResultsIter;

	//only used for avg
	map<Value, float> countResults;

	Value defaultValue;

	void initialize();

	void getAggregateValue(void * data, Value& groupAttrValue, float& aggAttrValue);

	void updateAggregateResult(const Value& groupAttrValue, float aggAttrValue);

	void updateAggregateResult(float& aggResult, float aggAttrValue, AggregateOp op);

	// Aggregation operator
public:
	// Mandatory
	// Basic aggregation
	Aggregate(Iterator *input,          // Iterator of input R
			Attribute aggAttr,        // The attribute over which we are computing an aggregate
			AggregateOp op            // Aggregate operation
			);
	// Optional for everyone: 5 extra-credit points
	// Group-based hash aggregation
	Aggregate(Iterator *input,             // Iterator of input R
			Attribute aggAttr,           // The attribute over which we are computing an aggregate
			Attribute groupAttr,         // The attribute over which we are grouping the tuples
			AggregateOp op              // Aggregate operation
			);

	~Aggregate()
	{
	}

	RC getNextTuple(void *data);

};

#endif

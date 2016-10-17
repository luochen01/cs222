#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <cstdarg>
#include <cassert>

#include "../rbf/rbfm.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator
#define TABLES_TABLE  "Tables"
#define COLUMNS_TABLE  "Columns"

typedef struct {
    int table_id;
    string table_name;
    string file_name;
} TableRecord;

typedef struct {
    int table_id;
    string column_name;
    AttrType column_type;
    int column_length;
    int column_position;
} ColumnRecord;

class DatumType
{
protected:
	bool _isNull;
	void * _val;
public:
	DatumType() :
			_isNull(true), _val(NULL)
	{

	}
	virtual ~DatumType()
	{
		if (_val != NULL)
		{
			delete _val;
			_val = NULL;
		}
	}

	bool isNull()
	{
		return _isNull;
	}

	virtual void getValue(void * val) = 0;

	virtual void setValue(const void * val)=0;

	void reset()
	{
		_isNull = true;
	}
};

class IntType: public DatumType
{
private:
public:
	IntType() :
			DatumType()
	{
		_val = new int;
	}
	IntType(int val) :
			DatumType()
	{
		_val = new int(val);
		_isNull = false;
	}

	void getValue(void * val);

	void setValue(const void * val);

};

class FloatType: public DatumType
{
private:

public:
	FloatType() :
			DatumType()
	{
		_val = new float;
	}
	FloatType(float val) :
			DatumType()
	{
		_val = new float(val);
		_isNull = false;
	}
	void getValue(void * val);

	void setValue(const void * val);
};

class StringType: public DatumType
{
private:

public:
	StringType() :
			DatumType()
	{
		_val = new string;
	}
	StringType(string val) :
			DatumType()
	{
		_val = new string(val);
		_isNull = false;
	}
	void getValue(void * val);

	void setValue(const void * val);
};

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator
{
public:
	RM_ScanIterator() :
			rbfm(NULL)
	{
	}
	;
	~RM_ScanIterator()
	{
	}
	;

	// "data" follows the same format as RelationManager::insertTuple()
	RC getNextTuple(RID &rid, void *data);
	RC close();
	RC init(const string &tableName);

private:
	friend class RelationManager;

	vector<Attribute> attrs;
	RBFM_ScanIterator rbfmsi;
	RecordBasedFileManager *rbfm;
	FileHandle fileHandle;
};

// Placeholder for Catalog class
class Catalog
{
public:
    Catalog() {};
    ~Catalog() {};
    
    RC createCatalog() { return -1; };
    RC deleteCatalog() { return -1; };

    int getTableID(const string &tableName) { return 0; };
    RC createTable(const string &tableName, const vector<Attribute> &attrs) { return -1; };
    RC deleteTable(const string &tableName) { return -1; };
    RC getAttributes(const string &tableName, const vector<Attribute> &attrs) { return -1; };
    
private:
    vector<TableRecord> tableCatalog;
    vector<ColumnRecord> columnCatalog;
};

// Relation Manager
class RelationManager
{
private:
	byte * tupleBuffer;

	void clearTuple(vector<DatumType *> & tuple);

public:
	static RelationManager* instance();

	RC createCatalog();

	RC deleteCatalog();

	RC createTable(const string &tableName, const vector<Attribute> &attrs);

	RC deleteTable(const string &tableName);

	RC getAttributes(const string &tableName, vector<Attribute> &attrs);

	RC insertTuple(const string &tableName, const void *data, RID &rid);

	RC deleteTuple(const string &tableName, const RID &rid);

	RC updateTuple(const string &tableName, const void *data, const RID &rid);

	RC readTuple(const string &tableName, const RID &rid, void *data);

	// Print a tuple that is passed to this utility method.
	// The format is the same as printRecord().
	RC printTuple(const vector<Attribute> &attrs, const void *data);

	RC readAttribute(const string &tableName, const RID &rid, const string &attributeName,
			void *data);

	// Scan returns an iterator to allow the caller to go through the results one by one.
	// Do not store entire results in the scan iterator.
	RC scan(const string &tableName, const string &conditionAttribute, const CompOp compOp, // comparison type such as "<" and "="
			const void *value,                    // used in the comparison
			const vector<string> &attributeNames, // a list of projected attributes
			RM_ScanIterator &rm_ScanIterator);

// Extra credit work (10 points)
public:
	RC addAttribute(const string &tableName, const Attribute &attr);

	RC dropAttribute(const string &tableName, const string &attributeName);

	// hard-coded info
	vector<Attribute> tableRecordDescriptor;
	vector<Attribute> columnRecordDescriptor;

protected:
	RelationManager();
	~RelationManager();

private:
	static RelationManager *_rm;

	// Added methods
	RC doInsertTuple(const string &tableName, const void *data, RID &rid);
    RC deleteTuples(const string &tableName, const vector<RID> &rids);

	void initializeCatalogAttrs();
	void formatRecord(void *record, int &recordSize, const vector<Attribute> &recordDescriptor,
			const vector<DatumType*> &attrValues);
	RC createCatalogTables(const vector<Attribute> &tableAttrs,
			const vector<Attribute> &columnAttrs);
	int getLastTableID();
	RC addTableToCatalog(const string &tableName, const vector<Attribute> &attrs);
	void parseIteratorData(vector<DatumType*> &parsedData, void *returnedData,
			const vector<Attribute> &recordDescriptor, const vector<string> &attrNames);
	bool getBit(unsigned char byte, unsigned pos);
	void setBit(unsigned char *byte, unsigned pos);
	void clearBit(unsigned char *byte, unsigned pos);
};

#endif

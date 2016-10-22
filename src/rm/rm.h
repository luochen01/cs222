#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <cstdarg>
#include <cassert>
#include <algorithm>

#include "../rbf/rbfm.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator
#define TABLES_TABLE "Tables"
#define COLUMNS_TABLE "Columns"

class MetaObject
{
protected:
	friend class RelationManager;
	RID rid;
public:

	virtual void writeTo(void * data)=0;

	virtual void readFrom(void * data)=0;

	virtual ~MetaObject()
	{

	}

};

class ColumnRecord: public MetaObject
{
public:
	int tableId;
	string columnName;
	AttrType columnType;
	int columnLength;
	int columnPosition;

	ColumnRecord() :
			tableId(0), columnType(TypeInt), columnLength(0), columnPosition(0)
	{
	}

	ColumnRecord(int tableId, Attribute attr, int position)
	{
		this->tableId = tableId;
		this->columnName = attr.name;
		this->columnType = attr.type;
		this->columnLength = attr.length;
		this->columnPosition = position;
	}

	~ColumnRecord()
	{
	}
	ColumnRecord(int tableId, string columnName, AttrType columnType, int columnLength,
			int columnPosition);

	Attribute toAttribute();

	void writeTo(void * data);

	void readFrom(void * data);
};

class TableRecord: public MetaObject
{
private:
	vector<ColumnRecord*> columns;
	friend class RelationManager;
	friend class Catalog;

public:
	int tableId;
	string tableName;
	string fileName;

	~TableRecord();

	void addColumn(ColumnRecord* column);

	vector<Attribute> getAttributes();

	vector<string> getAttributeNames();

	const vector<ColumnRecord*>& getColumns();

	void writeTo(void * data);

	void readFrom(void * data);
};

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator
{
public:
	RM_ScanIterator()
	{
		rbfm = RecordBasedFileManager::instance();
	}
	~RM_ScanIterator()
	{
	}

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
}
;

// Placeholder for Catalog class
class Catalog
{
protected:
	friend class RelationManager;
	Catalog();
	~Catalog();

	TableRecord* tablesTable;
	TableRecord* columnsTable;

public:
	bool isMetaTable(const string& name);

	static Catalog* instance();
	// Tested
	RC createCatalog();
	RC deleteCatalog();

	void addTable(TableRecord* table);
	void addColumn(ColumnRecord* column);
	void deleteTableByName(const string& tableName);

	TableRecord* getTablesTable();
	TableRecord* getColumnsTable();
	TableRecord* getTableById(unsigned id);
	TableRecord* getTableByName(const string& name);
	unsigned getNextTableId();

private:
	static Catalog *_ctlg;

	vector<TableRecord*> tables;

	unsigned nextTableId;

	RC loadCatalog();

	// hard-coded info
	vector<Attribute> tableRecordDescriptor;
	vector<Attribute> columnRecordDescriptor;

	RC getTableIDs(vector<int> &tids, vector<RID> &rids);
	RC getTableIDsFromFile(vector<int> &tids, vector<RID> &rids);
};

// Relation Manager
class RelationManager
{
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
	RC addAttribute(const string &tableName, const Attribute &attr);

	RC dropAttribute(const string &tableName, const string &attributeName);

protected:
	RelationManager();

	~RelationManager();

private:
	static RelationManager *_rm;

	//Added fields
	byte * tupleBuffer;
	Catalog * catalog;
	RecordBasedFileManager *rbfm;

	// Added methods
	RC loadCatalog();
	RC doInsertTuple(const string &tableName, const void *data, RID &rid);
	RC doDeleteTuple(const string &tableName, const RID& rid);
	RC doUpdateTuple(const string &tableName, const void * data, const RID& rid);

};

#endif

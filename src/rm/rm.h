#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <cstdarg>
#include <cassert>
#include <algorithm>

#include "catalog.h"
#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

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

// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator
{
private:
	IX_ScanIterator it;
	IXFileHandle fileHandle;

	friend class RelationManager;

public:
	RM_IndexScanIterator()
	{
	}
	;  	// Constructor
	~RM_IndexScanIterator()
	{
	}
	; 	// Destructor

	// "key" follows the same format as in IndexManager::insertEntry()
	RC getNextEntry(RID &rid, void *key)
	{
		return it.getNextEntry(rid, key);
	}
	;  	// Get next matching entry
	RC close()
	{
		it.close();
		return IndexManager::instance()->closeFile(fileHandle);
	}
	;

	RC init(const string& indexName)
	{
		return IndexManager::instance()->openFile(indexName, fileHandle);
	}
	// Terminate index scan
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

	RC createIndex(const string &tableName, const string &attributeName);

	RC destroyIndex(const string &tableName, const string &attributeName);

	// indexScan returns an iterator to allow the caller to go through qualified entries in index
	RC indexScan(const string &tableName, const string &attributeName, const void *lowKey,
			const void *highKey, bool lowKeyInclusive, bool highKeyInclusive,
			RM_IndexScanIterator &rm_IndexScanIterator);

protected:
	RelationManager();

	~RelationManager();

private:
	static RelationManager *_rm;

	//Added fields
	byte * tupleBuffer;

	Catalog * catalog;

	RecordBasedFileManager *rbfm;

	IndexManager* im;

	// Added methods
	RC loadCatalog();

	RC doInsertTuple(const string &tableName, const void *data, RID &rid);

	RC doDeleteTuple(const string &tableName, const RID& rid);

	RC doUpdateTuple(const string &tableName, const void * data, const RID& rid);

	RC insertIndexEntry(const string & indexName, const void * key, const RID& rid,
			const Attribute& attr);

	RC deleteIndexEntry(const string & indexName, const void * key, const RID& rid,
			const Attribute& attr);

	RC updateIndexEntry(const string & indexName, const void * oldKey, const void * newKey,
			const RID& rid, const Attribute& attr);

	void writeToColumnsTable(const vector<ColumnRecord*>& columns);

	string getIndexName(const string &tableName, const string & attributeName);

	RC getAttribute(const string& tableName, const string& attributeName, Attribute& attr);
};

#endif

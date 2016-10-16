
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

class DatumType {
public:
    DatumType()
        : _isNull(true) {};
    ~DatumType() {};

    bool isNull() { return _isNull; };
    void getValue(int &val) { val = _valInt; };
    void getValue(float &val) { val = _valFloat; };
    void getValue(string &val) { val = _valString; };
    void setValue(int val) { _valInt = val; _isNull = false; };
    void setValue(float val) { _valFloat = val; _isNull = false; };
    void setValue(string val) { _valString = val; _isNull = false; };

    int _valInt;
    float _valFloat;
    string _valString;
    
    bool _isNull;
};

class IntType : public DatumType {
public:
    IntType() : DatumType() {};
    IntType(int val) { _valInt = val; _isNull = false; };
};

class FloatType : public DatumType {
public:
    FloatType() : DatumType() {};
    FloatType(float val) { _valFloat = val; _isNull = false; };
};

class StringType : public DatumType {
public:
    StringType() : DatumType() {};
    StringType(string val) { _valString = val; _isNull = false; };
};

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

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

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
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
  void initializeCatalogAttrs();
  void formatRecord(void *record, int &recordSize, const vector<Attribute> &recordDescriptor, const vector<DatumType *> &attrValues);
  RC createCatalogTables(const vector<Attribute> &tableAttrs, const vector<Attribute> &columnAttrs);
  int getLastTableID();
  RC addTableToCatalog(const string &tableName, const vector<Attribute> &attrs);
  void parseIteratorData(vector<DatumType *> &parsedData, void *returnedData, const vector<Attribute> &recordDescriptor, const vector<string> &attrNames);
  bool getBit(unsigned char byte, unsigned pos);
  void setBit(unsigned char *byte, unsigned pos);
  void clearBit(unsigned char *byte, unsigned pos);
};

#endif

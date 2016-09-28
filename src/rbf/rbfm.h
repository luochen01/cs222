#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>

#include "../rbf/pfm.h"
#include "util.h"

using namespace std;

// Record ID
typedef struct
{
	unsigned pageNum;    // page number
	unsigned slotNum;    // slot number in the page
} RID;

// Attribute
typedef enum
{
	TypeInt = 0, TypeReal, TypeVarChar
} AttrType;

typedef unsigned AttrLength;

struct Attribute
{
	string name;     // attribute name
	AttrType type;     // attribute type
	AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum
{
	EQ_OP = 0, // no condition// =
	LT_OP,      // <
	LE_OP,      // <=
	GT_OP,      // >
	GE_OP,      // >=
	NE_OP,      // !=
	NO_OP       // no condition
} CompOp;

/********************************************************************************
 The scan iterator is NOT required to be implemented for the part 1 of the project
 ********************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator
{
public:
	RBFM_ScanIterator()
	{
	}
	;
	~RBFM_ScanIterator()
	{
	}
	;

	// Never keep the results in the memory. When getNextRecord() is called,
	// a satisfying record needs to be fetched from the file.
	// "data" follows the same format as RecordBasedFileManager::insertRecord().
	RC getNextRecord(RID &rid, void *data)
	{
		return RBFM_EOF;
	}
	;
	RC close()
	{
		return -1;
	}
	;
};

class RecordSlot
{
public:
	uint offset;
	uint size;
};

const int RECORD_SLOT_SIZE = sizeof(unsigned int) * 2;

const int RECORD_PAGE_HEADER_SIZE = sizeof(unsigned int) * 2;

class RecordBasedFileManager;

class RecordPage
{
private:
	friend class RecordBasedFileManager;

	void * data;

	//simple primitives to operate a record page.
	//warning: integrity checks are performed by callers.
	void readSlot(uint slotNum, RecordSlot& slot);

	void writeSlot(uint slotNum, const RecordSlot& slot);

	void appendSlot(const RecordSlot& slot);

	void readRecord(const RecordSlot& slot, const vector<Attribute> &recordDescriptor, void * data);

	void appendRecord(const RecordSlot& slot, const vector<Attribute>& recordDescritor,
			void * data);

	void flush();

	void setNull(void * data, uint attrNum, bool isNull);

	bool isNull(void * data, uint attrNum);

public:
	uint recordStart;
	uint slotSize;

	int slotEnd()
	{
		return RECORD_PAGE_HEADER_SIZE + RECORD_SLOT_SIZE * slotSize;
	}

	int slotOffset(int slotNum)
	{
		return RECORD_PAGE_HEADER_SIZE + RECORD_SLOT_SIZE * slotNum;
	}

	void reset(void * data);

	RecordPage() :
			data(NULL), recordStart(0), slotSize(0)
	{

	}

	~RecordPage()
	{

	}

};

class Record
{
private:
	friend class RecordPage;

	void * data;
	uint attributes;

	const int HEADER_LENGTH = (attributes + 1) * sizeof(uint);

	int attrOffset(uint attrNum)
	{
		return read<uint>(data, attrNum * sizeof(uint));
	}

	void attrOffset(uint attrNum, uint offset)
	{
		write<uint>(data, offset, attrNum * sizeof(uint));
	}

public:
	Record(void * data, uint attributes);
	~Record();

	bool nullAttr(uint attrNum);
	uint readAttr(uint attrNum, const Attribute& attrDescriptor, void * dest, uint destOffset);
	uint appendAttr(uint attrNum, const Attribute& attrDescriptor, void * src, uint srcOffset);

	static uint RecordSize(const vector<Attribute>& recordDescriptor, void * data);

};

class RecordBasedFileManager
{
private:
	uchar * pageBuffer;
	RecordPage curPage;
	uint curPageNum;

public:
	static RecordBasedFileManager* instance();

	RC createFile(const string &fileName);

	RC destroyFile(const string &fileName);

	RC openFile(const string &fileName, FileHandle &fileHandle);

	RC closeFile(FileHandle &fileHandle);

	//  Format of the data passed into the function is the following:
	//  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
	//  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
	//     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
	//     Each bit represents whether each field value is null or not.
	//     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
	//     If k-th bit from the left is set to 0, k-th field contains non-null values.
	//     If there are more than 8 fields, then you need to find the corresponding byte first,
	//     then find a corresponding bit inside that byte.
	//  2) Actual data is a concatenation of values of the attributes.
	//  3) For Int and Real: use 4 bytes to store the value;
	//     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
	//  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
	// For example, refer to the Q8 of Project 1 wiki page.
	RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
			const void *data, RID &rid);

	RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid,
			void *data);

	// This method will be mainly used for debugging/testing.
	// The format is as follows:
	// field1-name: field1-value  field2-name: field2-value ... \n
	// (e.g., age: 24  height: 6.1  salary: 9000
	//        age: NULL  height: 7.5  salary: 7500)
	RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

	/******************************************************************************************************************************************************************
	 IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for the part 1 of the project
	 ******************************************************************************************************************************************************************/
	RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
			const RID &rid);

	// Assume the RID does not change after an update
	RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
			const void *data, const RID &rid);

	RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
			const RID &rid, const string &attributeName, void *data);

	// Scan returns an iterator to allow the caller to go through the results one by one.
	RC scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
			const string &conditionAttribute, const CompOp compOp, // comparision type such as "<" and "="
			const void *value,                    // used in the comparison
			const vector<string> &attributeNames, // a list of projected attributes
			RBFM_ScanIterator &rbfm_ScanIterator);

public:

protected:
	RecordBasedFileManager();
	~RecordBasedFileManager();

private:
	static RecordBasedFileManager *_rbf_manager;
};

#endif

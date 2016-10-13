#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>

#include "../rbf/pfm.h"
//#include "util.h"

using namespace std;

struct Attribute;

int attributeIndex(const vector<Attribute>& recordDescriptor, const string& attributeName);

bool equals(float left, float right);

ushort copyAttributeData(void * to, ushort toOffset, const Attribute& attribute, const void * from,
		ushort fromOffset);

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

class RecordBasedFileManager;

class RecordPage;

/**
 * I used an offset-table based format to store each record. Specially, a record with N fields is stored as follows:
 *  offset1, offset2, ..., offsetN, field1, field2, ..., fieldN
 *
 *  An offset table containing N offsets (each takes 2 bytes) is placed at the beginning of the record,
 *  where the ith offset indiciates the end offset of the ith field.
 *  Thus, the start offset of the ith field is simply the end address of the previous field, i.e., offset i-1. In particular, for the first field, its start offset is just the end offset of the offset table, i.e., offset0 = N * 2.
 *  If a field is NULL, then its corresponding end offset is just set as its start offset.
 *  To access the ith field, one first needs to access the i-1th and ith item in the offset table (note offset0 can be implicitly computed) to get the start and end offset of that field.
 *  Then, the ith field can be retrieved by accessing record data between offseti-1 and offseti.
 *  Note if offseti-1 equals to offseti, which indicates the field is NULL, then the memory read operation can be simply skipped.
 *
 *
 *
 */

class Record
{
private:
	byte * data;

	const vector<Attribute> * pRecordDescriptor;

	ushort recordSize;

public:
	Record();

	~Record();

	ushort getRecordSize()
	{
		return recordSize;
	}

	void reset(void * data, const vector<Attribute>& recordDescriptor, ushort recordSize);

	ushort recordHeaderSize();

	ushort attributeStartOffset(ushort attrNum);

	ushort attributeEndOffset(ushort attrNum);

	void attributeEndOffset(ushort attrNum, ushort offset);

	bool isAttributeNull(ushort attrNum);

	ushort attributeSize(ushort attrNum);

	void * attribute(ushort attrNum);

	void insertAttribute(ushort attrNum, ushort size);
};

const ushort RECORD_SLOT_SIZE = sizeof(ushort) * 2;

class RecordSlot
{
public:
	ushort offset;
	ushort size;
};

const ushort RECORD_PAGE_HEADER_SIZE = sizeof(ushort) * 2;

const ushort OVERFLOW_MARKER_SIZE = sizeof(unsigned) * 2;

class RecordPage
{
private:

	const FileHandle* pFileHandle;

	unsigned pageNum;

	bool samePage(FileHandle& fileHandle, unsigned pageNum);

public:

	byte * data;

	void setOverflowRecord(unsigned slotNum, unsigned overflowPageNum, unsigned overflowSlotNum);

	void writeOverflowMarker(RecordSlot& slot, unsigned overflowPageNum, unsigned overflowSlotNum);

	void readOverflowMarker(RecordSlot& slot, unsigned& overflowPageNum, unsigned& overflowSlotNum);

	void increaseSpace(ushort slotNum, ushort offset);

	void readRecordSlot(ushort slotNum, RecordSlot& slot);

	void writeRecordSlot(ushort slotNum, const RecordSlot& slot);

	void deleteRecordSlot(ushort slotNum);

	void markRecordSlot(ushort slotNum);

	void increaseRecordSlot(ushort slotNum, ushort offset);

	void appendRecordSlot(const RecordSlot& slot);

	void deleteRecord(ushort slotNum);

	void readRecord(const RecordSlot& slot, const vector<Attribute>& recordDescriptor,
			Record& record);

	void insertRecord(const vector<Attribute>& recordDescriptor, Record& record, ushort recordSize,
			unsigned& slotNum);

	void updateRecord(const vector<Attribute>& recordDescriptor, Record& record,
			ushort newRecordSize, unsigned slotNum);

	ushort locateRecordSlot();

	ushort slotEnd();

	ushort slotOffset(ushort slotNum);

	ushort emptySpace();

	ushort recordStart();

	void recordStart(ushort value);

	ushort slotSize();

	void slotSize(ushort value);

	RecordPage();

	~RecordPage();

	RC readPage(FileHandle& fileHandle, unsigned pageNum);

	void reset(FileHandle& fileHandle, unsigned pageNum);

	void invalidate(FileHandle& fileHandle, unsigned pageNum);

	void invalidate();

};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum
{
	EQ_OP = 0,	// =
	LT_OP,	// <
	LE_OP,	// <=
	GT_OP,	// >
	GE_OP,	// >=
	NE_OP,	// !=
	NO_OP	// no condition
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
private:
	friend class RecordBasedFileManager;

	RecordPage curPage;
	unsigned curPageNum;
	ushort curSlotNum;
	FileHandle* pFileHandle;

	const vector<Attribute>* pRecordDescriptor;
	unsigned conditionNum;
	vector<unsigned> projectNums;
	CompOp compOp;
	const void *conditionValue;

	bool end;

	void init();

	void readNextPage();

	RC getNextRecord(Record& record);

	RC getNextRecordWithinPage(Record& record);

	bool select(const void *conditionData, const void *conditionValue, CompOp compOp,
			const Attribute& attributeDescriptor);

	bool selectInt(const void * left, const void * right, CompOp compOp);

	bool selectReal(const void * left, const void * right, CompOp compOp);
public:
	RBFM_ScanIterator();

	~RBFM_ScanIterator();

// Never keep the results in the memory. When getNextRecord() is called,
// a satisfying record needs to be fetched from the file.
// "data" follows the same format as RecordBasedFileManager::insertRecord().
	RC getNextRecord(RID &rid, void *data);

	RC close();
};

class RecordBasedFileManager
{
private:

	RecordPage curPage;

	RecordPage overflowPage;

	unsigned locatePage(RecordPage& page, ushort recordSize, FileHandle& fileHandle);

	void fillRecord(const vector<Attribute>& recordDescriptor, Record& record, const void *data);

	void fillData(const vector<Attribute> & recordDescriptor, Record& record, void * data);

	void updateRecordWithinPage(FileHandle& fileHandle, RecordPage& page,
			const vector<Attribute>& recordDescriptor, unsigned pageNum, unsigned slotNum,
			unsigned newRecordSize, const void * data);

public:
	static RecordBasedFileManager* instance();

	static ushort RecordSize(const vector<Attribute>& recordDescriptor, const void * src);

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
	RC printRecord(const vector<Attribute> & recordDescriptor, const void *data);

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

protected:
	RecordBasedFileManager();
	~RecordBasedFileManager();

private:
	static RecordBasedFileManager *_rbf_manager;
};

#endif

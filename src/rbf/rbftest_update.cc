#include <fstream>
#include <iostream>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h> 
#include <string.h>
#include <stdexcept>
#include <stdio.h> 

#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

using namespace std;

void *record = malloc(2000);
void *returnedData = malloc(2000);
vector<Attribute> recordDescriptor;
unsigned char *nullsIndicator = NULL;
FileHandle fileHandle;

void readRecord(RecordBasedFileManager *rbfm, const RID& rid, string str)
{
	int recordSize;
	prepareRecord(recordDescriptor.size(), nullsIndicator, str.length(), str, 25, 177.8, 6200,
			record, &recordSize);

	RC rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);
	assert(rc == success && "Reading a record should not fail.");

	// Compare whether the two memory blocks are the same
	assert(memcmp(record, returnedData, recordSize) == 0 && "Returned Data should be the same");
}

void insertRecord(RecordBasedFileManager *rbfm, RID& rid, string str)
{
	int recordSize;
	prepareRecord(recordDescriptor.size(), nullsIndicator, str.length(), str, 25, 177.8, 6200,
			record, &recordSize);

	RC rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
	assert(rc == success && "Inserting a record should not fail.");

}

void updateRecord(RecordBasedFileManager *rbfm, RID& rid, string str)
{
	int recordSize;
	prepareRecord(recordDescriptor.size(), nullsIndicator, str.length(), str, 25, 177.8, 6200,
			record, &recordSize);

	RC rc = rbfm->updateRecord(fileHandle, recordDescriptor, record, rid);
	assert(rc == success && "Updating a record should not fail.");

}

int RBFTest_Update(RecordBasedFileManager *rbfm)
{
	// Functions tested
	// 1. Create Record-Based File
	// 2. Open Record-Based File
	// 3. Insert Record
	// 4. Read Record
	// 5. Close Record-Based File
	// 6. Destroy Record-Based File
	cout << endl << "***** In RBF Test Case Update *****" << endl;

	RC rc;
	string fileName = "test_update";

	// Create a file named "testlc1"
	rc = rbfm->createFile(fileName);
	assert(rc == success && "Creating the file should not fail.");

	rc = createFileShouldSucceed(fileName);
	assert(rc == success && "Creating the file should not fail.");

	// Open the file "testlc1"

	rc = rbfm->openFile(fileName, fileHandle);
	assert(rc == success && "Opening the file should not fail.");

	RID rid;

	createRecordDescriptor(recordDescriptor);

	string longstr;
	for (int i = 0; i < 1000; i++)
	{
		longstr.push_back('a');
	}

	string shortstr;
	for (int i = 0; i < 10; i++)
	{
		shortstr.push_back('s');
	}

	string midstr;
	for (int i = 0; i < 100; i++)
	{
		midstr.push_back('m');
	}

	// Initialize a NULL field indicator
	int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
	nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
	memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

	insertRecord(rbfm, rid, longstr);
	insertRecord(rbfm, rid, shortstr);

	RID shortRID = rid;

	insertRecord(rbfm, rid, midstr);

	RID midRID = rid;

	insertRecord(rbfm, rid, longstr);

	updateRecord(rbfm, shortRID, midstr);

	readRecord(rbfm, shortRID, midstr);

	insertRecord(rbfm, rid, longstr);
	insertRecord(rbfm, rid, longstr);

	readRecord(rbfm, midRID, midstr);

	updateRecord(rbfm, shortRID, longstr);

	readRecord(rbfm, shortRID, longstr);

	readRecord(rbfm, midRID, midstr);

	rbfm->deleteRecord(fileHandle, recordDescriptor, shortRID);

	rc = rbfm->readRecord(fileHandle, recordDescriptor, shortRID, returnedData);

	assert(rc != success && "Read a deleted record should not success.");

	// Close the file "testlc1"
	rc = rbfm->closeFile(fileHandle);
	assert(rc == success && "Closing the file should not fail.");

	// Destroy the file
	rc = rbfm->destroyFile(fileName);
	assert(rc == success && "Destroying the file should not fail.");

	rc = destroyFileShouldSucceed(fileName);
	assert(rc == success && "Destroying the file should not fail.");

	free(record);
	free(returnedData);

	cout << "RBF Test Case Update Finished! The result will be examined." << endl << endl;

	return 0;
}

int main()
{
// To test the functionality of the record-based file manager
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

	remove("test_update");

	RC rcmain = RBFTest_Update(rbfm);
	return rcmain;
}

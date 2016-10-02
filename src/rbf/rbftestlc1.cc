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

int RBFTest_Lc1(RecordBasedFileManager *rbfm)
{
	// Functions tested
	// 1. Create Record-Based File
	// 2. Open Record-Based File
	// 3. Insert Record
	// 4. Read Record
	// 5. Close Record-Based File
	// 6. Destroy Record-Based File
	cout << endl << "***** In RBF Test Case LC 1 *****" << endl;

	RC rc;
	string fileName = "testlc1";

	// Create a file named "testlc1"
	rc = rbfm->createFile(fileName);
	assert(rc == success && "Creating the file should not fail.");

	rc = createFileShouldSucceed(fileName);
	assert(rc == success && "Creating the file should not fail.");

	// Open the file "testlc1"
	FileHandle fileHandle;
	rc = rbfm->openFile(fileName, fileHandle);
	assert(rc == success && "Opening the file should not fail.");

	RID rid;
	int recordSize = 0;
	void *record = malloc(100);
	void *returnedData = malloc(100);

	vector<Attribute> recordDescriptor;
	createRecordDescriptor(recordDescriptor);

	// Initialize a NULL field indicator
	int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
	unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
	memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

	// Insert a record into a file and print the record
	prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "", 25, 177.8, 6200, record,
			&recordSize);
	cout << endl << "Inserting Data:" << endl;
	rbfm->printRecord(recordDescriptor, record);

	rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
	assert(rc == success && "Inserting a record should not fail.");

	// Given the rid, read the record from file
	rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);
	assert(rc == success && "Reading a record should not fail.");

	cout << endl << "Returned Data:" << endl;
	rbfm->printRecord(recordDescriptor, returnedData);

	// Compare whether the two memory blocks are the same
	if (memcmp(record, returnedData, recordSize) != 0)
	{
		cout << "[FAIL] Test Case LC1 Failed!" << endl << endl;
		free(record);
		free(returnedData);
		return -1;
	}

	cout << endl;

	memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

	// Insert a record into a file and print the record

	nullsIndicator[0] = 128;
	prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "", 25, 177.8, 6200, record,
			&recordSize);
	cout << endl << "Inserting Data:" << endl;
	rbfm->printRecord(recordDescriptor, record);

	rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
	assert(rc == success && "Inserting a record should not fail.");

	// Given the rid, read the record from file
	rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);
	assert(rc == success && "Reading a record should not fail.");

	cout << endl << "Returned Data:" << endl;
	rbfm->printRecord(recordDescriptor, returnedData);

	// Compare whether the two memory blocks are the same
	if (memcmp(record, returnedData, recordSize) != 0)
	{
		cout << "[FAIL] Test Case LC1 Failed!" << endl << endl;
		free(record);
		free(returnedData);
		return -1;
	}

	cout << endl;

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

	cout << "RBF Test Case LC1 Finished! The result will be examined." << endl << endl;

	return 0;
}

int main()
{
// To test the functionality of the record-based file manager
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

	remove("testlc1");

	RC rcmain = RBFTest_Lc1(rbfm);
	return rcmain;
}

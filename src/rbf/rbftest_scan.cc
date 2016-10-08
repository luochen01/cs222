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

int RBFTest_Scan(RecordBasedFileManager *rbfm, vector<RID> &rids, vector<int> &sizes)
{
	// Functions tested
	// 1. Create Record-Based File
	// 2. Open Record-Based File
	// 3. Insert Multiple Records
	// 4. Close Record-Based File
	cout << endl << "***** In RBF Test Case Scan *****" << endl;

	RC rc;
	string fileName = "test_scan";

	// Create a file named "test scan"
	rc = rbfm->createFile(fileName);
	assert(rc == success && "Creating the file should not fail.");

	rc = createFileShouldSucceed(fileName);
	assert(rc == success && "Creating the file failed.");

	// Open the file "test scan"
	FileHandle fileHandle;
	rc = rbfm->openFile(fileName, fileHandle);
	assert(rc == success && "Opening the file should not fail.");

	RID rid;
	void *record = malloc(1000);
	int numRecords = 1000;

	vector<Attribute> recordDescriptor;
	createLargeRecordDescriptor(recordDescriptor);

	for (unsigned i = 0; i < recordDescriptor.size(); i++)
	{
		cout << "Attr Name: " << recordDescriptor[i].name << " Attr Type: "
				<< (AttrType) recordDescriptor[i].type << " Attr Len: "
				<< recordDescriptor[i].length << endl;
	}
	cout << endl;

	// NULL field indicator
	int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
	unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
	memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

	// Insert 1000 records into file
	for (int i = 0; i < numRecords; i++)
	{
		// Test insert Record
		int size = 0;
		memset(record, 0, 1000);
		prepareLargeRecord(recordDescriptor.size(), nullsIndicator, i, record, &size);

		rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
		assert(rc == success && "Inserting a record should not fail.");

		rids.push_back(rid);
		sizes.push_back(size);
	}
	// Close the file "test_scan"
	rc = rbfm->closeFile(fileHandle);
	assert(rc == success && "Closing the file should not fail.");

	rc = rbfm->openFile(fileName, fileHandle);

	int value = 28;
	vector<string> names;
	names.push_back(recordDescriptor[0].name);
	names.push_back(recordDescriptor[1].name);
	names.push_back(recordDescriptor[2].name);
	names.push_back(recordDescriptor[10].name);
	RBFM_ScanIterator iterator;
	rbfm->scan(fileHandle, recordDescriptor, recordDescriptor[1].name, LE_OP, &value, names,
			iterator);

	vector<Attribute> outputDescriptor;
	outputDescriptor.push_back(recordDescriptor[0]);
	outputDescriptor.push_back(recordDescriptor[1]);
	outputDescriptor.push_back(recordDescriptor[2]);
	outputDescriptor.push_back(recordDescriptor[10]);

	while (iterator.getNextRecord(rid, record) != RBFM_EOF)
	{
		rbfm->printRecord(outputDescriptor, record);
	}

	free(record);

	cout << "RBF Test Case Scan Finished! The result will be examined." << endl << endl;

	return 0;
}

int main()
{
	// To test the functionality of the record-based file manager
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

	remove("test_scan");

	vector<RID> rids;
	vector<int> sizes;
	RC rcmain = RBFTest_Scan(rbfm, rids, sizes);
	return rcmain;
}

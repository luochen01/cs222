#include "rm.h"
#include <iostream>

using namespace std;

RelationManager* RelationManager::_rm = 0;

void IntType::getValue(void * val)
{
	*(int *) val = *(int *) _val;
}

void IntType::setValue(const void * val)
{
	*(int *) _val = *(int *) val;
	_isNull = false;
}

void FloatType::getValue(void * val)
{
	*(float *) val = *(float *) _val;
}

void FloatType::setValue(const void * val)
{
	*(float *) _val = *(float *) val;
	_isNull = false;
}

void StringType::getValue(void * val)
{
	*(string *) val = *(string *) _val;
}

void StringType::setValue(const void * val)
{
	*(string *) _val = *(string *) val;
	_isNull = false;
}

void RelationManager::clearTuple(vector<DatumType*> & tuple)
{
	for (DatumType * type : tuple)
	{
		delete type;
	}
}

RelationManager* RelationManager::instance()
{
	if (!_rm)
		_rm = new RelationManager();

	return _rm;
}

RelationManager::RelationManager()
{
	tupleBuffer = new byte[PAGE_SIZE];

	initializeCatalogAttrs();
}

RelationManager::~RelationManager()
{
	delete[] tupleBuffer;
}

// Tested
RC RelationManager::createCatalog()
{
	return createCatalogTables(tableRecordDescriptor, columnRecordDescriptor);
}

RC RelationManager::deleteCatalog()
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	rbfm->destroyFile(COLUMNS_TABLE);
	rbfm->destroyFile(TABLES_TABLE);
	// TODO
	return -1;
}

// Tested
RC RelationManager::addTableToCatalog(const string &tableName, const vector<Attribute> &attrs)
{
	RID rid;
	int tableID = 0;

	if (tableName == TABLES_TABLE)
	{
		tableID = 1;
	}
	else if (tableName == COLUMNS_TABLE)
	{
		tableID = 2;
	}
	else
	{
		tableID = getLastTableID();
		if (tableID <= 0)
		{
			return -1;
		}
		tableID++;
	}

	int recordSize = 0;

	vector<DatumType*> tableRecordValues;

	tableRecordValues.push_back(new IntType(tableID));
	tableRecordValues.push_back(new StringType(tableName));
	tableRecordValues.push_back(new StringType(tableName));

	formatRecord(tupleBuffer, recordSize, tableRecordDescriptor, tableRecordValues);
	doInsertTuple(TABLES_TABLE, tupleBuffer, rid);

	clearTuple(tableRecordValues);

	void *tmp = malloc(1024);
	readTuple(TABLES_TABLE, rid, tmp);
	printTuple(tableRecordDescriptor, tmp);
	free(tmp);

	for (int i = 0; i < attrs.size(); i++)
	{
		vector<DatumType*> columnRecordValues;
		columnRecordValues.push_back(new IntType(tableID));
		columnRecordValues.push_back(new StringType(attrs[i].name));
		columnRecordValues.push_back(new IntType(attrs[i].type));
		columnRecordValues.push_back(new IntType(attrs[i].length));
		columnRecordValues.push_back(new IntType(i + 1));    // column-position, starts with 1
		formatRecord(tupleBuffer, recordSize, columnRecordDescriptor, columnRecordValues);
		doInsertTuple(COLUMNS_TABLE, tupleBuffer, rid);

		void *tmp = malloc(330);
		readTuple(COLUMNS_TABLE, rid, tmp);
		printTuple(columnRecordDescriptor, tmp);
		free(tmp);

		clearTuple(columnRecordValues);
	}

	return 0;
}

// FIXME
int RelationManager::getLastTableID()
{
	RID rid;
	int maxTableID = 0;
	int tid = 0;
	RM_ScanIterator rmsi;
	vector<string> tableAttrNames;
	vector<DatumType*> tableParsedData;
	tableAttrNames.push_back("table-id");
	tableParsedData.push_back(new IntType());

	vector<Attribute> parsedDescriptor;
	parsedDescriptor.push_back(tableRecordDescriptor[0]);

	if (scan(TABLES_TABLE, "", NO_OP, NULL, tableAttrNames, rmsi) != 0)
		return -1;
	while (rmsi.getNextTuple(rid, tupleBuffer) != RM_EOF)
	{
		RecordBasedFileManager::instance()->printRecord(parsedDescriptor, tupleBuffer);
		parseIteratorData(tableParsedData, tupleBuffer, tableRecordDescriptor, tableAttrNames);
		if (tableParsedData[0]->isNull())
		{
			return -1;
		}
		tableParsedData[0]->getValue(&tid);
		if (maxTableID < tid)
			maxTableID = tid;
		// FIXME: only use the last matched table id
	}

	clearTuple(tableParsedData);
	rmsi.close();
	return maxTableID;
}

RC RelationManager::createCatalogTables(const vector<Attribute> &tableAttrs,
		const vector<Attribute> &columnAttrs)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	if (rbfm->createFile(TABLES_TABLE) != 0 || rbfm->createFile(COLUMNS_TABLE) != 0)
		return -1;

	addTableToCatalog(TABLES_TABLE, tableAttrs);
	addTableToCatalog(COLUMNS_TABLE, columnAttrs);
	return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    if (tableName == TABLES_TABLE || tableName == COLUMNS_TABLE) return -1;

	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	if (addTableToCatalog(tableName, attrs) != 0)
		return -1;

	if (rbfm->createFile(tableName) != 0)
		return -1;
	return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
    if (tableName == TABLES_TABLE || tableName == COLUMNS_TABLE)
        return -1;
	
    // Delete table
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    rbfm->destroyFile(tableName);
	
    // Get table id
    RID rid;
	int tid;
	void *returnedData = malloc(PAGE_SIZE);
	RM_ScanIterator rmsi;
	vector<string> tableAttrNames;
	vector<DatumType *> tableParsedData;
	tableAttrNames.push_back("table-id");
	tableParsedData.push_back(new IntType());

	if (scan(TABLES_TABLE, "table-name", EQ_OP, (void *) &tableName, tableAttrNames, rmsi)
			!= 0)
		return -1;
	while (rmsi.getNextTuple(rid, returnedData) != RM_EOF)
	{
		parseIteratorData(tableParsedData, returnedData, tableRecordDescriptor, tableAttrNames);
		tableParsedData[0]->getValue(&tid);
		if (tableParsedData[0]->isNull())
		{
			clearTuple(tableParsedData);
			return -1;
		}
	}
	rmsi.close();
	clearTuple(tableParsedData);
    
    // delete column info from COLUMNS_TABLE
	
    Attribute attr;
	vector<string> columnAttrNames;
	columnAttrNames.push_back("column-name");
	columnAttrNames.push_back("column-type");
	columnAttrNames.push_back("column-length");
	columnAttrNames.push_back("column-position");

	vector<DatumType*> columnParsedData;
	columnParsedData.push_back(new StringType());
	columnParsedData.push_back(new IntType());
	columnParsedData.push_back(new IntType());
	columnParsedData.push_back(new IntType());

	// FIXME
	if (scan(COLUMNS_TABLE, "table-id", EQ_OP, (void *) &tid, columnAttrNames, rmsi) != 0)
		return -1;
	while (rmsi.getNextTuple(rid, returnedData) != RM_EOF)
	{
		parseIteratorData(columnParsedData, returnedData, columnRecordDescriptor, columnAttrNames);
		string name;
		int type;
		int length;
		columnParsedData[0]->getValue(&name);
		columnParsedData[1]->getValue(&type);
		columnParsedData[2]->getValue(&length);
		attr.name = name;
		attr.type = (AttrType) type;
		attr.length = length;
        // FIXME: delete attribute record
	}
	clearTuple(columnParsedData);
	rmsi.close();
    
    // delete table info from TABLES_TABLE
	return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	if (tableName == TABLES_TABLE)
	{
		attrs = tableRecordDescriptor;
		return 0;
	}
	if (tableName == COLUMNS_TABLE)
	{
		attrs = columnRecordDescriptor;
		return 0;
	}

	RID rid;
	int tid;
	void *returnedData = malloc(PAGE_SIZE);
	RM_ScanIterator rmsi;
	vector<string> tableAttrNames;
	vector<DatumType *> tableParsedData;
	tableAttrNames.push_back("table-id");
	tableParsedData.push_back(new IntType());

    //int compareValueSize = tableName.size();
    //char *compareValue = (char *) malloc(compareValueSize + 4);
    //memcpy(compareValue, &compareValueSize, 4);
    //memcpy(compareValue+4, tableName.c_str(), compareValueSize);
	// FIXME: string format
	if (scan(TABLES_TABLE, "table-name", EQ_OP, (void *) &tableName, tableAttrNames, rmsi)
			!= 0)
		return -1;
	while (rmsi.getNextTuple(rid, returnedData) != RM_EOF)
	{
		parseIteratorData(tableParsedData, returnedData, tableRecordDescriptor, tableAttrNames);
		tableParsedData[0]->getValue(&tid);
		if (tableParsedData[0]->isNull())
		{
			clearTuple(tableParsedData);
			return -1;
		}
		// FIXME: only use the last matched table id
	}
	rmsi.close();
    //free(compareValue);
	clearTuple(tableParsedData);

	Attribute attr;
	vector<string> columnAttrNames;
	columnAttrNames.push_back("column-name");
	columnAttrNames.push_back("column-type");
	columnAttrNames.push_back("column-length");
	columnAttrNames.push_back("column-position");

	vector<DatumType*> columnParsedData;
	columnParsedData.push_back(new StringType());
	columnParsedData.push_back(new IntType());
	columnParsedData.push_back(new IntType());
	columnParsedData.push_back(new IntType());

	// FIXME
	if (scan(COLUMNS_TABLE, "table-id", EQ_OP, (void *) &tid, columnAttrNames, rmsi) != 0)
		return -1;
	while (rmsi.getNextTuple(rid, returnedData) != RM_EOF)
	{
		parseIteratorData(columnParsedData, returnedData, columnRecordDescriptor, columnAttrNames);
		string name;
		int type;
		int length;
		columnParsedData[0]->getValue(&name);
		columnParsedData[1]->getValue(&type);
		columnParsedData[2]->getValue(&length);
		attr.name = name;
		attr.type = (AttrType) type;
		attr.length = length;
		attrs.push_back(attr);
	}
	clearTuple(columnParsedData);
	rmsi.close();
	return 0;
}

RC RelationManager::doInsertTuple(const string &tableName, const void *data, RID &rid)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != 0)
		return -1;

	vector<Attribute> attrs;
	getAttributes(tableName, attrs);
	if (rbfm->insertRecord(fileHandle, attrs, data, rid) != 0)
		return -1;
	rbfm->closeFile(fileHandle);
	return 0;
}

// Tested
RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    if (tableName == TABLES_TABLE || tableName == COLUMNS_TABLE) 
        return -1;

    return doInsertTuple(tableName, data, rid);
}

// FIXME
RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	vector<Attribute> attrs;
	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != 0)
		return -1;

	getAttributes(tableName, attrs);
	int result = rbfm->deleteRecord(fileHandle, attrs, rid);
	rbfm->closeFile(fileHandle);
	return result;
}

// FIXME
RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	vector<Attribute> attrs;
	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != 0)
		return -1;

	getAttributes(tableName, attrs);
	int result = rbfm->updateRecord(fileHandle, attrs, data, rid);
	rbfm->closeFile(fileHandle);
	return result;
}

// Tested
RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	vector<Attribute> attrs;
	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != 0)
		return -1;

	getAttributes(tableName, attrs);
	int result = rbfm->readRecord(fileHandle, attrs, rid, data);
	rbfm->closeFile(fileHandle);
	return result;
}

// Tested
RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid,
		const string &attributeName, void *data)
{
	return -1;
}

RC RelationManager::scan(const string &tableName, const string &conditionAttribute,
		const CompOp compOp, const void *value, const vector<string> &attributeNames,
		RM_ScanIterator &rm_ScanIterator)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	if (rm_ScanIterator.init(tableName) != 0)
		return -1;
	getAttributes(tableName, rm_ScanIterator.attrs);
	if (rbfm->scan(rm_ScanIterator.fileHandle, rm_ScanIterator.attrs, conditionAttribute, compOp,
			value, attributeNames, rm_ScanIterator.rbfmsi) != 0)
	{
		rm_ScanIterator.close();
		return -1;
	}
	return 0;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
	return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
	return -1;
}

void RelationManager::initializeCatalogAttrs()
{
	Attribute attr;
	attr.name = "table-id";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	tableRecordDescriptor.push_back(attr);

	attr.name = "table-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 50;
	tableRecordDescriptor.push_back(attr);

	attr.name = "file-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 50;
	tableRecordDescriptor.push_back(attr);

	attr.name = "table-id";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	columnRecordDescriptor.push_back(attr);

	attr.name = "column-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 50;
	columnRecordDescriptor.push_back(attr);

	attr.name = "column-type";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	columnRecordDescriptor.push_back(attr);

	attr.name = "column-length";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	columnRecordDescriptor.push_back(attr);

	attr.name = "column-position";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	columnRecordDescriptor.push_back(attr);
}

void RelationManager::formatRecord(void *record, int &recordSize,
		const vector<Attribute> &recordDescriptor, const vector<DatumType *> &attrValues)
{
	unsigned numOfAttr = recordDescriptor.size();
	unsigned lenOfIndr = (numOfAttr % 8 == 0) ? numOfAttr / 8 : numOfAttr / 8 + 1;

	unsigned offset = lenOfIndr;

	for (unsigned i = 0; i < numOfAttr; i++)
	{
		Attribute attr = recordDescriptor[i];
		bool isNull = attrValues[i]->isNull();
		if (isNull)
		{
			setAttrNull(record, i, true);
		}
		else
		{
			setAttrNull(record, i, false);
			switch (attr.type)
			{
			case TypeInt:
			{
				int valInt = 0;
				attrValues[i]->getValue(&valInt);
				write(record, valInt, offset);
				offset += 4;
				break;
			}
			case TypeReal:
			{
				float valFloat = 0.0;
				attrValues[i]->getValue(&valFloat);
				write(record, valFloat, offset);
				offset += 4;
				break;
			}
			case TypeVarChar:
			{
				string valString;
				attrValues[i]->getValue(&valString);
				write(record, valString.size(), offset);
				offset += 4;
				writeBuffer(record, offset, valString.c_str(), 0, valString.size());
				offset += valString.size();
				break;
			}
			}
		}
	}
	recordSize = offset;
}

// Tested
void RelationManager::parseIteratorData(vector<DatumType *> &parsedData, void *returnedData,
		const vector<Attribute> &recordDescriptor, const vector<string> &attrNames)
{
	vector<Attribute> parsedDescriptor;
	for (Attribute attr : recordDescriptor)
	{
		for (string name : attrNames)
		{
			if (attr.name == name)
			{
				parsedDescriptor.push_back(attr);
			}
		}
	}

	// parse returnedData using parsedDescriptor, then fill the parsedData vec
	unsigned numOfAttr = parsedDescriptor.size();
	unsigned lenOfIndr = (numOfAttr % 8 == 0) ? numOfAttr / 8 : numOfAttr / 8 + 1;

	unsigned offset = lenOfIndr;

	for (unsigned i = 0; i < numOfAttr; i++)
	{
		Attribute attr = parsedDescriptor[i];

		float valFloat = 0.0;
		string valString;
		int length = 0;

		bool isNull = isAttrNull(returnedData, i);

		if (!isNull)
		{
			switch (attr.type)
			{
			case TypeInt:
			{
				int valInt = 0;
				read(returnedData, valInt, offset);
				parsedData[i]->setValue(&valInt);
				offset += 4;
				break;
			}
			case TypeReal:
			{
				int valFloat = 0;
				read(returnedData, valFloat, offset);
				parsedData[i]->setValue(&valFloat);
				offset += 4;
				break;
			}
			case TypeVarChar:
				int length = 0;
				read(returnedData, length, offset);
				offset += 4;
				string valString = string((char*) returnedData + offset, length);
				parsedData[i]->setValue(&valString);
				offset += length;
				break;
			}
		}
	}

}

bool RelationManager::getBit(unsigned char byte, unsigned pos)
{
	assert(pos < 8 && pos >= 0);
	unsigned char mask = 0x80 >> pos;
	return ((byte & mask) == 0) ? false : true;
}

void RelationManager::setBit(unsigned char *byte, unsigned pos)
{
	assert(pos < 8 && pos >= 0);
	unsigned char mask = 0x80 >> pos;
	*byte |= mask;
}

void RelationManager::clearBit(unsigned char *byte, unsigned pos)
{
	assert(pos < 8 && pos >= 0);
	unsigned char mask = 0x80 >> pos;
	*byte &= ~mask;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
	if (rbfmsi.getNextRecord(rid, data) == RBFM_EOF)
		return RM_EOF;
	else
		return 0;
}

RC RM_ScanIterator::close()
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	if (rbfmsi.close() != 0 || rbfm->closeFile(fileHandle) != 0)
		return -1;
	else
		return 0;
}

RC RM_ScanIterator::init(const string &tableName)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	if (rbfm->openFile(tableName, fileHandle) != 0)
		return -1;
	return 0;
}


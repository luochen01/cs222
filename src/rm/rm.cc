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

Catalog::Catalog()
{
	tupleBuffer = new byte[PAGE_SIZE];
    initializeCatalogAttrs();
}

Catalog::~Catalog() 
{
	delete[] tupleBuffer;
}
    
RC Catalog::createCatalog() 
{ 
	return createCatalogTables(tableRecordDescriptor, columnRecordDescriptor);
}

RC Catalog::deleteCatalog() 
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	rbfm->destroyFile(COLUMNS_TABLE);
	rbfm->destroyFile(TABLES_TABLE);
	// TODO
	return 0;
}

RC Catalog::getColumnAttributes(const int tableID, vector<Attribute> &attrs, vector<RID> &rids) 
{
	Attribute attr;
	RM_ScanIterator rmsi;
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

    RID rid;
    RelationManager* rm = RelationManager::instance();
	if (rm->scan(COLUMNS_TABLE, "table-id", EQ_OP, (void *) &tableID, columnAttrNames, rmsi) != 0) {
		return -1;
    }
	while (rmsi.getNextTuple(rid, tupleBuffer) != RM_EOF)
	{
		rm->parseIteratorData(columnParsedData, tupleBuffer, columnRecordDescriptor, columnAttrNames);
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

        rids.push_back(rid);
	}
	rm->clearTuple(columnParsedData);
	rmsi.close();
    return 0;
}

RC Catalog::getTableID(const string &tableName, int &tid, RID &rid) 
{ 
    RelationManager* rm = RelationManager::instance();
	RM_ScanIterator rmsi;
	vector<string> tableAttrNames;
	vector<DatumType *> tableParsedData;
	tableAttrNames.push_back("table-id");
	tableParsedData.push_back(new IntType());

	if (rm->scan(TABLES_TABLE, "table-name", EQ_OP, (void *) &tableName, tableAttrNames, rmsi)
			!= 0)
		return -1;
	while (rmsi.getNextTuple(rid, tupleBuffer) != RM_EOF)
	{
		rm->parseIteratorData(tableParsedData, tupleBuffer, tableRecordDescriptor, tableAttrNames);
		tableParsedData[0]->getValue(&tid);
		if (tableParsedData[0]->isNull())
		{
			rm->clearTuple(tableParsedData);
			return -1;
		}
		// FIXME: only use the last matched table id
	}
	rmsi.close();
	rm->clearTuple(tableParsedData);
    return 0; 
}

RC Catalog::getAttributes(const string &tableName, vector<Attribute> &attrs) 
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

    RelationManager* rm = RelationManager::instance();
	RID rid;
	RM_ScanIterator rmsi;
    int tid;
    if (getTableID(tableName, tid, rid) != 0)
        return -1;

    vector<RID> rids;
    return getColumnAttributes(tid, attrs, rids);
}

void Catalog::initializeCatalogAttrs()
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

RC Catalog::createCatalogTables(const vector<Attribute> &tableAttrs,
		const vector<Attribute> &columnAttrs)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	if (rbfm->createFile(TABLES_TABLE) != 0 || rbfm->createFile(COLUMNS_TABLE) != 0)
		return -1;

	addTableToCatalog(TABLES_TABLE, tableAttrs);
	addTableToCatalog(COLUMNS_TABLE, columnAttrs);
	return 0;
}

int Catalog::getLastTableID()
{
	RID rid;
	int maxTableID = 0;
	int tid = 0;
	RM_ScanIterator rmsi;
    RelationManager* rm = RelationManager::instance();
	vector<string> tableAttrNames;
	vector<DatumType*> tableParsedData;
	tableAttrNames.push_back("table-id");
	tableParsedData.push_back(new IntType());

	vector<Attribute> parsedDescriptor;
	parsedDescriptor.push_back(tableRecordDescriptor[0]);

	if (rm->scan(TABLES_TABLE, "", NO_OP, NULL, tableAttrNames, rmsi) != 0)
        return -1;
	while (rmsi.getNextTuple(rid, tupleBuffer) != RM_EOF)
	{
		rm->parseIteratorData(tableParsedData, tupleBuffer, tableRecordDescriptor, tableAttrNames);
		if (tableParsedData[0]->isNull())
		{
			return -1;
		}
		tableParsedData[0]->getValue(&tid);
		if (maxTableID < tid)
			maxTableID = tid;
		// FIXME: only use the last matched table id
	}

	rm->clearTuple(tableParsedData);
	rmsi.close();
	return maxTableID;
}

RC Catalog::addTableToCatalog(const string &tableName, const vector<Attribute> &attrs)
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

    RelationManager* rm = RelationManager::instance();
	int recordSize = 0;

	vector<DatumType*> tableRecordValues;

	tableRecordValues.push_back(new IntType(tableID));
	tableRecordValues.push_back(new StringType(tableName));
	tableRecordValues.push_back(new StringType(tableName));

	rm->formatRecord(tupleBuffer, recordSize, tableRecordDescriptor, tableRecordValues);
	rm->doInsertTuple(TABLES_TABLE, tupleBuffer, rid);
	rm->clearTuple(tableRecordValues);

	void *tmp = malloc(1024);
	rm->readTuple(TABLES_TABLE, rid, tmp);
	rm->printTuple(tableRecordDescriptor, tmp);
	free(tmp);

	for (int i = 0; i < attrs.size(); i++)
	{
		vector<DatumType*> columnRecordValues;
		columnRecordValues.push_back(new IntType(tableID));
		columnRecordValues.push_back(new StringType(attrs[i].name));
		columnRecordValues.push_back(new IntType(attrs[i].type));
		columnRecordValues.push_back(new IntType(attrs[i].length));
		columnRecordValues.push_back(new IntType(i + 1));    // column-position, starts with 1
		rm->formatRecord(tupleBuffer, recordSize, columnRecordDescriptor, columnRecordValues);
		rm->doInsertTuple(COLUMNS_TABLE, tupleBuffer, rid);

		void *tmp = malloc(330);
		rm->readTuple(COLUMNS_TABLE, rid, tmp);
		rm->printTuple(columnRecordDescriptor, tmp);
		free(tmp);

		rm->clearTuple(columnRecordValues);
	}

	return 0;
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
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
	return ctlg.createCatalog();
}

RC RelationManager::deleteCatalog()
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	rbfm->destroyFile(COLUMNS_TABLE);
	rbfm->destroyFile(TABLES_TABLE);
	// TODO
	return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    if (tableName == TABLES_TABLE || tableName == COLUMNS_TABLE) return -1;

	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	if (ctlg.addTableToCatalog(tableName, attrs) != 0)
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
	
    // Get table id and rid
    RID rid_table;
	int tid;
    if (ctlg.getTableID(tableName, tid, rid_table) != 0)
        return -1;
    
    // Get rids matches table_id in COLUMNS_TABLE
    RID rid_column;
    vector<RID> rids_column;
    vector<Attribute> attrs;
    if (ctlg.getColumnAttributes(tid, attrs, rids_column) != 0)
        return -1;
    
    // Delete records from catalog
    if (deleteTuples(COLUMNS_TABLE, rids_column) != 0)
        return -1;
    if (deleteTuple(TABLES_TABLE, rid_table) != 0)
        return -1;
    
	return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    return ctlg.getAttributes(tableName, attrs);
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

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    if (tableName == TABLES_TABLE || tableName == COLUMNS_TABLE) 
        return -1;

    return doInsertTuple(tableName, data, rid);
}

RC RelationManager::deleteTuples(const string &tableName, const vector<RID> &rids)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	vector<Attribute> attrs;
	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != 0)
		return -1;

	getAttributes(tableName, attrs);
    for (RID rid : rids) {
	    if (rbfm->deleteRecord(fileHandle, attrs, rid) != 0)
            return -1;
    }
	rbfm->closeFile(fileHandle);
	return 0;
}

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

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid,
		const string &attributeName, void *data)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	vector<Attribute> attrs;
	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != 0)
		return -1;

	getAttributes(tableName, attrs);
	int result = rbfm->readAttribute(fileHandle, attrs, rid, attributeName, data);

	rbfm->closeFile(fileHandle);
	return result;
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

	unsigned numOfAttr = parsedDescriptor.size();
	unsigned lenOfIndr = (numOfAttr % 8 == 0) ? numOfAttr / 8 : numOfAttr / 8 + 1;
	unsigned offset = lenOfIndr;
	for (unsigned i = 0; i < numOfAttr; i++)
	{
		Attribute attr = parsedDescriptor[i];
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
				float valFloat = 0;
				read(returnedData, valFloat, offset);
				parsedData[i]->setValue(&valFloat);
				offset += 4;
				break;
			}
			case TypeVarChar:
            {
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


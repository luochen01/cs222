#include "rm.h"
#include <iostream>

#include <math.h>

using namespace std;

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
	if (!_rm)
	{
		_rm = new RelationManager();
	}
	return _rm;
}

RelationManager::RelationManager()
{
	catalog = Catalog::instance();
	rbfm = RecordBasedFileManager::instance();
	im = IndexManager::instance();
	loadCatalog();
}

RelationManager::~RelationManager()
{
}

RC RelationManager::loadCatalog()
{
	FileHandle tableHandle;
	FileHandle columnHandle;
	if (rbfm->openFile(TABLES_TABLE, tableHandle) != 0
			|| rbfm->openFile(COLUMNS_TABLE, columnHandle) != 0)
	{
		return -1;
	}

	catalog->createCatalog();

	RM_ScanIterator tableIt;
	TableRecord* tablesTable = catalog->getTablesTable();
	vector<string> tableAttrNames = tablesTable->getAttributeNames();
	scan(TABLES_TABLE, "", NO_OP, (void *) NULL, tableAttrNames, tableIt);
	RID rid;
	while (tableIt.getNextTuple(rid, tupleBuffer) != EOF)
	{
		TableRecord * table = new TableRecord;
		table->rid = rid;
		table->readFrom(tupleBuffer);
		catalog->addTable(table);
		if (catalog->nextTableId <= table->tableId)
		{
			catalog->nextTableId = table->tableId + 1;
		}
	}
	tableIt.close();

	RM_ScanIterator columnIt;
	TableRecord* columnsTable = catalog->getColumnsTable();
	vector<string> columnAttrNames = columnsTable->getAttributeNames();
	scan(COLUMNS_TABLE, "", NO_OP, (void *) NULL, columnAttrNames, columnIt);

	while (columnIt.getNextTuple(rid, tupleBuffer) != EOF)
	{
		ColumnRecord* column = new ColumnRecord;
		column->rid = rid;
		column->readFrom(tupleBuffer);
		catalog->loadColumn(column);
	}

	return 0;

}

RC RelationManager::createCatalog()
{
	if (rbfm->createFile(TABLES_TABLE) != 0 || rbfm->createFile(COLUMNS_TABLE) != 0)
	{
		return -1;
	}

	catalog->createCatalog();

	TableRecord* tablesTable = catalog->getTablesTable();
	TableRecord* columnsTable = catalog->getColumnsTable();

	RID rid;

	tablesTable->writeTo(tupleBuffer);
	doInsertTuple(TABLES_TABLE, tupleBuffer, rid);
	columnsTable->writeTo(tupleBuffer);
	doInsertTuple(TABLES_TABLE, tupleBuffer, rid);

	//Tables/Columns only have one version
	for (ColumnRecord* column : tablesTable->currentVersion->columns)
	{
		column->writeTo(tupleBuffer);
		doInsertTuple(COLUMNS_TABLE, tupleBuffer, rid);
	}
	writeToColumnsTable(tablesTable->currentVersion->columns);
	writeToColumnsTable(columnsTable->currentVersion->columns);
	return 0;
}

RC RelationManager::deleteCatalog()
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	rbfm->destroyFile(COLUMNS_TABLE);
	rbfm->destroyFile(TABLES_TABLE);

	catalog->deleteCatalog();
	return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	if (catalog->isMetaTable(tableName))
	{
		return -1;
	}

	TableRecord* table = catalog->getTableByName(tableName);
	//already exists
	if (table != NULL)
	{
		return -1;
	}
	table = new TableRecord;

	//create file
	if (rbfm->createFile(tableName) != 0)
	{
		return -1;
	}

	table->tableId = catalog->getNextTableId();
	table->tableName = tableName;
	table->fileName = tableName;

	int pos = 1;
	for (Attribute attr : attrs)
	{
		table->addColumn(new ColumnRecord(attr, pos++));
	}

	//insert into meta table
	RID rid;
	table->writeTo(tupleBuffer);
	doInsertTuple(TABLES_TABLE, tupleBuffer, rid);
	table->rid = rid;

	writeToColumnsTable(table->currentVersion->columns);

	catalog->addTable(table);
	return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	if (catalog->isMetaTable(tableName))
	{
		return -1;
	}

	TableRecord* table = catalog->getTableByName(tableName);
	if (table == NULL)
	{
		return -1;
	}
	// delete table file
	rbfm->destroyFile(tableName);

	doDeleteTuple(TABLES_TABLE, table->rid);

	for (TableVersion* version : table->versions)
	{
		for (ColumnRecord* column : version->columns)
		{
			doDeleteTuple(COLUMNS_TABLE, column->rid);
		}
	}

	catalog->deleteTableByName(tableName);
	return 0;

}
RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	if (catalog->isMetaTable(tableName))
	{
		return -1;
	}

	return doInsertTuple(tableName, data, rid);
}

RC RelationManager::doInsertTuple(const string &tableName, const void *data, RID &rid)
{
	vector<Attribute> attrs;
	if (getAttributes(tableName, attrs) != 0)
	{
		return -1;
	}
	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != 0)
	{
		return -1;
	}
	int result = rbfm->insertRecord(fileHandle, attrs, data, rid);
	rbfm->closeFile(fileHandle);

	vector<ColumnRecord*> columns = catalog->getTableColumns(tableName);
	unsigned offset = ceil((double) columns.size() / 8);

	for (int i = 0; i < columns.size(); i++)
	{
		ColumnRecord* column = columns[i];
		Attribute attr = column->toAttribute();

		if (column->hasIndex)
		{
			string indexName = getIndexName(tableName, column->columnName);
			if (insertIndexEntry(indexName, (byte*) data + offset, rid, attr) != 0)
			{
				result = -1;
			}
		}
		if (!isAttrNull(data, i))
		{
			offset += attributeSize(attr.type, (byte*) data + offset);
		}
	}

	return result;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	if (catalog->isMetaTable(tableName))
	{
		return -1;
	}
	return doDeleteTuple(tableName, rid);
}

RC RelationManager::doDeleteTuple(const string &tableName, const RID& rid)
{
// we need to read the original data
	byte oldTuple[PAGE_SIZE];

	if (readTuple(tableName, rid, oldTuple) != 0)
	{
		return -1;
	}

	vector<Attribute> attrs;
	if (getAttributes(tableName, attrs) != 0)
	{
		return -1;
	}

	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != 0)
	{
		return -1;
	}

	int result = rbfm->deleteRecord(fileHandle, attrs, rid);
	rbfm->closeFile(fileHandle);

	vector<ColumnRecord*> columns = catalog->getTableColumns(tableName);

	unsigned offset = ceil((double) columns.size() / 8);

	for (int i = 0; i < columns.size(); i++)
	{
		ColumnRecord* column = columns[i];
		Attribute attr = attrs[i];
		if (column->hasIndex)
		{
			string indexName = getIndexName(tableName, column->columnName);
			if (deleteIndexEntry(indexName, (byte*) oldTuple + offset, rid, attr) != 0)
			{
				result = -1;
			}
		}
		if (!isAttrNull(oldTuple, i))
		{
			offset += attributeSize(attr.type, (byte*) oldTuple + offset);
		}
	}

	return result;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	if (catalog->isMetaTable(tableName))
	{
		return -1;
	}
	return doUpdateTuple(tableName, data, rid);
}

RC RelationManager::doUpdateTuple(const string &tableName, const void * data, const RID& rid)
{
	byte oldTuple[PAGE_SIZE];
	if (readTuple(tableName, rid, oldTuple) != 0)
	{
		return -1;
	}

	vector<Attribute> attrs;
	if (getAttributes(tableName, attrs) != 0)
	{
		return -1;
	}
	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != 0)
	{
		return -1;
	}

	if (rbfm->updateRecord(fileHandle, attrs, data, rid) != 0)
	{
		return -1;
	}
	rbfm->closeFile(fileHandle);

	vector<ColumnRecord*> columns = catalog->getTableColumns(tableName);

	unsigned oldOffset = ceil((double) columns.size() / 8);
	unsigned newOffset = oldOffset;

	int result = 0;

	for (int i = 0; i < columns.size(); i++)
	{
		ColumnRecord* column = columns[i];
		Attribute attr = attrs[i];
		if (column->hasIndex)
		{
			string indexName = getIndexName(tableName, column->columnName);
			if (updateIndexEntry(indexName, (byte*) oldTuple + oldOffset,
					(byte*) data + newOffset, rid, attr) != 0)
			{
				result = -1;
			}
		}
		if (!isAttrNull(oldTuple, i))
		{
			oldOffset += attributeSize(attr.type, (byte*) oldTuple + oldOffset);
		}
		if (!isAttrNull(data, i))
		{
			newOffset += attributeSize(attr.type, (byte*) data + newOffset);
		}
	}

	return result;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	vector<Attribute> attrs;
	if (getAttributes(tableName, attrs) != 0)
	{
		return -1;
	}

	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != 0)
	{
		return -1;
	}

	int result = rbfm->readRecord(fileHandle, attrs, rid, data);
	rbfm->closeFile(fileHandle);
	return result;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	TableRecord* table = catalog->getTableByName(tableName);
	if (table == NULL)
	{
		return -1;
	}
	attrs = table->getAttributes();
	return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid,
		const string &attributeName, void *data)
{
	vector<Attribute> attrs;
	if (getAttributes(tableName, attrs) != 0)
	{
		return -1;
	}
	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != 0)
	{
		return -1;
	}

	int result = rbfm->readAttribute(fileHandle, attrs, rid, attributeName, data);
	rbfm->closeFile(fileHandle);
	return result;
}

RC RelationManager::scan(const string &tableName, const string &conditionAttribute,
		const CompOp compOp, const void *value, const vector<string> &attributeNames,
		RM_ScanIterator &rm_ScanIterator)
{
	if (getAttributes(tableName, rm_ScanIterator.attrs) != 0)
	{
		return -1;
	}
	if (rm_ScanIterator.init(tableName) != 0)
	{
		return -1;
	}
	if (rbfm->scan(rm_ScanIterator.fileHandle, rm_ScanIterator.attrs, conditionAttribute, compOp,
			value, attributeNames, rm_ScanIterator.rbfmsi) != 0)
	{
		rm_ScanIterator.close();
		return -1;
	}
	return 0;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
	if (rbfmsi.getNextRecord(rid, data) == RBFM_EOF)
	{
		return RM_EOF;
	}
	else
	{
		return 0;
	}
}

RC RM_ScanIterator::close()
{
	if (rbfmsi.close() != 0 || rbfm->closeFile(fileHandle) != 0)
	{
		return -1;
	}
	else
	{
		return 0;
	}
}

RC RM_ScanIterator::init(const string &tableName)
{
	if (rbfm->openFile(tableName, fileHandle) != 0)
	{
		return -1;
	}
	return 0;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
	TableRecord* table = catalog->getTableByName(tableName);
	if (table == NULL)
	{
		return -1;
	}
	if (table->getColumnByName(attributeName) == NULL)
	{
		return -1;
	}

	table->createNewVersion();
	table->deleteColumnByName(attributeName);
	writeToColumnsTable(table->currentVersion->columns);

	return 0;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
	TableRecord* table = catalog->getTableByName(tableName);
	if (table == NULL)
	{
		return -1;
	}
	if (table->getColumnByName(attr.name) != NULL)
	{
		return -1;
	}
	table->createNewVersion();
	table->addColumn(new ColumnRecord(attr, table->getColumnsNum()));

//write columns into Columns table
	writeToColumnsTable(table->currentVersion->columns);

	return 0;
}

void RelationManager::writeToColumnsTable(const vector<ColumnRecord*>& columns)
{
	RID rid;
	for (ColumnRecord * column : columns)
	{
		column->writeTo(tupleBuffer);
		doInsertTuple(COLUMNS_TABLE, tupleBuffer, rid);
		column->rid = rid;
	}
}

string RelationManager::getIndexName(const string &tableName, const string & attributeName)
{
	return tableName + "." + attributeName + ".idx";
}

RC RelationManager::getAttribute(const string& tableName, const string& attributeName,
		Attribute& attr)
{
	ColumnRecord* columnRecord = catalog->getColumnByName(tableName, attributeName);
	if (columnRecord == NULL)
	{
		return -1;
	}
	else
	{
		attr = columnRecord->toAttribute();
		return 0;
	}
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	ColumnRecord * column = catalog->getColumnByName(tableName, attributeName);
	if (column == NULL)
	{
		return -1;
	}
	Attribute attr = column->toAttribute();
	if (column->hasIndex)
	{
		return -1;
	}
//create file here
	string indexName = getIndexName(tableName, attributeName);

	if (im->createFile(indexName) != 0)
	{
		return -1;
	}

	IXFileHandle fileHandle;
	if (im->openFile(indexName, fileHandle) != 0)
	{
		im->destroyFile(indexName);
		return -1;
	}

	RM_ScanIterator it;
	vector<string> attrNames;
	attrNames.push_back(attributeName);
	if (scan(tableName, "", NO_OP, NULL, attrNames, it) != 0)
	{
		im->destroyFile(indexName);
		return -1;
	}
	RID rid;
	while (it.getNextTuple(rid, tupleBuffer) != EOF)
	{
		if (im->insertEntry(fileHandle, attr, (byte*) tupleBuffer + 1, rid) != 0)
		{
			im->closeFile(fileHandle);
			im->destroyFile(indexName);
			return -1;
		}
	}
	it.close();
	if (im->closeFile(fileHandle) != 0)
	{
		im->destroyFile(indexName);
		return -1;
	}

	column->hasIndex = 1;

	column->writeTo(tupleBuffer);
	return doUpdateTuple(COLUMNS_TABLE, tupleBuffer, column->rid);
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{

	ColumnRecord * column = catalog->getColumnByName(tableName, attributeName);

	if (!column->hasIndex)
	{
		return -1;
	}
	Attribute attr = column->toAttribute();

//delete file here
	string indexName = getIndexName(tableName, attributeName);
	if (im->destroyFile(indexName) != 0)
	{
		return -1;
	}

	column->hasIndex = false;

	column->writeTo(tupleBuffer);
	return doUpdateTuple(COLUMNS_TABLE, tupleBuffer, column->rid);
}

RC RelationManager::insertIndexEntry(const string & indexName, const void * key, const RID& rid,
		const Attribute& attr)
{
	IXFileHandle fileHandle;
	if (im->openFile(indexName, fileHandle) != 0)
	{
		return -1;
	}

	if (im->insertEntry(fileHandle, attr, key, rid) != 0)
	{
		im->closeFile(fileHandle);
		return -1;
	}

	return im->closeFile(fileHandle);
}

RC RelationManager::deleteIndexEntry(const string & indexName, const void * key, const RID& rid,
		const Attribute& attr)
{
	IXFileHandle fileHandle;
	if (im->openFile(indexName, fileHandle) != 0)
	{
		return -1;
	}

	if (im->deleteEntry(fileHandle, attr, key, rid) != 0)
	{
		im->closeFile(fileHandle);
		return -1;
	}

	return im->closeFile(fileHandle);
}

RC RelationManager::updateIndexEntry(const string & indexName, const void * oldKey,
		const void * newKey, const RID& rid, const Attribute& attr)
{
	IXFileHandle fileHandle;
	if (im->openFile(indexName, fileHandle) != 0)
	{
		return -1;
	}

	if (im->deleteEntry(fileHandle, attr, oldKey, rid) != 0)
	{
		im->closeFile(fileHandle);
		return -1;
	}

	if (im->insertEntry(fileHandle, attr, newKey, rid) != 0)
	{
		im->closeFile(fileHandle);
		return -1;
	}

	return im->closeFile(fileHandle);
}

RC RelationManager::indexScan(const string &tableName, const string &attributeName,
		const void *lowKey, const void *highKey, bool lowKeyInclusive, bool highKeyInclusive,
		RM_IndexScanIterator &rm_IndexScanIterator)
{
	string indexName = getIndexName(tableName, attributeName);
	Attribute attr;
	if (getAttribute(tableName, attributeName, attr) != 0)
	{
		return -1;
	}
	if (rm_IndexScanIterator.init(indexName) != 0)
	{
		return -1;
	}

	if (im->scan(rm_IndexScanIterator.fileHandle, attr, lowKey, highKey, lowKeyInclusive,
			highKeyInclusive, rm_IndexScanIterator.it) != 0)
	{
		rm_IndexScanIterator.close();
		return -1;
	}
	return 0;
}


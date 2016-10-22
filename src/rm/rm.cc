#include "rm.h"
#include <iostream>

using namespace std;

RelationManager* RelationManager::_rm = 0;
Catalog* Catalog::_ctlg = 0;

TableRecord::~TableRecord()
{
	for (ColumnRecord* column : columns)
	{
		delete column;
	}

}
void TableRecord::addColumn(ColumnRecord* column)
{
	columns.push_back(column);
}

vector<Attribute> TableRecord::getAttributes()
{
	vector<Attribute> attributes;
	for (ColumnRecord* c : columns)
	{
		attributes.push_back(c->toAttribute());
	}

	return attributes;

}

vector<string> TableRecord::getAttributeNames()
{
	vector<string> names;
	for (ColumnRecord* c : columns)
	{
		names.push_back(c->columnName);
	}
	return names;
}

const vector<ColumnRecord*>& TableRecord::getColumns()
{
	return columns;
}

void TableRecord::writeTo(void * data)
{
	int offset = 1;
	memset(data, 0, 1);
	offset += write(data, tableId, offset);
	offset += writeString(data, tableName, offset);
	offset += writeString(data, fileName, offset);
}

void TableRecord::readFrom(void * data)
{
	int offset = 1;

	offset += read(data, tableId, offset);
	offset += readString(data, tableName, offset);
	offset += readString(data, fileName, offset);
}

ColumnRecord::ColumnRecord(int tableId, string columnName, AttrType columnType, int columnLength,
		int columnPosition)
{
	this->tableId = tableId;
	this->columnName = columnName;
	this->columnType = columnType;
	this->columnLength = columnLength;
	this->columnPosition = columnPosition;
}

void ColumnRecord::writeTo(void * data)
{
	unsigned offset = 1;
	memset(data, 0, 1);

	offset += write(data, tableId, offset);
	offset += writeString(data, columnName, offset);
	offset += write(data, columnType, offset);
	offset += write(data, columnLength, offset);
	offset += write(data, columnPosition, offset);

}

void ColumnRecord::readFrom(void * data)
{
	unsigned offset = 1;

	offset += read(data, tableId, offset);
	offset += readString(data, columnName, offset);
	offset += read(data, columnType, offset);
	offset += read(data, columnLength, offset);
	offset += read(data, columnPosition, offset);

}

Attribute ColumnRecord::toAttribute()
{
	Attribute attr;
	attr.name = this->columnName;
	attr.length = this->columnLength;
	attr.type = this->columnType;
	return attr;
}

Catalog* Catalog::instance()
{
	if (!_ctlg)
		_ctlg = new Catalog();

	return _ctlg;
}

Catalog::Catalog()
{
	nextTableId = 0;
	tablesTable = NULL;
	columnsTable = NULL;
}

Catalog::~Catalog()
{
}

bool Catalog::isMetaTable(const string& name)
{
	return name == TABLES_TABLE || name == COLUMNS_TABLE;
}

TableRecord* Catalog::getTablesTable()
{
	return tablesTable;
}

TableRecord* Catalog::getColumnsTable()
{
	return columnsTable;
}

TableRecord * Catalog::getTableById(unsigned id)
{
	for (TableRecord* t : tables)
	{
		if (t->tableId == id)
		{
			return t;
		}
	}
	return NULL;
}

TableRecord * Catalog::getTableByName(const string& name)
{
	for (TableRecord* t : tables)
	{
		if (t->tableName == name)
		{
			return t;
		}
	}
	return NULL;
}

RC Catalog::createCatalog()
{
	for (TableRecord * t : tables)
	{
		delete t;
	}
	tables.clear();
	tablesTable = new TableRecord();
	//hard coded tables
	tablesTable->tableId = 1;
	tablesTable->tableName = TABLES_TABLE;
	tablesTable->fileName = TABLES_TABLE;
	tablesTable->addColumn(new ColumnRecord(1, "table-id", TypeInt, 4, 1));
	tablesTable->addColumn(new ColumnRecord(1, "table-name", TypeVarChar, 50, 2));
	tablesTable->addColumn(new ColumnRecord(1, "file-name", TypeVarChar, 50, 3));

	columnsTable = new TableRecord();
	columnsTable->tableId = 2;
	columnsTable->tableName = COLUMNS_TABLE;
	columnsTable->fileName = COLUMNS_TABLE;
	columnsTable->addColumn(new ColumnRecord(2, "table-id", TypeInt, 4, 1));
	columnsTable->addColumn(new ColumnRecord(2, "column-name", TypeVarChar, 50, 2));
	columnsTable->addColumn(new ColumnRecord(2, "column-type", TypeInt, 4, 3));
	columnsTable->addColumn(new ColumnRecord(2, "column-length", TypeInt, 4, 4));
	columnsTable->addColumn(new ColumnRecord(2, "column-position", TypeInt, 4, 5));

	tables.push_back(tablesTable);
	tables.push_back(columnsTable);

	nextTableId = 3;

	return 0;
}

RC Catalog::deleteCatalog()
{
	for (TableRecord * table : tables)
	{
		delete table;
	}
	tables.clear();
	return 0;
}

unsigned Catalog::getNextTableId()
{
	return nextTableId++;
}

void Catalog::addTable(TableRecord* table)
{
	if (isMetaTable(table->tableName))
	{
		//already hard coded
		return;
	}
	tables.push_back(table);
}

void Catalog::addColumn(ColumnRecord* column)
{
	TableRecord * table = getTableById(column->tableId);
	assert(table != NULL);
	if (!isMetaTable(table->tableName))
	{
		table->addColumn(column);
	}
}

void Catalog::deleteTableByName(const string& tableName)
{
	vector<TableRecord*>::iterator it = tables.begin();
	while (it != tables.end())
	{
		TableRecord* t = *it;
		if (t->tableName == tableName)
		{
			delete t;
			tables.erase(it);
			return;
		}
		it++;
	}
}

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
	tupleBuffer = new byte[PAGE_SIZE];
	catalog = Catalog::instance();
	rbfm = RecordBasedFileManager::instance();
	loadCatalog();
}

RelationManager::~RelationManager()
{
	delete[] tupleBuffer;
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
		catalog->addColumn(column);
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

	for (ColumnRecord* column : tablesTable->columns)
	{
		column->writeTo(tupleBuffer);
		doInsertTuple(COLUMNS_TABLE, tupleBuffer, rid);
	}
	for (ColumnRecord* column : columnsTable->columns)
	{
		column->writeTo(tupleBuffer);
		doInsertTuple(COLUMNS_TABLE, tupleBuffer, rid);
	}
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
		table->addColumn(new ColumnRecord(table->tableId, attr, pos++));
	}

	//insert into meta table
	RID rid;
	table->writeTo(tupleBuffer);
	doInsertTuple(TABLES_TABLE, tupleBuffer, rid);
	table->rid = rid;

	for (ColumnRecord* column : table->columns)
	{
		column->writeTo(tupleBuffer);
		doInsertTuple(COLUMNS_TABLE, tupleBuffer, rid);
		column->rid = rid;
	}

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
	for (ColumnRecord* column : table->columns)
	{
		doDeleteTuple(COLUMNS_TABLE, column->rid);
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

	int result = rbfm->updateRecord(fileHandle, attrs, data, rid);
	rbfm->closeFile(fileHandle);
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
	return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
	return -1;
}


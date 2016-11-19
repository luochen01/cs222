/*
 * catalog.h
 *
 *  Created on: 2016年10月23日
 *      Author: luochen
 */

#ifndef RM_CATALOG_H_
#define RM_CATALOG_H_

#include <string>
#include <vector>
#include <assert.h>

#include "../rbf/rbfm.h"

#define TABLES_TABLE "Tables"
#define COLUMNS_TABLE "Columns"

using namespace std;

class MetaObject
{
protected:
	friend class RelationManager;
	RID rid;
public:

	virtual void writeTo(void * data)=0;

	virtual void readFrom(void * data)=0;

	virtual ~MetaObject()
	{

	}

};

class ColumnRecord: public MetaObject
{
public:
	int tableId;
	string columnName;
	AttrType columnType;
	int columnLength;
	int columnPosition;
	int columnVersion;
	int hasIndex;

	ColumnRecord() :
			tableId(0), columnType(TypeInt), columnLength(0), columnPosition(0), columnVersion(0), hasIndex(
					0)
	{
	}
	ColumnRecord(const ColumnRecord& right)
	{
		this->tableId = right.tableId;
		this->columnName = right.columnName;
		this->columnType = right.columnType;
		this->columnLength = right.columnLength;
		this->columnPosition = right.columnPosition;
		this->columnVersion = right.columnVersion;
		this->hasIndex = right.hasIndex;
	}

	ColumnRecord(Attribute attr, int position)
	{
		this->tableId = 0;
		this->columnName = attr.name;
		this->columnType = attr.type;
		this->columnLength = attr.length;
		this->columnPosition = position;
		this->columnVersion = 0;
		this->hasIndex = 0;
	}
	~ColumnRecord()
	{

	}

	ColumnRecord(string columnName, AttrType columnType, int columnLength, int columnPosition)
	{
		this->tableId = 0;
		this->columnName = columnName;
		this->columnType = columnType;
		this->columnLength = columnLength;
		this->columnPosition = columnPosition;
		this->columnVersion = 0;
		this->hasIndex = 0;
	}

	Attribute toAttribute()
	{
		Attribute attr;
		attr.name = this->columnName;
		attr.length = this->columnLength;
		attr.type = this->columnType;
		return attr;
	}

	void writeTo(void* data)
	{
		unsigned offset = 1;
		memset(data, 0, 1);
		offset += write(data, tableId, offset);
		offset += writeString(data, columnName, offset);
		offset += write(data, columnType, offset);
		offset += write(data, columnLength, offset);
		offset += write(data, columnPosition, offset);
		offset += write(data, columnVersion, offset);
		offset += write(data, hasIndex, offset);
	}

	void readFrom(void* data)
	{
		unsigned offset = 1;
		offset += read(data, tableId, offset);
		offset += readString(data, columnName, offset);
		offset += read(data, columnType, offset);
		offset += read(data, columnLength, offset);
		offset += read(data, columnPosition, offset);
		offset += read(data, columnVersion, offset);
		offset += read(data, hasIndex, offset);
	}
};

class TableVersion
{
public:
	int versionId;
	vector<ColumnRecord*> columns;

	TableVersion() :
			versionId(0)
	{
	}

	~TableVersion()
	{
		for (ColumnRecord* column : columns)
		{
			delete column;
		}
	}

};

class TableRecord: public MetaObject
{
private:
	vector<TableVersion*> versions;
	TableVersion* currentVersion;
	int nextVersionId = 0;
	friend class RelationManager;
	friend class Catalog;

	vector<Attribute> getAttributes(TableVersion* version)
	{
		vector<Attribute> attributes;
		for (ColumnRecord* c : version->columns)
		{
			attributes.push_back(c->toAttribute());
		}
		return attributes;
	}
	vector<string> getAttributeNames(TableVersion* version)
	{
		vector<string> names;
		for (ColumnRecord* c : currentVersion->columns)
		{
			names.push_back(c->columnName);
		}
		return names;
	}

public:
	int tableId;
	string tableName;
	string fileName;

	TableRecord() :
			nextVersionId(0), tableId(0)
	{
		currentVersion = new TableVersion();
		currentVersion->versionId = (nextVersionId++);
		versions.push_back(currentVersion);
	}

	~TableRecord()
	{
		for (TableVersion* version : versions)
		{
			delete version;
		}
		versions.clear();
		currentVersion = NULL;
	}

	void addColumn(ColumnRecord* column)
	{
		column->tableId = tableId;
		column->columnVersion = currentVersion->versionId;
		currentVersion->columns.push_back(column);
	}

	int getColumnsNum()
	{
		return currentVersion->columns.size();
	}

	vector<Attribute> getAttributes()
	{
		return getAttributes(currentVersion);
	}

	void deleteColumnByName(const string& columnName)
	{
		for (vector<ColumnRecord*>::iterator it = currentVersion->columns.begin();
				it != currentVersion->columns.end(); it++)
		{
			ColumnRecord* column = *it;
			if (column->columnName == columnName)
			{
				delete column;
				currentVersion->columns.erase(it);
				return;
			}
		}
	}

	vector<Attribute> getAttributes(int versionId)
	{
		for (TableVersion* version : versions)
		{
			if (version->versionId == versionId)
			{
				return getAttributes(version);
			}
		}
		return vector<Attribute>();
	}

	vector<string> getAttributeNames()
	{
		return getAttributeNames(currentVersion);
	}

	vector<string> getAttributeNames(int versionId)
	{
		for (TableVersion* version : versions)
		{
			if (version->versionId == versionId)
			{
				return getAttributeNames(version);
			}
		}
		return vector<string>();
	}

	ColumnRecord* getColumnByName(const string& name)
	{
		for (ColumnRecord* column : currentVersion->columns)
		{
			if (column->columnName == name)
			{
				return column;
			}
		}
		return NULL;
	}

	void createNewVersion()
	{
		TableVersion* preVersion = currentVersion;
		currentVersion = new TableVersion();
		currentVersion->versionId = nextVersionId++;
		//copy columns from the previous version
		for (ColumnRecord* column : preVersion->columns)
		{
			ColumnRecord* newColumn = new ColumnRecord(*column);
			newColumn->columnVersion = currentVersion->versionId;
			currentVersion->columns.push_back(newColumn);
		}
		versions.push_back(currentVersion);
	}

	void writeTo(void* data)
	{
		int offset = 1;
		memset(data, 0, 1);
		offset += write(data, tableId, offset);
		offset += writeString(data, tableName, offset);
		offset += writeString(data, fileName, offset);
	}

	void readFrom(void* data)
	{
		int offset = 1;
		offset += read(data, tableId, offset);
		offset += readString(data, tableName, offset);
		offset += readString(data, fileName, offset);
	}
};

// Placeholder for Catalog class
class Catalog
{
protected:
	friend class RelationManager;
	Catalog()
	{
		initialized = false;
		nextTableId = 0;
		tablesTable = NULL;
		columnsTable = NULL;
	}
	~Catalog()
	{
	}

	TableRecord* tablesTable;
	TableRecord* columnsTable;

	bool initialized;

public:
	bool isMetaTable(const string& name)
	{
		return name == TABLES_TABLE || name == COLUMNS_TABLE;
	}

	static Catalog* instance()
	{
		if (!_ctlg)
			_ctlg = new Catalog();

		return _ctlg;
	}
	bool isInitialized()
	{
		return initialized;
	}

// Tested
	RC createCatalog()
	{
		for (TableRecord* t : tables)
		{
			delete t;
		}
		tables.clear();
		tablesTable = new TableRecord();
		//hard coded meta tables
		tablesTable->tableId = 1;
		tablesTable->tableName = TABLES_TABLE;
		tablesTable->fileName = TABLES_TABLE;
		tablesTable->addColumn(new ColumnRecord("table-id", TypeInt, 4, 1));
		tablesTable->addColumn(new ColumnRecord("table-name", TypeVarChar, 50, 2));
		tablesTable->addColumn(new ColumnRecord("file-name", TypeVarChar, 50, 3));
		columnsTable = new TableRecord();
		columnsTable->tableId = 2;
		columnsTable->tableName = COLUMNS_TABLE;
		columnsTable->fileName = COLUMNS_TABLE;
		columnsTable->addColumn(new ColumnRecord("table-id", TypeInt, 4, 1));
		columnsTable->addColumn(new ColumnRecord("column-name", TypeVarChar, 50, 2));
		columnsTable->addColumn(new ColumnRecord("column-type", TypeInt, 4, 3));
		columnsTable->addColumn(new ColumnRecord("column-length", TypeInt, 4, 4));
		columnsTable->addColumn(new ColumnRecord("column-position", TypeInt, 4, 5));
		columnsTable->addColumn(new ColumnRecord("column-version", TypeInt, 4, 6));
		columnsTable->addColumn(new ColumnRecord("hasIndex", TypeInt, 4, 7));
		tables.push_back(tablesTable);
		tables.push_back(columnsTable);
		nextTableId = 3;
		initialized = true;
		return 0;
	}
	RC deleteCatalog()
	{
		for (TableRecord* table : tables)
		{
			delete table;
		}
		tables.clear();
		return 0;
	}

	void addTable(TableRecord* table)
	{
		if (isMetaTable(table->tableName))
		{
			//already hard coded
			return;
		}
		tables.push_back(table);
	}
//used during loading catalog
	void loadColumn(ColumnRecord* column)
	{
		TableRecord* table = getTableById(column->tableId);
		assert(table != NULL);
		if (isMetaTable(table->tableName))
		{
			return;
		}
		if (table->currentVersion->versionId == column->columnVersion)
		{
			table->currentVersion->columns.push_back(column);
		}
		else
		{
			table->currentVersion = new TableVersion();
			table->currentVersion->versionId = (table->nextVersionId++);
			table->versions.push_back(table->currentVersion);
			table->currentVersion->columns.push_back(column);
		}
	}
	void deleteTableByName(const string& tableName)
	{
		for (vector<TableRecord*>::iterator it = tables.begin(); it != tables.end(); it++)
		{
			TableRecord* t = *it;
			if (t->tableName == tableName)
			{
				delete t;
				tables.erase(it);
				return;
			}
		}

	}

	TableRecord* getTablesTable()
	{
		return tablesTable;
	}
	TableRecord* getColumnsTable()
	{
		return columnsTable;
	}
	TableRecord* getTableById(unsigned id)
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
	TableRecord* getTableByName(const string& name)
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

	ColumnRecord* getColumnByName(const string& tableName, const string& columnName)
	{
		TableRecord * t = getTableByName(tableName);
		if (t == NULL)
		{
			return NULL;
		}
		else
		{
			return t->getColumnByName(columnName);
		}
	}

	vector<ColumnRecord *> getTableColumns(const string& name)
	{
		TableRecord * t = getTableByName(name);
		if (t == NULL)
		{
			return vector<ColumnRecord*>();
		}
		else
		{
			return t->currentVersion->columns;
		}
	}

	unsigned getNextTableId()
	{
		return nextTableId++;
	}

	int getTableCurrentVersionId(const string& tableName)
	{
		TableRecord* table = getTableByName(tableName);
		assert(table!=NULL);
		return table->currentVersion->versionId;
	}

private:
	static Catalog *_ctlg;

	vector<TableRecord*> tables;

	unsigned nextTableId;

	RC loadCatalog();

// hard-coded info
	vector<Attribute> tableRecordDescriptor;
	vector<Attribute> columnRecordDescriptor;

	RC getTableIDs(vector<int> &tids, vector<RID> &rids);
	RC getTableIDsFromFile(vector<int> &tids, vector<RID> &rids);
};
#endif /* RM_CATALOG_H_ */

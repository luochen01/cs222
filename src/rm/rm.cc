
#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
    initializeCatalogAttrs();
}

RelationManager::~RelationManager()
{
}

// Tested
RC RelationManager::createCatalog()
{
    if (createCatalogTables(tableRecordDescriptor, columnRecordDescriptor) != 0)
        return -1;

    return 0;
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
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RID rid;
    int tableID = 0;

    if (tableName == TABLES_TABLE) 
        tableID = 1;
    else if (tableName == COLUMNS_TABLE) 
        tableID = 2;
    else { 
        tableID = getLastTableID();
        if (tableID <= 0)
            return -1;
        tableID++;
    }

    void *tableRecord = malloc(120);
    void *columnRecord = malloc(80);
    int recordSize = 0;

    vector<DatumType *> tableRecordValues(3);
    vector<DatumType *> columnRecordValues(4);

    tableRecordValues[0] = new IntType(tableID);
    tableRecordValues[1] = new StringType(tableName);
    tableRecordValues[2] = new StringType(tableName);

    formatRecord(tableRecord, recordSize, tableRecordDescriptor, tableRecordValues);
    insertTuple(TABLES_TABLE, tableRecord, rid);

    void *tmp = malloc(110);
    readTuple(TABLES_TABLE, rid, tmp);
    printTuple(tableRecordDescriptor, tmp);
    free(tmp);

    delete tableRecordValues[0];
    delete tableRecordValues[1];
    delete tableRecordValues[2];

    for (int i = 0; i < attrs.size(); i++) {
        columnRecordValues[0] = new IntType(tableID);
        columnRecordValues[1] = new StringType(attrs[i].name);
        columnRecordValues[2] = new IntType(attrs[i].type);
        columnRecordValues[3] = new IntType(attrs[i].length);
        columnRecordValues[4] = new IntType(i+1);     // column-position, starts with 1
        formatRecord(columnRecord, recordSize, columnRecordDescriptor, columnRecordValues);
        insertTuple(COLUMNS_TABLE, columnRecord, rid);
        
        void *tmp = malloc(330);
        readTuple(COLUMNS_TABLE, rid, tmp);
        printTuple(columnRecordDescriptor, tmp);
        free(tmp);

        delete columnRecordValues[0];
        delete columnRecordValues[1];
        delete columnRecordValues[2];
        delete columnRecordValues[3];
        delete columnRecordValues[4];
    }

    free(tableRecord);
    free(columnRecord);
    return 0;
}

// FIXME
int RelationManager::getLastTableID()
{ 
    RID rid;
    int maxTableID = 0;
    int tid = 0;
    RM_ScanIterator rmsi;
    void *returnedData = malloc(110);
    vector<string> tableAttrNames;
    vector<DatumType *> tableParsedData;
    tableAttrNames.push_back("table-id");
    tableParsedData.push_back(new IntType());

    if (scan(TABLES_TABLE, "table-id", NO_OP, NULL, tableAttrNames, rmsi) != 0)
        return -1;
    while (rmsi.getNextTuple(rid, returnedData) != RM_EOF) {
        parseIteratorData(tableParsedData, returnedData, tableRecordDescriptor, tableAttrNames);
        if (tableParsedData[0]->isNull()) {
            delete tableParsedData[0];
            return -1;
        }
        tableParsedData[0]->getValue(tid);
        if (maxTableID < tid)
            maxTableID = tid;
        memset(returnedData, 0, 110);
        // FIXME: only use the last matched table id
    }
    rmsi.close();
    delete tableParsedData[0];
    free(returnedData);
    return maxTableID;
}

RC RelationManager::createCatalogTables(const vector<Attribute> &tableAttrs, const vector<Attribute> &columnAttrs)
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
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    if (addTableToCatalog(tableName, attrs) != 0)
        return -1;
    
    if (rbfm->createFile(tableName) != 0)
        return -1;
    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    if (tableName == TABLES_TABLE) {
        attrs = tableRecordDescriptor;
        return 0;
    }
    if (tableName == COLUMNS_TABLE) { 
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

    // FIXME: string format
    if (scan(TABLES_TABLE, "table-name", EQ_OP, (void *)tableName.c_str(), tableAttrNames, rmsi) != 0)
        return -1;
    while (rmsi.getNextTuple(rid, returnedData) != RM_EOF) {
        parseIteratorData(tableParsedData, returnedData, tableRecordDescriptor, tableAttrNames);
        tableParsedData[0]->getValue(tid);
        if (tableParsedData[0]->isNull()) {
            delete tableParsedData[0];
            return -1;
        }
        // FIXME: only use the last matched table id
    }
    rmsi.close();
    delete tableParsedData[0];
 
    Attribute attr;
    vector<string> columnAttrNames;
    vector<DatumType *> columnParsedData;
    columnAttrNames.push_back("column-name");
    columnAttrNames.push_back("column-type");
    columnAttrNames.push_back("column-length");
    columnAttrNames.push_back("column-position");
    columnParsedData.push_back(new StringType());
    columnParsedData.push_back(new IntType());
    columnParsedData.push_back(new IntType());
    columnParsedData.push_back(new IntType());

    // FIXME
    if (scan(COLUMNS_TABLE, "table-id", EQ_OP, (void *)&tid, columnAttrNames, rmsi) != 0)
        return -1;
    while (rmsi.getNextTuple(rid, returnedData) != RM_EOF) {
        parseIteratorData(columnParsedData, returnedData, columnRecordDescriptor, columnAttrNames);
        string name;
        int type;
        int length;
        columnParsedData[0]->getValue(name);
        columnParsedData[1]->getValue(type);
        columnParsedData[2]->getValue(length);
        attr.name = name;
        attr.type = (AttrType)type;
        attr.length = length;
        attrs.push_back(attr);
    }
    rmsi.close();
    delete columnParsedData[0];
    delete columnParsedData[1];
    delete columnParsedData[2];
    delete columnParsedData[3];

    free(returnedData);
    return 0;
}

// Tested
RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    if (rbfm->openFile(tableName, fileHandle) != 0) 
        return -1;

    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    if (rbfm->insertRecord(fileHandle, attrs, data, rid) != 0)
        return -1;
    //if (rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData) != 0)
    //    return -1;
    rbfm->closeFile(fileHandle);
    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
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
    if (rbfm->readRecord(fileHandle, attrs, rid, data) != 0)
        return -1;
    else
        return 0;
}

// Tested
RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
}

RC RelationManager::scan(const string &tableName,
        const string &conditionAttribute,
        const CompOp compOp,                  
        const void *value,                    
        const vector<string> &attributeNames,
        RM_ScanIterator &rm_ScanIterator)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    if (rm_ScanIterator.init(tableName) != 0)
        return -1;
    getAttributes(tableName, rm_ScanIterator.attrs);
    if (rbfm->scan(rm_ScanIterator.fileHandle, rm_ScanIterator.attrs, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfmsi) != 0) {
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

void RelationManager::formatRecord(void *record, int &recordSize, const vector<Attribute> &recordDescriptor, const vector<DatumType *> &attrValues)
{
    unsigned numOfAttr = recordDescriptor.size();
    unsigned lenOfIndr = (numOfAttr%8 == 0) ? numOfAttr/8 : numOfAttr/8 + 1;

    char *indrPtr = (char *)record;
    char *fieldPtr = (char *)record+lenOfIndr;

    for (unsigned i = 0; i < numOfAttr; i++) {
        Attribute attr = recordDescriptor[i];
        bool isNull = attrValues[i]->isNull();
        int valInt = 0;
        float valFloat = 0.0;
        string valString;

        if (i != 0 && i%8 == 0) indrPtr++;

        if (isNull) {
            setBit((unsigned char *)indrPtr, i%8);
        } else {
            clearBit((unsigned char *)indrPtr, i%8);
            switch (attr.type) {
                case TypeInt:
                    attrValues[i]->getValue(valInt);
                    *(int *)fieldPtr = valInt;
                    fieldPtr += 4;
                    break;
                case TypeReal:
                    attrValues[i]->getValue(valFloat);
                    *(float *)fieldPtr = valFloat;
                    fieldPtr += 4;
                    break;
                case TypeVarChar:
                    attrValues[i]->getValue(valString);
                    *(int *)fieldPtr = valString.size();
                    fieldPtr += 4;
                    memcpy(fieldPtr, valString.c_str(), valString.size());
                    fieldPtr += valString.size();
                    break;
            }
        }
    }
    recordSize = fieldPtr - (char *)record;
}

// Tested
void RelationManager::parseIteratorData(vector<DatumType *> &parsedData, void *returnedData, 
        const vector<Attribute> &recordDescriptor, const vector<string> &attrNames)
{
    vector<Attribute> parsedDescriptor;
    for (Attribute attr : recordDescriptor) {
        for (string name : attrNames) {
            if (attr.name == name) {
                parsedDescriptor.push_back(attr);
            }
        }
    }

    // parse returnedData using parsedDescriptor, then fill the parsedData vec
    unsigned numOfAttr = parsedDescriptor.size();
    unsigned lenOfIndr = (numOfAttr%8 == 0) ? numOfAttr/8 : numOfAttr/8 + 1;

    char *indrPtr = (char *)returnedData;
    char *fieldPtr = (char *)returnedData+lenOfIndr;

    for (unsigned i = 0; i < numOfAttr; i++) {
        Attribute attr = recordDescriptor[i];
        int valInt = 0;
        float valFloat = 0.0;
        string valString;
        int length = 0;

        if (i != 0 && i%8 == 0) indrPtr++;
        bool isNull = getBit(*indrPtr, i%8);

        if (!isNull) {
            switch (attr.type) {
                case TypeInt:
                    parsedData[i]->setValue(*(int *)fieldPtr);
                    fieldPtr += 4;
                    break;
                case TypeReal:
                    parsedData[i]->setValue(*(float *)fieldPtr);
                    *(float *)fieldPtr = valFloat;
                    fieldPtr += 4;
                    break;
                case TypeVarChar:
                    length = *(int *)fieldPtr;
                    fieldPtr += 4;
                    parsedData[i]->setValue(string((char *)fieldPtr, length));
                    fieldPtr += length;
                    break;
            }
        }
    }

}

bool RelationManager::getBit(unsigned char byte, unsigned pos) 
{
    assert(pos < 8 && pos >= 0);
    unsigned char mask = 0x80 >> pos;
    return ((byte&mask) == 0) ? false : true;
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


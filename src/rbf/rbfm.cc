#include "rbfm.h"

#include <math.h>
#include <cstring>
#include <iostream>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

Record::Record() :
		data(NULL), pRecordDescriptor(0), recordSize(0)
{

}

Record::~Record()
{

}

void Record::reset(void * data, const vector<Attribute>& recordDescriptor, ushort recordSize)
{
	this->data = (byte *) data;
	this->pRecordDescriptor = &recordDescriptor;
	this->recordSize = recordSize;
}

ushort Record::recordHeaderSize()
{
	return pRecordDescriptor->size() * sizeof(ushort);
}

void Record::attributeEndOffset(ushort attrNum, ushort offset)
{
	write(data, offset, attrNum * sizeof(ushort));
}

bool Record::isAttributeNull(ushort attrNum)
{
	return attributeSize(attrNum) == 0;
}

ushort Record::attributeStartOffset(ushort attrNum)
{
	if (attrNum == 0)
	{
		return recordHeaderSize();
	}
	else
	{
		return attributeEndOffset(attrNum - 1);
	}

}

ushort Record::attributeEndOffset(ushort attrNum)
{
	ushort offset;
	read(data, offset, attrNum * sizeof(ushort));
	return offset;
}

ushort Record::attributeSize(ushort attrNum)
{
	return attributeEndOffset(attrNum) - attributeStartOffset(attrNum);

}

void* Record::attribute(ushort attrNum)
{
	if (isAttributeNull(attrNum))
	{
		return NULL;
	}
	ushort startOffset = attributeStartOffset(attrNum);

	return this->data + startOffset;
}

void Record::insertAttribute(ushort attrNum, ushort size)
{
	ushort startOffset = attributeStartOffset(attrNum);

	attributeEndOffset(attrNum, startOffset + size);
}

RecordPage::RecordPage() :
		pFileHandle(NULL), pageNum(0)
{
	data = new byte[PAGE_SIZE];
}

RecordPage::~RecordPage()
{
	delete[] data;
}

void RecordPage::reset(void * data, FileHandle& fileHandle, unsigned pageNum)
{
	this->data = (byte *) data;
	this->pFileHandle = &fileHandle;
	this->pageNum = pageNum;
}

bool RecordPage::samePage(unsigned pageNum, FileHandle& fileHandle)
{
	return this->pageNum == pageNum && this->pFileHandle == &fileHandle;
}

void RecordPage::readRecordSlot(ushort slotNum, RecordSlot& slot)
{
	ushort offset = slotOffset(slotNum);

	read(data, slot.offset, offset);
	read(data, slot.size, offset + sizeof(ushort));
}

void RecordPage::writeRecordSlot(ushort slotNum, const RecordSlot& slot)
{
	ushort offset = slotOffset(slotNum);
	write(data, slot.offset, offset);
	write(data, slot.size, offset + sizeof(ushort));

}

void RecordPage::appendRecordSlot(const RecordSlot& slot)
{
	ushort offset = slotEnd();
	write(data, slot.offset, offset);
	write(data, slot.size, offset + sizeof(ushort));
	slotSize(slotSize() + 1);
}

ushort RecordPage::locateRecordSlot()
{

	RecordSlot tmp;
	ushort size = slotSize();
	for (int i = 0; i < size; i++)
	{
		readRecordSlot(i, tmp);
		if (tmp.offset == 0)
		{
			return i;
		}
	}

//create a new slot
	ushort i = size++;
	slotSize(size);
	return i;
}

ushort RecordPage::slotEnd()
{
	return RECORD_PAGE_HEADER_SIZE + RECORD_SLOT_SIZE * slotSize();
}

ushort RecordPage::slotOffset(ushort slotNum)
{
	return RECORD_PAGE_HEADER_SIZE + RECORD_SLOT_SIZE * slotNum;
}

void RecordPage::readRecord(const RecordSlot& slot, const vector<Attribute>& recordDescriptor,
		Record& record)
{
	record.reset(this->data + slot.offset, recordDescriptor, slot.size);

}

void RecordPage::insertRecord(const vector<Attribute>& recordDescriptor, Record& record,
		ushort recordSize, unsigned& slotNum)
{
	//try to locate a proper slot
	slotNum = locateRecordSlot();
	RecordSlot slot;
	slot.offset = recordStart() - recordSize;
	slot.size = recordSize;
	writeRecordSlot(slotNum, slot);

	record.reset(this->data + slot.offset, recordDescriptor, recordSize);

	recordStart(slot.offset);

}

ushort RecordPage::emptySpace()
{
	return recordStart() - slotEnd();
}

ushort RecordPage::recordStart()
{
	ushort value;
	read(this->data, value, 0);
	return value;
}

void RecordPage::recordStart(ushort value)
{
	write(this->data, value, 0);
}

ushort RecordPage::slotSize()
{
	ushort value;
	read(this->data, value, sizeof(ushort));
	return value;
}

void RecordPage::slotSize(ushort value)
{
	write(this->data, value, sizeof(ushort));
}

RBFM_ScanIterator::RBFM_ScanIterator()
{
	buffer = NULL;
	curPageNum = -1;
	curSlotNum = -1;
	end = false;

	pFileHandle = NULL;
	compOp = EQ_OP;
	conditionValue = NULL;
	conditionNum = 0;
	pRecordDescriptor = NULL;

}
RBFM_ScanIterator::~RBFM_ScanIterator()
{
	if (buffer)
	{
		delete[] buffer;
		buffer = NULL;
	}
	pFileHandle = NULL;
	conditionValue = NULL;
	end = false;
}

void RBFM_ScanIterator::init()
{
	if (buffer == NULL)
	{
		buffer = new byte[PAGE_SIZE];
	}

	curPageNum = -1;
	curSlotNum = -1;
	end = false;

	readNextPage();
}

void RBFM_ScanIterator::readNextPage()
{
	if (curPage.readPage(*pFileHandle, ++curPageNum) == 0)
	{
		curSlotNum = -1;
	}
	else
	{
		end = true;
	}
}

RC RBFM_ScanIterator::getNextRecordWithinPage(Record& record)
{
	if (end)
	{
		return -1;
	}
	RecordSlot slot;
	while (++curSlotNum < curPage.slotSize())
	{
		curPage.readRecordSlot(curSlotNum, slot);
		if (slot.offset != 0)
		{
			curPage.readRecord(slot, *pRecordDescriptor, record);
			return 0;
		}

	}
	return -1;
}

bool RBFM_ScanIterator::select(void *conditionData, void *conditionValue, CompOp compOp,
		const Attribute& attributeDescriptor)
{
	switch (attributeDescriptor.type)
	{
	case TypeInt:
		selectInt(conditionData, conditionValue, compOp);
		break;
	case TypeReal:
		selectReal(conditionData, conditionValue, compOp);
		break;
	}

	return compOp == NO_OP;
}

bool RBFM_ScanIterator::selectInt(void *conditionData, void *conditionValue, CompOp compOp)
{
//might be null
	int left = conditionData != NULL ? *(int *) conditionData : 0;
	int right = conditionValue != NULL ? *(int *) conditionValue : 0;

	switch (compOp)
	{
	case EQ_OP:	// =
		return (conditionData == NULL && conditionValue == NULL)
				|| (conditionData != NULL && conditionValue != NULL && left == right);
	case LT_OP:
		return conditionData != NULL && conditionValue != NULL && left < right;
	case LE_OP:
		return conditionData != NULL && conditionValue != NULL && left <= right;
	case GT_OP:
		return conditionData != NULL && conditionValue != NULL && left > right;
	case GE_OP:
		return conditionData != NULL && conditionValue != NULL && left >= right;
	case NE_OP:
		return (conditionData == NULL && conditionValue != NULL)
				|| (conditionData != NULL && conditionValue != NULL) || left != right;
	case NO_OP:
		return true;
	}

	return false;
}

bool RBFM_ScanIterator::selectReal(void *conditionData, void *conditionValue, CompOp compOp)
{
	float left = conditionData != NULL ? *(float *) conditionData : 0.0f;
	float right = conditionValue != NULL ? *(float *) conditionValue : 0.0f;

	switch (compOp)
	{
	case EQ_OP:	// =
		return (conditionData == NULL && conditionValue == NULL)
				|| (conditionData != NULL && conditionValue != NULL && equals(left, right));
	case LT_OP:
		return conditionData != NULL && conditionValue != NULL && left < right;
	case LE_OP:
		return conditionData != NULL && conditionValue != NULL && left <= right;
	case GT_OP:
		return conditionData != NULL && conditionValue != NULL && left > right;
	case GE_OP:
		return conditionData != NULL && conditionValue != NULL && left >= right;
	case NE_OP:
		return (conditionData == NULL && conditionValue != NULL)
				|| (conditionData != NULL && conditionValue != NULL) || !equals(left, right);
	case NO_OP:
		return true;
	}
	return false;
}
// Never keep the results in the memory. When getNextRecord() is called,
// a satisfying record needs to be fetched from the file.
// "data" follows the same format as RecordBasedFileManager::insertRecord().
RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
	if (end)
	{
		return RBFM_EOF;
	}

	Record record;
	while (getNextRecord(record) == 0)
	{
		//check this record
		void *conditionData = record.attribute(conditionNum);

		if (!select(conditionData, conditionValue, compOp, (*pRecordDescriptor)[conditionNum]))
		{
			continue;
		}

		ushort recordOffset = ceil((double) projectNums.size() / 8);
		//project data
		for (int i = 0; i < projectNums.size(); i++)
		{
			int num = projectNums[i];
			void * attributeData = record.attribute(num);
			if (num == NULL)
			{
				setAttrNull(data, num, true);
			}
			else
			{
				setAttrNull(data, num, false);
				recordOffset += copyAttributeData(data, recordOffset, (*pRecordDescriptor)[num],
						attributeData, 0);
			}
		}

		return 0;
	}

	end = true;

	return RBFM_EOF;
}
RC RBFM_ScanIterator::close()
{
	if (buffer)
	{
		delete[] buffer;
		buffer = NULL;
	}

	return -1;
}

RC RecordBasedFileManager::readPage(unsigned pageNum, FileHandle& fileHandle)
{
	if (curPage.samePage(pageNum, fileHandle))
	{
		return 0;
	}
	if (fileHandle.readPage(pageNum, data) != 0)
	{
		return -1;
	}
	curPage.reset(data, fileHandle, pageNum);
	return 0;
}

RecordBasedFileManager * RecordBasedFileManager::instance()
{
	if (!_rbf_manager)
		_rbf_manager = new RecordBasedFileManager();

	return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
	data = new byte[PAGE_SIZE];
}

RecordBasedFileManager::~RecordBasedFileManager()
{
	delete[] data;
}

RC RecordBasedFileManager::createFile(const string &fileName)
{
	return PagedFileManager::instance()->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName)
{
	return PagedFileManager::instance()->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	return PagedFileManager::instance()->openFile(fileName, fileHandle);

}

RC RecordBasedFileManager::closeFile(FileHandle & fileHandle)
{
	return PagedFileManager::instance()->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data, RID &rid)
{
	ushort recordSize = RecordSize(recordDescriptor, data);

//locate a suitable page
	locatePage(recordSize, fileHandle, rid);

//we have a good page now

	Record record;

	curPage.insertRecord(recordDescriptor, record, recordSize, rid.slotNum);

//parse input data, and insert into record
	ushort recordOffset = ceil((double) recordDescriptor.size() / 8);

	for (int i = 0; i < recordDescriptor.size(); i++)
	{
		const Attribute& attr = recordDescriptor[i];
		int attributeSize = 0;
		if (!::isAttrNull(data, i))
		{
			void * attrData = record.attribute(i);
			attributeSize = copyAttributeData(attrData, 0, attr, data, recordOffset);

		}
		record.insertAttribute(i, attributeSize);
		recordOffset += attributeSize;

	}

//finish append record
	fileHandle.writePage(rid.pageNum, curPage.getData());
	return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{
	if (readPage(rid.pageNum, fileHandle) != 0)
	{
		return -1;
	}

	if (rid.slotNum >= curPage.slotSize())
	{
		//check slot size
		logError("Fail to read record, because " + rid.slotNum +" is not a valid slot number!");
		return -1;
	}

	RecordSlot slot;
	curPage.readRecordSlot(rid.slotNum, slot);
	if (slot.offset == 0)
	{
		//check slot exists
		logError("Fail to read record, because " + rid.slotNum + " is not a valid slot number!");
		return -1;
	}

	Record record;
	curPage.readRecord(slot, recordDescriptor, record);

	ushort recordOffset = ceil((double) recordDescriptor.size() / 8);

	for (int i = 0; i < recordDescriptor.size(); i++)
	{
		if (record.isAttributeNull(i))
		{
			setAttrNull(data, i, true);
		}
		else
		{
			setAttrNull(data, i, false);
			void* attrData = record.attribute(i);
			const Attribute& attribute = recordDescriptor[i];

			recordOffset += copyAttributeData(data, recordOffset, attribute, attrData, 0);
		}

	}
	return 0;

}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid)
{
	return 0;
}

// Assume the RID does not change after an update
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{

	return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName,
		void *data)
{

	if (readPage(rid.pageNum, fileHandle) != 0)
	{
		return -1;
	}

	if (rid.slotNum >= curPage.slotSize())
	{
		//check slot size
		logError("Fail to read record attribute, because " + rid.slotNum +" is not a valid slot number!");
		return -1;
	}

	RecordSlot slot;
	curPage.readRecordSlot(rid.slotNum, slot);

	if (slot.offset == 0)
	{
		//check slot exists
		logError("Fail to read record attribute, because " + rid.slotNum + " is not a valid slot number!");
		return -1;
	}

	Record record;
	curPage.readRecord(slot, recordDescriptor, record);

	int index = attributeIndex(recordDescriptor, attributeName);
	void * attrData = record.attribute(index);

	copyAttributeData(data, (ushort) 0, recordDescriptor[index], attrData, (ushort) 0);
	return 0;

}

// Scan returns an iterator to allow the caller to go through the results one by one.
RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
		const string &conditionAttribute, const CompOp compOp, // comparision type such as "<" and "="
		const void *value, // used in the comparison
		const vector<string> &attributeNames, // a list of projected attributes
		RBFM_ScanIterator &rbfm_ScanIterator)
{
	rbfm_ScanIterator.compOp = compOp;
	rbfm_ScanIterator.conditionNum = attributeIndex(recordDescriptor, conditionAttribute);
	rbfm_ScanIterator.conditionValue = value;

	rbfm_ScanIterator.pFileHandle = &fileHandle;
	rbfm_ScanIterator.pRecordDescriptor = &recordDescriptor;
	rbfm_ScanIterator.projectNums.clear();
	for (string name : attributeNames)
	{
		rbfm_ScanIterator.projectNums.push_back(attributeIndex(recordDescriptor, name));
	}

	sort(rbfm_ScanIterator.projectNums.begin(), rbfm_ScanIterator.projectNums.end());

	rbfm_ScanIterator.init();
	return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> & recordDescriptor, const void *data)
{
	ushort offset = ceil((double) recordDescriptor.size() / 8);
	for (int i = 0; i < recordDescriptor.size(); i++)
	{
		const Attribute& attr = recordDescriptor[i];
		cout << attr.name << ": ";
		if (isAttrNull(data, i))
		{
			cout << "NULL";
		}
		else
		{
			switch (attr.type)
			{
			case TypeInt:
			{
				int ivalue;
				read(data, ivalue, offset, attr.length);
				cout << ivalue;
				offset += attr.length;
				break;
			}
			case TypeReal:
			{
				float fvalue;
				read(data, fvalue, offset, attr.length);
				cout << fvalue;
				offset += attr.length;
				break;
			}
			case TypeVarChar:
			{
				unsigned size;
				read(data, size, offset, 4);
				string svalue((byte*) data + offset + 4, (byte*) data + offset + 4 + size);
				cout << svalue;
				offset = offset + 4 + size;
				break;
			}
			}
		}
		if (i < recordDescriptor.size() - 1)
		{
			cout << " ";
		}
		else
		{
			cout << endl;
		}

	}

	return 0;

}

RC RecordBasedFileManager::locatePage(ushort recordSize, FileHandle& fileHandle, RID& rid)
{
	if (fileHandle.pages > 0)
	{
		//first check the last page
		readPage(fileHandle.pages - 1, fileHandle);
		if (curPage.emptySpace() > recordSize + RECORD_SLOT_SIZE)
		{
			rid.pageNum = fileHandle.pages - 1;
			return 0;
		}

		//go through all the pages
		for (int i = 0; i < fileHandle.pages - 1; i++)
		{
			readPage(i, fileHandle);
			if (curPage.emptySpace() > recordSize + RECORD_SLOT_SIZE)
			{
				rid.pageNum = i;
				return 0;
			}
		}

	}

//we need to create a new page
	memset(data, 0, PAGE_SIZE);
	curPage.reset(data, fileHandle, fileHandle.pages);
	rid.pageNum = fileHandle.pages;

	curPage.recordStart(PAGE_SIZE);
	fileHandle.appendPage(data);

	return 0;
}

ushort RecordBasedFileManager::RecordSize(const vector<Attribute>& recordDescriptor,
		const void * src)
{

	ushort size = (recordDescriptor.size()) * sizeof(ushort);
	ushort srcOffset = ceil((double) recordDescriptor.size() / 8);
	for (int i = 0; i < recordDescriptor.size(); i++)
	{
		if (::isAttrNull(src, i))
		{
			continue;
		}
		const Attribute& attr = recordDescriptor[i];
		switch (attr.type)
		{
		case TypeInt:
		case TypeReal:
			size += attr.length;
			srcOffset += attr.length;
			break;
		case TypeVarChar:
		{
			unsigned varSize;
			read(src, varSize, srcOffset, 4);
			size = size + varSize + 4;
			srcOffset = srcOffset + varSize + 4;
			break;
		}

		}
	}
	return size;

}

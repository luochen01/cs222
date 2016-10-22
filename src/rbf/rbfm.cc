#include "rbfm.h"

#include <math.h>
#include <algorithm>
#include <cassert>
#include <cstring>
#include <iostream>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

int attributeIndex(const vector<Attribute>& recordDescriptor, const string& attributeName)
{
	for (int i = 0; i < recordDescriptor.size(); i++)
	{
		if (recordDescriptor[i].name == attributeName)
		{
			return i;
		}
	}
	return -1;
}

bool equals(float left, float right)
{
	return fabs(left - right) < numeric_limits<float>::epsilon();
}

ushort copyAttributeData(void * to, ushort toOffset, const Attribute& attribute, const void * from,
		ushort fromOffset)
{
	switch (attribute.type)
	{
	case TypeInt:
	case TypeReal:
		writeBuffer(to, toOffset, from, fromOffset, attribute.length);
		return attribute.length;
	case TypeVarChar:
	{
		int size;
		read(from, size, fromOffset, sizeof(int));
		writeBuffer(to, toOffset, from, fromOffset, sizeof(int) + size);
		return 4 + size;
	}
	}
	return 0;
}

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

RecordPage::RecordPage()
{
	data = new byte[PAGE_SIZE];
}

RecordPage::~RecordPage()
{
	if (data)
	{
		delete[] data;
		data = NULL;
	}

}

void RecordPage::setOverflowRecord(unsigned slotNum, unsigned overflowPageNum,
		unsigned overflowSlotNum)
{
	RecordSlot slot;
	readRecordSlot(slotNum, slot);

	ushort increment = OVERFLOW_MARKER_SIZE - slot.size;

	increaseSpace(slotNum, increment);

	slot.offset = slot.offset - increment;
	slot.size = 0;

	writeRecordSlot(slotNum, slot);
	writeOverflowMarker(slot, overflowPageNum, overflowSlotNum);

}

void RecordPage::writeOverflowMarker(RecordSlot& slot, unsigned overflowPageNum,
		unsigned overflowSlotNum)
{
	assert(slot.size == 0);
	write(data, overflowPageNum, slot.offset);
	write(data, overflowSlotNum, slot.offset + sizeof(unsigned));
}

void RecordPage::readOverflowMarker(RecordSlot& slot, unsigned& overflowPageNum,
		unsigned& overflowSlotNum)
{
	assert(slot.size == 0);
	read(data, overflowPageNum, slot.offset);
	read(data, overflowSlotNum, slot.offset + sizeof(unsigned));
}

void RecordPage::increaseSpace(ushort slotNum, short offset)
{
	RecordSlot slot;
	readRecordSlot(slotNum, slot);

	ushort toOffset = recordStart() - offset;
	ushort fromOffset = recordStart();
	ushort size = slot.offset - recordStart();

	memmove(data + toOffset, data + fromOffset, size);

	for (int i = 0; i < slotSize(); i++)
	{
		RecordSlot existSlot;
		readRecordSlot(i, existSlot);
		if (existSlot.offset != 0 && existSlot.offset < slot.offset)
		{
			existSlot.offset -= offset;
			writeRecordSlot(i, existSlot);
		}
	}

	recordStart(recordStart() - offset);

}

RC RecordPage::readPage(FileHandle& fileHandle, unsigned pageNum)
{

	if (fileHandle.readPage(pageNum, data) != 0)
	{
		return -1;
	}

	return 0;
}

void RecordPage::readRecordSlot(ushort slotNum, RecordSlot& slot)
{
	ushort offset = slotOffset(slotNum);

	read(data, slot.offset, offset);
	read(data, slot.size, offset + sizeof(ushort));
}

void RecordPage::deleteRecordSlot(ushort slotNum)
{
	RecordSlot slot;
	slot.offset = 0;
	slot.size = 0;
	writeRecordSlot(slotNum, slot);
}

void RecordPage::markRecordSlot(ushort slotNum)
{
	RecordSlot slot;
	readRecordSlot(slotNum, slot);
	slot.size = 0;
	writeRecordSlot(slotNum, slot);
}

void RecordPage::increaseRecordSlot(ushort slotNum, ushort offset)
{
	RecordSlot slot;
	readRecordSlot(slotNum, slot);
	if (slot.offset != 0)
	{
		slot.offset -= offset;
	}
	writeRecordSlot(slotNum, slot);
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

void RecordPage::deleteRecord(ushort slotNum)
{
	RecordSlot slot;
	readRecordSlot(slotNum, slot);
	if (slot.size == 0)
	{
		//handle overflow record
		slot.size = OVERFLOW_MARKER_SIZE;
	}

	increaseSpace(slotNum, -slot.size);

	deleteRecordSlot(slotNum);
}

void RecordPage::updateRecord(const vector<Attribute>& recordDescriptor, Record& record,
		ushort newRecordSize, unsigned slotNum)
{
	RecordSlot slot;
	readRecordSlot(slotNum, slot);
	ushort increment = newRecordSize - slot.size;

	increaseSpace(slotNum, increment);

	slot.offset = slot.offset - increment;
	slot.size = newRecordSize;
	writeRecordSlot(slotNum, slot);

	readRecord(slot, recordDescriptor, record);
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
	pFileHandle = NULL;
	conditionValue = NULL;
	end = false;
}

void RBFM_ScanIterator::init()
{

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

RC RBFM_ScanIterator::getNextRecord(Record& record)
{
	if (end)
	{
		return -1;
	}
	while (curPageNum < pFileHandle->pages && !end)
	{
		if (getNextRecordWithinPage(record) == 0)
		{
			return 0;
		}
		readNextPage();
	}
	return -1;
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
		//ensure the record exists, and not a overflow record
		if (slot.offset != 0 && slot.size != 0)
		{
			curPage.readRecord(slot, *pRecordDescriptor, record);
			return 0;
		}
	}
	return -1;
}

bool RBFM_ScanIterator::select(const void *conditionData, const void *conditionValue, CompOp compOp,
		const Attribute& attributeDescriptor)
{
	switch (attributeDescriptor.type)
	{
	case TypeInt:
		return selectInt(conditionData, conditionValue, compOp);
	case TypeReal:
		return selectReal(conditionData, conditionValue, compOp);
	case TypeVarChar:
		return selectVarchar(conditionData, conditionValue, compOp);
	default:
		return compOp == NO_OP;
	}

}

bool RBFM_ScanIterator::selectInt(const void *conditionData, const void *conditionValue,
		CompOp compOp)
{
	if (conditionData == NULL || conditionValue == NULL)
	{
		return false;
	}
//might be null
	int left = *(int *) conditionData;
	int right = *(int *) conditionValue;

	switch (compOp)
	{
	case EQ_OP:	// =
		return left == right;
	case LT_OP:
		return left < right;
	case LE_OP:
		return left <= right;
	case GT_OP:
		return left > right;
	case GE_OP:
		return left >= right;
	case NE_OP:
		return left != right;
	case NO_OP:
		return true;
	}

	return false;
}

bool RBFM_ScanIterator::selectReal(const void *conditionData, const void *conditionValue,
		CompOp compOp)
{
	if (conditionData == NULL || conditionValue == NULL)
	{
		return false;
	}
//might be null
	float left = *(float *) conditionData;
	float right = *(float *) conditionValue;

	switch (compOp)
	{
	case EQ_OP:	// =
		return left == right;
	case LT_OP:
		return left < right;
	case LE_OP:
		return left <= right;
	case GT_OP:
		return left > right;
	case GE_OP:
		return left >= right;
	case NE_OP:
		return left != right;
	case NO_OP:
		return true;
	}

	return false;
}

bool RBFM_ScanIterator::selectVarchar(const void *conditionData, const void *conditionValue,
		CompOp compOp)
{
	if (conditionData == NULL || conditionValue == NULL)
	{
		return false;
	}
	int size;
	read(conditionData, size, 0);
	string left = string((char *) conditionData + 4, (char *) conditionData + 4 + size);
	read(conditionValue, size, 0);
	string right = string((char*) conditionValue + 4, (char*) conditionValue + 4 + size);
	int comp = left.compare(right);

	switch (compOp)
	{
	case EQ_OP:	// =
		return comp == 0;
	case LT_OP:
		return comp < 0;
	case LE_OP:
		return comp == 0 || comp < 0;
	case GT_OP:
		return comp > 0;
	case GE_OP:
		return comp > 0 || comp == 0;
	case NE_OP:
		return comp != 0;
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
		if (compOp != NO_OP)
		{
			//check this record
			void *conditionData = record.attribute(conditionNum);

			if (!select(conditionData, conditionValue, compOp, (*pRecordDescriptor)[conditionNum]))
			{
				continue;
			}
		}

		ushort recordOffset = ceil((double) projectNums.size() / 8);
		//project data
		for (int i = 0; i < projectNums.size(); i++)
		{
			int num = projectNums[i];
			void * attributeData = record.attribute(num);
			if (attributeData == NULL)
			{
				setAttrNull(data, i, true);
			}
			else
			{
				setAttrNull(data, i, false);
				recordOffset += copyAttributeData(data, recordOffset, (*pRecordDescriptor)[num],
						attributeData, 0);
			}
		}
		rid.pageNum = curPageNum;
		rid.slotNum = curSlotNum;

		return 0;
	}

	end = true;

	return RBFM_EOF;
}
RC RBFM_ScanIterator::close()
{
	end = true;

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
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

void RecordBasedFileManager::fillRecord(const vector<Attribute>& recordDescriptor, Record& record,
		const void *data)
{
	memset(record.getData(), 0, record.getRecordSize());

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

}

void RecordBasedFileManager::fillData(const vector<Attribute> & recordDescriptor, Record& record,
		void * data)
{

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
	rid.pageNum = locatePage(curPage, recordSize, fileHandle);

//we have a good page now

	Record record;

	curPage.insertRecord(recordDescriptor, record, recordSize, rid.slotNum);

//parse input data, and insert into record
	fillRecord(recordDescriptor, record, data);

//finish append record
	fileHandle.writePage(rid.pageNum, curPage.data);

	return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{
	if (curPage.readPage(fileHandle, rid.pageNum) != 0)
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

	if (slot.size != 0)
	{
		curPage.readRecord(slot, recordDescriptor, record);
		fillData(recordDescriptor, record, data);
	}
	else
	{
		//an overflow record
		unsigned pageNum, slotNum;
		curPage.readOverflowMarker(slot, pageNum, slotNum);
		overflowPage.readPage(fileHandle, pageNum);
		overflowPage.readRecordSlot(slotNum, slot);
		overflowPage.readRecord(slot, recordDescriptor, record);
		fillData(recordDescriptor, record, data);

		//overflowPage.invalidate();
	}

	return 0;

}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid)
{
	if (curPage.readPage(fileHandle, rid.pageNum) != 0)
	{
		return -1;
	}

	if (rid.slotNum >= curPage.slotSize())
	{
		//check slot size
		logError("Fail to delete record, because " + rid.slotNum +" is not a valid slot number!");
		return -1;
	}

	RecordSlot slot;
	curPage.readRecordSlot(rid.slotNum, slot);
	if (slot.offset == 0)
	{
		//check slot exists
		logError("Fail to delete record, because " + rid.slotNum + " is not a valid slot number!");
		return -1;
	}

	if (slot.size != 0)
	{
		curPage.deleteRecord(rid.slotNum);
		fileHandle.writePage(rid.pageNum, curPage.data);
	}
	else
	{
		//handle overflow record
		unsigned pageNum, slotNum;
		curPage.readOverflowMarker(slot, pageNum, slotNum);
		overflowPage.readPage(fileHandle, pageNum);
		overflowPage.deleteRecord(slotNum);
		//overflowPage.invalidate();
		fileHandle.writePage(pageNum, overflowPage.data);

		curPage.deleteRecord(rid.slotNum);
		fileHandle.writePage(rid.pageNum, curPage.data);
	}

	return 0;
}

// Assume the RID does not change after an update
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
	if (curPage.readPage(fileHandle, rid.pageNum) != 0)
	{
		return -1;
	}

	if (rid.slotNum >= curPage.slotSize())
	{
		//check slot size
		logError("Fail to update record, because " + rid.slotNum +" is not a valid slot number!");
		return -1;
	}

	RecordSlot slot;
	curPage.readRecordSlot(rid.slotNum, slot);
	if (slot.offset == 0)
	{
		//check slot exists
		logError("Fail to update record, because " + rid.slotNum + " is not a valid slot number!");
		return -1;
	}
	ushort newRecordSize = RecordSize(recordDescriptor, data);

	if (slot.size != 0)
	{
		if (newRecordSize - slot.size <= curPage.emptySpace())
		{

			updateRecordWithinPage(fileHandle, curPage, recordDescriptor, rid.pageNum, rid.slotNum,
					newRecordSize, data);
		}
		else
		{
			//we need to find a new page
			unsigned overflowPageNum = locatePage(overflowPage, newRecordSize, fileHandle);

			unsigned overflowSlotNum;
			Record record;
			overflowPage.insertRecord(recordDescriptor, record, newRecordSize, overflowSlotNum);
			fillRecord(recordDescriptor, record, data);
			fileHandle.writePage(overflowPageNum, overflowPage.data);

			//invalidate cached pages
			//curPage.invalidate(fileHandle, overflowPageNum);
			//overflowPage.invalidate();

			curPage.setOverflowRecord(rid.slotNum, overflowPageNum, overflowSlotNum);
			fileHandle.writePage(rid.pageNum, curPage.data);

		}
	}
	else
	{
		//handle overflow record
		unsigned overflowPageNum, overflowSlotNum;
		curPage.readOverflowMarker(slot, overflowPageNum, overflowSlotNum);
		overflowPage.readPage(fileHandle, overflowPageNum);
		RecordSlot overflowSlot;
		overflowPage.readRecordSlot(overflowSlotNum, overflowSlot);
		if (newRecordSize - overflowSlot.size <= overflowPage.emptySpace())
		{
			//update in place
			updateRecordWithinPage(fileHandle, overflowPage, recordDescriptor, overflowPageNum,
					overflowSlotNum, newRecordSize, data);
			//overflowPage.invalidate();
		}
		else
		{
			//use another overflow page
			overflowPage.deleteRecord(overflowPageNum);
			fileHandle.writePage(overflowPageNum, overflowPage.data);
			//overflowPage.invalidate();

			overflowPageNum = locatePage(overflowPage, newRecordSize, fileHandle);
			if (overflowPageNum != rid.pageNum)
			{
				Record record;
				overflowPage.insertRecord(recordDescriptor, record, newRecordSize, overflowSlotNum);
				fillRecord(recordDescriptor, record, data);
				fileHandle.writePage(overflowPageNum, overflowPage.data);
				//overflowPage.invalidate();

				curPage.writeOverflowMarker(slot, overflowPageNum, overflowSlotNum);
				fileHandle.writePage(rid.pageNum, curPage.data);
			}
			else
			{
				//now the curPage has enough space again
				//overflowPage.invalidate();
				//clear marker
				slot.size = OVERFLOW_MARKER_SIZE;
				curPage.writeRecordSlot(rid.slotNum, slot);

				updateRecordWithinPage(fileHandle, curPage, recordDescriptor, rid.pageNum,
						rid.slotNum, newRecordSize, data);
			}

		}

	}

	return 0;
}

void RecordBasedFileManager::updateRecordWithinPage(FileHandle& fileHandle, RecordPage& page,
		const vector<Attribute>& recordDescriptor, unsigned pageNum, unsigned slotNum,
		unsigned newRecordSize, const void * data)
{
	Record record;
	page.updateRecord(recordDescriptor, record, newRecordSize, slotNum);
	fillRecord(recordDescriptor, record, data);
	fileHandle.writePage(pageNum, page.data);
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName,
		void *data)
{

	if (curPage.readPage(fileHandle, rid.pageNum) != 0)
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
	if (slot.size != 0)
	{
		curPage.readRecord(slot, recordDescriptor, record);
	}
	else
	{
		//handle overflow record
		unsigned pageNum, slotNum;
		curPage.readOverflowMarker(slot, pageNum, slotNum);
		overflowPage.readPage(fileHandle, pageNum);
		overflowPage.readRecordSlot(slotNum, slot);
		overflowPage.readRecord(slot, recordDescriptor, record);

		//overflowPage.invalidate();
	}

	int index = attributeIndex(recordDescriptor, attributeName);
	if (!record.isAttributeNull(index))
	{
		setAttrNull(data, 0, false);
		void * attrData = record.attribute(index);
		copyAttributeData(data, (ushort) 1, recordDescriptor[index], attrData, (ushort) 0);
	}
	else
	{
		setAttrNull(data, 0, true);
	}

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
				int size;
				read(data, size, offset, sizeof(int));

				string svalue((byte*) data + offset + 4,
						(byte*) data + offset + sizeof(int) + size);
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

unsigned RecordBasedFileManager::locatePage(RecordPage& page, ushort recordSize,
		FileHandle& fileHandle)
{
	if (fileHandle.pages > 0)
	{
		//first check the last page
		page.readPage(fileHandle, fileHandle.pages - 1);
		if (page.emptySpace() > recordSize + RECORD_SLOT_SIZE)
		{
			return fileHandle.pages - 1;
		}

		//go through all the pages
		for (int i = 0; i < fileHandle.pages - 1; i++)
		{
			page.readPage(fileHandle, i);
			if (page.emptySpace() > recordSize + RECORD_SLOT_SIZE)
			{
				return i;
			}
		}

	}

//we need to create a new page
	memset(page.data, 0, PAGE_SIZE);

	page.recordStart(PAGE_SIZE);
	fileHandle.appendPage(page.data);

	return fileHandle.pages - 1;
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
			int varSize;
			read(src, varSize, srcOffset, sizeof(int));
			size = size + varSize + sizeof(int);
			srcOffset = srcOffset + varSize + sizeof(int);
			break;
		}

		}
	}
	return size;

}

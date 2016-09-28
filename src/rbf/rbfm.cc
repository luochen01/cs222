#include "rbfm.h"
#include"util.h"

#include<iostream>
#include<math.h>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

void RecordPage::reset(void * data)
{
	this->data = data;
	recordStart = read<int>(data, 0);
	slotSize = read<int>(data, sizeof(uint));
}

void RecordPage::readSlot(uint slotNum, RecordSlot& slot)
{
	int offset = slotOffset(slotNum);
	slot.offset = read<uint>(data, offset);
	slot.size = read<uint>(data, offset + sizeof(uint));
}

void RecordPage::writeSlot(uint slotNum, const RecordSlot& slot)
{
	int offset = slotOffset(slotNum);
	write(data, slot.offset, offset);
	write(data, slot.size, offset + sizeof(uint));
}

void RecordPage::appendSlot(const RecordSlot& slot)
{
	int offset = slotEnd();
	write(data, slot.offset, offset);
	write(data, slot.size, offset + sizeof(uint));
	slotSize++;
}

void RecordPage::readRecord(const RecordSlot& slot, const vector<Attribute> &recordDescriptor,
		void * data)
{
	uint recordOffset = ceil((double) recordDescriptor.size() / 8);

	Record record(this->data, slot.offset);
	for (int i = 0; i < recordDescriptor.size(); i++)
	{
		const Attribute& attr = recordDescriptor[i];

		uint byteRead = record.readAttr(i, attr, data, recordOffset);
		recordOffset += byteRead;
		setNull(data, i, byteRead == 0);
	}

}

void RecordPage::appendRecord(const RecordSlot& slot, const vector<Attribute>& recordDescriptor,
		void * data)
{
	uint recordOffset = ceil((double) recordDescriptor.size() / 8);
	Record record(this->data, slot.offset);
	for (int i = 0; i < recordDescriptor.size(); i++)
	{
		const Attribute& attr = recordDescriptor[i];
		uint byteWrite = record.appendAttr(i, attr, data, recordOffset);
		setNull(data, i, byteWrite == 0);
	}

}

void RecordPage::flush()
{
	write(data, recordStart, 0);
	write(data, recordStart, sizeof(uint));
}

void RecordPage::setNull(void * data, uint attrNum, bool isNull)
{
	uint bytes = 0;
	uint pos = 0;
	getByteOffset(attrNum, bytes, pos);
	setBit(*((uchar *) data + bytes), isNull, pos);
}

bool RecordPage::isNull(void * data, uint attrNum)
{
	uint bytes = 0;
	uint pos = 0;
	getByteOffset(attrNum, bytes, pos);
	return readBit(*((uchar *) data + bytes), pos);
}

Record::Record(void * _data, uint _attributes) :
		data(_data), attributes(_attributes)
{
//initialize the first attribute offset
	attrOffset(0, HEADER_LENGTH);
}
Record::~Record()
{

}

bool Record::nullAttr(uint attrNum)
{
	return attrOffset(attrNum) == attrOffset(attrNum + 1);
}

uint Record::readAttr(uint attrNum, const Attribute& attrDescriptor, void * dest, uint destOffset)
{
	if (nullAttr(attrNum))
	{
		return 0;
	}

	switch (attrDescriptor.type)
	{
	case TypeInt:
	case TypeReal:
		writeBuffer(dest, destOffset, data, attrOffset(attrNum), 4);
		return 4;
	case TypeVarChar:
		uint size = read<uint>(data, attrOffset(attrNum), 4);
		writeBuffer(dest, destOffset, data, attrOffset(attrNum), 4 + size);
		return 4 + size;
	}

	return 0;

}
uint Record::appendAttr(uint attrNum, const Attribute& attrDescriptor, void * src, uint srcOffset)
{
	int offset = attrOffset(attrNum);
	if (nullAttr(attrNum))
	{
		attrOffset(attrNum + 1, offset);
		return 0;
	}

	switch (attrDescriptor.type)
	{
	case TypeInt:
	case TypeReal:
		writeBuffer(data, offset, src, srcOffset, 4);
		attrOffset(attrNum + 1, offset + 4);
		return 4;
	case TypeVarChar:
		uint size = read<uint>(src, srcOffset, 4);
		writeBuffer(data, offset, src, srcOffset, 4 + size);
		attrOffset(attrNum + 1, offset + 4 + size);
		return 4 + size;
	}
	return 0;
}

uint Record::RecordSize(const vector<Attribute>& recordDescriptor, void * data)
{
	uint size = (recordDescriptor.size() + 1) * sizeof(uint);
	uint offset = ceil((double) recordDescriptor.size() / 8);
	for (int i = 0; i < recordDescriptor.size(); i++)
	{
		const Attribute& attr = recordDescriptor[i];
		switch (attr.type)
		{
		case TypeInt:
		case TypeReal:
			size += 4;
			offset += 4;
			break;
		case TypeVarChar:
			uint varSize = read<uint>(data, offset, 4);
			size = size + 4 + varSize;
			offset = offset + 4 + varSize;
			break;
		}
	}
	return size;

}

RecordBasedFileManager* RecordBasedFileManager::instance()
{
	if (!_rbf_manager)
		_rbf_manager = new RecordBasedFileManager();

	return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
	pageBuffer = new uchar[PAGE_SIZE];
	curPageNum = -1;
}

RecordBasedFileManager::~RecordBasedFileManager()
{
	delete[] pageBuffer;
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

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle)
{
	return PagedFileManager::instance()->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data, RID &rid)
{

	return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{
	if (curPageNum != rid.pageNum)
	{
		if (!fileHandle.readPage(rid.pageNum, pageBuffer))
		{
//fail to read page
			curPage.reset(pageBuffer);
			return -1;
		}

		curPageNum = rid.pageNum;
	}

	if (rid.slotNum >= curPage.slotSize)
	{
		//check slot size
		cerr << "Fail to read record, because " << rid.slotNum << " is not a valid slot number!";
		return -1;
	}

	return -1;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data)
{
	return -1;
}

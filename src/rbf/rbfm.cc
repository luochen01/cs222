#include "rbfm.h"

#include <math.h>
#include <cstring>
#include <iostream>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

Record::Record(void * _data, ushort _attributes) :
		data(_data), RECORD_HEADER_LENGTH((_attributes) * sizeof(ushort))
{

}

Record::~Record()
{

}

ushort Record::attrStartOffset(ushort attrNum)
{
	if (attrNum == 0)
	{
		return RECORD_HEADER_LENGTH;
	}
	else
	{
		return attrEndOffset(attrNum - 1);
	}

}

ushort Record::attrEndOffset(ushort attrNum)
{
	ushort offset;
	read(data, offset, attrNum * sizeof(ushort));
	return offset;
}

ushort Record::attrSize(ushort attrNum)
{
	return attrEndOffset(attrNum) - attrStartOffset(attrNum);

}

ushort Record::readAttr(ushort attrNum, const Attribute& attr, void * dest, ushort destOffset)
{
	if (isAttrNull(attrNum))
	{
		return 0;
	}
	ushort startOffset = attrStartOffset(attrNum);
	switch (attr.type)
	{
	case TypeInt:
	case TypeReal:
		writeBuffer(dest, destOffset, data, startOffset, attr.length);
		return attr.length;
	case TypeVarChar:
	{
		ushort size = 0;
		read(data, size, startOffset);
		write(dest, (unsigned) size, destOffset, 4);
		writeBuffer(dest, destOffset + 4, data, startOffset + sizeof(ushort), size);
		return 4 + size;
	}
	}
	return 0;

}
ushort Record::insertAttr(ushort attrNum, const Attribute& attr, const void * src, ushort srcOffset)
{
	ushort startOffset = attrStartOffset(attrNum);

	if (::isAttrNull(src, attrNum))
	{
		attrEndOffset(attrNum, attrStartOffset(attrNum));
		return 0;
	}

	switch (attr.type)
	{
	case TypeInt:
	case TypeReal:
		writeBuffer(data, startOffset, src, srcOffset, attr.length);
		attrEndOffset(attrNum, startOffset + 4);
		return 4;
	case TypeVarChar:
	{
		unsigned size;
		read(src, size, srcOffset, 4);

		//write byte content
		write(data, (ushort) size, startOffset);
		writeBuffer(data, startOffset + sizeof(ushort), src, srcOffset + 4, size);
		attrEndOffset(attrNum, startOffset + size + sizeof(ushort));
		return size + 4;
	}
	}
	return 0;
}

ushort Record::RecordSize(const vector<Attribute>& recordDescriptor, const void * src)
{
	ushort size = (recordDescriptor.size() + 1) * sizeof(ushort);
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
			size = size + varSize + sizeof(ushort);
			srcOffset = srcOffset + varSize + 4;
			break;
		}

		}
	}
	return size;

}

void Record::readRecord(const vector<Attribute>& recordDescriptor, void * dest)
{
	ushort recordOffset = ceil((double) recordDescriptor.size() / 8);

	for (int i = 0; i < recordDescriptor.size(); i++)
	{
		const Attribute& attr = recordDescriptor[i];

		if (isAttrNull(i))
		{
			setAttrNull(dest, i, true);
		}
		else
		{
			ushort bytesRead = readAttr(i, attr, dest, recordOffset);
			recordOffset += bytesRead;
			setAttrNull(dest, i, false);

		}
	}

}

void Record::insertRecord(const vector<Attribute>& recordDescriptor, const void * src)
{
	ushort recordOffset = ceil((double) recordDescriptor.size() / 8);
	for (int i = 0; i < recordDescriptor.size(); i++)
	{
		const Attribute& attr = recordDescriptor[i];
		ushort bytesWrite = insertAttr(i, attr, src, recordOffset);
		recordOffset += bytesWrite;
	}

}

RecordPage::RecordPage() :
		data(NULL), recordStart(0), pFileHandle(NULL), pageNum(0), slotSize(0)
{

}

RecordPage::~RecordPage()
{

}

void RecordPage::reset(void * data, const FileHandle& fileHandle, unsigned pageNum)
{
	this->data = data;
	this->pFileHandle = &fileHandle;
	this->pageNum = pageNum;

	read(this->data, recordStart, 0);
	read(this->data, slotSize, sizeof(ushort));
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
	slotSize++;
}

ushort RecordPage::locateRecordSlot()
{

	RecordSlot tmp;
	for (int i = 0; i < slotSize; i++)
	{
		readRecordSlot(i, tmp);
		if (tmp.offset == 0)
		{
			return i;
		}
	}

//create a new slot
	return slotSize++;
}

void RecordPage::readRecord(const RecordSlot& slot, const vector<Attribute> &recordDescriptor,
		void * dest)
{
	Record record((byte*) this->data + slot.offset, recordDescriptor.size());
	record.readRecord(recordDescriptor, dest);
}

ushort RecordPage::insertRecord(const vector<Attribute>& recordDescriptor, const void * src,
		ushort recordSize)
{
	//try to locate a proper slot
	ushort slotNum = locateRecordSlot();
	RecordSlot slot;
	slot.offset = recordStart - recordSize;
	slot.size = recordSize;
	writeRecordSlot(slotNum, slot);

	Record record((byte*) this->data + slot.offset, recordDescriptor.size());
	record.insertRecord(recordDescriptor, src);

	recordStart -= recordSize;

	return slotNum;
}

void RecordPage::flush()
{
	write(data, recordStart, 0);
	write(data, slotSize, sizeof(ushort));

}

ushort RecordPage::emptySpace()
{
	return recordStart - slotEnd();
}

RecordBasedFileManager* RecordBasedFileManager::instance()
{
	if (!_rbf_manager)
		_rbf_manager = new RecordBasedFileManager();

	return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
	pageBuffer = new byte[PAGE_SIZE];
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
	ushort recordSize = Record::RecordSize(recordDescriptor, data);

	//locate a suitable page
	locatePage(recordSize, fileHandle, rid);

	//we have a good page now

	rid.slotNum = curPage.insertRecord(recordDescriptor, data, recordSize);

	//finish append record
	curPage.flush();
	fileHandle.writePage(rid.pageNum, curPage.data);

	return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{

	if (readPage(rid.pageNum, fileHandle) != 0)
	{
		return -1;
	}

	if (rid.slotNum >= curPage.slotSize)
	{
		//check slot size
		cerr << "Fail to read record, because " << rid.slotNum << " is not a valid slot number!";
		return -1;
	}

	RecordSlot slot;
	curPage.readRecordSlot(rid.slotNum, slot);
	if (slot.offset == 0)
	{
		//check slot exists
		cerr << "Fail to read record, because " << rid.slotNum << " is not a valid slot number!";
		return -1;
	}

	curPage.readRecord(slot, recordDescriptor, data);
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

RC RecordBasedFileManager::readPage(unsigned pageNum, FileHandle& fileHandle)
{
	if (curPage.pFileHandle == &fileHandle && curPage.pageNum == pageNum)
	{
		return 0;
	}
	RC rc = fileHandle.readPage(pageNum, pageBuffer);
	if (!rc)
	{
		curPage.reset(pageBuffer, fileHandle, pageNum);
	}
	return rc;
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

		if (deleteEnabled)
		{
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

	}

	//we need to create a new page
	memset(pageBuffer, 0, PAGE_SIZE);
	curPage.reset(pageBuffer, fileHandle, fileHandle.pages);
	curPage.recordStart = PAGE_SIZE;
	curPage.flush();
	fileHandle.appendPage(pageBuffer);

	rid.pageNum = curPage.pageNum;
	return 0;
}


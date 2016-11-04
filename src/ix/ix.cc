#include "ix.h"

#include <iostream>

#include "../rbf/pfm.h"
#include <assert.h>

IndexManager* IndexManager::_index_manager = 0;

int compareInt(int left, int right)
{
	if (left < right)
	{
		return -1;
	}
	else if (left == right)
	{
		return 0;
	}
	else
	{
		return 1;
	}
}

int compareFloat(float left, float right)
{
	if (left < right)
	{
		return -1;
	}
	else if (left > right)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}

int compareString(const string& left, const string& right)
{
	return left.compare(right);
}

/**
 * < 0: this < rhs
 * 0, this == rhs
 * > 0, this > rhs
 */

int BTreeKey::compare(const void * rhs, const Attribute& attr) const
{
	switch (attr.type)
	{
	case TypeInt:
		return compareInt(*(int *) value, *(int *) rhs);
	case TypeReal:
		return compareFloat(*(float*) value, *(float*) rhs);
	case TypeVarChar:
	{
		string left;
		string right;
		readString(value, left, 0);
		readString(rhs, right, 0);
		return compareString(left, right);
	}
	}

	return 0;
}

/**
 * < 0: this < rhs
 * 0, this == rhs
 * > 0, this > rhs
 */
int BTreeKey::compare(const BTreeKey& rhs, const Attribute& attr) const
{
	int result = compare(rhs.value, attr);
	if (result != 0)
	{
		return result;
	}
	//now compare rid
	result = compareInt(rid.pageNum, rhs.rid.pageNum);
	if (result != 0)
	{
		return result;
	}
	return compareInt(rid.slotNum, rhs.rid.slotNum);
}

ushort BTreeKey::keySize(const Attribute& attr) const
{
	ushort size = attributeSize(attr, value);
	return size + 2 * sizeof(unsigned);
}

BTreePage::BTreePage(void * data, int headerSize) :
		PAGE_HEADER_SIZE(headerSize)
{
	this->data = data;
}

void BTreePage::initialize()
{
	numEntry(0);
	nextFreeSpace(PAGE_HEADER_SIZE);

}

ushort BTreePage::readKey(BTreeKey& key, ushort offset, const Attribute& attr)
{
	ushort newOffset = offset;
	key.value = (byte*) data + newOffset;
	newOffset += attributeSize(attr, key.value);
	newOffset += read(data, key.rid.pageNum, newOffset);
	newOffset += read(data, key.rid.slotNum, newOffset);
	return newOffset - offset;
}

ushort BTreePage::writeKey(const BTreeKey& key, ushort offset, const Attribute& attr)
{
	ushort newOffset = offset;
	newOffset += writeBuffer(data, newOffset, key.value, 0, attributeSize(attr, key.value));
	newOffset += write(data, key.rid.pageNum, newOffset);
	newOffset += write(data, key.rid.slotNum, newOffset);
	return newOffset - offset;
}

void BTreePage::getKey(int num, BTreeKey& key, const Attribute& attr)
{
	ushort offset = getKeyOffset(num, attr);
	readKey(key, offset, attr);
}

ushort BTreePage::numEntry()
{
	ushort result;
	read(data, result, sizeof(bool));
	return result;
}

void BTreePage::numEntry(ushort value)
{
	write(data, value, sizeof(bool));
}

ushort BTreePage::nextFreeSpace()
{
	ushort result;
	read(data, result, sizeof(bool) + sizeof(ushort));
	return result;
}

void BTreePage::nextFreeSpace(ushort value)
{
	write(data, value, sizeof(bool) + sizeof(ushort));
}

//is there enough space to hold a new entry?
bool BTreePage::isFull(const Attribute& attr, const void* value)
{
	//a new key (value + RID) + page num
	return nextFreeSpace() + attributeSize(attr, value) + 2 * sizeof(ushort) + sizeof(unsigned)
			< PAGE_SIZE;
}

bool BTreePage::isHalfFull()
{
	return nextFreeSpace() * 2 > PAGE_SIZE;
}

void BTreePage::increaseSpace(ushort offset, int size)
{
	ushort freeSpace = nextFreeSpace();
	memmove((byte*) data + offset + size, (byte*) data + offset, freeSpace - offset);
	nextFreeSpace(freeSpace + size);
}

InternalPage::InternalPage(void * data) :
		BTreePage(data, sizeof(bool) + 2 * sizeof(ushort))
{
}

void InternalPage::initialize()
{
	BTreePage::initialize();

	//set isLeaf
	write(data, (bool) false, 0);

}

ushort InternalPage::readPageNum(PageNum& pageNum, ushort offset)
{
	return read(data, pageNum, offset);
}

ushort InternalPage::writePageNum(PageNum pageNum, ushort offset)
{
	return write(data, pageNum, offset);
}

ushort InternalPage::writeEntry(const BTreeKey& key, PageNum pageNum, ushort offset,
		const Attribute& attr)
{
	ushort newOffset = offset;
	newOffset += writeKey(key, newOffset, attr);
	newOffset += writePageNum(pageNum, newOffset);
	return newOffset - offset;
}

ushort InternalPage::readEntry(BTreeKey& key, PageNum pageNum, ushort offset, const Attribute& attr)
{
	ushort newOffset = offset;
	newOffset += readKey(key, newOffset, attr);
	newOffset += readPageNum(pageNum, newOffset);
	return newOffset - offset;
}

void InternalPage::getPageNum(int num, PageNum& pageNum, const Attribute& attr)
{
	if (num > numEntry())
	{
		logError("Invalid entry num "+ num +", since the page only has "+numEntry()+" entries");
		return;
	}

	ushort offset = getPageNumOffset(num, attr);
	readPageNum(pageNum, offset);
}

ushort InternalPage::getKeyOffset(int num, const Attribute& attr)
{
	return getEntryOffset(num, attr);
}

ushort InternalPage::getPageNumOffset(int num, const Attribute& attr)
{
	if (num == 0)
	{
		return PAGE_HEADER_SIZE;
	}

	ushort offset = getEntryOffset(num, attr);
	return offset + keySize(offset, attr);
}

void InternalPage::updateKey(const BTreeKey& oldKey, const BTreeKey& newKey, const Attribute& attr)
{
	BTreeKey key;
	int keyNum = 1;
	ushort offset = getKeyOffset(keyNum, attr);
	while (keyNum <= numEntry())
	{
		ushort keySize = readKey(key, offset, attr);

		int comp = oldKey.compare(key, attr);
		if (comp == 0)
		{
			//update key here
			ushort newKeySize = newKey.keySize(attr);
			if (newKeySize != keySize)
			{
				increaseSpace(offset, newKeySize - keySize);
			}
			writeKey(newKey, offset, attr);
			return;
		}
		keyNum++;
		offset += keySize;
	}

}

void InternalPage::deleteKey(const BTreeKey& key, const Attribute& attr)
{
	BTreeKey oldKey;
	int keyNum = 1;
	ushort offset = getKeyOffset(keyNum, attr);
	while (keyNum <= numEntry())
	{
		ushort oldKeySize = readKey(oldKey, offset, attr);

		int comp = oldKey.compare(key, attr);
		if (comp == 0)
		{
			//delete key and page num here
			increaseSpace(offset, -(oldKeySize + sizeof(PageNum)));
			numEntry(numEntry() - 1);
			return;
		}
		keyNum++;
		offset += keySize;
	}

}

void InternalPage::insertKey(const BTreeKey& key, PageNum pageNum, const Attribute& attr)
{
	BTreeKey oldKey;
	PageNum oldPageNum;
	int keyNum = 1;
	ushort offset = getKeyOffset(keyNum, attr);
	while (keyNum <= numEntry())
	{
		ushort oldSize = readEntry(oldKey, oldPageNum, offset, attr);

		int comp = oldKey.compare(key, attr);
		if (comp > 0)
		{
			//place the new key here
			increaseSpace(offset, key.keySize(attr) + sizeof(PageNum));
			writeEntry(key, pageNum, offset, attr);
			numEntry(numEntry() + 1);
			return;
		}
		else if (comp == 0)
		{
			logError("Try to insert duplicate key into internal page, rid:" + key.rid);
			return;
		}
		keyNum++;
		offset += oldSize;

	}

//place the new key here
	ushort freeSpace = nextFreeSpace();
	assert(offset == freeSpace);
	freeSpace += writeEntry(key, pageNum, offset, attr);

	numEntry(numEntry() + 1);
	nextFreeSpace(freeSpace);
}

ushort InternalPage::getEntryOffset(int num, const Attribute& attr)
{
	ushort offset = PAGE_HEADER_SIZE;
	offset += pageNumSize(offset);
	for (int i = 1; i < num; i++)
	{
		offset += keySize(offset, attr);
		offset += pageNumSize(offset);
	}

	return offset;
}

LeafPage::LeafPage(void * data) :
		BTreePage(data, sizeof(bool) + 2 * sizeof(ushort) + sizeof(PageNum))
{
}

void LeafPage::initialize()
{
	BTreePage::initialize();
	write(data, (bool) true, 0);
}

ushort LeafPage::getKeyOffset(int num, const Attribute& attr)
{
	ushort offset = this->PAGE_HEADER_SIZE;

	for (int i = 1; i < num; i++)
	{
		offset += keySize(offset, attr);
	}
	return offset;
}

PageNum LeafPage::sibling()
{
	PageNum pageNum;
	read(data, pageNum, sizeof(bool) + 2 * sizeof(ushort));
	return pageNum;
}

void LeafPage::sibling(PageNum pageNum)
{
	write(data, pageNum, sizeof(bool) + 2 * sizeof(ushort));
}

void LeafPage::insertKey(const BTreeKey& key, const Attribute& attr)
{
	BTreeKey oldKey;
	int keyNum = 1;
	ushort offset = getKeyOffset(keyNum, attr);
	while (keyNum <= numEntry())
	{
		ushort oldKeySize = readKey(oldKey, offset, attr);

		int comp = oldKey.compare(key, attr);
		if (comp > 0)
		{
			//place the new key here
			increaseSpace(offset, key.keySize(attr));
			writeKey(key, offset, attr);
			numEntry(numEntry() + 1);

			return;
		}
		else if (comp == 0)
		{
			logError("Try to insert duplicate key into leaf page, rid:" + key.rid);
			return;
		}

		keyNum++;
		offset += oldKeySize;

	}

//place the new key here
	ushort freeSpace = nextFreeSpace();
	freeSpace += writeKey(key, offset, attr);

	numEntry(numEntry() + 1);
	nextFreeSpace(freeSpace);

}

void LeafPage::deleteKey(const BTreeKey& key, const Attribute& attr)
{
	BTreeKey oldKey;
	int keyNum = 1;
	ushort offset = getKeyOffset(keyNum, attr);
	while (keyNum <= numEntry())
	{
		ushort oldKeySize = readKey(oldKey, offset, attr);

		int comp = oldKey.compare(key, attr);
		if (comp == 0)
		{
			//delete the key here
			offset += oldKeySize;
			increaseSpace(offset, -oldKeySize);
			numEntry(numEntry() - 1);
			return;
		}
		keyNum++;
		offset += oldKeySize;

	}
}

IndexManager * IndexManager::instance()
{
	if (!_index_manager)
		_index_manager = new IndexManager();

	return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

BTreePage* IndexManager::readPage(IXFileHandle& fileHandle, PageNum pageNum)
{
	void * data = malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum, data);
	bool isLeaf;
	read(data, isLeaf, 0);
	if (isLeaf)
	{
		return new LeafPage(data);
	}
	else
	{
		return new InternalPage(data);
	}
}

RC IndexManager::createFile(const string &fileName)
{
	return PagedFileManager::instance()->createFile(fileName);

}

RC IndexManager::destroyFile(const string &fileName)
{
	return PagedFileManager::instance()->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
	return PagedFileManager::instance()->openFile(fileName, ixfileHandle.handle);
}

RC IndexManager::closeFile(IXFileHandle & ixfileHandle)
{
	return PagedFileManager::instance()->closeFile(ixfileHandle.handle);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *key, const RID &rid)
{
	return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *key, const RID &rid)
{
	return -1;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *lowKey,
		const void *highKey, bool lowKeyInclusive, bool highKeyInclusive,
		IX_ScanIterator &ix_ScanIterator)
{
	return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const
{
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	return -1;
}

RC IX_ScanIterator::close()
{
	return -1;
}

IXFileHandle::IXFileHandle()
{
	ixReadPageCounter = 0;
	ixWritePageCounter = 0;
	ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
		unsigned &appendPageCount)
{
	readPageCount = ixReadPageCounter;
	writePageCount = ixWritePageCounter;
	appendPageCount = ixAppendPageCounter;
	return 0;
}

const string& IXFileHandle::getFileName()
{
	return handle.getFileName();
}

RC IXFileHandle::readPage(PageNum pageNum, void *data)
{
	RC result = handle.readPage(pageNum, data);
	if (result == 0)
	{
		ixReadPageCounter++;
	}
	return result;
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data)
{
	RC result = handle.writePage(pageNum, data);
	if (result == 0)
	{
		ixWritePageCounter++;
	}
	return result;
}

RC IXFileHandle::appendPage(const void *data)
{
	RC result = handle.appendPage(data);
	if (result == 0)
	{
		ixAppendPageCounter++;
	}
	return result;
}

unsigned IXFileHandle::getNumberOfPages()
{
	return handle.getNumberOfPages();
}

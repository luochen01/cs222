#include "ix.h"

#include <iostream>

#include "../rbf/pfm.h"
#include <assert.h>
#include <algorithm>

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

BTreeKey::BTreeKey()
{
	this->value = NULL;
}

BTreeKey::BTreeKey(const Attribute& attr, const void * value, const RID& rid)
{
	copyValue(attr, value);
	this->rid = rid;
}

void BTreeKey::free()
{
	delete[] (byte*) this->value;
	this->value = NULL;
}

ushort BTreeKey::copyValue(const Attribute& attr, const void * value)
{
	ushort size = attributeSize(attr, value);
	this->value = new byte[size];
	memcpy(this->value, value, size);
	return size;
}

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

ushort BTreeKey::readFrom(const Attribute& attr, const void * data)
{
	ushort offset = 0;
	offset += copyValue(attr, data);
	offset += read(data, rid.pageNum, offset);
	offset += read(data, rid.slotNum, offset);
	return offset;
}

ushort BTreeKey::writeTo(const Attribute& attr, void * data) const
{
	ushort offset = 0;
	offset += copyAttributeData(data, 0, attr, this->value, 0);
	offset += write(data, rid.pageNum, offset);
	offset += write(data, rid.slotNum, offset);
	return offset;
}

BTreePage::BTreePage(void * data, PageNum pageNum, int headerSize) :
		PAGE_HEADER_SIZE(headerSize)
{
	this->data = data;
	this->pageNum = pageNum;
	numEntries = 0;
	spaceUsed = 0;
}

void BTreePage::reset()
{
	numEntries = 0;
	spaceUsed = PAGE_HEADER_SIZE;
	keys.clear();

//insert a dummy key here
	keys.push_back(BTreeKey());
}

void BTreePage::initialize(const Attribute& attr)
{
	read(data, numEntries, NUM_ENTRIES_OFFSET);
	read(data, spaceUsed, SPACE_USED_OFFSET);
}

void BTreePage::flush(const Attribute& attr)
{
	write(data, numEntries, NUM_ENTRIES_OFFSET);
	write(data, spaceUsed, SPACE_USED_OFFSET);
}

ushort BTreePage::readKey(BTreeKey& key, ushort offset, const Attribute& attr)
{
	return key.readFrom(attr, (byte*) data + offset);
}

ushort BTreePage::writeKey(const BTreeKey& key, ushort offset, const Attribute& attr)
{
	return key.writeTo(attr, (byte*) data + offset);
}

void BTreePage::getKey(int num, BTreeKey& key)
{
	key = keys[num];
}

bool BTreePage::isHalfFull()
{
	return spaceUsed * 2 > PAGE_SIZE;
}

InternalPage::InternalPage() :
		BTreePage(malloc(PAGE_SIZE), 0, sizeof(bool) + 2 * sizeof(ushort))
{
}

InternalPage::InternalPage(void * data, PageNum pageNum) :
		BTreePage(data, pageNum, sizeof(bool) + 2 * sizeof(ushort))
{
}

//is there enough space to hold a new entry?
bool InternalPage::isFull(const BTreeKey& key, const Attribute& attr)
{
//a new key (value + RID) + page num
	return spaceUsed + key.keySize(attr) + sizeof(unsigned) < PAGE_SIZE;
}

ushort InternalPage::entrySize(const BTreeKey& key, PageNum pageNum, const Attribute& attr)
{
	return key.keySize(attr) + sizeof(PageNum);
}

void InternalPage::initialize(const Attribute& attr)
{
	BTreePage::initialize(attr);

//load keys and page nums
	PageNum pageNum;
	BTreeKey key;

	ushort offset = PAGE_HEADER_SIZE;
	offset += readPageNum(pageNum, offset);
	pageNums.push_back(pageNum);
//push a dummy key here
	keys.push_back(key);

	for (int i = 1; i <= numEntries; i++)
	{
		offset += readEntry(key, pageNum, offset, attr);
		pageNums.push_back(pageNum);
		keys.push_back(key);
	}

}

void InternalPage::reset()
{
	BTreePage::reset();
}

void InternalPage::flush(const Attribute& attr)
{
	BTreePage::flush(attr);

	write(data, (bool) false, 0);

	ushort offset = PAGE_HEADER_SIZE;
	offset += writePageNum(pageNums[0], offset);

	for (int i = 1; i <= numEntries; i++)
	{
		offset += writeEntry(keys[i], pageNums[i], offset, attr);
		keys[i].free();
	}
}

void InternalPage::distributeTo(InternalPage& rhs, const Attribute& attribute, BTreeKey& newKey,
		PageNum& newPage)
{
	//insert the key/page num here
	insertEntry(newKey, newPage, attribute);

	vector<BTreeKey>::iterator keysIt = keys.begin();
	vector<PageNum>::iterator pagesIt = pageNums.begin();
	//skip the first key
	keysIt++;
	pagesIt++;

	int size = PAGE_HEADER_SIZE;
	while (size < spaceUsed / 2)
	{
		size += entrySize(*(keysIt++), *(pagesIt++), attribute);
	}
	vector<BTreeKey>::iterator eraseKeysBegin = keysIt;
	vector<PageNum>::iterator erasePagesBegin = pagesIt;

	//set the first key
	newKey = *(keysIt++);
	rhs.appendPageNum(*(pagesIt++), attribute);

	while (keysIt != keys.end())
	{
		//append the remaining keys
		rhs.appendEntry(*(keysIt++), *(pagesIt++), attribute);
	}

	//erase the remaining keys from this page
	keys.erase(eraseKeysBegin, keys.end());
	pageNums.erase(erasePagesBegin, pageNums.end());
	spaceUsed = size;
	numEntries = keys.size();

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

ushort InternalPage::readEntry(BTreeKey& key, PageNum& pageNum, ushort offset,
		const Attribute& attr)
{
	ushort newOffset = offset;
	newOffset += readKey(key, newOffset, attr);
	newOffset += readPageNum(pageNum, newOffset);
	return newOffset - offset;
}

void InternalPage::getPageNum(int num, PageNum& pageNum)
{
	pageNum = pageNums[num];
}

PageNum InternalPage::findSubtree(const BTreeKey & key, const Attribute& attr)
{

	for (int i = 1; i <= numEntries; i++)
	{
		int comp = keys[i].compare(key, attr);
		if (comp > 0)
		{
			return pageNums[i - 1];
		}
	}

	return pageNums[numEntries];

}

void InternalPage::updateKey(const BTreeKey& oldKey, const BTreeKey& newKey, const Attribute& attr)
{
	for (int i = 1; i <= numEntries; i++)
	{
		int comp = keys[i].compare(oldKey, attr);
		if (comp == 0)
		{
			//update the new key here
			keys[i] = newKey;
			spaceUsed = spaceUsed - oldKey.keySize(attr) + newKey.keySize(attr);
		}
	}
}

RC InternalPage::deleteEntry(const BTreeKey& key, const Attribute& attr)
{
	vector<BTreeKey>::iterator keyIt = keys.begin();
	vector<PageNum>::iterator pageIt = pageNums.begin();

	keyIt++;
	pageIt++;
	for (int i = 1; i <= numEntries; i++)
	{
		int comp = (*keyIt).compare(key, attr);
		if (comp == 0)
		{
			//we delete the key here
			(*keyIt).free();
			keys.erase(keyIt);
			pageNums.erase(pageIt);
			numEntries--;
			spaceUsed -= entrySize(key, 0, attr);
			return 0;
		}

		keyIt++;
		pageIt++;
	}

    return -1;
}

void InternalPage::insertEntry(const BTreeKey& key, PageNum pageNum, const Attribute& attr)
{
	vector<PageNum>::iterator pageIt = pageNums.begin();
	vector<BTreeKey>::iterator keyIt = keys.begin();

	pageIt++;
	keyIt++;

	for (int i = 1; i <= numEntries; i++)
	{
		int comp = (*keyIt).compare(key, attr);

		if (comp > 0)
		{
			//we should insert the key here
			keys.insert(keyIt, key);
			pageNums.insert(pageIt, pageNum);
			spaceUsed += entrySize(key, pageNum, attr);
			numEntries++;
			return;
		}
		else if (comp == 0)
		{
			logError("Try to insert duplicate key into non-leaf page, rid:" + key.rid);
			return;
		}

		pageIt++;
		keyIt++;

	}
//place the new key here

	appendEntry(key, pageNum, attr);

}

void InternalPage::appendEntry(const BTreeKey& key, PageNum pageNum, const Attribute& attr)
{
	keys.push_back(key);
	pageNums.push_back(pageNum);
	spaceUsed += entrySize(key, pageNum, attr);
	numEntries++;

}

// numEntries does not include the 0th key
void InternalPage::getKeyNum(const BTreeKey& key, const Attribute& attr, int &num)
{
	vector<PageNum>::iterator pageIt = pageNums.begin();
	vector<BTreeKey>::iterator keyIt = keys.begin();

	pageIt++;
	keyIt++;

	for (num = 0; num < numEntries; num++)
	{
		int comp = (*keyIt).compare(key, attr);

		if (comp > 0)
		{
			return;
		}
		else if (comp == 0)
		{
            num++;
			return;
		}

		pageIt++;
		keyIt++;
	}

    return;
}

void InternalPage::redistribute(const Attribute& attr, BTreePage *neighbor, bool isLeftNeighbor, BTreeKey &newKey)
{
    assert(!this->isHalfFull());
    assert(neighbor->isHalfFull());
}

void InternalPage::merge(const Attribute& attr, BTreePage *neighbor, bool isLeftNeighbor)
{
    assert(!this->isHalfFull());
    assert(!neighbor->isHalfFull());
}

void InternalPage::appendPageNum(PageNum pageNum, const Attribute& attr)
{
	pageNums.push_back(pageNum);
	spaceUsed += sizeof(PageNum);
}

LeafPage::LeafPage() :
		BTreePage(malloc(PAGE_SIZE), 0, sizeof(bool) + 2 * sizeof(ushort) + sizeof(PageNum))
{
	siblingPage = 0;
}

LeafPage::LeafPage(void * data, PageNum pageNum) :
		BTreePage(data, pageNum, sizeof(bool) + 2 * sizeof(ushort) + sizeof(PageNum))
{
	siblingPage = 0;
}

void LeafPage::initialize(const Attribute& attr)
{
	BTreePage::initialize(attr);

	read(data, siblingPage, SIBLING_OFFSET);

	BTreeKey key;
	keys.push_back(key);

	ushort offset = PAGE_HEADER_SIZE;
	for (int i = 1; i <= numEntries; i++)
	{
		offset += readKey(key, offset, attr);
		keys.push_back(key);
	}
}

void LeafPage::reset()
{
	BTreePage::reset();

	siblingPage = 0;

}

void LeafPage::flush(const Attribute& attr)
{
	BTreePage::flush(attr);

	write(data, (bool) true, 0);

	write(data, siblingPage, SIBLING_OFFSET);

	ushort offset = PAGE_HEADER_SIZE;

	for (int i = 1; i <= numEntries; i++)
	{
		offset += writeKey(keys[i], offset, attr);
		keys[i].free();
	}

}

void LeafPage::getPageNum(int num, PageNum& pageNum)
{
    logError("Leaf Page doesn't have PageNum!");
    return;
}

//is there enough space to hold a new entry?
bool LeafPage::isFull(const BTreeKey& key, const Attribute& attr)
{
//a new key (value + RID) + page num
	return spaceUsed + key.keySize(attr) <= PAGE_SIZE;
}

void LeafPage::distributeTo(LeafPage& rhs, const Attribute& attr, const BTreeKey& key,
		BTreeKey& newKey)
{
	insertKey(key, attr);

	vector<BTreeKey>::iterator it = keys.begin();
	//skip the first key
	it++;

	int size = PAGE_HEADER_SIZE;
	while (size < spaceUsed / 2)
	{
		size += (*it++).keySize(attr);
	}
	vector<BTreeKey>::iterator eraseBegin = it;

	newKey = *it;

	while (it != keys.end())
	{
		rhs.appendKey(*it, attr);
		it++;
	}

	//erase the remaining keys from this page
	keys.erase(eraseBegin, keys.end());
	spaceUsed = size;
	numEntries = keys.size();

}

void LeafPage::insertKey(const BTreeKey& key, const Attribute& attr)
{
	vector<BTreeKey>::iterator keyIt = keys.begin();

	keyIt++;

	for (int i = 1; i <= numEntries; i++)
	{
		int comp = (*keyIt).compare(key, attr);

		if (comp > 0)
		{
			//we should insert the key here
			keys.insert(keyIt, key);
			spaceUsed += key.keySize(attr);
			numEntries++;
			return;
		}
		else if (comp == 0)
		{
			logError("Try to insert duplicate key into leaf page, rid:" + key.rid);
			return;
		}

		keyIt++;

	}
//place the new key here

	appendKey(key, attr);

}

void LeafPage::appendKey(const BTreeKey& key, const Attribute& attr)
{
	keys.push_back(key);
	spaceUsed += key.keySize(attr);
	numEntries++;
}

void LeafPage::updateKey(const BTreeKey& oldKey, const BTreeKey& newKey, const Attribute& attr)
{
	for (int i = 1; i <= numEntries; i++)
	{
		int comp = keys[i].compare(oldKey, attr);
		if (comp == 0)
		{
			//update the new key here
			keys[i] = newKey;
			spaceUsed = spaceUsed - oldKey.keySize(attr) + newKey.keySize(attr);
		}
	}
}

RC LeafPage::deleteKey(const BTreeKey& key, const Attribute& attr)
{
	vector<BTreeKey>::iterator keyIt = keys.begin();
	keyIt++;
	for (int i = 1; i <= numEntries; i++)
	{
		int comp = (*keyIt).compare(key, attr);
		if (comp == 0)
		{
			//delete the target key
			(*keyIt).free();
			keys.erase(keyIt);
			spaceUsed -= key.keySize(attr);
			numEntries--;
			return 0;
		}
		keyIt++;

	}

    return -1;
}

void LeafPage::redistribute(const Attribute& attr, BTreePage *neighbor, bool isLeftNeighbor, BTreeKey &newKey)
{
    assert(!this->isHalfFull());
    assert(neighbor->isHalfFull());

    LeafPage* neighborPage = (LeafPage *) neighbor;

    int numThisKeys = this->getNumEntries();
    int numNeighborKeys = neighborPage->getNumEntries();
    int mid = (numThisKeys + numNeighborKeys)/2;
    if (isLeftNeighbor)
    {
        BTreeKey tmpFirst;
        for (int i = 0; i < numNeighborKeys - mid; i++)
        {
            BTreeKey tmpKey;
            // FIXME: double check num
            neighborPage->getKey(neighborPage->getNumEntries(), tmpKey);   // Always get from end
            this->insertKey(tmpKey, attr);
            this->getKey(1, tmpFirst);
            assert(0==tmpFirst.compare(tmpKey, attr));
            neighborPage->deleteKey(tmpKey, attr);
        }

        if (!this->isHalfFull())
        {
            logError("Redistribution failed, now size is "
                    + this->getNumEntries() + ", originally " + numThisKeys);
            return;
        }

        this->getKey(1, newKey);    // set newKey for the right page
    } else {
        BTreeKey tmpLast;
        for (int i = 0; i < numNeighborKeys - mid; i++)
        {
            BTreeKey tmpKey;
            neighborPage->getKey(1, tmpKey);   // Always get from beginning
            this->insertKey(tmpKey, attr);
            this->getKey(this->getNumEntries(), tmpLast);
            assert(0==tmpLast.compare(tmpKey, attr));
            neighborPage->deleteKey(tmpKey, attr);
        }

        if (!this->isHalfFull())
        {
            logError("Redistribution failed, now size is "
                    + this->getNumEntries() + ", originally " + numThisKeys);
            return;
        }

        neighborPage->getKey(1, newKey);    // set newKey for the right page
    }
}

bool LeafPage::containsKey(const BTreeKey& key, const Attribute& attr)
{
	for (int i = 1; i <= numEntries; i++)
	{
		if (keys[i].compare(key, attr) == 0)
		{
			return true;
		}
	}
	return false;
}

void LeafPage::merge(const Attribute& attr, BTreePage *neighbor, bool isLeftNeighbor)
{
    assert(!this->isHalfFull());
    assert(!neighbor->isHalfFull());

    // TODO
}


IndexManager * IndexManager::instance()
{
	if (!_index_manager)
		_index_manager = new IndexManager();

	return _index_manager;
}

IndexManager::IndexManager()
{
	buffer = malloc(PAGE_SIZE);
}

IndexManager::~IndexManager()
{
	free(buffer);
}

BTreePage* IndexManager::readPage(IXFileHandle& fileHandle, PageNum pageNum, const Attribute& attr)
{
	void * data = malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum, data);
	bool isLeaf;
	read(data, isLeaf, 0);
	BTreePage *page = NULL;
	if (isLeaf)
	{
		page = new LeafPage(data, pageNum);
	}
	else
	{
		page = new InternalPage(data, pageNum);
	}

	page->initialize(attr);
	return page;

}

void IndexManager::writePage(IXFileHandle & fileHandle, BTreePage* page, const Attribute& attr)
{
	page->flush(attr);
	fileHandle.writePage(page->pageNum, page->data);
}

void IndexManager::appendPage(IXFileHandle & fileHandle, BTreePage* page, const Attribute& attr)
{
	page->flush(attr);
	fileHandle.appendPage(page->data);
	page->pageNum = fileHandle.getNumberOfPages() - 1;
}

RC IndexManager::createFile(const string &fileName)
{
	PagedFileManager * pfm = PagedFileManager::instance();
	if (pfm->createFile(fileName) != 0)
	{
		return -1;
	}
	IXFileHandle fileHandle;

	pfm->openFile(fileName, fileHandle.handle);

	memset(buffer, 0, PAGE_SIZE);
	fileHandle.appendPage(buffer);

	fileHandle.setRootPage(0);
	pfm->closeFile(fileHandle.handle);

	return 0;
}

RC IndexManager::destroyFile(const string &fileName)
{
	return PagedFileManager::instance()->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
	if (PagedFileManager::instance()->openFile(fileName, ixfileHandle.handle) != 0)
	{
		return -1;
	}
//read the first page to get root page num
	ixfileHandle.readPage(0, buffer);
	PageNum rootPage = 0;
	read(buffer, rootPage, 0);
	ixfileHandle.setRootPage(rootPage);
	return 0;
}

RC IndexManager::closeFile(IXFileHandle & ixfileHandle)
{
//flush root page num
	write(buffer, ixfileHandle.getRootPage(), 0);
	ixfileHandle.writePage(0, buffer);
	return PagedFileManager::instance()->closeFile(ixfileHandle.handle);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *key, const RID &rid)
{
	BTreePage * rootPage = readPage(ixfileHandle, ixfileHandle.rootPage, attribute);
	BTreeKey treeKey(attribute, key, rid);
	BTreeKey newKey;
	PageNum newPage;
	bool splitted = false;
	RC result = doInsert(ixfileHandle, rootPage, attribute, treeKey, splitted, newKey, newPage);
	if (result != 0)
	{
		return -1;
	}
	if (splitted)
	{
		//we need to create a new root page
		InternalPage * newRootPage = new InternalPage();
		newRootPage->reset();

		newRootPage->appendPageNum(rootPage->pageNum, attribute);
		newRootPage->appendEntry(newKey, newPage, attribute);

		appendPage(ixfileHandle, newRootPage, attribute);
		ixfileHandle.setRootPage(newRootPage->pageNum);

		delete newRootPage;
	}

	return 0;
}

RC IndexManager::doInsert(IXFileHandle & ixfileHandle, BTreePage * page, const Attribute& attribute,
		const BTreeKey& key, bool& splitted, BTreeKey & newKey, PageNum& newPage)
{
	if (page->isLeaf())
	{
		LeafPage * leafPage = (LeafPage *) page;
		if (leafPage->containsKey(key, attribute))
		{
			return -1;
		}
		if (!leafPage->isFull(key, attribute))
		{
			//place the key here
			leafPage->insertKey(key, attribute);
			writePage(ixfileHandle, leafPage, attribute);
			delete leafPage;
			splitted = false;
			return 0;
		}
		else
		{
			//we need to split
			LeafPage * splittedPage = new LeafPage();
			splittedPage->reset();

			leafPage->distributeTo(*splittedPage, attribute, key, newKey);
			splittedPage->setSibling(leafPage->getSibling());
			appendPage(ixfileHandle, splittedPage, attribute);
			newPage = splittedPage->pageNum;

			leafPage->setSibling(splittedPage->pageNum);
			writePage(ixfileHandle, leafPage, attribute);

			delete leafPage;
			delete splittedPage;

			splitted = true;
			return 0;
		}
	}
	else
	{	//handle internal page here

		InternalPage * internalPage = (InternalPage *) page;
		//choose a proper subtree to recursively insert
		PageNum childPageNum = internalPage->findSubtree(key, attribute);
		BTreePage * childPage = readPage(ixfileHandle, childPageNum, attribute);

		RC result = doInsert(ixfileHandle, childPage, attribute, key, splitted, newKey, newPage);
		if (result != 0)
		{
			return -1;
		}
		if (splitted)
		{
			//we need to handle newKey and newPageNum
			if (!internalPage->isFull(newKey, attribute))
			{
				//we have space here
				internalPage->insertEntry(newKey, newPage, attribute);
				writePage(ixfileHandle, internalPage, attribute);
				delete internalPage;
				splitted = false;
				return 0;
			}
			else
			{
				//unfortunately, we need to split internalPage again
				InternalPage * splittedPage = new InternalPage();
				splittedPage->reset();
				internalPage->distributeTo(*splittedPage, attribute, newKey, newPage);

				appendPage(ixfileHandle, splittedPage, attribute);
				writePage(ixfileHandle, internalPage, attribute);

				newPage = splittedPage->pageNum;

				delete internalPage;
				delete splittedPage;
				splitted = true;
				return 0;
			}
		}
		else
		{
			splitted = false;
			return 0;
		}
	}
}

RC IndexManager::doDelete(IXFileHandle &ixfileHandle, const Attribute& attr, PageNum parent,
        PageNum node, const BTreeKey &key, bool &oldEntryNull, BTreeKey oldEntryKey)
{
    BTreePage* page = readPage(ixfileHandle, node, attr);
    if (!page->isLeaf()) {
        InternalPage* internalPage = (InternalPage *) page;

        int num = 0;
        PageNum pageNum;
        internalPage->getKeyNum(key, attr, num);
        internalPage->getPageNum(num, pageNum);
        if (doDelete(ixfileHandle, attr, node, pageNum, key, oldEntryNull, oldEntryKey) != 0) {
            delete internalPage;
            return -1;
        }
        if (oldEntryNull) {
            writePage(ixfileHandle, internalPage, attr);
            delete internalPage;
            return 0;
        } else {
            if (internalPage->deleteEntry(oldEntryKey, attr) != 0) {
                delete internalPage;
                return -1;
            }
            if (internalPage->isHalfFull()) {
                writePage(ixfileHandle, internalPage, attr);
                delete internalPage;
                oldEntryNull = true;
                return 0;
            } else {
                // get a sibling S of N
                // if S has extra entries,
                //     redistribute evenly between N and S through parent
                //     set oldchildentry to Null, return
                // else, merge N and S
                //     oldchildentry = &(current entry in parent for M)
                //     PULL splitting key from parent DOWN into node on left
                //     move all entries from M to node on left
                //     discard empty node M, return

                writePage(ixfileHandle, internalPage, attr);
                delete internalPage;
                //oldEntryNull = true;
                return 0;
            }
        }

    } else {
        LeafPage* leafPage = (LeafPage *) page;

        if (leafPage->isHalfFull()) {
            // remove entry
            if (leafPage->deleteKey(key, attr) != 0) {
                delete leafPage;
                return -1;
            }
            writePage(ixfileHandle, leafPage, attr);
            delete leafPage;
            oldEntryNull = true;
            return 0;
        } else {
            // get a sibling S of L using parent pointer
            // if S has extra entries
            //     redistribute evenly between L and S
            //     find entry in parent for node on right
            //     replace key value in parent entry by new low-key value in M
            //     set oldchildentry to Null, return
            // else, merge L and S
            //     oldchildentry = &(current entry in parent for M)
            //     move all entries from M to node on left
            //     discard empty node M, adjust sibling pointers, return
            InternalPage* parentPage = (InternalPage *) readPage(ixfileHandle, parent, attr);

            bool isNeighborLeft = false;
            int keyNum = -1;
            int neighborNum = -1;
            parentPage->getKeyNum(key, attr, keyNum);
            int numEntries = parentPage->getNumEntries();
            if (keyNum == numEntries) { // last entry, merging left
                neighborNum = keyNum-1;
                isNeighborLeft = true;
            } else if (keyNum < numEntries && keyNum >= 0) {
                neighborNum = keyNum+1;
                isNeighborLeft = false;
            } else {
                logError("Invalid key entry " + keyNum
                        + ", num of entries " + numEntries
                        + ", check getKeyNum method!");
                return -1;
            }

            BTreeKey newKey;
            BTreeKey oldKey;
            if (isNeighborLeft) {
                parentPage->getKey(keyNum, oldKey);
            } else {
                parentPage->getKey(neighborNum, oldKey);
            }

            PageNum neighborPageNum;
            parentPage->getPageNum(neighborNum, neighborPageNum);
            LeafPage* neighborPage = (LeafPage *) readPage(ixfileHandle, neighborPageNum, attr);
            if (neighborPage->isHalfFull()) {
                leafPage->redistribute(attr, neighborPage, isNeighborLeft, newKey);

                // replace key value in parent entry by newKey
                parentPage->updateKey(oldKey, newKey, attr);
                parentPage->deleteEntry(oldKey, attr);

                writePage(ixfileHandle, parentPage, attr);
                delete parentPage;
                writePage(ixfileHandle, neighborPage, attr);
                delete neighborPage;
                writePage(ixfileHandle, leafPage, attr);
                delete leafPage;

                oldEntryNull = true;
                return 0;
            } else {
                // else, merge L and S
                //     oldchildentry = &(current entry in parent for M)
                //     move all entries from M to node on left
                //     discard empty node M, adjust sibling pointers, return

            }
        }
    }

    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *key, const RID &rid)
{
    PageNum root = ixfileHandle.getRootPage();
	BTreeKey bKey(attribute, key, rid);

    bool isNullNode = true;
    BTreeKey dummyKey;

    return doDelete(ixfileHandle, attribute, root, root, bKey, isNullNode, dummyKey);
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
	rootPage = 0;
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

PageNum IXFileHandle::getRootPage()
{
	return rootPage;
}

void IXFileHandle::setRootPage(PageNum rootPage)
{
	this->rootPage = rootPage;
}

unsigned IXFileHandle::getNumberOfPages()
{
	return handle.getNumberOfPages();
}

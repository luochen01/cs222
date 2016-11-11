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

void BTreePage::getKey(int num, BTreeKey& key)
{
	key = keys[num];
}

//is there enough space to hold a new entry?
bool BTreePage::isFull(const Attribute& attr, const void* value)
{
	//a new key (value + RID) + page num
	return spaceUsed + attributeSize(attr, value) + 2 * sizeof(ushort) + sizeof(unsigned)
			< PAGE_SIZE;
}

bool BTreePage::isHalfFull()
{
	return spaceUsed * 2 > PAGE_SIZE;
}

InternalPage::InternalPage(void * data) :
		BTreePage(data, sizeof(bool) + 2 * sizeof(ushort))
{
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
	}
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

RC InternalPage::deleteKey(const BTreeKey& key, const Attribute& attr)
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

void InternalPage::insertKey(const BTreeKey& key, PageNum pageNum, const Attribute& attr)
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

RC InternalPage::redistribute(const Attribute& attr, BTreePage *neighbor, bool isLeftNeighbor, BTreeKey &newKey)
{
    assert(!neighbor->isLeaf());
    assert(!this->isHalfFull());
    assert(neighbor->isHalfFull());
    return -1;
}

RC InternalPage::merge(const Attribute& attr, BTreePage *neighbor, bool isLeftNeighbor)
{
    assert(!neighbor->isLeaf());
    assert(!this->isHalfFull());
    assert(!neighbor->isHalfFull());
    return -1;
}

LeafPage::LeafPage(void * data) :
		BTreePage(data, sizeof(bool) + 2 * sizeof(ushort) + sizeof(PageNum))
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
	}

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

	keys.push_back(key);
	spaceUsed += key.keySize(attr);
	numEntries++;

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
			keys.erase(keyIt);
			spaceUsed -= key.keySize(attr);
			numEntries--;
			return 0;
		}
		keyIt++;

	}

    return -1;
}

RC LeafPage::redistribute(const Attribute& attr, BTreePage *neighbor, bool isLeftNeighbor, BTreeKey &newKey)
{
    assert(neighbor->isLeaf());
    assert(!this->isHalfFull());
    assert(neighbor->isHalfFull());

    int numThisKeys = this->getNumEntries();
    int numNeighborKeys = neighbor->getNumEntries();
    int mid = (numThisKeys + numNeighborKeys)/2;
    if (isLeftNeighbor) {
        // start: for verification
        BTreeKey tmpFirst;
        for (int i = 0; i < numNeighborKeys - mid; i++) {
            BTreeKey tmpKey;
            // FIXME: double check num
            neighbor->getKey(neighbor->getNumEntries(), tmpKey);   // Always get from end
            this->insertKey(tmpKey, attr);
            this->getKey(1, tmpFirst);
            assert(0==tmpFirst.compare(tmpKey, attr));
            neighbor->deleteKey(tmpKey, attr);
        }

        if (!this->isHalfFull()) {
            logError("Redistribution failed, now size is " + this->getNumEntries() + ", originally " + numThisKeys);
            return -1;
        }

        //PageNum sibling;
        //this->getSibling(sibling);
        //neighbor->setSibling(sibling);
        this->getKey(1, newKey);    // set newKey for the right page
        return 0;
    } else {
        BTreeKey tmpLast;
        for (int i = 0; i < numNeighborKeys - mid; i++) {
            BTreeKey tmpKey;
            // FIXME: double check num
            neighbor->getKey(1, tmpKey);   // Always get from beginning
            this->insertKey(tmpKey, attr);
            this->getKey(this->getNumEntries(), tmpLast);
            assert(0==tmpLast.compare(tmpKey, attr));
            neighbor->deleteKey(tmpKey, attr);
        }

        if (!this->isHalfFull()) {
            logError("Redistribution failed, now size is " + this->getNumEntries() + ", originally " + numThisKeys);
            return -1;
        }

        //PageNum sibling;
        //this->getSibling(sibling);
        //neighbor->setSibling(sibling);
        neighbor->getKey(1, newKey);    // set newKey for the right page
        return 0;

    }

    return -1;
}

RC LeafPage::merge(const Attribute& attr, BTreePage *neighbor, bool isLeftNeighbor)
{
    assert(neighbor->isLeaf());
    assert(!this->isHalfFull());
    assert(!neighbor->isHalfFull());

    // TODO

    return -1;
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
	return -1;
}

// TODO: be careful about the value field in Key
// Better to pass by value
RC IndexManager::doDelete(IXFileHandle &ixfileHandle, const Attribute& attr, PageNum parent, PageNum node,
        const BTreeKey &key, bool &oldEntryNull, BTreeKey oldEntryKey)
{
    // delete(parentpointer, nodepointer, entry, oldchildentry)
    // oldchildentry is Null initially, and Null upon return unless child deleted
    // if (*nodepointer is non-leaf node, say N),
    //      find i such that Ki <= entry's key value < Ki + 1
    //      delete(nodepointer, Pi, entry, oldchildentry)
    //      if oldchildentry is Null, return;
    //      else
    //          remove *oldchildentry from N,
    //          if N has entries to spare,
    //              set oldchildentry to Null, return
    //          else
    //              get a sibling S of N
    //              if S has extra entries,
    //                  redistribute evenly between N and S through parent
    //                  set oldchildentry to Null, return
    //              else, merge N and S
    //                  oldchildentry = &(current entry in parent for M)
    //                  PULL splitting key from parent DOWN into node on left
    //                  move all entries from M to node on left
    //                  discard empty node M, return
    //
    // if *nodepointer is a leaf node, say L
    //      if L has entries to spare,
    //          remove entry, set oldchildentry to Null, and return
    //      else
    //          get a sibling S of L using parent pointer
    //          if S has extra entries
    //              redistribute evenly between L and S
    //              find entry in parent for node on right
    //              replace key value in parent entry by new low-key value in M
    //              set oldchildentry to Null, return
    //          else, merge L and S
    //              oldchildentry = &(current entry in parent for M)
    //              move all entries from M to node on left
    //              discard empty node M, adjust sibling pointers, return

    int num = 0;
    PageNum pageNum;
    BTreePage* pg = readPage(ixfileHandle, node);
    pg->reset();
    pg->initialize(attr);
    if (!pg->isLeaf()) {
        pg->getKeyNum(key, attr, num);
        pg->getPageNum(num, pageNum);
        if (doDelete(ixfileHandle, attr, node, pageNum, key, oldEntryNull, oldEntryKey) != 0) {
            pg->clear();
            return -1;
        }
        if (oldEntryNull) {
            pg->flush(attr);
            pg->clear();
            return 0;
        } else {
            if (pg->deleteKey(oldEntryKey, attr) != 0) {
                pg->clear();
                return -1;
            }
            if (pg->isHalfFull()) {
                pg->flush(attr);
                pg->clear();
                oldEntryNull = true;
                return 0;
            } else {
                // FIXME
    //              get a sibling S of N
    //              if S has extra entries,
    //                  redistribute evenly between N and S through parent
    //                  set oldchildentry to Null, return
    //              else, merge N and S
    //                  oldchildentry = &(current entry in parent for M)
    //                  PULL splitting key from parent DOWN into node on left
    //                  move all entries from M to node on left
    //                  discard empty node M, return

                pg->flush(attr);
                pg->clear();
                //oldEntryNull = true;
                return 0;
            }
        }

    } else {
        if (pg->isHalfFull()) {
            // remove entry
            if (pg->deleteKey(key, attr) != 0) {
                pg->clear();
                return -1;
            }
            pg->flush(attr);
            pg->clear();
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
            BTreePage* ppg = readPage(ixfileHandle, parent);
            ppg->reset();
            ppg->initialize(attr);

            bool isNeighborLeft = false;
            int keyNum = -1;
            int neighborNum = -1;
            ppg->getKeyNum(key, attr, keyNum);
            int numEntries = ppg->getNumEntries();
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
                ppg->getKey(keyNum, oldKey);
            } else {
                ppg->getKey(neighborNum, oldKey);
            }
            PageNum neighborPageNum;
            ppg->getPageNum(neighborNum, neighborPageNum);
            BTreePage* npg = readPage(ixfileHandle, neighborPageNum);
            npg->reset();
            npg->initialize(attr);
            if (npg->isHalfFull()) {
                pg->redistribute(attr, npg, isNeighborLeft, newKey);

                // replace key value in parent entry by newKey
                ppg->updateKey(oldKey, newKey, attr);
                ppg->deleteKey(oldKey, attr);

                ppg->flush(attr);
                ppg->clear();
                npg->flush(attr);
                npg->clear();
                pg->flush(attr);
                pg->clear();
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
    BTreeKey BKey;
    // FIXME
    BKey.value = key;
    BKey.rid = rid;

    return doDelete(ixfileHandle, attribute, root, root, BKey, true, root);
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

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

BTreeKey::BTreeKey(const Attribute& attr, const void * _value, const RID& rid)
{
	this->value = copyAttribute(attr.type, _value);
	this->rid = rid;
}

BTreeKey::BTreeKey(const Attribute& attr, const void * _value)
{
	this->value = copyAttribute(attr.type, _value);
	this->rid.pageNum = 0;
	this->rid.slotNum = 0;
}

void BTreeKey::free()
{
	if (this->value != NULL)
	{
		//cout << "free key:" << "(" << rid.pageNum << "," << rid.slotNum << ")";
		delete[] (byte*) this->value;
		this->value = NULL;
	}
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
	ushort size = attributeSize(attr.type, value);
	return size + 2 * sizeof(unsigned);
}

ushort BTreeKey::readFrom(const Attribute& attr, const void * data)
{
	ushort offset = 0;
	this->value = copyAttribute(attr.type, data);
	offset += attributeSize(attr.type, this->value);
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

void BTreeKey::copyFrom(const Attribute& attr, const BTreeKey &rhs)
{
	value = copyAttribute(attr.type, rhs.value);
	rid = rhs.rid;
}

void BTreeKey::print(const Attribute& attr) const
{
	cout << '"';

	printAttribute(value, attr.type);

	cout << "(" << rid.pageNum << "," << rid.slotNum << ")";

	cout << '"';

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

BTreeKey BTreePage::getKey(int num)
{
	return keys[num];
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
	return spaceUsed + key.keySize(attr) + sizeof(unsigned) > PAGE_SIZE;
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
	pageNums.clear();
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
	while (size < PAGE_SIZE / 2)
	{
		size += entrySize(*(keysIt++), *(pagesIt++), attribute);
	}

	size -= entrySize(*(--keysIt), *(--pagesIt), attribute);

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
	numEntries = keys.size() - 1;

}

int InternalPage::getKeyNum(const BTreeKey& key, const Attribute& attr)
{
	vector<PageNum>::iterator pageIt = pageNums.begin();
	vector<BTreeKey>::iterator keyIt = keys.begin();

	pageIt++;
	keyIt++;

	int i = 0;
	for (; i < numEntries; i++)
	{
		int comp = (*keyIt).compare(key, attr);
		if (comp > 0)
		{
			return i;
		}
		else if (comp == 0)
		{
			i++;
			return i;
		}

		pageIt++;
		keyIt++;
	}

	return i;

//	for (int i = 1; i <= numEntries; i++)
//	{
//		if (key.compare(keys[i], attr) == 0)
//		{
//			return i;
//		}
//	}
//    // Return 0th because the key is smaller than all keys in the page
//	return 0;
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

PageNum InternalPage::getPageNum(int num)
{
	return pageNums[num];
}

void InternalPage::setPageNum(int num, PageNum pageNum)
{
	pageNums[num] = pageNum;
}

PageNum InternalPage::findSubtree(const BTreeKey & key, const Attribute& attr)
{
	if (key.value == NULL)
	{
		return pageNums[0];
	}
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
	ushort oldKeySize = oldKey.keySize(attr);
	ushort newKeySize = newKey.keySize(attr);
	for (int i = 1; i <= numEntries; i++)
	{
		int comp = keys[i].compare(oldKey, attr);
		if (comp == 0)
		{
//update the new key here
			//keys[i] = newKey;
			keys[i].copyFrom(attr, newKey);
			spaceUsed = spaceUsed - oldKeySize + newKeySize;
		}
	}
}

RC InternalPage::deleteEntry(const BTreeKey& key, const Attribute& attr)
{
	vector<BTreeKey>::iterator keyIt = keys.begin();
	vector<PageNum>::iterator pageIt = pageNums.begin();

	ushort keySize = entrySize(key, 0, attr);

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
			spaceUsed -= keySize;
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

void InternalPage::insertHeadKeyAndPageNum(const BTreeKey& key, PageNum pageNum,
		const Attribute& attr)
{
// Insert key and a dummy page number
	insertEntry(key, -1, attr);

// copy 0th page number to 1st
	PageNum headerPN = getPageNum(0);
	setPageNum(1, headerPN);

// update 0th page number as the pageNum
	setPageNum(0, pageNum);
}

void InternalPage::deleteHeadKeyAndPageNum(const Attribute& attr)
{
	BTreeKey firstKey = this->getKey(1);
	PageNum firstPageNum = this->getPageNum(1);
	this->deleteEntry(firstKey, attr);
	this->setPageNum(0, firstPageNum);
	firstKey.free();
}

void InternalPage::appendEntry(const BTreeKey& key, PageNum pageNum, const Attribute& attr)
{
	keys.push_back(key);
	pageNums.push_back(pageNum);
	spaceUsed += entrySize(key, pageNum, attr);
	numEntries++;

}

void InternalPage::getChildNeighborNum(const int childNum, int &neighborNum, bool &isLeftNeighbor)
{
	isLeftNeighbor = false;
	if (childNum == numEntries)
	{ // last entry, merging left
		neighborNum = childNum - 1;
		isLeftNeighbor = true;
	}
	else if (childNum < numEntries && childNum >= 0)
	{
		neighborNum = childNum + 1;
		isLeftNeighbor = false;
	}
	else
	{
		logError("Invalid child entry " + childNum
				+ ", num of entries " + numEntries
				+ ", check getKeyNum method!");
	}
}

void InternalPage::redistributeInternals(const Attribute& attr, BTreeKey &key,
		InternalPage *leftPage, InternalPage *rightPage)
{
	vector<BTreeKey> leftKeys = leftPage->getKeys();
	vector<BTreeKey> rightKeys = rightPage->getKeys();
	vector<PageNum> leftPNs = leftPage->getPageNums();
	vector<PageNum> rightPNs = rightPage->getPageNums();

	leftKeys.push_back(key);
	leftKeys.insert(leftKeys.end(), rightKeys.begin() + 1, rightKeys.end());
	leftPNs.insert(leftPNs.end(), rightPNs.begin(), rightPNs.end());

	int midKeyIdx = leftKeys.size() / 2;
	//int midPNIdx = midKeyIdx + 1;
	//TODO:
	int midPNIdx = midKeyIdx;

	BTreeKey newKey = *(leftKeys.begin() + midKeyIdx);

	rightKeys.erase(rightKeys.begin() + 1, rightKeys.end());
	rightKeys.insert(rightKeys.begin() + 1, leftKeys.begin() + midKeyIdx + 1, leftKeys.end());
	leftKeys.erase(leftKeys.begin() + midKeyIdx, leftKeys.end());

	rightPNs.clear();
	rightPNs.insert(rightPNs.begin(), leftPNs.begin() + midPNIdx, leftPNs.end());
	leftPNs.erase(leftPNs.begin() + midPNIdx, leftPNs.end());

	leftPage->replaceVectors(attr, leftKeys, leftPNs);
	rightPage->replaceVectors(attr, rightKeys, rightPNs);

	this->updateKey(key, newKey, attr);

	key = newKey;
}

void InternalPage::mergeInternals(const Attribute& attr, BTreeKey &key, InternalPage *leftPage,
		InternalPage *rightPage)
{
// Get separation key --> key
// Get leftmost PageNum from right page
	PageNum zerothPageNumInRightPage = rightPage->getPageNum(0);

// Write separationKey+leftmostPN to left page
	leftPage->appendEntry(key, zerothPageNumInRightPage, attr);

// Wrtie all entries from right page to left page
	for (int i = 1; i <= rightPage->getNumEntries(); i++)
	{
		BTreeKey iterK = rightPage->getKey(i);
		PageNum iterPN = rightPage->getPageNum(i);
		leftPage->appendEntry(iterK, iterPN, attr);
	}

	rightPage->reset();
}

void InternalPage::mergeToRoot(const Attribute& attr, BTreeKey &key, InternalPage *leftPage,
		InternalPage *rightPage)
{
	vector<BTreeKey> leftKeys = leftPage->getKeys();
	vector<BTreeKey> rightKeys = rightPage->getKeys();
	vector<PageNum> leftPNs = leftPage->getPageNums();
	vector<PageNum> rightPNs = rightPage->getPageNums();

	leftKeys.push_back(key);
	leftKeys.insert(leftKeys.end(), rightKeys.begin() + 1, rightKeys.end());
	leftPNs.insert(leftPNs.end(), rightPNs.begin(), rightPNs.end());

	this->replaceVectors(attr, leftKeys, leftPNs);

	leftPage->reset();
	rightPage->reset();
}

void InternalPage::appendPageNum(PageNum pageNum, const Attribute& attr)
{
	pageNums.push_back(pageNum);
	spaceUsed += sizeof(PageNum);
}

void InternalPage::replaceVectors(const Attribute& attr, vector<BTreeKey>& newKeys,
		vector<PageNum>& newPNs)
{
	reset();
	keys = newKeys;
	pageNums = newPNs;

	numEntries = newKeys.size() - 1;
	spaceUsed += sizeof(PageNum);
	for (int i = 1; i <= numEntries; i++)
	{
		spaceUsed += newKeys[i].keySize(attr);
		spaceUsed += sizeof(PageNum);
	}
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
	}

}

//is there enough space to hold a new entry?
bool LeafPage::isFull(const BTreeKey& key, const Attribute& attr)
{
//a new key (value + RID) + page num
	return spaceUsed + key.keySize(attr) > PAGE_SIZE;
}

void LeafPage::distributeTo(LeafPage& rhs, const Attribute& attr, const BTreeKey& key,
		BTreeKey& newKey)
{
	insertKey(key, attr);

	vector<BTreeKey>::iterator it = keys.begin();
//skip the first key
	it++;

	int size = PAGE_HEADER_SIZE;
	while (size < PAGE_SIZE / 2)
	{
		size += (*it).keySize(attr);
		it++;
	}

	size -= (*(--it)).keySize(attr);

	vector<BTreeKey>::iterator eraseBegin = it;

	newKey.copyFrom(attr, *it);

	while (it != keys.end())
	{
		rhs.appendKey(*it, attr);
		it++;
	}

//erase the remaining keys from this page
	keys.erase(eraseBegin, keys.end());
	spaceUsed = size;
	numEntries = keys.size() - 1;

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

int LeafPage::deleteKey(const BTreeKey& key, const Attribute& attr)
{
	ushort keySize = key.keySize(attr);

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
			spaceUsed -= keySize;
			numEntries--;
			return i;
		}
		keyIt++;

	}

	return -1;
}

int LeafPage::redistribute(const Attribute& attr, BTreePage *neighbor, bool isLeftNeighbor,
		BTreeKey &newKey)
{
	assert(!this->isHalfFull());
	assert(neighbor->isHalfFull());

	LeafPage* neighborPage = (LeafPage *) neighbor;

	int numThisKeys = this->getNumEntries();
	int numNeighborKeys = neighborPage->getNumEntries();
	int mid = (numThisKeys + numNeighborKeys) / 2;
	int count = 0;
	if (isLeftNeighbor)
	{
		BTreeKey tmpFirst;
		for (int i = 0; i < numNeighborKeys - mid; i++)
		{
			BTreeKey tmpKey = neighborPage->getKey(neighborPage->getNumEntries()); // Always get from end
			this->insertKey(tmpKey, attr);
			tmpFirst = this->getKey(1);
			assert(0 == tmpFirst.compare(tmpKey, attr));
			neighborPage->eraseKey(neighborPage->getNumEntries(), tmpKey, attr);
			count++;
		}

		if (!this->isHalfFull())
		{
			logError("Redistribution failed, now size is "
					+ this->getNumEntries() + ", originally " + numThisKeys);
			return 0;
		}

		newKey = this->getKey(1);    // set newKey for the right page
		return count;
	}
	else
	{
		BTreeKey tmpLast;
		for (int i = 0; i < numNeighborKeys - mid; i++)
		{
			BTreeKey tmpKey = neighborPage->getKey(1);   // Always get from beginning
			this->insertKey(tmpKey, attr);
			tmpLast = this->getKey(this->getNumEntries());
			assert(0 == tmpLast.compare(tmpKey, attr));
			neighborPage->eraseKey(1, tmpKey, attr);
			count++;
		}

		if (!this->isHalfFull())
		{
			logError("Redistribution failed, now size is "
					+ this->getNumEntries() + ", originally " + numThisKeys);
			return 0;
		}

		newKey = neighborPage->getKey(1);    // set newKey for the right page
		return count;
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

int LeafPage::merge(const Attribute& attr, LeafPage *neighborPage, bool isLeftNeighbor)
{
	int count = 0;
	if (isLeftNeighbor)
	{
		// Still merge to current page --> for scan operation
		int numLeftKeys = neighborPage->getNumEntries();
		for (int i = numLeftKeys; i > 0; i--)
		{
			BTreeKey tmpKey = neighborPage->getKey(i);
			this->insertKey(tmpKey, attr);
			BTreeKey tmpFirst = this->getKey(1);
			assert(0 == tmpFirst.compare(tmpKey, attr));
			count++;
		}
		//neighborPage->setSibling(this->getSibling());

		// Reset current page
		neighborPage->reset();
		return count;
	}
	else
	{
		int numRightKeys = neighborPage->getNumEntries();
		for (int i = 1; i <= numRightKeys; i++)
		{
			BTreeKey tmpKey = neighborPage->getKey(i);
			this->insertKey(tmpKey, attr);
			BTreeKey tmpLast = this->getKey(this->getNumEntries());
			assert(0 == tmpLast.compare(tmpKey, attr));
			count++;
		}

		this->setSibling(neighborPage->getSibling());

		// Reset neighbor page
		neighborPage->reset();
		return count;
	}
}

void LeafPage::eraseKey(int num, const BTreeKey &key, const Attribute &attr)
{
	assert(num > 0);
	keys.erase(keys.begin() + num);
	spaceUsed -= key.keySize(attr);
	numEntries = keys.size() - 1;
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

BTreePage* IndexManager::readPage(IXFileHandle& ixfileHandle, PageNum pageNum,
		const Attribute& attr) const
{
	void * data = malloc(PAGE_SIZE);
	ixfileHandle.readPage(pageNum, data);
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

void IndexManager::deleteIterator(IX_ScanIterator* iterator)
{
	for (vector<IX_ScanIterator*>::iterator it = iterators.begin(); it != iterators.end(); it++)
	{
		if (*it == iterator)
		{
			iterators.erase(it);
			return;
		}

	}

}

void IndexManager::updateIterator(IXFileHandle& ixfileHandle, PageNum pageNum, unsigned keyNum,
		int rightOffset, const Attribute& attr)
{

	for (int i = 0; i < iterators.size(); i++)
	{
		IX_ScanIterator * it = iterators[i];
		if (it->pageNum == pageNum && it->keyNum >= keyNum)
		{
			it->keyNum = it->keyNum - 1 + rightOffset;
			delete it->leafPage;
			it->leafPage = (LeafPage *) readPage(ixfileHandle, pageNum, attr);
		}
	}
}

void IndexManager::deleteIteratorKey(IXFileHandle& ixfileHandle, PageNum pageNum, unsigned keyNum,
		const BTreeKey& key, const Attribute& attr)
{
	for (int i = 0; i < iterators.size(); i++)
	{
		IX_ScanIterator * it = iterators[i];
		// We cannot delete key from other ixfileHandles
		// since each one corresponding to a separate index file
		// Same for updateIteratorPage
		if (it->pageNum == pageNum && it->fileHandle == &ixfileHandle)
		{
			it->leafPage->deleteKey(key, attr);
			if (it->keyNum >= keyNum)
			{
				it->keyNum--;
			}
		}
	}
}

void IndexManager::updateIteratorPage(IXFileHandle& ixfileHandle, PageNum pageNum, int newKeyCount,
		const Attribute& attr)
{
	for (int i = 0; i < iterators.size(); i++)
	{
		IX_ScanIterator * it = iterators[i];
		if (it->pageNum == pageNum && it->fileHandle == &ixfileHandle)
		{
			it->keyNum = it->keyNum + newKeyCount;
			delete it->leafPage;
			it->leafPage = (LeafPage *) readPage(ixfileHandle, pageNum, attr);
		}
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
//append two pages here, one for root page num, one for (empty) root page
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

BTreePage * IndexManager::getRootPage(IXFileHandle& ixfileHandle, const Attribute& attribute)
{
	BTreePage* rootPage = NULL;
	if (ixfileHandle.getRootPage() > 0)
	{
		rootPage = readPage(ixfileHandle, ixfileHandle.getRootPage(), attribute);
	}
	else
	{
//create a new root page here
		rootPage = new LeafPage();
		rootPage->reset();
		appendPage(ixfileHandle, rootPage, attribute);
		ixfileHandle.setRootPage(rootPage->pageNum);
	}
	return rootPage;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *key, const RID &rid)
{
	if (!ixfileHandle.handle.opened)
	{
		return -1;
	}

	BTreePage * rootPage = getRootPage(ixfileHandle, attribute);
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
		newRootPage->appendPageNum(ixfileHandle.getRootPage(), attribute);
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
			delete leafPage;
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
			delete internalPage;
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
			internalPage->flush(attribute);
			delete internalPage;
			splitted = false;
			return 0;
		}
	}
}

RC IndexManager::doDelete(IXFileHandle &ixfileHandle, const Attribute &attr, BTreePage* parent,
		BTreePage* current, const BTreeKey &key, bool &oldEntryNull, int &oldEntryNum)
{
	if (!current->isLeaf())
	{
		InternalPage* currentPage = (InternalPage *) current;

		//TODO: changed by luochen
		PageNum childPageNum = currentPage->findSubtree(key, attr);
		InternalPage* childPage = (InternalPage*) readPage(ixfileHandle, childPageNum, attr);

		if (doDelete(ixfileHandle, attr, currentPage, childPage, key, oldEntryNull, oldEntryNum)
				!= 0)
		{
			delete currentPage;
			return -1;
		}
		if (oldEntryNull)
		{
			//no recursive delete, we can return now
			// FIXME: Double free happens here
			writePage(ixfileHandle, currentPage, attr);
			delete currentPage;
			return 0;
		}
		else
		{
			//perform recursive delete
			BTreeKey oldKey = currentPage->getKey(oldEntryNum);
			//TODO: changed by luochen. This function must return 0.
			currentPage->deleteEntry(oldKey, attr);

			// If the page is half full or is the root, no further stuff needed
			if (currentPage->isHalfFull() || currentPage->pageNum == ixfileHandle.getRootPage())
			{
				writePage(ixfileHandle, currentPage, attr);
				delete currentPage;
				oldEntryNull = true;
				return 0;
			}
			else
			{
				InternalPage* parentPage = (InternalPage *) parent;

				int currentNum = parentPage->getKeyNum(key, attr);
				int neighborNum = -1;
				bool isLeftNeighbor = false;
				parentPage->getChildNeighborNum(currentNum, neighborNum, isLeftNeighbor);

				//TODO: currentKey == key?
				//Junjie: Not necessary, in internal pages, key might be different from the currentKey
				BTreeKey currentKey = parentPage->getKey(currentNum);
				BTreeKey neighborKey = parentPage->getKey(neighborNum);

				PageNum neighborPageNum = parentPage->getPageNum(neighborNum);
				InternalPage* neighborPage = (InternalPage *) readPage(ixfileHandle,
						neighborPageNum, attr);

				InternalPage* leftPage = (isLeftNeighbor) ? neighborPage : currentPage;
				InternalPage* rightPage = (isLeftNeighbor) ? currentPage : neighborPage;

				BTreeKey midKey;
				if (isLeftNeighbor)
				{
					midKey.copyFrom(attr, currentKey);
				}
				else
				{
					midKey.copyFrom(attr, neighborKey);
					//BTreeKey midKey = (isLeftNeighbor) ? currentKey : neighborKey;
				}

				if (neighborPage->isHalfFull())
				{
					parentPage->redistributeInternals(attr, midKey, leftPage, rightPage);

					oldEntryNull = true;
					//TODO: parent page should be flushed/deleted at upper level? or current level?
					//Junjie: if using pageNum, current level; if using BTreePage, upper
					//writePage(ixfileHandle, parentPage, attr);
					//delete parentPage;
					writePage(ixfileHandle, leftPage, attr);
					writePage(ixfileHandle, rightPage, attr);
					delete currentPage;
					delete neighborPage;

					return 0;
				}
				else
				{
					if (parentPage->getNumEntries() == 1
							&& parentPage->pageNum == ixfileHandle.getRootPage())
					{
						parentPage->mergeToRoot(attr, midKey, leftPage, rightPage);
						oldEntryNull = true;
						delete currentPage;
						delete neighborPage;
						return 0;
					}
					else
					{
						parentPage->mergeInternals(attr, midKey, leftPage, rightPage);
						oldEntryNull = false;
						oldEntryNum = (isLeftNeighbor) ? currentNum : neighborNum;
						// TODO: delete the empty page
						//TODO: this is a merge case. if a page is deleted, no need to write it back, right?
						writePage(ixfileHandle, leftPage, attr);
						delete currentPage;
						delete neighborPage;
						return 0;
					}

				}
			}
		}
	}
	else
	{

		LeafPage* currentPage = (LeafPage *) current;
		int index = currentPage->deleteKey(key, attr);

		// update iterator keys
		deleteIteratorKey(ixfileHandle, currentPage->pageNum, index, key, attr);

		if (index < 0)
		{
			delete currentPage;
			return -1;
		}
		if (currentPage->isHalfFull() || currentPage->pageNum == ixfileHandle.getRootPage())
		{
			//TODO: root page
			writePage(ixfileHandle, currentPage, attr);
			delete currentPage;
			oldEntryNull = true;

			//no need to update iterator here
			return 0;
		}
		else
		{
			InternalPage* parentPage = (InternalPage *) parent;

			int currentNum = parentPage->getKeyNum(key, attr);
			int neighborNum = -1;
			bool isLeftNeighbor = false;
			parentPage->getChildNeighborNum(currentNum, neighborNum, isLeftNeighbor);

			BTreeKey newKey;
			BTreeKey oldKey =
					(isLeftNeighbor) ?
							parentPage->getKey(currentNum) : parentPage->getKey(neighborNum);

			PageNum neighborPageNum = parentPage->getPageNum(neighborNum);
			LeafPage* neighborPage = (LeafPage *) readPage(ixfileHandle, neighborPageNum, attr);
			if (neighborPage->isHalfFull())
			{
				int count = currentPage->redistribute(attr, neighborPage, isLeftNeighbor, newKey);

				// replace key value in parent entry by newKey
				parentPage->updateKey(oldKey, newKey, attr);
				//parentPage->deleteEntry(oldKey, attr);

				oldEntryNull = true;

				//writePage(ixfileHandle, parentPage, attr);
				//delete parentPage;
				writePage(ixfileHandle, currentPage, attr);
				writePage(ixfileHandle, neighborPage, attr);
				delete currentPage;
				delete neighborPage;

				//redistribute

				if (isLeftNeighbor)
				{
					updateIteratorPage(ixfileHandle, currentPage->pageNum, count, attr);
					//updateIterator(ixfileHandle, currentPage->pageNum, index, count, attr);
				}
				else
				{
					updateIteratorPage(ixfileHandle, currentPage->pageNum, 0, attr);
					//updateIterator(ixfileHandle, currentPage->pageNum, index, 0, attr);
				}

				return 0;
			}
			else
			{
				// else, merge L and S
				//     oldchildentry = &(current entry in parent for M)
				//     move all entries from M to node on left
				//     discard empty node M, adjust sibling pointers, return
				int count = currentPage->merge(attr, neighborPage, isLeftNeighbor);

				if (parentPage->getNumEntries() == 1
						&& parentPage->pageNum == ixfileHandle.getRootPage())
				{
					ixfileHandle.setRootPage(currentPage->pageNum);

					oldEntryNull = true;

					writePage(ixfileHandle, currentPage, attr);
					//writePage(ixfileHandle, neighborPage, attr);
					delete currentPage;
					delete neighborPage;

					if (isLeftNeighbor)
					{
						updateIteratorPage(ixfileHandle, currentPage->pageNum, count, attr);
						//updateIterator(ixfileHandle, currentPage->pageNum, index, count, attr);
					}
					else
					{
						updateIteratorPage(ixfileHandle, currentPage->pageNum, 0, attr);
						//updateIterator(ixfileHandle, currentPage->pageNum, index, 0, attr);
					}

					return 0;
				}

				if (isLeftNeighbor && neighborNum >= 1)
				{
					PageNum leftLeftNeighborPageNum = parentPage->getPageNum(neighborNum - 1);
					LeafPage* leftLeftNeighborPage = (LeafPage *) readPage(ixfileHandle,
							leftLeftNeighborPageNum, attr);
					leftLeftNeighborPage->setSibling(currentPage->pageNum);
					writePage(ixfileHandle, leftLeftNeighborPage, attr);
					delete leftLeftNeighborPage;
				}

				oldEntryNull = false;
				oldEntryNum = (isLeftNeighbor) ? currentNum : neighborNum;
				// TODO: delete the empty page

				writePage(ixfileHandle, currentPage, attr);
				//writePage(ixfileHandle, neighborPage, attr);
				if (isLeftNeighbor)
				{
					updateIteratorPage(ixfileHandle, currentPage->pageNum, count, attr);
					//updateIterator(ixfileHandle, currentPage->pageNum, index, count, attr);
				}
				else
				{
					updateIteratorPage(ixfileHandle, currentPage->pageNum, 0, attr);
					//updateIterator(ixfileHandle, currentPage->pageNum, index, 0, attr);
				}
				delete currentPage;
				delete neighborPage;
				return 0;
			}
		}
	}

	return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *key, const RID &rid)
{
	if (!ixfileHandle.handle.opened)
	{
		return -1;
	}
	BTreePage * rootPage = getRootPage(ixfileHandle, attribute);
	BTreeKey treeKey(attribute, key, rid);

	bool isNullNode = true;
	int dummyNum;

	RC rc = doDelete(ixfileHandle, attribute, rootPage, rootPage, treeKey, isNullNode, dummyNum);

	treeKey.free();
	return rc;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *lowKey,
		const void *highKey, bool lowKeyInclusive, bool highKeyInclusive,
		IX_ScanIterator &ix_ScanIterator)
{
	if (!ixfileHandle.handle.opened)
	{
		return -1;
	}
	ix_ScanIterator.fileHandle = &ixfileHandle;
	ix_ScanIterator.attribute = attribute;
	ix_ScanIterator.lowKey = lowKey;
	ix_ScanIterator.highKey = highKey;
	ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
	ix_ScanIterator.highKeyInclusive = highKeyInclusive;

	ix_ScanIterator.end = false;

	BTreeKey treeKey;
	bool found = false;
	ix_ScanIterator.leafPage = findKey(ixfileHandle, lowKey, lowKeyInclusive, attribute,
			ix_ScanIterator.pageNum, ix_ScanIterator.keyNum, found, treeKey);

//we need to check treeKey
	if (found != true || ix_ScanIterator.checkEnd(treeKey))
	{
		ix_ScanIterator.end = true;
	}
	//add iterator
	iterators.push_back(&ix_ScanIterator);
	return 0;
}

// Junjie: added a boolean varaible in case the treeKey is not found
// This case can happen when the leafPage is empty already
LeafPage* IndexManager::findKey(IXFileHandle& ixfilehandle, const void * key, bool inclusive,
		const Attribute& attribute, PageNum& pageNum, unsigned& keyNum, bool& found,
		BTreeKey & treeKey)
{
	found = false;

	BTreePage * page = getRootPage(ixfilehandle, attribute);
	while (!page->isLeaf())
	{
		InternalPage * internalPage = (InternalPage*) page;
		BTreeKey keyWrapper(attribute, key);
		PageNum childPageNum = internalPage->findSubtree(keyWrapper, attribute);
		keyWrapper.free();
		delete page;
		page = readPage(ixfilehandle, childPageNum, attribute);
	}

	LeafPage* leafPage = (LeafPage*) page;

	pageNum = leafPage->pageNum;

	keyNum = 1;
	if (key != NULL)
	{
		for (; keyNum <= leafPage->getNumEntries(); keyNum++)
		{
			treeKey = leafPage->getKey(keyNum);
			int comp = treeKey.compare(key, attribute);
			if (comp == 0 && inclusive)
			{
				//we get the key
				found = true;
				break;
			}
			else if (comp > 0)
			{
				//we also get the key
				found = true;
				break;
			}
		}
	}
	else
	{
//we simply get the first key
		if (leafPage->getNumEntries() >= keyNum)
		{
			found = true;
			treeKey = leafPage->getKey(keyNum);
		}
	}

	return leafPage;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const
{
	BTreePage * rootPage = readPage(ixfileHandle, ixfileHandle.getRootPage(), attribute);

	printBTreePage(ixfileHandle, rootPage, attribute, 0);
	cout << endl;

}

void IndexManager::printBTreePage(IXFileHandle &ixfileHandle, BTreePage * page,
		const Attribute& attribute, int height) const
{
	padding(height);
	cout << "{";

	if (page->pageNum == ixfileHandle.getRootPage())
	{
		cout << endl;
		padding(height);
	}

	cout << "\"keys\":";
	cout << "[";

	for (int i = 1; i <= page->getNumEntries(); i++)
	{
		BTreeKey key = page->getKey(i);
		key.print(attribute);
		if (i != page->getNumEntries())
		{
			cout << ",";
		}
	}

	cout << "]";

	if (!page->isLeaf())
	{
		cout << "," << endl;

		padding(height);
		cout << "\"children\":[" << endl;

		InternalPage * internalPage = (InternalPage*) page;
		for (int i = 0; i <= page->getNumEntries(); i++)
		{
			PageNum childPageNum = internalPage->getPageNum(i);
			BTreePage * childPage = readPage(ixfileHandle, childPageNum, attribute);

			printBTreePage(ixfileHandle, childPage, attribute, height + 1);
			if (i != page->getNumEntries())
			{
				cout << ",";
			}
			cout << endl;
		}

		padding(height);
		cout << "]";
	}
	if (page->pageNum == ixfileHandle.getRootPage())
	{
		cout << endl;
		padding(height);
	}

	cout << "}";

	delete page;

}

void IndexManager::padding(int height) const
{
	for (int i = 0; i < height; i++)
	{
		cout << "  ";
	}
}

IX_ScanIterator::IX_ScanIterator()
{
	pageNum = 0;
	keyNum = 0;
	leafPage = NULL;
	lowKey = NULL;
	highKey = NULL;
	lowKeyInclusive = false;
	highKeyInclusive = false;
	fileHandle = NULL;
	end = false;
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextKeyWithinPage(BTreeKey& key)
{
	if (keyNum > leafPage->getNumEntries())
	{
		return -1;
	}
	key = leafPage->getKey(keyNum++);
	return 0;
}

bool IX_ScanIterator::checkEnd(const BTreeKey& key)
{
	if (highKey != NULL)
	{
		int comp = key.compare(highKey, attribute);
		if ((comp == 0 && !highKeyInclusive) || comp > 0)
		{
			return true;
		}
	}
	return false;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	if (end)
	{
		return -1;
	}

	BTreeKey treeKey;
	if (getNextKeyWithinPage(treeKey) != 0)
	{
//load next page
		pageNum = leafPage->getSibling();
		if (pageNum == 0)
		{
//no more sibling page
			end = true;
			return -1;
		}
		delete leafPage;
		leafPage = (LeafPage *) IndexManager::instance()->readPage(*fileHandle, pageNum, attribute);
//start over
		keyNum = 1;
		getNextKeyWithinPage(treeKey);
	}

//check key
	if (checkEnd(treeKey))
	{
		end = true;
		return -1;
	}

	rid = treeKey.rid;
	copyAttributeData(key, 0, attribute, treeKey.value, 0);
	return 0;
}

RC IX_ScanIterator::close()
{
	if (leafPage != NULL)
	{
		delete leafPage;
		leafPage = NULL;
	}

	IndexManager::instance()->deleteIterator(this);

	return 0;
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

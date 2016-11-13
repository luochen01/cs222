#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;
class IXFileHandle;

class BTreeKey
{
private:
	ushort copyValue(const Attribute& attr, const void * value);

public:
	void * value;
	RID rid;

	BTreeKey();

	BTreeKey(const Attribute& attr, const void * value, const RID& rid);

	BTreeKey(const Attribute& attr, const void * value);

	void free();

	/**
	 * < 0: this < rhs
	 * 0, this == rhs
	 * > 0, this > rhs
	 */
	int compare(const void * rhs, const Attribute& attr) const;

	/**
	 * < 0: this < rhs
	 * 0, this == rhs
	 * > 0, this > rhs
	 */
	int compare(const BTreeKey& rhs, const Attribute& attr) const;

	ushort keySize(const Attribute& attr) const;

	ushort readFrom(const Attribute& attr, const void * data);

	ushort writeTo(const Attribute& attr, void * data) const;

	void copyFrom(const Attribute& attr, const BTreeKey &rhs);

	void print(const Attribute& attr) const;
};

/**
 * bool isLeaf, ushort numEntry, ushort spaceUsed, unsigned sibling(for leaf),
 * page0, key1, page1, key2, page2, ..., key n, page n
 *nextFreeSpace points to the end of page n+1
 */
class BTreePage
{
protected:
	friend class IndexManager;

	PageNum pageNum;

	void * data;

	vector<BTreeKey> keys;

	ushort numEntries;

	ushort spaceUsed;

	const int PAGE_HEADER_SIZE;

	static const int NUM_ENTRIES_OFFSET = sizeof(bool);

	static const int SPACE_USED_OFFSET = NUM_ENTRIES_OFFSET + sizeof(ushort);

	ushort readKey(BTreeKey& key, ushort offset, const Attribute& attr);

	ushort writeKey(const BTreeKey& key, ushort offset, const Attribute& attr);

public:
	BTreePage(void * data, PageNum pageNum, int headerSize);

	virtual void initialize(const Attribute& attr);

	virtual void flush(const Attribute& attr);

	virtual void reset();

	BTreeKey getKey(int num);

	virtual bool isLeaf() = 0;

	//is there enough space to hold a new entry?
	virtual bool isFull(const BTreeKey& key, const Attribute& attr) = 0;

	bool isHalfFull();

	ushort keySize(ushort offset, const Attribute& attr)
	{
		return attributeSize(attr, (byte*) data + offset) + 2 * sizeof(unsigned);
	}

	//TODO needs API for redistribute, merge, split two pages

	virtual ~BTreePage()
	{
		free(data);
	}

	vector<BTreeKey>& getKeys()
	{
		return keys;
	}

	ushort getSpaceUsed()
	{
		return spaceUsed;
	}

	ushort getNumEntries()
	{
		return numEntries;
	}

};

class InternalPage: public BTreePage
{
protected:
	friend class IndexManager;
	vector<PageNum> pageNums;

	ushort readPageNum(PageNum& pageNum, ushort offset);

	ushort writePageNum(PageNum pageNum, ushort offsert);

	ushort writeEntry(const BTreeKey& key, PageNum pageNum, ushort offset, const Attribute& attr);

	ushort readEntry(BTreeKey& key, PageNum& pageNum, ushort offset, const Attribute& attr);

	ushort entrySize(const BTreeKey& key, PageNum pageNum, const Attribute& attr);

public:

	InternalPage();

	InternalPage(void * data, PageNum pageNum);

	void initialize(const Attribute& attr);

	void reset();

	void flush(const Attribute& attr);

	PageNum getPageNum(int num);

	void setPageNum(int num, PageNum pageNum);

	PageNum findSubtree(const BTreeKey & key, const Attribute& attr);

	void updateKey(const BTreeKey& oldKey, const BTreeKey& newKey, const Attribute& attr);

	RC deleteEntry(const BTreeKey& key, const Attribute& attr);

	void insertEntry(const BTreeKey& key, PageNum pageNum, const Attribute& attr);

	void insertHeadKeyAndPageNum(const BTreeKey& key, PageNum pageNum, const Attribute& attr);

	void deleteHeadKeyAndPageNum(const Attribute& attr);

	void appendEntry(const BTreeKey& key, PageNum pageNum, const Attribute& attr);

	void appendPageNum(PageNum pageNum, const Attribute& attr);

	bool isFull(const BTreeKey & key, const Attribute& attribute);

	void distributeTo(InternalPage& rhs, const Attribute& attribute, BTreeKey& newKey,
			PageNum& newPage);

	int getKeyNum(const BTreeKey& key, const Attribute& attr);

	ushort entrySize(ushort offset, const Attribute& attr)
	{
		ushort result = pageNumSize(offset);
		result += keySize(offset + result, attr);
		return result;
	}

	ushort pageNumSize(ushort offset)
	{
		return sizeof(ushort);
	}

	bool isLeaf()
	{
		return false;
	}

	virtual ~InternalPage()
	{

	}

	vector<PageNum>& getPageNums()
	{
		return pageNums;
	}

	void getChildNeighborNum(const int childNum, int &neighborNum, bool &isLeftNeighbor);

	void redistributeInternals(const Attribute& attr, BTreeKey &key, InternalPage *leftPage,
			InternalPage *rightPage);

	void mergeInternals(const Attribute& attr, BTreeKey &key, InternalPage *leftPage,
			InternalPage *rightPage);

	//void redistribute(const Attribute& attr, BTreePage *neighbor, bool isLeftNeighbor,
	//		BTreeKey &newKey);

	//void merge(const Attribute& attr, BTreePage *neighbor, bool isLeftNeighbor);
};

class LeafPage: public BTreePage
{
protected:
	static const int SIBLING_OFFSET = SPACE_USED_OFFSET + sizeof(ushort);

	PageNum siblingPage;

public:
	LeafPage();

	LeafPage(void * data, PageNum pageNum);

	void initialize(const Attribute& attr);

	void reset();

	void flush(const Attribute& attr);

	void insertKey(const BTreeKey& key, const Attribute& attr);

	void updateKey(const BTreeKey& oldKey, const BTreeKey& newKey, const Attribute& attr);

	int deleteKey(const BTreeKey& key, const Attribute& attr);

	void appendKey(const BTreeKey& key, const Attribute& attr);

	//void deleteKey(const BTreeKey& key, const Attribute& attr);

	bool containsKey(const BTreeKey& key, const Attribute& attr);

	bool isFull(const BTreeKey& key, const Attribute& attr);

	void distributeTo(LeafPage& rhs, const Attribute& attr, const BTreeKey& key, BTreeKey& newKey);

	bool isLeaf()
	{
		return true;
	}

	virtual ~LeafPage()
	{

	}

	PageNum getSibling()
	{
		return siblingPage;
	}

	void setSibling(PageNum pageNum)
	{
		this->siblingPage = pageNum;
	}

	void redistribute(const Attribute& attr, BTreePage *neighbor, bool isLeftNeighbor,
			BTreeKey &newKey);

	void merge(const Attribute& attr, BTreePage *neighbor, bool isLeftNeighbor);
};

class IndexManager
{
public:
	static IndexManager* instance();

	BTreePage* readPage(IXFileHandle & ixfileHandle, PageNum pageNum, const Attribute& attr) const;

	void writePage(IXFileHandle & ixfileHandle, BTreePage* page, const Attribute& attr);

	void appendPage(IXFileHandle & ixfileHandle, BTreePage* page, const Attribute& attr);

	// Create an index file.
	RC createFile(const string &fileName);

	// Delete an index file.
	RC destroyFile(const string &fileName);

	// Open an index and return an ixfileHandle.
	RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

	// Close an ixfileHandle for an index.
	RC closeFile(IXFileHandle &ixfileHandle);

	// Insert an entry into the given index that is indicated by the given ixfileHandle.
	RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key,
			const RID &rid);

	// Delete an entry from the given index that is indicated by the given ixfileHandle.
	RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key,
			const RID &rid);

	// Initialize and IX_ScanIterator to support a range search
	RC scan(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *lowKey,
			const void *highKey, bool lowKeyInclusive, bool highKeyInclusive,
			IX_ScanIterator &ix_ScanIterator);

	// Print the B+ tree in pre-order (in a JSON record format)
	void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

	void deleteIterator(IX_ScanIterator* iterator);

protected:
	IndexManager();
	~IndexManager();

private:
	static IndexManager *_index_manager;

	vector<IX_ScanIterator*> iterators;

	void * buffer;

	RC doDelete(IXFileHandle &ixfileHandle, const Attribute &attr, BTreePage* parent,
			BTreePage * current, const BTreeKey &key, bool &oldEntryNull, int &oldEntryNum);

	RC doInsert(IXFileHandle & ixfileHandle, BTreePage * page, const Attribute& attribute,
			const BTreeKey& key, bool& splitted, BTreeKey & newKey, PageNum & newPage);

	LeafPage* findKey(IXFileHandle& ixfilehandle, const void * key, bool inclusive,
			const Attribute& attribute, PageNum& pageNum, unsigned& keyNum, BTreeKey & treeKey);

	void printBTreePage(IXFileHandle &ixfileHandle, BTreePage* page, const Attribute &attribute,
			int height) const;

	void padding(int height) const;

	void updateIterator(IXFileHandle& ixfileHandle, PageNum pageNum, unsigned keyNum,
			const Attribute& attr);

	BTreePage * getRootPage(IXFileHandle & ixfileHandle, const Attribute& attribute);

};

class IX_ScanIterator
{
private:
	friend class IndexManager;

	IXFileHandle * pFileHandle;
	PageNum pageNum;
	unsigned keyNum;
	LeafPage * leafPage;
	Attribute attribute;
	const void * lowKey;
	const void * highKey;
	bool lowKeyInclusive;
	bool highKeyInclusive;

	bool end;

	RC getNextKeyWithinPage(BTreeKey& key);

	bool checkEnd(const BTreeKey& key);

public:

	// Constructor
	IX_ScanIterator();

	// Destructor
	~IX_ScanIterator();

	// Get next matching entry
	RC getNextEntry(RID &rid, void *key);

	// Terminate index scan
	RC close();
};

class IXFileHandle
{
private:
	friend class IndexManager;

	FileHandle handle;

	PageNum rootPage;

public:

	// variables to keep counter for each operation
	unsigned ixReadPageCounter;
	unsigned ixWritePageCounter;
	unsigned ixAppendPageCounter;

	// Constructor
	IXFileHandle();

	// Destructor
	~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
			unsigned &appendPageCount);

	RC readPage(PageNum pageNum, void *data);                             // Get a specific page

	RC writePage(PageNum pageNum, const void *data);                      // Write a specific page

	RC appendPage(const void *data);                                      // Append a specific page

	PageNum getRootPage();

	void setRootPage(PageNum rootPage);

	unsigned getNumberOfPages();                              // Get the number of pages in the file

};

#endif

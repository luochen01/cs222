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
public:
	void * value;
	RID rid;

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

};

/**
 * bool isLeaf, ushort numEntry, ushort spaceUsed, unsigned sibling(for leaf),
 * page0, key1, page1, key2, page2, ..., key n, page n
 *nextFreeSpace points to the end of page n+1
 */
class BTreePage
{
protected:
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
	BTreePage(void * data, int headerSize);

	virtual void initialize(const Attribute& attr);

	virtual void flush(const Attribute& attr);

	virtual void reset();

	void getKey(int num, BTreeKey& key);

	virtual bool isLeaf() = 0;

	//is there enough space to hold a new entry?
	bool isFull(const Attribute& attr, const void* value);

	bool isHalfFull();

	ushort keySize(ushort offset, const Attribute& attr)
	{
		return attributeSize(attr, (byte*) data + offset) + 2 * sizeof(unsigned);
	}

	//TODO needs API for redistribute, merge, split two pages

	virtual ~BTreePage()
	{

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

	virtual void updateKey(const BTreeKey& oldKey, const BTreeKey& newKey, const Attribute& attr);
	virtual void insertKey(const BTreeKey& key, const Attribute& attr);
	virtual void insertKey(const BTreeKey& key, PageNum pageNum, const Attribute& attr);
    virtual void getKeyNum(const BTreeKey& key, const Attribute& attr, int &num);
	virtual void getPageNum(int num, PageNum& pageNum);
	virtual RC deleteKey(const BTreeKey& key, const Attribute& attr);
    virtual void clear();
    virtual RC redistribute(const Attribute& attr, BTreePage *neighbor, bool isLeftNeighbor, BTreeKey &newKey);
    virtual RC merge(const Attribute& attr, BTreePage *neighbor, bool isLeftNeighbor);
};

class InternalPage: public BTreePage
{
protected:
	vector<PageNum> pageNums;

	ushort readPageNum(PageNum& pageNum, ushort offset);

	ushort writePageNum(PageNum pageNum, ushort offsert);

	ushort writeEntry(const BTreeKey& key, PageNum pageNum, ushort offset, const Attribute& attr);

	ushort readEntry(BTreeKey& key, PageNum& pageNum, ushort offset, const Attribute& attr);

	ushort entrySize(const BTreeKey& key, PageNum pageNum, const Attribute& attr);

public:

	InternalPage(void * data);

	void initialize(const Attribute& attr);

	void reset();

	void flush(const Attribute& attr);

	void getPageNum(int num, PageNum& pageNum);

	void updateKey(const BTreeKey& oldKey, const BTreeKey& newKey, const Attribute& attr);

	RC deleteKey(const BTreeKey& key, const Attribute& attr);

	void insertKey(const BTreeKey& key, PageNum pageNum, const Attribute& attr);

    void getKeyNum(const BTreeKey& key, const Attribute& attr, int &num);

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

    void clear()
    {
        free(data);
        reset();
    }

    RC redistribute(const Attribute& attr, BTreePage *neighbor, bool isLeftNeighbor, BTreeKey &newKey);

    RC merge(const Attribute& attr, BTreePage *neighbor, bool isLeftNeighbor);
};

class LeafPage: public BTreePage
{
protected:
	static const int SIBLING_OFFSET = SPACE_USED_OFFSET + sizeof(ushort);

	PageNum siblingPage;

public:

	LeafPage(void * data);

	void initialize(const Attribute& attr);

	void reset();

	void flush(const Attribute& attr);

	void insertKey(const BTreeKey& key, const Attribute& attr);

	RC deleteKey(const BTreeKey& key, const Attribute& attr);

    void getKeyNum(const BTreeKey& key, const Attribute& attr, int &num) {/* FIXME */ return -1;};

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

    void clear()
    {
        free(data);
        reset();
    }

    RC redistribute(const Attribute& attr, BTreePage *neighbor, bool isLeftNeighbor, BTreeKey &newKey);

    RC merge(const Attribute& attr, BTreePage *neighbor, bool isLeftNeighbor);
};

class IndexManager
{
private:
	void * buffer;

public:
	static IndexManager* instance();

	BTreePage* readPage(IXFileHandle & fileHandle, PageNum pageNum);

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

protected:
	IndexManager();
	~IndexManager();

private:
	static IndexManager *_index_manager;

    RC doDelete(IXFileHandle &ixfileHandle, const Attribute& attr, PageNum parent, PageNum node,
            const BTreeKey &key, bool oldEntryNull, BTreeKey &oldEntryKey);
};

class IX_ScanIterator
{
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

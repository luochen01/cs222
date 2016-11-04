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
 * bool isLeaf, ushort numEntry, ushort nextFreeSpace, unsigned sibling(for leaf),
 * page0, key1, page1, key2, page2, ..., key n, page n
 *nextFreeSpace points to the end of page n+1
 */
class BTreePage
{
protected:
	void * data;

	const int PAGE_HEADER_SIZE;

	ushort readKey(BTreeKey& key, ushort offset, const Attribute& attr);

	ushort writeKey(const BTreeKey& key, ushort offset, const Attribute& attr);

public:
	BTreePage(void * data, int headerSize);

	virtual void initialize();

	void getKey(int num, BTreeKey& key, const Attribute& attr);

	virtual ushort getKeyOffset(int num, const Attribute& attr) = 0;

	ushort numEntry();

	void numEntry(ushort value);

	ushort nextFreeSpace();

	void nextFreeSpace(ushort value);

	virtual bool isLeaf() = 0;

	//is there enough space to hold a new entry?
	bool isFull(const Attribute& attr, const void* value);

	bool isHalfFull();

	void increaseSpace(ushort offset, int size);

	ushort keySize(ushort offset, const Attribute& attr)
	{
		return attributeSize(attr, (byte*) data + offset) + 2 * sizeof(unsigned);
	}

	//TODO needs API for redistribute, merge, split two pages

	virtual ~BTreePage()
	{

	}

};

class InternalPage: public BTreePage
{
protected:
	ushort readPageNum(PageNum& pageNum, ushort offset);

	ushort writePageNum(PageNum pageNum, ushort offsert);

	ushort InternalPage::writeEntry(const BTreeKey& key, PageNum pageNum, ushort offset,
			const Attribute& attr);

	ushort InternalPage::readEntry(BTreeKey& key, PageNum pageNum, ushort offset,
			const Attribute& attr);

public:

	InternalPage(void * data);

	void initialize();

	void getPageNum(int num, PageNum& pageNum, const Attribute& attr);

	/**
	 * An entry logically contains a page num and a key
	 */
	ushort getEntryOffset(int num, const Attribute& attr);

	ushort getKeyOffset(int num, const Attribute& attr);

	ushort getPageNumOffset(int num, const Attribute& attr);

	void updateKey(const BTreeKey& oldKey, const BTreeKey& newKey, const Attribute& attr);

	void deleteKey(const BTreeKey& key, const Attribute& attr);

	void insertKey(const BTreeKey& key, PageNum pageNum, const Attribute& attr);

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

};

class LeafPage: public BTreePage
{
protected:
public:

	LeafPage(void * data);

	void initialize();

	PageNum sibling();

	void sibling(PageNum pageNum);

	ushort getKeyOffset(int num, const Attribute& attr);

	void insertKey(const BTreeKey& key, const Attribute& attr);

	void deleteKey(const BTreeKey& key, const Attribute& attr);

	bool isLeaf()
	{
		return true;
	}

	virtual ~LeafPage()
	{

	}
};

class IndexManager
{
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

	const string& getFileName();

	RC readPage(PageNum pageNum, void *data);                             // Get a specific page

	RC writePage(PageNum pageNum, const void *data);                      // Write a specific page

	RC appendPage(const void *data);                                      // Append a specific page

	unsigned getNumberOfPages();                              // Get the number of pages in the file

};

#endif

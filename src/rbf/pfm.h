#ifndef _pfm_h_
#define _pfm_h_

typedef unsigned PageNum;
typedef int RC;
typedef char byte;

typedef unsigned short ushort;

#define PAGE_SIZE 4096

#include <string>
#include <cstring>
#include <fstream>
#include <climits>
#include <map>
#include <sys/stat.h>

using namespace std;

#ifdef LOG_ENABLED
#define logError(msg) cerr<<msg<<endl
#else
#define logError(msg)
#endif

template<typename T> void read(istream& is, T& value)
{
	is.read((byte*) &value, sizeof(T));
}

template<typename T> T read(istream& is, unsigned offset, T& value)
{
	is.seekg(offset);
	is.read((byte*) &value, sizeof(T));
}

template<typename T> void write(ostream& os, const T& value)
{
	os.write((char*) &value, sizeof(T));
}

template<typename T> void write(ostream& os, unsigned offset, const T& value)
{
	os.seekp(offset);
	os.write((char*) &value, sizeof(T));
}

string readString(istream& is);

void writeString(ostream& os, const string& value);

void write(ostream& os, const void * data, unsigned size);

void read(istream& is, void * data, unsigned size);

void write(ostream& os, unsigned offset, const void * data, unsigned dataOffset, unsigned size);

void read(istream& os, unsigned offset, void * data, unsigned dataOffset, unsigned size);

void write(ostream& os, unsigned offset, const void * data, unsigned size);

void read(istream& os, unsigned offset, void * data, unsigned size);

template<typename T> int read(const void * data, T& result, unsigned offset, unsigned size)
{
	memcpy(&result, ((byte*) data + offset), size);
	return size;
}

template<typename T> int write(void * data, const T& value, unsigned offset, unsigned size)
{
	memcpy((byte*) data + offset, &value, size);
	return size;
}

template<typename T> int read(const void * data, T& value, unsigned offset)
{
	return read(data, value, offset, sizeof(T));
}

template<typename T> int write(void * data, const T& value, unsigned offset)
{
	return write(data, value, offset, sizeof(T));
}

int readString(const void * data, string & value, unsigned offset);

int writeString(void * data, const string& value, unsigned offset);

int writeBuffer(void * to, unsigned toOffset, const void * from, unsigned fromOffset,
		unsigned size);

bool exists(const string &fileName);

void setBit(byte& in, bool value, unsigned offset);

bool readBit(byte in, unsigned offset);

void getByteOffset(unsigned pos, unsigned& byteNum, unsigned& offset);

void setAttrNull(void * data, ushort attrNum, bool isNull);

bool isAttrNull(const void * data, ushort attrNum);

class FileHandle;

class PagedFileManager
{

private:

public:
	static PagedFileManager* instance();                       // Access to the _pf_manager instance

	RC createFile(const string &fileName);                            // Create a new file

	RC destroyFile(const string &fileName);                            // Destroy a file

	RC openFile(const string &fileName, FileHandle &fileHandle);    // Open a file

	RC closeFile(FileHandle &fileHandle);                            // Close a file

protected:
	PagedFileManager();                                                   // Constructor

	~PagedFileManager();                                                  // Destructor

private:
	static PagedFileManager *_pf_manager;
};

const int FILE_HEADER_SIZE = sizeof(unsigned);

class FileHandle
{
private:
	friend class PagedFileManager;

	fstream fs;

	unsigned pageOffset(int pageNum);

public:
	unsigned pages;

	bool opened;

	// variables to keep the counter for each operation
	unsigned readPageCounter;
	unsigned writePageCounter;
	unsigned appendPageCounter;

	FileHandle();                                                         // Default constructor

	~FileHandle();                                                        // Destructor

	RC readPage(PageNum pageNum, void *data);                             // Get a specific page
	RC writePage(PageNum pageNum, const void *data);                      // Write a specific page
	RC appendPage(const void *data);                                      // Append a specific page
	unsigned getNumberOfPages();                              // Get the number of pages in the file
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
			unsigned &appendPageCount); // Put the current counter values into variables
};

#endif

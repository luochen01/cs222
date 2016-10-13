#include "pfm.h"
//#include "util.h"

#include<fstream>
#include<iostream>

void write(ostream& os, const void * data, unsigned size)
{
	os.write((byte*) data, size);
}

void read(istream& is, void * data, unsigned size)
{
	is.read((byte*) data, size);
}

void write(ostream& os, unsigned offset, const void * data, unsigned dataOffset, unsigned size)
{
	os.seekp(offset);
	os.write((byte*) data + dataOffset, size);

}

void read(istream& is, unsigned offset, void * data, unsigned dataOffset, unsigned size)
{
	is.seekg(offset);
	is.read((byte*) data + dataOffset, size);
}

void write(ostream& os, unsigned offset, const void * data, unsigned size)
{
	write(os, offset, data, 0, size);
}

void read(istream& is, unsigned offset, void * data, unsigned size)
{
	read(is, offset, data, 0, size);
}

string readString(istream& is)
{
	unsigned length;
	read(is, length);

	char* buffer = new char[length];
	read(is, buffer, length);
	string str(buffer, buffer + length);
	delete[] buffer;
	return str;
}

void writeString(ostream& os, const string& str)
{
	write(os, (ushort) str.size());
	os.write(str.c_str(), str.size());
}

string readString(void * data, unsigned offset)
{
	ushort size;
	read(data, size, offset);
	offset += sizeof(ushort);

	char* buffer = new char[size];
	memcpy(buffer, (byte *) data + offset, size);
	string str(buffer, buffer + size);
	delete[] buffer;
	return str;
}

void writeString(void * data, const string& value, unsigned offset)
{
	write(data, (ushort) value.size(), offset);
	offset += sizeof(ushort);

	memcpy((byte*) data + offset, value.c_str(), value.size());
}

void writeBuffer(void * to, unsigned toOffset, const void * from, unsigned fromOffset,
		unsigned size)
{
	memcpy((byte *) to + toOffset, (byte*) from + fromOffset, size);
}

bool exists(const string& fileName)
{
	struct stat info;
	if (stat(fileName.c_str(), &info) == 0)
		return true;
	else
		return false;
}

void setBit(byte& src, bool value, unsigned offset)
{
	if (value)
	{
		src |= 1 << offset;
	}
	else
	{
		src &= ~(1 << offset);
	}
}

bool readBit(byte src, unsigned offset)
{
	return (src >> offset) & 1;
}

void setAttrNull(void * src, ushort attrNum, bool isNull)
{
	unsigned bytes = 0;
	unsigned pos = 0;
	getByteOffset(attrNum, bytes, pos);
	setBit(*((byte *) src + bytes), isNull, pos);
}

bool isAttrNull(const void * src, ushort attrNum)
{
	unsigned bytes = 0;
	unsigned pos = 0;
	getByteOffset(attrNum, bytes, pos);
	return readBit(*((byte *) src + bytes), pos);
}

void getByteOffset(unsigned pos, unsigned& bytes, unsigned& offset)
{
	bytes = pos / 8;

	offset = 7 - pos % 8;
}

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
	if (!_pf_manager)
		_pf_manager = new PagedFileManager();

	return _pf_manager;
}

PagedFileManager::PagedFileManager()
{
}

PagedFileManager::~PagedFileManager()
{

}

RC PagedFileManager::createFile(const string &fileName)
{
	if (exists(fileName))
	{
		logError("Fail to create file:" + fileName + ", because it already exists!")
		return -1;
	}

	//create an empty file here
	ofstream os;
	os.open(fileName, ios::binary);
	write(os, (unsigned) 0);
	os.flush();
	os.close();

	return 0;
}

RC PagedFileManager::destroyFile(const string &fileName)
{
	if (!exists(fileName))
	{
		logError("Fail to destroy file:" + fileName + ", because it does not exist!");
		return -1;
	}

	remove(fileName.c_str());

	return 0;

}

RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	if (!exists(fileName))
	{
		logError("Fail to open file: " + fileName + ", because it does not exist!");
		return -1;
	}

	if (fileHandle.opened)
	{
		logError("Fail to open file: " + fileName + ", because the fileHandle is already bounded to other file!");
		return -1;
	}
	fileHandle.opened = true;
	fileHandle.fs.open(fileName, ios::in | ios::out | ios::binary | ios::ate);
	fileHandle.fs.seekg(0);
	read(fileHandle.fs, fileHandle.pages);
	return 0;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	if (!fileHandle.opened)
	{

		logError("Fail to close file, because the fileHandle is not opened before!");
		return -1;
	}
	fileHandle.opened = false;
	fileHandle.pages = 0;
	fileHandle.fs.flush();
	fileHandle.fs.close();
	return 0;
}

FileHandle::FileHandle() :
		pages(0), opened(false), readPageCounter(0), writePageCounter(0), appendPageCounter(0)
{

}

FileHandle::~FileHandle()
{

}

RC FileHandle::readPage(PageNum pageNum, void *data)
{
	if (pageNum >= pages)
	{
		logError("Fail to read page: "+pageNum+", because it not a valid page number!");
		return -1;
	}

	read(fs, pageOffset(pageNum), data, PAGE_SIZE);

	readPageCounter++;
	return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	if (pageNum >= pages)
	{
		logError("Fail to write page: " + pageNum + ", because it not a valid page number!");
		return -1;
	}

	write(fs, pageOffset(pageNum), data, PAGE_SIZE);
	fs.flush();

	writePageCounter++;
	return 0;
}

RC FileHandle::appendPage(const void *data)
{
	pages++;
	write(fs, (unsigned) 0, pages);
	write(fs, pageOffset(pages - 1), data, PAGE_SIZE);

	fs.flush();

	appendPageCounter++;
	return 0;
}

unsigned FileHandle::getNumberOfPages()
{
	return pages;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
		unsigned &appendPageCount)
{
	readPageCount = this->readPageCounter;
	writePageCount = this->writePageCounter;
	appendPageCount = this->appendPageCounter;

	return 0;
}

unsigned FileHandle::pageOffset(int pageNum)
{
	return FILE_HEADER_SIZE + pageNum * PAGE_SIZE;
}

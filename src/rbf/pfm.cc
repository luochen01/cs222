#include "pfm.h"
#include "util.h"

#include<fstream>
#include<iostream>

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
		cerr << "Fail to create file:" << fileName << ", because it already exists!" << endl;
		return -1;
	}

	//create an empty file here
	ofstream os;
	os.open(FILE_DIR + fileName, ios::binary);
	write(os, (unsigned) 0);
	os.close();

	return 0;
}

RC PagedFileManager::destroyFile(const string &fileName)
{
	if (!exists(fileName))
	{
		cerr << "Fail to destroy file:" << fileName << ", because it does not exist!" << endl;
		return -1;
	}

	remove(fileName.c_str());

	return 0;

}

RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	if (!exists(fileName))
	{
		cerr << "Fail to open file: " << fileName << ", because it does not exist!" << endl;
		return -1;
	}

	if (fileHandle.opened)
	{
		cerr << "Fail to open file: " << fileName
				<< ", because the fileHandle is already bounded to other file!" << endl;
		return -1;
	}
	fileHandle.opened = true;
	fileHandle.fs.open(FILE_DIR + fileName, ios::in | ios::out | ios::binary | ios::ate);
	fileHandle.fs.seekg(0);
	read(fileHandle.fs, fileHandle.pages);
	return 0;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	if (!fileHandle.opened)
	{
		cerr << "Fail to close file, because the fileHandle is not opened before!" << endl;
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
		cerr << "Fail to read page: " << pageNum << ", because it not a valid page number!" << endl;
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
		cerr << "Fail to write page: " << pageNum << ", because it not a valid page number!"
				<< endl;
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

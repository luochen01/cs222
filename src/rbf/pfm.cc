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
	os << (uint) 0;
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
				<< ", because the fileHandle is already bound to other file!" << endl;
		return -1;
	}
	fileHandle.opened = true;
	fileHandle.file.open(FILE_DIR + fileName, ios::in | ios::out | ios::binary);
	fileHandle.file >> fileHandle.pages;
	return 0;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
//TODO flush pages into disks
	if (fileHandle.opened)
	{
		cerr << "Fail to close file, because the fileHandle is not opened before!";
		return -1;
	}
	fileHandle.opened = false;
	fileHandle.pages = 0;
	fileHandle.file.flush();
	fileHandle.file.close();
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

	file.seekg(offset(pageNum));
	file.read((char *) data, PAGE_SIZE);

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
	file.seekp(offset(pageNum));
	file.write((char *) data, PAGE_SIZE);

	writePageCounter++;
	return 0;
}

RC FileHandle::appendPage(const void *data)
{
	pages++;
	file.seekp(0);
	write(file, pages);

	file.seekp(offset(pages - 1));
	file.write((char*) data, PAGE_SIZE);

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

uint FileHandle::offset(int pageNum)
{
	return FILE_HEADER_SIZE + pageNum * PAGE_SIZE;
}

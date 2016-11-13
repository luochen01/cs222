#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ix_test_util.h"

IndexManager *indexManager;

RC testCase_LeafPage()
{
	void * data = malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);

	LeafPage leafPage(data, 0);
	leafPage.reset();

	leafPage.setSibling(10);

	BTreeKey key1, key2, key3, key4;

	key1.value = new int(2);
	key1.rid.pageNum = 1;
	key1.rid.slotNum = 5;

	key2.value = new int(5);
	key2.rid.pageNum = 1;
	key2.rid.slotNum = 10;

	key3.value = new int(9);
	key3.rid.pageNum = 1;
	key3.rid.slotNum = 6;

	key4.value = new int(10);
	key4.rid.pageNum = 1;
	key4.rid.slotNum = 8;

	Attribute attr;
	attr.length = 4;
	attr.type = TypeInt;
	attr.name = "Age";

	leafPage.insertKey(key1, attr);
	leafPage.insertKey(key3, attr);
	leafPage.insertKey(key2, attr);
	leafPage.insertKey(key4, attr);

	leafPage.deleteKey(key4, attr);
	leafPage.deleteKey(key1, attr);

	leafPage.flush(attr);

	key1.value = new int(2);
	key1.rid.pageNum = 1;
	key1.rid.slotNum = 5;

	key2.value = new int(5);
	key2.rid.pageNum = 1;
	key2.rid.slotNum = 10;

	key3.value = new int(9);
	key3.rid.pageNum = 1;
	key3.rid.slotNum = 6;

	key4.value = new int(10);
	key4.rid.pageNum = 1;
	key4.rid.slotNum = 8;

	void * newData = malloc(PAGE_SIZE);
	memcpy(newData, data, PAGE_SIZE);

	LeafPage newLeafPage(newData, 0);
	newLeafPage.initialize(attr);

	BTreeKey key;
	assert(newLeafPage.getNumEntries() == 2);

	key = newLeafPage.getKey(1);
	assert(key.compare(key2, attr) == 0);

	key = newLeafPage.getKey(2);
	assert(key.compare(key3, attr) == 0);

	assert(newLeafPage.getSibling() == 10);

	cout << "Test Case Leaf Page Success." << endl;

	return 0;
}

RC testCase_InternalPage()
{
	void * data = malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);

	InternalPage page(data, 0);
	page.reset();

	BTreeKey key1, key2, key3, key4;

	key1.value = new int(2);
	key1.rid.pageNum = 1;
	key1.rid.slotNum = 5;

	key2.value = new int(5);
	key2.rid.pageNum = 1;
	key2.rid.slotNum = 10;

	key3.value = new int(9);
	key3.rid.pageNum = 1;
	key3.rid.slotNum = 6;

	key4.value = new int(10);
	key4.rid.pageNum = 1;
	key4.rid.slotNum = 8;

	Attribute attr;
	attr.length = 4;
	attr.type = TypeInt;
	attr.name = "Age";

	//only used for test
	page.getPageNums().push_back(0);

	page.insertEntry(key1, 1, attr);
	page.insertEntry(key3, 3, attr);
	page.insertEntry(key2, 2, attr);
	page.insertEntry(key4, 4, attr);

	cout << "before delete " << endl;
	page.deleteEntry(key4, attr);
	page.deleteEntry(key1, attr);

	page.flush(attr);

	void * newData = malloc(PAGE_SIZE);
	memcpy(newData, data, PAGE_SIZE);
	InternalPage newPage(newData, 0);
	newPage.initialize(attr);

	key1.value = new int(2);
	key1.rid.pageNum = 1;
	key1.rid.slotNum = 5;

	key2.value = new int(5);
	key2.rid.pageNum = 1;
	key2.rid.slotNum = 10;

	key3.value = new int(9);
	key3.rid.pageNum = 1;
	key3.rid.slotNum = 6;

	key4.value = new int(10);
	key4.rid.pageNum = 1;
	key4.rid.slotNum = 8;

	BTreeKey key;
	PageNum pageNum;
	assert(newPage.getNumEntries() == 2);

	key = newPage.getKey(1);
	pageNum = newPage.getPageNum(1);
	assert(key.compare(key2, attr) == 0 && pageNum == 2);

	key = newPage.getKey(2);
	pageNum = newPage.getPageNum(2);
	assert(key.compare(key3, attr) == 0 && pageNum == 3);

	cout << "Test Case Internal Page Success." << endl;

	return 0;

}

int main()
{
	testCase_LeafPage();
	testCase_InternalPage();
}


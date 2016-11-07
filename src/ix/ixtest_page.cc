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

	LeafPage leafPage(data);
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

	LeafPage newLeafPage(data);
	newLeafPage.initialize(attr);

	BTreeKey key;
	assert(newLeafPage.getNumEntries() == 2);

	newLeafPage.getKey(1, key);
	assert(key.compare(key2, attr) == 0);

	newLeafPage.getKey(2, key);
	assert(key.compare(key3, attr) == 0);

	assert(newLeafPage.getSibling() == 10);

	cout << "Test Case Leaf Page Success." << endl;

	return 0;
}

RC testCase_InternalPage()
{
	void * data = malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);

	InternalPage page(data);
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

	page.insertKey(key1, 1, attr);
	page.insertKey(key3, 3, attr);
	page.insertKey(key2, 2, attr);
	page.insertKey(key4, 4, attr);

	page.deleteKey(key4, attr);
	page.deleteKey(key1, attr);

	page.flush(attr);

	InternalPage newPage(data);
	newPage.initialize(attr);

	BTreeKey key;
	PageNum pageNum;
	assert(newPage.getNumEntries() == 2);

	newPage.getKey(1, key);
	newPage.getPageNum(1, pageNum);
	assert(key.compare(key2, attr) == 0 && pageNum == 2);

	newPage.getKey(2, key);
	newPage.getPageNum(2, pageNum);
	assert(key.compare(key3, attr) == 0 && pageNum == 3);

	cout << "Test Case Internal Page Success." << endl;

	return 0;

}

int main()
{
	testCase_LeafPage();
	testCase_InternalPage();
}

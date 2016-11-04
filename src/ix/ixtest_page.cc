#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ix_test_util.h"

IndexManager *indexManager;

RC testCase_Page()
{
	void * data = malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);

	LeafPage leafPage(data);
	leafPage.initialize();

	leafPage.sibling(10);

	PageNum siblingPage = leafPage.sibling();
	assert(siblingPage == 10);

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

	BTreeKey key;
	leafPage.getKey(1, key, attr);
	assert(key.compare(key1, attr) == 0);

	leafPage.getKey(2, key, attr);
	assert(key.compare(key2, attr) == 0);

	leafPage.getKey(3, key, attr);
	assert(key.compare(key3, attr) == 0);

	leafPage.getKey(4, key, attr);
	assert(key.compare(key4, attr) == 0);

	int numEntry = leafPage.numEntry();
	assert(numEntry == 4);

	leafPage.deleteKey(key4, attr);
	leafPage.deleteKey(key2, attr);
	leafPage.deleteKey(key1, attr);
	leafPage.deleteKey(key3, attr);

	assert(leafPage.numEntry() == 0);

	cout << "Test Case Page Success.";

	return 0;
}

int main()
{
	testCase_Page();

}


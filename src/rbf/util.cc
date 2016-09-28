/*
 * util.cc
 *
 *  Created on: 2016年9月27日
 *      Author: luochen
 */

#include "util.h"

string readString(istream& is)
{
	uint size;
	is >> size;

	char* buffer = new char[size];
	is.read(buffer, size);
	string str(buffer, buffer + size);
	delete[] buffer;
	return str;
}

void writeString(ostream& os, const string& str)
{
	os << (uint) str.size();
	os.write(str.c_str(), str.size());
}

string readString(void * data, int offset)
{
	uint size = read<uint>(data, offset);
	offset += sizeof(uint);

	char* buffer = new char[size];
	memcpy(buffer, (uchar *) data + offset, size);
	string str(buffer, buffer + size);
	delete[] buffer;
	return str;
}

void writeString(void * data, const string& value, int offset)
{
	write(data, (uint) value.size(), offset);
	offset += sizeof(uint);

	memcpy((uchar*) data + offset, value.c_str(), value.size());
}

void writeBuffer(void * to, uint toOffset, void * from, uint fromOffset, uint size)
{
	memcpy((uchar *) to + toOffset, (uchar*) from + fromOffset, size);
}

bool exists(const string& fileName)
{
	struct stat info;
	if (stat(fileName.c_str(), &info) == 0)
		return true;
	else
		return false;
}

void setBit(uchar& byte, bool value, int offset)
{
	if (value)
	{
		byte |= 1 << offset;
	}
	else
	{
		byte &= ~(1 << offset);
	}
}

bool readBit(uchar byte, int offset)
{
	return (byte >> offset) & 1;
}

void getByteOffset(uint pos, uint& byte, uint& offset)
{
	byte = pos / 8;

	//TODO check it's the left part
	offset = 7 - pos % 8;
}


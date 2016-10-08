/*
 * util.cc
 *
 *  Created on: 2016年9月27日
 *      Author: luochen
 */

#include<math.h>
#include<iostream>

#include "util.h"

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

int attributeIndex(const vector<Attribute>& recordDescriptor, const string& attributeName)
{
	for (int i = 0; i < recordDescriptor.size(); i++)
	{
		if (recordDescriptor[i].name == attributeName)
		{
			return i;
		}
	}
	return -1;
}

bool equals(float left, float right)
{
	return fabs(left - right) < numeric_limits<float>::epsilon();
}

ushort copyAttributeData(void * to, ushort toOffset, const Attribute& attribute, const void * from,
		ushort fromOffset)
{
	switch (attribute.type)
	{
	case TypeInt:
	case TypeReal:
		writeBuffer(to, toOffset, from, fromOffset, attribute.length);
		return attribute.length;
	case TypeVarChar:
	{
		int size;
		read(from, size, fromOffset, sizeof(int));
		writeBuffer(to, toOffset, from, fromOffset, sizeof(int) + size);

		read(to, size, toOffset, sizeof(int));
		return 4 + size;
	}
	}
	return 0;
}


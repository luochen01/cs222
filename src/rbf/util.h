/*
 * util.h
 *
 *  Created on: 2016年9月27日
 *      Author: luochen
 */

#ifndef RBF_UTIL_H_
#define RBF_UTIL_H_

#include <cstring>
#include <istream>
#include <sys/stat.h>
#include"pfm.h"

using namespace std;

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

template<typename T> void read(const void * data, T& result, unsigned offset, unsigned size)
{
	memcpy(&result, ((byte*) data + offset), size);
}


template<typename T> void write(void * data, const T& value, unsigned offset, unsigned size)
{
	memcpy((byte*) data + offset, &value, size);
}


template<typename T> void read(const void * data, T& value, unsigned offset)
{
	return read(data, value, offset, sizeof(T));
}

template<typename T> void write(void * data, const T& value, unsigned offset)
{
	write(data, value, offset, sizeof(T));
}

string readString(const void * data, unsigned offset);

void writeString(void * data, const string& value, unsigned offset);

void writeBuffer(void * to, unsigned toOffset, const void * from, unsigned fromOffset,
		unsigned size);

bool exists(const string &fileName);

void setBit(byte& in, bool value, unsigned offset);

bool readBit(byte in, unsigned offset);

void getByteOffset(unsigned pos, unsigned& byteNum, unsigned& offset);

void setAttrNull(void * data, ushort attrNum, bool isNull);

bool isAttrNull(const void * data, ushort attrNum);

#endif /* RBF_UTIL_H_ */

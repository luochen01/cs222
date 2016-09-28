/*
 * util.h
 *
 *  Created on: 2016年9月27日
 *      Author: luochen
 */

#ifndef RBF_UTIL_H_
#define RBF_UTIL_H_

#include<istream>
#include<ostream>
#include<string>
#include <sys/stat.h>

using namespace std;

typedef unsigned int uint;
typedef unsigned char uchar;

string readString(istream& is);

template<typename T> T read(istream& is)
{
	T value;
	is >> value;
	return value;

}

void writeString(ostream& os, const string& value);

template<typename T> void write(ostream& os, const T& value)
{
	os << value;
}

template<typename T> T read(void * data, int offset)
{
	return *(T*) ((uchar*) data + offset);
}

template<typename T> void write(void * data, const T& value, int offset)
{
	*(T*) ((uchar *) data + offset) = value;
}

template<typename T> T read(void * data, int offset, int size)
{
	T result;
	memcpy(&result, ((uchar*) data + offset), size);
	return result;
}

string readString(void * data, int offset);

void writeString(void * data, const string& value, int offset);

void writeBuffer(void * to, uint toOffset, void * from, uint fromOffset, uint size);

bool exists(const string &fileName);

void setBit(uchar& byte, bool value, int offset);

bool readBit(uchar byte, int offset);

void getByteOffset(uint pos, uint& byte, uint& offset);

#endif /* RBF_UTIL_H_ */

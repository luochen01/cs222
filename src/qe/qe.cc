#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition)
{
	this->input = input;
	this->condition = condition;
}

Filter::~Filter()
{

}

RC Filter::getNextTuple(void *data)
{

}

// For attribute in vector<Attribute>, name it as rel.attr
void Filter::getAttributes(vector<Attribute> &attrs) const
{
	input->getAttributes(attrs);
}

// ... the rest of your implementations go here

Project::Project(Iterator *input, // Iterator of input R
		const vector<string> &attrNames)
{
	this->input = input;

	this->input->getAttributes(inputAttrs);

	this->attrs = ::getAttributes(inputAttrs, attrNames);
	this->attrIndexes = ::getAttributeIndexes(inputAttrs, attrNames);
}

Project::~Project()
{
}

RC Project::getNextTuple(void *data)
{
	if (end)
	{
		return QE_EOF;
	}

	if (input->getNextTuple(buffer) == QE_EOF)
	{
		end = true;
		return QE_EOF;
	}

	unsigned offset = ceil((double) attrs.size() / 8);

	for (int i = 0; i < attrIndexes.size(); i++)
	{
		int index = attrIndexes[i];
		if (::isAttrNull(buffer, index))
		{
			::setAttrNull(data, i, true);
		}
		else
		{
			::setAttrNull(data, i, false);
			unsigned attrOffset = getAttributeOffset(buffer, index, inputAttrs);
			int attrSize = ::attributeSize(inputAttrs[index], (byte*) buffer + attrOffset);
			offset += writeBuffer(data, offset, buffer, attrOffset, attrSize);
		}
	}
	return 0;
}

unsigned Project::getAttributeOffset(void * data, int index,
		const vector<Attribute>& recordDescriptor)
{
	unsigned offset = ceil((double) recordDescriptor.size() / 8);
	for (int i = 0; i < index; i++)
	{
		offset += ::attributeSize(recordDescriptor[i], (byte*) data + offset);
	}
	return offset;
}

// For attribute in vector<Attribute>, name it as rel.attr
void Project::getAttributes(vector<Attribute> &attrs) const
{
	input->getAttributes(attrs);
}

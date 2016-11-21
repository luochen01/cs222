#include "qe.h"

#include <math.h>

const string AggregateOpNames[] =
{ "MIN", "MAX", "COUNT", "SUM", "AVG" };

float initialValue(AggregateOp op)
{
	switch (op)
	{
	case SUM:
	case COUNT:
	case AVG:
		return 0;
	case MIN:
		return numeric_limits<float>::max();
	case MAX:
		return numeric_limits<float>::min();
	default:
		assert(false);
		return 0;
	}
}

Value getAttributeValue(const void * data, int index, const vector<Attribute>& attrs)
{
	unsigned offset = attributeOffset(data, index, attrs);
	return Value(attrs[index].type, (byte*) data + offset);
}

void outputJoinedTuple(void *data, const void * left, const void * right,
		const vector<Attribute>& leftAttrs, const vector<Attribute>& rightAttrs)
{
	unsigned offset = ceil(((double) (leftAttrs.size() + rightAttrs.size())) / 8);
	int index = 0;

	unsigned leftOffset = ceil((double) (leftAttrs.size()) / 8);
	for (int i = 0; i < leftAttrs.size(); i++)
	{
		if (isAttrNull(left, i))
		{
			setAttrNull(data, index, true);
		}
		else
		{
			setAttrNull(data, index, false);
			unsigned bytesCopied = copyAttributeData(data, offset, leftAttrs[i], left, leftOffset);
			offset += bytesCopied;
			leftOffset += bytesCopied;
		}
		index++;
	}

	unsigned rightOffset = ceil((double) (rightAttrs.size()) / 8);
	for (int i = 0; i < rightAttrs.size(); i++)
	{
		if (isAttrNull(right, i))
		{
			setAttrNull(data, index, true);
		}
		else
		{
			setAttrNull(data, index, false);
			unsigned bytesCopied = copyAttributeData(data, offset, rightAttrs[i], right,
					rightOffset);
			offset += bytesCopied;
			rightOffset += bytesCopied;
		}
		index++;
	}
}

//make a deep copy of value
Value Value::copy() const
{
	return Value(type, copyAttribute(type, data));
}

void Value::free()
{
	if (data != NULL)
	{
		delete[] (byte*) data;
		data = NULL;
	}
}

bool Value::operator<(const Value& rhs) const
{
	if (this->type != rhs.type)
	{
		throw exception();
	}
	assert(this->type == rhs.type);
	return compareAttribute(this->data, rhs.data, LT_OP, this->type);
}

UnaryOperator::UnaryOperator(Iterator* input)
{
	this->input = input;
	this->input->getAttributes(inputAttrs);
}

void UnaryOperator::getAttributes(vector<Attribute>& attrs) const
{
	attrs = this->outputAttrs;
}

Filter::Filter(Iterator* input, const Condition &condition) :
		UnaryOperator(input)
{
	this->outputAttrs = this->inputAttrs;

	this->condition = condition;

	this->leftIndex = attributeIndex(inputAttrs, condition.lhsAttr);
	if (condition.bRhsIsAttr)
	{
		this->rightIndex = attributeIndex(inputAttrs, condition.rhsAttr);
	}
	else
	{
		this->rightIndex = 0;
	}
}

Filter::~Filter()
{

}

RC Filter::getNextTuple(void *data)
{
	if (end)
	{
		return QE_EOF;
	}

	while (input->getNextTuple(data) != QE_EOF)
	{

		if (condition.op == NO_OP)
		{
			// we simply skip
			return 0;
		}

		Value leftValue = getAttributeValue(data, leftIndex, inputAttrs);
		Value rightValue;
		if (condition.bRhsIsAttr)
		{
			rightValue = getAttributeValue(data, rightIndex, inputAttrs);
		}
		else
		{
			rightValue = condition.rhsValue;
		}
		if (compareAttribute(leftValue.data, rightValue.data, condition.op, leftValue.type))
		{
			//accept this attribute
			return 0;
		}

	}
//EOF
	end = true;
	return QE_EOF;
}

// ... the rest of your implementations go here

Project::Project(Iterator *input, // Iterator of input R
		const vector<string> &attrNames) :
		UnaryOperator(input)
{

	this->outputAttrs = ::getAttributes(inputAttrs, attrNames);

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

	unsigned offset = ceil((double) inputAttrs.size() / 8);

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
			unsigned attrOffset = ::attributeOffset(buffer, index, inputAttrs);
			int attrSize = ::attributeSize(inputAttrs[index].type, (byte*) buffer + attrOffset);
			offset += writeBuffer(data, offset, buffer, attrOffset, attrSize);
		}
	}
	return 0;
}

Join::Join(Iterator* leftIn, Iterator* rightIn, const Condition& condition)
{
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;

	this->leftIn->getAttributes(leftAttrs);
	this->rightIn->getAttributes(rightAttrs);

	outputAttrs.insert(outputAttrs.end(), leftAttrs.begin(), leftAttrs.end());
	outputAttrs.insert(outputAttrs.end(), rightAttrs.begin(), rightAttrs.end());
}

void Join::getAttributes(vector<Attribute>& attrs) const
{
	attrs = this->outputAttrs;
}

BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition,
		const unsigned numPages) :
		Join(leftIn, rightIn, condition)
{
	this->numPages = numPages;
	this->condition = condition;
	this->leftIndex = ::attributeIndex(leftAttrs, condition.lhsAttr);
	this->rightIndex = ::attributeIndex(rightAttrs, condition.rhsAttr);
	this->nextJoinPos = 0;

	loadLeftTuples();

	if (loadNextRightTuple() == QE_EOF)
	{
		//right is empty
		end = true;
	}
}

RC BNLJoin::loadLeftTuples()
{
	clearLeftTuples();
	int bytesLoad = 0;
	while (bytesLoad < numPages * PAGE_SIZE)
	{
		if (leftIn->getNextTuple(leftBuffer) == QE_EOF)
		{
			//end
			return QE_EOF;
		}
		unsigned size = tupleSize(leftAttrs, leftBuffer);

		byte* data = new byte[size];
		memcpy(data, leftBuffer, size);

		Value value = getAttributeValue(data, leftIndex, leftAttrs);

		leftTuples[value].push_back(data);
		bytesLoad += size;
	}
	return 0;
}

RC BNLJoin::loadNextRightTuple()
{

	while (leftTuples.size() > 0)
	{
		while (rightIn->getNextTuple(rightBuffer) != QE_EOF)
		{
			rightValue = getAttributeValue(rightBuffer, rightIndex, rightAttrs);
			map<Value, vector<byte*>>::iterator it = leftTuples.find(rightValue);
			if (it != leftTuples.end())
			{
				nextJoinPos = 0;
				matchedLeftTuples = &(it->second);
				return 0;
			}
		}
		//now we have to load more left tuples
		loadLeftTuples();
		if (leftTuples.size() > 0)
		{
			TableScan* rightTableScan = (TableScan*) rightIn;
			rightTableScan->setIterator();
		}
	}

	//all left tuples are processed, but still haven't found a suitable right tuple to join
	return QE_EOF;
}

void BNLJoin::clearLeftTuples()
{
	for (map<Value, vector<byte*>>::iterator it = leftTuples.begin(); it != leftTuples.end(); it++)
	{
		for (byte* data : it->second)
		{
			delete[] data;
		}
	}
	leftTuples.clear();
}

RC BNLJoin::getNextTuple(void *data)
{
	if (nextJoinPos < matchedLeftTuples->size())
	{
		outputJoinedTuple(data, matchedLeftTuples->at(nextJoinPos++), rightBuffer, leftAttrs,
				rightAttrs);
		return 0;
	}

	if (loadNextRightTuple() == QE_EOF)
	{
		end = true;
		return QE_EOF;
	}
	outputJoinedTuple(data, matchedLeftTuples->at(nextJoinPos++), rightBuffer, leftAttrs,
			rightAttrs);

	return 0;
}

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) :
		Join(leftIn, rightIn, condition)
{

}

RC INLJoin::getNextTuple(void *data)
{
	return IX_EOF;
}

GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition,
		const unsigned numPartitions) :
		Join(leftIn, rightIn, condition)
{
	this->numPartitions = numPartitions;
}

RC GHJoin::getNextTuple(void *data)
{
	return QE_EOF;
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op) :
		UnaryOperator(input)
{
	this->groupBy = false;
	this->aggAttr = aggAttr;
	this->op = op;

	this->aggAttrIndex = attributeIndex(this->inputAttrs, aggAttr);

	Attribute attr = aggAttr;
	attr.name = AggregateOpNames[op] + "(" + aggAttr.name + ")";
	outputAttrs.push_back(attr);

	initialize();
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, Attribute groupAttr, AggregateOp op) :
		UnaryOperator(input)
{
	this->groupBy = true;
	this->aggAttr = aggAttr;
	this->groupAttr = groupAttr;
	this->op = op;

	this->aggAttrIndex = attributeIndex(this->inputAttrs, aggAttr);
	this->groupAttrIndex = attributeIndex(this->inputAttrs, groupAttr);

	outputAttrs.push_back(groupAttr);
	Attribute attr = aggAttr;
	attr.name = AggregateOpNames[op] + "(" + aggAttr.name + ")";
	outputAttrs.push_back(attr);

	initialize();
}

RC Aggregate::getNextTuple(void *data)
{
	if (end)
	{
		return QE_EOF;
	}

	if (aggResultsIter == aggResults.end())
	{
		end = true;
		return QE_EOF;
	}

	Value groupValue = aggResultsIter->first;
	float aggResult = aggResultsIter->second;

	if (op == AVG)
	{
		aggResult = aggResult / countResults[groupValue];
	}

	if (groupBy)
	{
		unsigned offset = 1;
		setAttrNull(data, 0, false);
		setAttrNull(data, 1, false);

		offset += copyAttributeData(data, offset, groupAttr, groupValue.data, 0);
		offset += write(data, aggResult, offset);

		groupValue.free();
	}
	else
	{
		unsigned offset = 1;
		setAttrNull(data, 0, false);
		offset += write(data, aggResult, offset);
	}

	++aggResultsIter;
	return 0;
}

void Aggregate::initialize()
{
	Value groupValue;
	float aggValue;
	while (input->getNextTuple(buffer) != QE_EOF)
	{
		getAggregateValue(buffer, groupValue, aggValue);
		if (!isAttrNull(buffer, aggAttrIndex) && (!groupBy || groupValue.data != NULL))
		{
			updateAggregateResult(groupValue, aggValue);
		}
	}

	aggResultsIter = aggResults.begin();
}

void Aggregate::getAggregateValue(void * data, Value& groupValue, float& aggValue)
{
	if (groupBy)
	{
		unsigned groupAttrOffset = attributeOffset(data, groupAttrIndex, inputAttrs);
		groupValue.type = groupAttr.type;
		groupValue.data = (byte*) data + groupAttrOffset;
	}
	else
	{
		groupValue = defaultValue;
	}

	unsigned aggAttrOffset = attributeOffset(data, aggAttrIndex, inputAttrs);
	assert(aggAttr.type == TypeInt || aggAttr.type == TypeReal);
	if (aggAttr.type == TypeInt)
	{
		int intValue = 0;
		read(data, intValue, aggAttrOffset);
		aggValue = intValue;
	}
	else
	{
		read(data, aggValue, aggAttrOffset);
	}

}

void Aggregate::updateAggregateResult(const Value& groupAttrValue, float aggAttrValue)
{
	map<Value, float>::iterator it = aggResults.find(groupAttrValue);
	if (it == aggResults.end())
	{
		aggResults[groupAttrValue.copy()] = initialValue(op);
		countResults[groupAttrValue.copy()] = initialValue(COUNT);
	}

	updateAggregateResult(aggResults[groupAttrValue], aggAttrValue, op);
	updateAggregateResult(countResults[groupAttrValue], aggAttrValue, COUNT);

}

void Aggregate::updateAggregateResult(float& aggResult, float aggAttrValue, AggregateOp op)
{
	switch (op)
	{
	case SUM:
	case AVG:
		aggResult += aggAttrValue;
		break;
	case COUNT:
		aggResult++;
		break;
	case MIN:
		if (aggAttrValue < aggResult)
		{
			aggResult = aggAttrValue;
		}
		break;
	case MAX:
		if (aggAttrValue > aggResult)
		{
			aggResult = aggAttrValue;
		}
		break;
	default:
		assert(false);
	}
}

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
	assert(this->type == rhs.type);
	return compareAttribute(this->data, rhs.data, LT_OP, this->type);
}

Filter::Filter(Iterator* input, const Condition &condition)
{
	this->input = input;
	this->condition = condition;
	this->input->getAttributes(inputAttrs);

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

		unsigned leftOffset = ::attributeOffset(data, leftIndex, inputAttrs);
		Value leftValue(inputAttrs[leftIndex].type, (byte*) data + leftOffset);
		Value rightValue;
		if (condition.bRhsIsAttr)
		{
			unsigned rightOffset = ::attributeOffset(data, rightIndex, inputAttrs);
			rightValue.data = (byte*) data + rightOffset;
			rightValue.type = inputAttrs[rightIndex].type;
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

// For attribute in vector<Attribute>, name it as rel.attr
void Filter::getAttributes(vector<Attribute> &attrs) const
{
	attrs = this->inputAttrs;
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
			unsigned attrOffset = ::attributeOffset(buffer, index, inputAttrs);
			int attrSize = ::attributeSize(inputAttrs[index].type, (byte*) buffer + attrOffset);
			offset += writeBuffer(data, offset, buffer, attrOffset, attrSize);
		}
	}
	return 0;
}

// For attribute in vector<Attribute>, name it as rel.attr
void Project::getAttributes(vector<Attribute> &attrs) const
{
	input->getAttributes(attrs);
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op)
{
	this->groupBy = false;
	this->input = input;
	this->aggAttr = aggAttr;
	this->op = op;
	this->input->getAttributes(this->inputAttrs);

	this->aggAttrIndex = attributeIndex(this->inputAttrs, aggAttr);
	initialize();
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, Attribute groupAttr, AggregateOp op)
{
	this->groupBy = true;
	this->input = input;
	this->aggAttr = aggAttr;
	this->groupAttr = groupAttr;
	this->op = op;
	this->input->getAttributes(this->inputAttrs);

	this->aggAttrIndex = attributeIndex(this->inputAttrs, aggAttr);
	this->groupAttrIndex = attributeIndex(this->inputAttrs, groupAttr);

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

// Please name the output attribute as aggregateOp(aggAttr)
// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
// output attrname = "MAX(rel.attr)"
void Aggregate::getAttributes(vector<Attribute> &attrs) const
{
	Attribute attr = aggAttr;
	attr.name = AggregateOpNames[op] + "(" + aggAttr.name + ")";
	attrs.clear();
	attrs.push_back(attr);
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

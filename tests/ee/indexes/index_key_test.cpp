/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "harness.h"
#include "indexes/indexkey.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/TupleSchema.h"
#include "common/tabletuple.h"
#include "common/ThreadLocalPool.h"

using namespace voltdb;

class IndexKeyTest : public Test {
    public:
        IndexKeyTest() {}
        ThreadLocalPool m_pool;
};

TEST_F(IndexKeyTest, Int64KeyTest) {
    std::vector<ValueType> columnTypes(1, ValueType::tBIGINT);
    std::vector<int32_t> columnLengths(1, NValue::getTupleStorageSize(ValueType::tBIGINT));
    std::vector<bool> columnAllowNull(1, true);
    TupleSchema *keySchema = TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);

    IntsKey<1>::KeyComparator comparator(keySchema);
    IntsKey<1>::KeyHasher hasher(keySchema);
    IntsKey<1>::KeyEqualityChecker equality(keySchema);

    TableTuple keyTuple(keySchema);
    keyTuple.move(new char[keyTuple.tupleLength()]());
    keyTuple.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(50)));

    TableTuple otherTuple(keySchema);
    otherTuple.move(new char[otherTuple.tupleLength()]());
    otherTuple.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(25)));

    IntsKey<1> keyKey(&keyTuple);

    IntsKey<1> otherKey(&otherTuple);

    EXPECT_FALSE(equality.operator()(keyKey, otherKey));
    EXPECT_EQ( 1, comparator.operator()(keyKey, otherKey));
    EXPECT_EQ( 0, comparator.operator()(keyKey,keyKey));
    EXPECT_EQ( 0, comparator.operator()(otherKey, otherKey));
    EXPECT_EQ(-1, comparator.operator()(otherKey, keyKey));

    TableTuple thirdTuple(keySchema);
    thirdTuple.move(new char[thirdTuple.tupleLength()]());
    IntsKey<1> thirdKey(
            &thirdTuple.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(50))));
    EXPECT_TRUE(equality.operator()(keyKey, thirdKey));

    IntsKey<1> anotherKey(
            &otherTuple.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(50))));

    EXPECT_TRUE(equality.operator()(keyKey, anotherKey));

    EXPECT_EQ( 0, comparator.operator()(keyKey, anotherKey));

    delete [] keyTuple.address();
    delete [] otherTuple.address();
    delete [] thirdTuple.address();
    TupleSchema::freeTupleSchema(keySchema);
}

TEST_F(IndexKeyTest, TwoInt64KeyTest) {
    std::vector<ValueType> columnTypes(2, ValueType::tBIGINT);
    std::vector<int32_t> columnLengths(2, NValue::getTupleStorageSize(ValueType::tBIGINT));
    std::vector<bool> columnAllowNull(2, true);
    TupleSchema *keySchema = TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);


    IntsKey<2>::KeyComparator comparator(keySchema);
    IntsKey<2>::KeyHasher hasher(keySchema);
    IntsKey<2>::KeyEqualityChecker equality(keySchema);

    TableTuple keyTuple(keySchema);
    keyTuple.move(new char[keyTuple.tupleLength()]());
    keyTuple.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(50)))
        .setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(70)));
    IntsKey<2> keyKey(&keyTuple);

    TableTuple otherTuple(keySchema);
    otherTuple.move(new char[otherTuple.tupleLength()]());
    IntsKey<2> otherKey(&otherTuple
            .setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(50)))
            .setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(50))));

    EXPECT_FALSE(equality.operator()(keyKey, otherKey));
    EXPECT_EQ( 1, comparator.operator()(keyKey, otherKey));
    EXPECT_EQ( 0, comparator.operator()(keyKey,keyKey));
    EXPECT_EQ( 0, comparator.operator()(otherKey, otherKey));
    EXPECT_EQ(-1, comparator.operator()(otherKey, keyKey));

    TableTuple thirdTuple(keySchema);
    thirdTuple.move(new char[thirdTuple.tupleLength()]());
    IntsKey<2> thirdKey(
            &thirdTuple.setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(50)))
            .setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(70))));
    EXPECT_TRUE(equality.operator()(keyKey, thirdKey));
    EXPECT_EQ( 0, comparator.operator()(keyKey, thirdKey));

    IntsKey<2> anotherKey(&otherTuple
            .setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(50)))
            .setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(70))));

    EXPECT_TRUE(equality.operator()(keyKey, anotherKey));

    EXPECT_EQ( 0, comparator.operator()(keyKey, anotherKey));

    delete [] keyTuple.address();
    delete [] otherTuple.address();
    delete [] thirdTuple.address();
    TupleSchema::freeTupleSchema(keySchema);
}

TEST_F(IndexKeyTest, TwoInt64RegressionKeyTest) {
    std::vector<ValueType> columnTypes(2, ValueType::tBIGINT);
    std::vector<int32_t> columnLengths(2, NValue::getTupleStorageSize(ValueType::tBIGINT));
    std::vector<bool> columnAllowNull(2, true);
    TupleSchema *keySchema = TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);


    IntsKey<2>::KeyComparator comparator(keySchema);
    IntsKey<2>::KeyHasher hasher(keySchema);
    IntsKey<2>::KeyEqualityChecker equality(keySchema);

    TableTuple firstTuple(keySchema);
    firstTuple.move(new char[firstTuple.tupleLength()]());
    IntsKey<2> firstKey(&firstTuple
            .setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(3)))
            .setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(1))));

    TableTuple secondTuple(keySchema);
    secondTuple.move(new char[secondTuple.tupleLength()]());
    IntsKey<2> secondKey(&secondTuple
            .setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(2)))
            .setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(0))));

    EXPECT_FALSE(equality.operator()(firstKey, secondKey));
    EXPECT_EQ( 1, comparator.operator()(firstKey, secondKey));
    EXPECT_EQ(-1, comparator.operator()(secondKey, firstKey));

    TableTuple thirdTuple(keySchema);
    thirdTuple.move(new char[thirdTuple.tupleLength()]());
    IntsKey<2> thirdKey(&thirdTuple
            .setNValue(0, ValueFactory::getBigIntValue(static_cast<int64_t>(1)))
            .setNValue(1, ValueFactory::getBigIntValue(static_cast<int64_t>(1))));
    EXPECT_FALSE(equality.operator()(firstKey, thirdKey));
    EXPECT_EQ( 1, comparator.operator()(firstKey, thirdKey));
    EXPECT_EQ(-1, comparator.operator()(thirdKey, firstKey));

    delete [] firstTuple.address();
    delete [] secondTuple.address();
    delete [] thirdTuple.address();
    TupleSchema::freeTupleSchema(keySchema);
}

TEST_F(IndexKeyTest, Int32AndTwoInt8KeyTest) {
    std::vector<ValueType> columnTypes;
    std::vector<int32_t> columnLengths;
    std::vector<bool> columnAllowNull(3, true);

    columnTypes.push_back(ValueType::tINTEGER);
    columnTypes.push_back(ValueType::tTINYINT);
    columnTypes.push_back(ValueType::tTINYINT);

    columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tINTEGER));
    columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tTINYINT));
    columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tTINYINT));

    TupleSchema *keySchema = TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);

    IntsKey<1>::KeyComparator comparator(keySchema);
    IntsKey<1>::KeyHasher hasher(keySchema);
    IntsKey<1>::KeyEqualityChecker equality(keySchema);

    TableTuple firstTuple(keySchema);
    firstTuple.move(new char[firstTuple.tupleLength()]());
    IntsKey<1> firstKey(&firstTuple
            .setNValue(0, ValueFactory::getIntegerValue(static_cast<int32_t>(3300)))
            .setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)))
            .setNValue(2, ValueFactory::getTinyIntValue(static_cast<int8_t>(1))));

    TableTuple secondTuple(keySchema);
    secondTuple.move(new char[secondTuple.tupleLength()]());
    IntsKey<1> secondKey(&secondTuple
            .setNValue(0, ValueFactory::getIntegerValue(static_cast<int32_t>(2200)))
            .setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)))
            .setNValue(2, ValueFactory::getTinyIntValue(static_cast<int8_t>(1))));

    EXPECT_FALSE(equality.operator()(firstKey, secondKey));
    EXPECT_EQ( 1, comparator.operator()(firstKey, secondKey));
    EXPECT_EQ(-1, comparator.operator()(secondKey, firstKey));

    TableTuple thirdTuple(keySchema);
    thirdTuple.move(new char[thirdTuple.tupleLength()]());
    IntsKey<1> thirdKey(&thirdTuple
            .setNValue(0, ValueFactory::getIntegerValue(static_cast<int32_t>(3300)))
            .setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)))
            .setNValue(2, ValueFactory::getTinyIntValue(static_cast<int8_t>(1))));
    EXPECT_TRUE(equality.operator()(firstKey, thirdKey));
    EXPECT_EQ( 0, comparator.operator()(firstKey, thirdKey));
    EXPECT_EQ( 0, comparator.operator()(thirdKey, firstKey));

    delete [] firstTuple.address();
    delete [] secondTuple.address();
    delete [] thirdTuple.address();
    TupleSchema::freeTupleSchema(keySchema);
}

TEST_F(IndexKeyTest, Int32AndTwoInt8KeyTest2) {

    std::vector<ValueType> columnTypes;
    std::vector<int32_t> columnLengths;
    std::vector<bool> columnAllowNull(3, true);

    columnTypes.push_back(ValueType::tTINYINT);
    columnTypes.push_back(ValueType::tTINYINT);
    columnTypes.push_back(ValueType::tINTEGER);


    columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tTINYINT));
    columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tTINYINT));
    columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tINTEGER));


    TupleSchema *keySchema = TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);

    IntsKey<1>::KeyComparator comparator(keySchema);
    IntsKey<1>::KeyHasher hasher(keySchema);
    IntsKey<1>::KeyEqualityChecker equality(keySchema);

    TableTuple firstTuple(keySchema);
    firstTuple.move(new char[firstTuple.tupleLength()]());
    IntsKey<1> firstKey(&firstTuple
            .setNValue(0, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)))
            .setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)))
            .setNValue(2, ValueFactory::getIntegerValue(static_cast<int32_t>(-1))));

    TableTuple secondTuple(keySchema);
    secondTuple.move(new char[secondTuple.tupleLength()]());
    IntsKey<1> secondKey(
            &secondTuple.setNValue(0, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)))
            .setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(32)))
            .setNValue(2, ValueFactory::getIntegerValue(static_cast<int32_t>(200))));

    EXPECT_FALSE(equality.operator()(firstKey, secondKey));
    EXPECT_EQ(-1, comparator.operator()(firstKey, secondKey));
    EXPECT_EQ( 1, comparator.operator()(secondKey, firstKey));

    TableTuple thirdTuple(keySchema);
    thirdTuple.move(new char[thirdTuple.tupleLength()]());
    IntsKey<1> thirdKey(&thirdTuple
            .setNValue(0, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)))
            .setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)))
            .setNValue(2, ValueFactory::getIntegerValue(static_cast<int32_t>(-1))));
    EXPECT_TRUE(equality.operator()(firstKey, thirdKey));
    EXPECT_EQ( 0, comparator.operator()(firstKey, thirdKey));
    EXPECT_EQ( 0, comparator.operator()(thirdKey, firstKey));

    TableTuple fourthTuple(keySchema);
    fourthTuple.move(new char[fourthTuple.tupleLength()]());
    IntsKey<1> fourthKey(&fourthTuple
            .setNValue(0, ValueFactory::getTinyIntValue(static_cast<int8_t>(2)))
            .setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)))
            .setNValue(2, ValueFactory::getIntegerValue(static_cast<int32_t>(-1))));

    EXPECT_FALSE(equality.operator ()(fourthKey, firstKey));
    EXPECT_FALSE(equality.operator ()(fourthKey, secondKey));
    EXPECT_FALSE(equality.operator ()(fourthKey, thirdKey));

    EXPECT_EQ(-1, comparator.operator()(firstKey, fourthKey));
    EXPECT_EQ( 1, comparator.operator()(fourthKey, firstKey));

    delete [] firstTuple.address();
    delete [] secondTuple.address();
    delete [] thirdTuple.address();
    delete [] fourthTuple.address();
    TupleSchema::freeTupleSchema(keySchema);
}

TEST_F(IndexKeyTest, Int32AndTwoInt8RegressionTest) {
    std::vector<ValueType> columnTypes;
    std::vector<int32_t> columnLengths;
    std::vector<bool> columnAllowNull(3, true);

    columnTypes.push_back(ValueType::tTINYINT);
    columnTypes.push_back(ValueType::tTINYINT);
    columnTypes.push_back(ValueType::tINTEGER);


    columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tTINYINT));
    columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tTINYINT));
    columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tINTEGER));

    TupleSchema *keySchema = TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);

    IntsKey<1>::KeyComparator comparator(keySchema);
    IntsKey<1>::KeyHasher hasher(keySchema);
    IntsKey<1>::KeyEqualityChecker equality(keySchema);

    TableTuple firstTuple(keySchema);
    firstTuple.move(new char[firstTuple.tupleLength()]());
    IntsKey<1> firstKey(&firstTuple
            .setNValue(0, ValueFactory::getTinyIntValue(static_cast<int8_t>(6)))
            .setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)))
            .setNValue(2, ValueFactory::getIntegerValue(static_cast<int32_t>(3001))));

    TableTuple secondTuple(keySchema);
    secondTuple.move(new char[secondTuple.tupleLength()]());
    IntsKey<1> secondKey(&secondTuple.
            setNValue(0, ValueFactory::getTinyIntValue(static_cast<int8_t>(7)))
            .setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(1)))
            .setNValue(2, ValueFactory::getIntegerValue(static_cast<int32_t>(3000))));

    EXPECT_FALSE(equality.operator()(firstKey, secondKey));
    EXPECT_EQ(-1, comparator.operator()(firstKey, secondKey));
    EXPECT_EQ( 1, comparator.operator()(secondKey, firstKey));

    delete [] firstTuple.address();
    delete [] secondTuple.address();
    TupleSchema::freeTupleSchema(keySchema);
}

TEST_F(IndexKeyTest, SingleVarChar30) {
    std::vector<ValueType> columnTypes;
    std::vector<int32_t> columnLengths;
    std::vector<bool> columnAllowNull(1, true);

    columnTypes.push_back(ValueType::tVARCHAR);
    columnLengths.push_back(30);

    TupleSchema *keySchema = TupleSchema::createKeySchema(columnTypes, columnLengths, columnAllowNull);

    GenericKey<40>::KeyComparator comparator(keySchema);
    GenericKey<40>::KeyHasher hasher(keySchema);
    GenericKey<40>::KeyEqualityChecker equality(keySchema);

    TableTuple firstTuple(keySchema);
    firstTuple.move(new char[firstTuple.tupleLength()]());
    NValue firstValue = ValueFactory::getStringValue("value");
    firstTuple.setNValue(0, firstValue);

    TableTuple secondTuple(keySchema);
    secondTuple.move(new char[secondTuple.tupleLength()]());
    NValue secondValue = ValueFactory::getStringValue("value2");
    secondTuple.setNValue(0, secondValue);

    TableTuple thirdTuple(keySchema);
    thirdTuple.move(new char[thirdTuple.tupleLength()]());
    NValue thirdValue = ValueFactory::getStringValue("value");
    thirdTuple.setNValue(0, thirdValue);

    GenericKey<40> firstKey(&firstTuple);
    GenericKey<40> secondKey(&secondTuple);
    GenericKey<40> thirdKey(&thirdTuple);

    EXPECT_FALSE(equality.operator()(firstKey, secondKey));
    EXPECT_TRUE(equality.operator()(firstKey, thirdKey));

    delete [] firstTuple.address();
    delete [] secondTuple.address();
    delete [] thirdTuple.address();
    firstValue.free();
    secondValue.free();
    thirdValue.free();
    TupleSchema::freeTupleSchema(keySchema);
}

TEST_F(IndexKeyTest, Int64Packing2Int32sWithSecondNull) {
    std::vector<ValueType> columnTypes;
    std::vector<int32_t> columnLengths;
    std::vector<bool> columnAllowNull(2, true);

    columnTypes.push_back(ValueType::tINTEGER);
    columnTypes.push_back(ValueType::tINTEGER);
    columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tINTEGER));
    columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tINTEGER));

    TupleSchema *keySchema = TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);

    IntsKey<1>::KeyComparator comparator(keySchema);

    TableTuple firstTuple(keySchema);
    firstTuple.move(new char[firstTuple.tupleLength()]());

    IntsKey<1> firstKey(&firstTuple
            .setNValue(0, ValueFactory::getIntegerValue(0))
            .setNValue(1, ValueFactory::getIntegerValue(INT32_NULL)));

    TableTuple secondTuple(keySchema);
    secondTuple.move(new char[secondTuple.tupleLength()]());
    IntsKey<1> secondKey(&secondTuple
            .setNValue(0, ValueFactory::getIntegerValue(0))
            .setNValue(1, ValueFactory::getIntegerValue(0)));

    TableTuple thirdTuple(keySchema);
    thirdTuple.move(new char[thirdTuple.tupleLength()]());
    IntsKey<1> thirdKey(&thirdTuple
            .setNValue(0, ValueFactory::getIntegerValue(0))
            .setNValue(1, ValueFactory::getIntegerValue(1)));

    EXPECT_EQ(-1, comparator.operator()(firstKey, thirdKey));
    EXPECT_EQ(-1, comparator.operator()(firstKey, secondKey));

    delete [] firstTuple.address();
    delete [] secondTuple.address();
    delete [] thirdTuple.address();
    TupleSchema::freeTupleSchema(keySchema);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}

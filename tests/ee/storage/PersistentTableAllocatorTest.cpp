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
#include "storage/TableTupleAllocator.hpp"
#include <algorithm>
#include <array>
#include <cstdio>
#include <functional>
#include <random>
#include <thread>

#include <common/NValue.hpp>
#include <execution/VoltDBEngine.h>

#include "common/TupleOutputStream.h"
#include "common/TupleOutputStreamProcessor.h"
#include "common/TupleSchema.h"
#include "common/types.h"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "expressions/expressions.h"
#include "indexes/tableindex.h"
#include "indexes/tableindexfactory.h"
#include "storage/DRTupleStream.h"
#include "storage/ElasticContext.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/TableStreamerContext.h"
#include "storage/tableutil.h"
#include <vector>

using namespace voltdb;
using namespace voltdb::storage;
using namespace std;
using PersistentTableAllocatorTest = Test;

/**
 * Realistic tests using persistent table for correctness of
 * snapshot process
 */
class ProcPersistenTable {
    unique_ptr<VoltDBEngine> m_engine{new VoltDBEngine{}};
    unique_ptr<PersistentTable> m_table;
    size_t m_rowId = 0;

    static char SIGNATURE[20];
    static int32_t const SITES_PER_HOST = 1;
    static vector<string> const COLUMN_NAMES;
    static TupleSchema const* SCHEMA;
public:
    ProcPersistenTable() {
        int const partitionId = 0;
        m_engine->initialize(1,                    // cluster index
                1,                                 // site id
                partitionId,
                SITES_PER_HOST,
                0,                                 // host id
                "Host_TableTupleAllocatorTest",    // host name
                0,                                 // dr cluster id
                1024,                              // default dr buffer size
                DEFAULT_TEMP_TABLE_MEMORY,
                false);                            // is lowest site id
        int const data[] = {
            static_cast<int>(htonl(1)),            // partition count
            static_cast<int>(htonl(100)),          // token count
            static_cast<int>(htonl(partitionId))
        };
        m_engine->updateHashinator(
                reinterpret_cast<char const*>(data),       // config
                nullptr,                           // config ptr
                0);                                // num tokens
        m_table.reset(
                dynamic_cast<PersistentTable*>(TableFactory::getPersistentTable(
                        0,                         // database id
                        "Foo",                     // table name
                        SCHEMA,
                        COLUMN_NAMES,
                        SIGNATURE,
                        false,                     // is materialized
                        0)));                      // partition column
    }
    size_t insert() {
        auto tempTuple = m_table->tempTuple();
    }
};

char ProcPersistenTable::SIGNATURE[20] = {};
vector<string> const ProcPersistenTable::COLUMN_NAMES{"ID", "STRING", "GEOGRAPHY"};

TupleSchema const* ProcPersistenTable::SCHEMA = TupleSchemaBuilder(3)
    .setColumnAtIndex(0, ValueType::tINTEGER, false)
    .setColumnAtIndex(1, ValueType::tVARCHAR, 512, false)
    .setColumnAtIndex(2, ValueType::tGEOGRAPHY, 2048, false)
    .build();

TEST_F(PersistentTableAllocatorTest, Dummy) {
    ProcPersistenTable t;
}

int main() {
    return TestSuite::globalInstance()->runAll();
}


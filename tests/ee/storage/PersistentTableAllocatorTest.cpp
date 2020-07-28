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
#include "common/TupleSchemaBuilder.h"
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
char message[512];

template<typename T>
vector<T> random_sample(vector<T> const& pool, size_t n) {
    if (n >= pool.size()) {
        return pool;
    } else {
        vector<T> r(pool.cbegin(), next(pool.cbegin(), n));
        for (auto i = n; i < pool.size(); ++i) {
            auto const j = rand() % i;
            if (j < n) {
                r[j] = pool[i];
            }
        }
        return r;
    }
}

struct Indexes {
    static TableIndex* createGeospatialIndex(TupleSchema const* schema, char const* name, int32_t col_index) {
        return TableIndexFactory::getInstance(
                TableIndexScheme(name, TableIndexType::covering_cell,
                    std::vector<int32_t>(1, col_index),
                    std::vector<AbstractExpression*>{},
                    nullptr,  // predicate
                    false, // unique
                    false, // countable
                    false, // migrating
                    "",    // expression as text
                    "",    // predicate as text
                    schema));
    }
    static TableIndex* createIndex(TupleSchema const* schema, bool uniq, char const* name, int32_t col_index) {
        return TableIndexFactory::getInstance(
                TableIndexScheme(name, TableIndexType::balanced_tree,
                    std::vector<int32_t>(1, col_index),
                    std::vector<AbstractExpression*>{},
                    nullptr,  // predicate
                    uniq, // unique
                    false, // countable
                    false, // migrating
                    "",    // expression as text
                    "",    // predicate as text
                    schema));
    }
};
/**
 * Drop-in replacements for server configuration, and table
 * schema, partition info, etc.
 */
struct Config1 {
    static int32_t const SITES_PER_HOST = 1;
    static vector<string> const COLUMN_NAMES;
    static TupleSchema const* SCHEMA;
    static array<TableIndex*, 3> const INDICES;
    static int const PK_INDEX_INDEX = -1;           // index into INDICES that is primary key
};
vector<string> const Config1::COLUMN_NAMES{"ID", "STRING", "GEOGRAPHY"};

TupleSchema const* Config1::SCHEMA = TupleSchemaBuilder(3)
    .setColumnAtIndex(0, ValueType::tINTEGER, false)
    .setColumnAtIndex(1, ValueType::tVARCHAR, 512, false)
    .setColumnAtIndex(2, ValueType::tGEOGRAPHY, 2048, false)
    .build();
array<TableIndex*, 3> const Config1::INDICES{
    Indexes::createIndex(Config1::SCHEMA, true, "pk", 0),
    Indexes::createIndex(Config1::SCHEMA, false, "vc", 1),
    Indexes::createGeospatialIndex(Config1::SCHEMA, "pol_index", 2)
};

/**
 * Realistic tests using persistent table for correctness of
 * snapshot process
 */
template<typename Config>
class ProcPersistenTable {
    unique_ptr<VoltDBEngine> m_engine{new VoltDBEngine{}};
    unique_ptr<PersistentTable> m_table;
    size_t m_rowId = 0;

    static char SIGNATURE[20];
    static NValue generate(size_t, ValueType, size_t limit);
    void insert_row(size_t);
public:
    ProcPersistenTable() {
        int const partitionId = 0;
        m_engine->initialize(1,                    // cluster index
                1,                                 // site id
                partitionId,
                Config1::SITES_PER_HOST,
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
                        Config1::SCHEMA,
                        Config1::COLUMN_NAMES,
                        SIGNATURE,
                        false,                     // is materialized
                        0)));                      // partition column
        for_each(Config::INDICES.cbegin(), Config::INDICES.cend(),
                [this](TableIndex* ind) { m_table->addIndex(ind); });
        if (Config::PK_INDEX_INDEX >= 0) {
            m_table->setPrimaryKeyIndex(Config::INDICES[Config::PK_INDEX_INDEX]);
        }
    }
    void insert(size_t nrows) {
        for (auto i = 0lu; i < nrows; ++i) {
            insert_row(i);
        }
    }
    PersistentTable& table() noexcept;
    PersistentTable const& table() const noexcept;
    static size_t id_of(TableTuple const&);
    vector<void const*> slots() const;             // get addresses in txn view
    static bool valid(TableTuple const&, size_t);
    static bool valid(TableTuple const&);
    size_t size() const noexcept {
        return table().visibleTupleCount();
    }
};

template<typename Config> char ProcPersistenTable<Config>::SIGNATURE[20] = {};

template<typename Config> PersistentTable& ProcPersistenTable<Config>::table() noexcept {
    return *m_table;
}

template<typename Config> PersistentTable const& ProcPersistenTable<Config>::table() const noexcept {
    return *m_table;
}

template<typename Config> vector<void const*> ProcPersistenTable<Config>::slots() const {
    vector<void const*> r;
    r.reserve(table().allocator().size());
    auto const& chunks = table().allocator().chunk_ranges();
    for_each(chunks.cbegin(), chunks.cend(),
            [&r, s = table().allocator().tupleSize()](pair<void const*, void const*> const& p) noexcept {
                for (auto const* cur = reinterpret_cast<char const*>(p.first); cur < p.second; cur += s) {
                    r.emplace_back(cur);
                }
            });
    assert(r.size() == table().allocator().size());
    return r;
}

template<typename Config> NValue ProcPersistenTable<Config>::generate(
        size_t id, ValueType vt, size_t limit) {
    static char const postfix[] =
        "abcdefgABCDEFGhijklmnHIJKLMNopqrstOPQRSTuvwxyzUVWXYZ`12345~!@#$%67890^&()=+[\\;',./]{}|:\"<>? ";
    switch (vt) {
        case ValueType::tINTEGER:
            return ValueFactory::getIntegerValue(id);
        case ValueType::tBIGINT:
            return ValueFactory::getBigIntValue(id);
        case ValueType::tVARCHAR:
            {
                auto const r = to_string(id).append(postfix);
                return ValueFactory::getTempStringValue(
                        limit > 0 && limit < r.length() ?
                        r.substr(0, limit) : r);
            }
        case ValueType::tGEOGRAPHY:
            {
                string r("POLYGON((");
                // long/lat must lie in [-180, 180]
                long lid = id % 360 - 180;
                r.append(to_string(lid)).append(" 0, ");
                for (auto i = 1l;
                        i < limit / 25;            // magic number 25 to avoid polygon too large error
                        ++i) {
                    r.append(to_string(i % 360 - 180)).append(" 0, ");
                }
                r.append(to_string(lid)).append(" 0))");
                return ValueFactory::getTempStringValue(r)
                    .callUnary<FUNC_VOLT_POLYGONFROMTEXT>();
            }
        default:
            throw logic_error("Unhandled column type");
    }
}

template<typename Config> void ProcPersistenTable<Config>::insert_row(size_t id) {
    m_table->insertTuple(
            Config::SCHEMA->build_with(
                m_table->tempTuple(),
                [id](size_t col_index, ValueType vt, TupleSchema::ColumnInfo const& info) -> NValue {
                    return ProcPersistenTable<Config>::generate(id, vt, info.length);
                }));
}

template<typename Config> size_t ProcPersistenTable<Config>::id_of(TableTuple const& tuple) {
    bool found = false;
    auto col = 0l;
    for (auto i = 0l; i < Config::SCHEMA->columnCount(); ++i) {
        if (Config::SCHEMA->columnType(i) == ValueType::tINTEGER) {
            if (found) {
                snprintf(message, sizeof message,
                        "Ambiguous: schema <%s> contains multiple INTEGER columns",
                        Config::SCHEMA->debug().c_str());
                throw logic_error(message);
            } else {
                found = true;
                col = tuple.getNValue(i).getInteger();
            }
        }
    }
    if (! found) {
        snprintf(message, sizeof message,
                "Schema <%s> does not contain any INTEGER column", Config::SCHEMA->debug().c_str());
        throw logic_error(message);
    } else {
        return col;
    }
}

template<typename Config> bool ProcPersistenTable<Config>::valid(TableTuple const& tuple, size_t id) {
    if (! Config::SCHEMA->equals(tuple.getSchema())) {
        return false;
    } else {
        bool matched = true;
        for (auto i = 0l; i < Config::SCHEMA->columnCount() && matched; ++i) {
            matched &= ! tuple.getNValue(i).compareNullAsMax(
                        generate(id, Config::SCHEMA->columnType(i),
                            Config::SCHEMA->getColumnInfo(i)->length));
        }
        return matched;
    }
}

template<typename Config> inline bool ProcPersistenTable<Config>::valid(TableTuple const& tuple) {
    return valid(tuple, id_of(tuple));
}

TEST_F(PersistentTableAllocatorTest, Dummy) {
    ProcPersistenTable<Config1> t;
    t.insert(50);
    size_t i = 0;
    ASSERT_FALSE(storage::until<typename PersistentTable::txn_const_iterator>(
                static_cast<typename PersistentTable::Alloc const&>(t.table().allocator()),
                [&i, &t](void const* p) { // until current tuple is not valid
                    ++i;
                    return ! ProcPersistenTable<Config1>::valid(
                            t.table().tempTuple().move(const_cast<void*>(p)));
                }));
    ASSERT_EQ(50lu, i);
    auto const& slots = t.slots();
    ASSERT_EQ(50lu, slots.size());
    i = 0;
    ASSERT_TRUE(all_of(slots.cbegin(), slots.cend(), [&t, &i](void const* p) noexcept {
                    return ProcPersistenTable<Config1>::valid(
                            t.table().tempTuple().move(const_cast<void*>(p)),
                            i++);
                }));
}

int main() {
    return TestSuite::globalInstance()->runAll();
}


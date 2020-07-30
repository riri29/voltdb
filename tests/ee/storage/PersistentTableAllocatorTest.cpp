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
#include <functional>
#include <random>

#include "common/executorcontext.hpp"
#include "common/ExecuteWithMpMemory.h"
#include "common/NValue.hpp"
#include "common/TupleOutputStream.h"
#include "common/TupleOutputStreamProcessor.h"
#include "common/TupleSchema.h"
#include "common/TupleSchemaBuilder.h"
#include "common/types.h"
#include "common/ValueFactory.hpp"
#include "expressions/expressions.h"
#include "execution/VoltDBEngine.h"
#include "indexes/tableindex.h"
#include "indexes/tableindexfactory.h"
#include "storage/DRTupleStream.h"
#include "storage/ElasticContext.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/TableStreamerContext.h"
#include "storage/tableutil.h"

using namespace voltdb;
using namespace voltdb::storage;
using namespace std;
using PersistentTableAllocatorTest = Test;
static char message[512];

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
    static int const PK_INDEX_INDEX = 0;           // index into INDICES that is primary key
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
 * Table operation sequence, delimited by space, e.g.
 * I50 D2 U5 I3 F D15 U3 S5 D6 S T U3 C ...
 * Meaning:
 * I[number]: insert [number] tuples
 * D[number]: delete [number] tuples at random spot
 * U: update one tuple at random spot
 * F: freeze
 * T: thaw
 * C: clear
 * S[number]: stream [number] tuples of frozen view (requires to be in frozen state)
 * S: stream all/rest tuples of frozen view (requires to be in frozen state)
 *
 * A sequence always starts with given number of active tuples,
 * and is not frozen at the start time.
 */
struct Spec {
    enum op_type {
        insertion, deletion, update, freeze, thaw, clear, stream
    };
    using spec_type = vector<pair<op_type, size_t>>;
    Spec(size_t init, string const& s, bool fixed_rd):
        m_arg(parse(s)), m_normalized(normalize(init, m_arg)),
        m_rd(fixed_rd ? RD_FIXED : RD_R) {}
    spec_type operator()() const noexcept {
        return m_normalized;
    }
    string to_string() const noexcept {
        return deparse(m_normalized);
    }
    mt19937& rd() noexcept {
        return m_rd;
    }
    static mt19937& rd_fixed() noexcept {
        return RD_FIXED;
    }
    static mt19937& rd_r() noexcept {
        return RD_R;
    }
    static Spec generate(size_t, size_t, bool);
private:
    spec_type const m_arg;
    spec_type const m_normalized;
    mt19937& m_rd;
    static mt19937 RD_FIXED, RD_R;
    static spec_type parse(string const&);
    static string deparse(spec_type const&);
    static spec_type normalize(size_t, spec_type const&);
};
mt19937 Spec::RD_FIXED{0}, Spec::RD_R{random_device{}()};

typename Spec::spec_type Spec::parse(string const& src) {
    vector<string> delimited;
    for (auto pos = 0;;) {
        auto const n = src.find_first_of(" ", pos);
        if (n != string::npos) {
            delimited.emplace_back(src.substr(pos, n - pos));
            pos = n + 1;
        } else {
            break;
        }
    }
    static auto const unexpected_trailing = [](string const& s) {
        if (! s.empty()) {
            throw logic_error("Expect empty trailing");
        }
    };
    static auto const number_of = [](char const* s) -> size_t {
        auto const n = atol(s);
        if (n < 0) {
            throw logic_error("Expect trailing number");
        } else if (n == 0) {
            throw logic_error("Expect non-zero trailing number");
        } else {
            return n;
        }
    };
    return accumulate(delimited.cbegin(), delimited.cend(), spec_type{},
            [&src](spec_type& acc, string const& s) {
                if (s.empty()) {
                    snprintf(message, sizeof message,
                            "Invalid empty delimited sequence: %s", src.c_str());
                    throw logic_error(message);
                } else {
                    pair<bool, size_t> number;
                    switch(s[0]) {
                        case 'F':
                            unexpected_trailing(s.substr(1));
                            acc.emplace_back(op_type::freeze, 0);
                            break;
                        case 'T':
                            unexpected_trailing(s.substr(1));
                            acc.emplace_back(op_type::thaw, 0);
                            break;
                        case 'C':
                            unexpected_trailing(s.substr(1));
                            acc.emplace_back(op_type::clear, 0);
                            break;
                        case 'I':
                            acc.emplace_back(op_type::insertion,
                                    number_of(s.substr(1).c_str()));
                            break;
                        case 'D':
                            acc.emplace_back(op_type::deletion,
                                    number_of(s.substr(1).c_str()));
                            break;
                        case 'U':
                            unexpected_trailing(s.substr(1));
                            acc.emplace_back(op_type::update, 0);
                            break;
                        case 'S':
                            acc.emplace_back(op_type::stream,
                                    s.size() == 1 ? 0 : number_of(s.substr(1).c_str()));
                            break;
                        default:
                            snprintf(message, sizeof message,
                                    "Unknown operation %c in %s", s[0], src.c_str());
                            message[sizeof message - 1] = 0;
                            throw logic_error(message);
                    }
                    return acc;
                }
            });
}

string Spec::deparse(typename Spec::spec_type const& src) {
    auto r = accumulate(src.cbegin(), src.cend(), string{},
            [](string& acc, pair<op_type, size_t> const& entry) {
                auto const payload = std::to_string(entry.second);
                switch (entry.first) {
                    case op_type::freeze:
                        return acc.append("F ");
                    case op_type::thaw:
                        return acc.append("T ");
                    case op_type::clear:
                        return acc.append("C ");
                    case op_type::insertion:
                        return acc.append("I").append(payload).append(" ");
                    case op_type::deletion:
                        return acc.append("D").append(payload).append(" ");
                    case op_type::update:
                        return acc.append("U").append(" ");
                    case op_type::stream:
                        acc.append("S");
                        if (entry.second) {
                            acc.append(payload);
                        }
                        return acc.append(" ");
                    default:
                        throw logic_error("???");
                }
            });
    return r.empty() ? r : r.substr(0, r.size() - 1);
}

typename Spec::spec_type Spec::normalize(size_t init, typename Spec::spec_type const& src) {
    struct state {
        size_t m_active;
        size_t m_frozen_cnt = 0;
        bool m_frozen = false;
        state(size_t a): m_active(a) {}
    };
    return accumulate(src.cbegin(), src.cend(), make_pair(state{init}, spec_type{}),
            [](pair<state, spec_type>& acc, typename spec_type::value_type const& entry) {
                auto& fst = acc.first;
                auto& snd = acc.second;
                auto const op = entry.first;
                auto const ops = entry.second;
                switch(op) {
                    case op_type::insertion:
                        fst.m_active += entry.second;
                        snd.emplace_back(entry);
                        break;
                    case op_type::update:
                        assert(ops == 0);
                        snd.emplace_back(entry);
                        break;
                    case op_type::deletion:
                        if (fst.m_active) {
                            auto const delta = min(fst.m_active, ops);
                            fst.m_active -= delta;
                            snd.emplace_back(op, delta);
                        }                          // deletion on empty table is no-op
                        break;
                    case op_type::clear:
                        assert(ops == 0);
                        if (fst.m_active) {            // ignore clear on empty table
                            fst.m_active = 0;
                            snd.emplace_back(entry);
                        }
                        break;
                    case op_type::freeze:
                        assert(ops == 0);
                        if (! fst.m_frozen) {      // ignore double freeze
                            fst.m_frozen = true;
                            fst.m_frozen_cnt = fst.m_active;
                            snd.emplace_back(entry);
                        }
                        break;
                    case op_type::thaw:
                        assert(ops == 0);
                        if (fst.m_frozen) {        // ignore double thaw
                            fst.m_frozen = false;
                            if (fst.m_frozen_cnt) {                // artificially drain the stream before
                                // thawing, unless stream had already drained
                                snd.emplace_back(op_type::stream, fst.m_frozen_cnt = 0);
                            }
                            // snd.emplace_back(entry); NOTE:
                            // thaw action is implicitly handled
                            // by CopyOnWriteContext::handleStreamMore()
                        }
                        break;
                    case op_type::stream:
                        if (fst.m_frozen) {        // ignore stream request unless frozen
                            if (ops <= fst.m_frozen_cnt) {
                                fst.m_frozen_cnt -= ops;
                                snd.emplace_back(entry);
                            } else if (fst.m_frozen_cnt) {         // stream everything
                                snd.emplace_back(op, fst.m_frozen_cnt = 0);
                            }                      // ignore if nothing left to stream
                        }
                }
                return acc;
            }).second;
}


Spec Spec::generate(size_t init_len, size_t len, bool rand_fixed) {
    static map<op_type, size_t> const prob{        // individual probabilities
        make_pair(op_type::update, 12),
        make_pair(op_type::insertion, 15),
        make_pair(op_type::deletion, 7),
        make_pair(op_type::clear, 2),
        make_pair(op_type::freeze, 3),             // let it freeze longer
        make_pair(op_type::thaw, 1),
        make_pair(op_type::stream, 10)
    };
    static auto const prob_acc = accumulate(prob.cbegin(), prob.cend(),
            make_pair(0lu, vector<pair<size_t, op_type>>{}),
            [] (pair<size_t, vector<pair<size_t, op_type>>>& acc,
                pair<op_type, size_t> const& entry) noexcept {
                acc.second.emplace_back(acc.first, entry.first);
                acc.first += entry.second;
                return acc;
            });
    static auto const sum_prop = prob_acc.first;
    static uniform_int_distribution<size_t> unif(0, sum_prop);
    static normal_distribution<> nrange(7, 3);

    auto& rgen = rand_fixed ? Spec::rd_fixed() : Spec::rd_r();
    auto const truncated_normal = [&rgen]() {
        auto const r = nrange(rgen);
        return max<size_t>(r, 0lu);
    };

    string init_seq{};
    for (auto i = 0; i < len; ++i) {
        auto const r = unif(rgen);
        switch(prev(find_if(prob_acc.second.cbegin(), prob_acc.second.cend(),
                        [r] (pair<size_t, op_type> const& entry) {
                            return entry.first > r;
                        }))->second) {
            case op_type::insertion:
                init_seq.append("I")
                    .append(std::to_string(max(1lu, truncated_normal())))
                    .append(" ");
                break;
            case op_type::deletion:
                init_seq.append("D")
                    .append(std::to_string(max(1lu, truncated_normal())))
                    .append(" ");
                break;
            case op_type::update:
                init_seq.append("U ");
                break;
            case op_type::clear:
                init_seq.append("C ");
                break;
            case op_type::freeze:
                init_seq.append("F ");
                break;
            case op_type::thaw:
                init_seq.append("T ");
                break;
            case op_type::stream:
                init_seq.append("S");
                if (unif(rgen)) {                 // if hits 0, stream everything
                    init_seq.append(std::to_string(max(1lu, truncated_normal())));
                }
                init_seq.append(" ");
        }
    }
    return {init_len,
        init_seq.empty() ? init_seq : init_seq.substr(0, init_seq.size() - 1),
        rand_fixed};
}

void teardown(PersistentTable* tbl) {
    if (tbl != nullptr) {
        ScopedReplicatedResourceLock::run([tbl]() {
                ExecuteWithMpMemory::run([tbl]() {delete tbl;}, tbl->isReplicatedTable());
            }, tbl->isReplicatedTable());
    }
}

/**
 * Realistic tests using persistent table for correctness of
 * snapshot process
 */
template<typename Config>
struct ProcPersistentTable {
    enum class partition_type : bool {replicated, partitioned};
    ProcPersistentTable(partition_type pt) {
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
        bool const replicated = pt == partition_type::replicated;
        ScopedReplicatedResourceLock::run([replicated, this]() {
                setup(replicated);
                ExecuteWithMpMemory::run([replicated, this]() {
                        m_table = table_ptr_type(dynamic_cast<PersistentTable*>(
                                    TableFactory::getPersistentTable(
                                        0,                         // database id
                                        "Foo",                     // table name
                                        Config1::SCHEMA,
                                        Config1::COLUMN_NAMES,
                                        SIGNATURE,
                                        false,                     // is materialized
                                        replicated ? -1 : 0,       // always partition on 0-th column, if the table is partitioned
                                        PERSISTENT,                // table type
                                        0,                         // table allocator target size
                                        numeric_limits<int>::max(),// tuple limit: max number of columns in a table
                                        false,                     // DR enabled
                                        replicated)),              // is replicated
                                &teardown);
                        }, replicated);
                }, replicated);
        assert(replicated == m_table->isReplicatedTable());
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
    void update(void const*, size_t);
    PersistentTable& table() noexcept;
    PersistentTable const& table() const noexcept;
    static size_t id_of(TableTuple const&);
    vector<void const*> slots() const;             // get addresses in txn view
    static bool valid(TableTuple const&, size_t);
    static bool valid(TableTuple const&);
    size_t size() const noexcept {
        return table().visibleTupleCount();
    }
private:
    unique_ptr<VoltDBEngine> m_engine{new VoltDBEngine{}};
    using table_ptr_type = unique_ptr<PersistentTable, decltype(&teardown)>;
    table_ptr_type m_table{nullptr, &teardown};
    size_t m_rowId = 0;

    static char SIGNATURE[20];
    static NValue generate(size_t, ValueType, size_t limit);
    void insert_row(size_t);
    void mutate(typename Spec::op_type, size_t, bool);
    void setup(bool);
};

template<typename Config> char ProcPersistentTable<Config>::SIGNATURE[20] = {};

template<typename Config> PersistentTable& ProcPersistentTable<Config>::table() noexcept {
    return *m_table;
}

template<typename Config> void ProcPersistentTable<Config>::setup(bool replicated) {
    if (replicated) {
        m_engine->setPartitionIdForTest(0);
        m_engine->setLowestSiteForTest();
        ThreadLocalPool::setPartitionIds(0);
        EngineLocals local(ExecutorContext::getExecutorContext());
        SynchronizedThreadLock::init(1/*sites per host*/, local);
    }
}

template<typename Config> PersistentTable const& ProcPersistentTable<Config>::table() const noexcept {
    return *m_table;
}

template<typename Config> vector<void const*> ProcPersistentTable<Config>::slots() const {
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

template<typename Config> NValue ProcPersistentTable<Config>::generate(
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

template<typename Config> void ProcPersistentTable<Config>::insert_row(size_t id) {
    bool const rep = table().isReplicatedTable();
    ScopedReplicatedResourceLock::run([this, id, rep]() {
//            if (rep) {
//                SynchronizedThreadLock::countDownGlobalTxnStartCount(true);
//            }
            ExecuteWithMpMemory::run([this, id, rep]() {                   // DRBinaryLog_test:539
                table().insertTuple(Config::SCHEMA->build_with(table().tempTuple(),
                            [id] (size_t col_index, ValueType vt, TupleSchema::ColumnInfo const& info) -> NValue {
                                return ProcPersistentTable<Config>::generate(id, vt, info.length);
                            }));
                }, rep);
//            if (rep) {
//                SynchronizedThreadLock::signalLowestSiteFinished();
//            }
        }, rep);
}

template<typename Config> void ProcPersistentTable<Config>::update(void const* p, size_t id) {
    table().updateTuple(
            Config::SCHEMA->build_with(            // dst
                table().tempTuple(),
                [id](size_t col_index, ValueType vt, TupleSchema::ColumnInfo const& info) -> NValue {
                    return ProcPersistentTable<Config>::generate(id, vt, info.length);
                }),
            table().tempTuple().move(const_cast<void*>(p)));       // src
}

template<typename Config> size_t ProcPersistentTable<Config>::id_of(TableTuple const& tuple) {
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

template<typename Config> bool ProcPersistentTable<Config>::valid(TableTuple const& tuple, size_t id) {
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

template<typename Config> inline bool ProcPersistentTable<Config>::valid(TableTuple const& tuple) {
    return valid(tuple, id_of(tuple));
}

template<typename Config> void ProcPersistentTable<Config>::mutate(
        typename Spec::op_type op, size_t payload, bool rand_fixed) {
    auto& rd = rand_fixed ? Spec::rd_fixed() : Spec::rd_r();
    auto const nrows = table().allocator().size();
    vector<void const*> addresses;
    static uniform_int_distribution<size_t> unif(0, numeric_limits<size_t>::max());
    bool status;
    char config[4] = {0};
    ReferenceSerializeInputBE input(config, 4);
    constexpr auto const BUFFER_SIZE = 131072;
    char serialBuf[BUFFER_SIZE] = {0};
    switch (op) {
        case Spec::op_type::insertion:
            for (auto i = 0lu; i < payload; ++i) {
                insert_row(nrows + i);
            }
            break;
        case Spec::op_type::update:
            assert(! payload);
            addresses = slots();
            update(slots[unif(rd) % slots.size()], unif(rd) % slots.size());
            break;
        case Spec::op_type::deletion:
            assert(payload);
            addresses = random_sample(slots(), payload);
            for_each(addresses.cbegin(), addresses.cend(), [this](void const* p) {
                        table().deleteTuple(
                                table().tempTuple().move(const_cast<void*>(p)));
                    });
            break;
        case Spec::op_type::clear:
            assert(! payload);
            table().truncateTable(m_engine.get());
            break;
        case Spec::op_type::freeze:
            assert(! payload);
            status = table().activateStream(
                    TableStreamType::snapshot, HiddenColumnFilter::NONE,
                    0,                             // TODO: partition id
                    0,                             // TODO: catalog table id
                    input);                        // ReferenceSerializeInputBE
            assert(status);
            break;
        case Spec::op_type::thaw:
            throw logic_error("Should not explicitly thaw");
        case Spec::op_type::stream:
            {
                vector<unique_ptr<char[]>> buffers(1/*npartitions*/,
                        unique_ptr<char[]>{});
                TupleOutputStreamProcessor os(serialBuf, sizeof serialBuf);
                for (auto i = 0; i < 1/*npartitions*/; i++) {
                    os.add((void*)buffers[i].get(), BUFFER_SIZE);
                }
                std::vector<int> retPositions;
                auto const payload_ub = payload == 0 ? numeric_limits<size_t>::max() : payload;
                auto i = 0lu;
                while (i++ < payload_ub) {
                    auto const remaining = table().streamMore(
                            os, TableStreamType::snapshot, retPositions);
                    // validate...
                    if (remaining < 0) {
                        assert(payload == 0 || i == payload);
                        break;
                    } else {
                        assert(os.size() == retPositions.size());
                    }
                }
            }
    }
}

TEST_F(PersistentTableAllocatorTest, TestSpec) {
    unsigned seed = 5;
    puts(Spec::generate(30, 200, seed).to_string().c_str());
    ASSERT_EQ(5, seed);
}

TEST_F(PersistentTableAllocatorTest, Dummy) {
    ProcPersistentTable<Config1> t(ProcPersistentTable<Config1>::partition_type::partitioned);
    t.insert(50);
    size_t i = 0;
    ASSERT_FALSE(storage::until<typename PersistentTable::txn_const_iterator>(
                static_cast<typename PersistentTable::Alloc const&>(t.table().allocator()),
                [&i, &t](void const* p) { // until current tuple is not valid
                    ++i;
                    return ! ProcPersistentTable<Config1>::valid(
                            t.table().tempTuple().move(const_cast<void*>(p)));
                }));
    ASSERT_EQ(50lu, i);
    auto const& slots = t.slots();
    ASSERT_EQ(50lu, slots.size());
    i = 0;
    ASSERT_TRUE(all_of(slots.cbegin(), slots.cend(), [&t, &i](void const* p) noexcept {
                    return ProcPersistentTable<Config1>::valid(
                            t.table().tempTuple().move(const_cast<void*>(p)),
                            i++);
                }));
}

int main() {
    return TestSuite::globalInstance()->runAll();
}


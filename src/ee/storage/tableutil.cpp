/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include <stdint.h>
#include <cstdlib>
#include <ctime>
#include "storage/tableutil.h"
#include "common/common.h"
#include "common/ValueFactory.hpp"
#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "storage/persistenttable.h"
#include "storage/tableiterator.h"

namespace voltdb {

bool tableutil::getRandomTuple(const voltdb::PersistentTable* table, voltdb::TableTuple &out) {
    int64_t cnt = table->visibleTupleCount();
    auto size = table->activeTupleCount();
    vassert(cnt <= size);
    if (cnt > 0) {
        int64_t idx = rand() % cnt;
        int64_t count = 0;
        storage::until<PersistentTable::txn_const_iterator>(table->allocator(),
                                   [&out, &count, idx](const void* p) {
              void *tupleAddress = const_cast<void*>(reinterpret_cast<void const *>(p));
              if (out.move(tupleAddress).isPendingDeleteOnUndoRelease()) {
                  out.move(nullptr);
                  return false;
              }
              if (count == idx) {
                  ++count;
                  return true;
              }
              ++count;
              return false;
           });
    }
    return (!out.isNullTuple() && !out.isPendingDeleteOnUndoRelease());
}

void tableutil::setRandomTupleValues(Table* table, TableTuple *tuple) {
    vassert(table);
    vassert(tuple);
    for (int col_ctr = 0, col_cnt = table->columnCount(); col_ctr < col_cnt; col_ctr++) {
        const TupleSchema::ColumnInfo *columnInfo = table->schema()->getColumnInfo(col_ctr);
        NValue value = ValueFactory::getRandomValue(columnInfo->getVoltType(), columnInfo->length);



        /*
         * getRandomValue() does an allocation for all strings it generates and those need to be freed
         * if the pointer wasn't transferred into the tuple.
         * The pointer won't be transferred into the tuple if the schema has that column inlined.
         */
        const TupleSchema::ColumnInfo *tupleColumnInfo =
            tuple->setNValue(col_ctr, value)
            .getSchema()->getColumnInfo(col_ctr);

        const ValueType t = tupleColumnInfo->getVoltType();
        if ((t == ValueType::tVARCHAR || t == ValueType::tVARBINARY) &&
                tupleColumnInfo->inlined) {
            value.free();
        }
    }
}

bool tableutil::addRandomTuples(Table* table, int num_of_tuples) {
    vassert(num_of_tuples >= 0);
    for (int ctr = 0; ctr < num_of_tuples; ctr++) {
        TableTuple &tuple = table->tempTuple();
        setRandomTupleValues(table, &tuple);
        // std::cout << std::endl << "Creating tuple" << std::endl
        //           << tuple.debug(table->name()) << std::endl;
        // VOLT_DEBUG("  Created random tuple: %s\n", tuple.debug(table->name()).c_str());
        if ( ! table->insertTuple(tuple)) {
            return false;
        }

        /*
         * The insert into the table (assuming a persistent table) will make a copy of the strings
         * so the string allocations for uninlined columns need to be freed here.
         */
        tuple.freeObjectColumns();
    }
    return true;
}

bool tableutil::addDuplicateRandomTuples(Table* table, int num_of_tuples) {
    vassert(num_of_tuples > 1);
    TableTuple &tuple = table->tempTuple();
    setRandomTupleValues(table, &tuple);
    for (int ctr = 0; ctr < num_of_tuples; ctr++) {
        //std::cout << std::endl << "Creating tuple " << std::endl << tuple.debug(table->name()) << std::endl;
        //VOLT_DEBUG("Created random tuple: %s", tuple.debug(table->name()).c_str());
        if ( ! table->insertTuple(tuple)) {
            return false;
        }
    }

    /*
     * The insert into the table (assuming a persistent table) will make a copy of the strings
     * so the string allocations for uninlined columns need to be freed here.
     */
    tuple.freeObjectColumns();
    return true;
}

}

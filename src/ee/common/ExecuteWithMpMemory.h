/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

#pragma once

#include "common/executorcontext.hpp"
#include "common/SynchronizedThreadLock.h"
#include "common/types.h"

namespace voltdb {

class ExecuteWithMpMemory {
    bool const m_switch;
public:
    ExecuteWithMpMemory(bool = true);
    ~ExecuteWithMpMemory();
    static void run(std::function<void(void)> const&&, bool);
};

class ConditionalExecuteWithMpMemory {
    bool const m_usingMpMemory;
public:
    ConditionalExecuteWithMpMemory(bool needMpMemory);
    ~ConditionalExecuteWithMpMemory();
};


class ConditionalExecuteOutsideMpMemory {
    bool const m_notUsingMpMemory;
public:
    ConditionalExecuteOutsideMpMemory(bool haveMpMemory);
    ~ConditionalExecuteOutsideMpMemory();
};

class ConditionalSynchronizedExecuteWithMpMemory {
    bool const m_usingMpMemoryOnLowestThread;
    bool const m_okToExecute;
public:
    ConditionalSynchronizedExecuteWithMpMemory(
            bool needMpMemoryOnLowestThread, bool isLowestSite, std::function<void()> initiator);
    ~ConditionalSynchronizedExecuteWithMpMemory();
    bool okToExecute() const {
        return m_okToExecute;
    }
};

class ExecuteWithAllSitesMemory {
    const EngineLocals m_engineLocals;
#ifndef NDEBUG
    const bool m_wasUsingMpMemory;
#endif
public:
    ExecuteWithAllSitesMemory();

    ~ExecuteWithAllSitesMemory();

    SharedEngineLocalsType::iterator begin();
    SharedEngineLocalsType::iterator end();
};

class ScopedReplicatedResourceLock {
    bool const m_lock;
public:
    ScopedReplicatedResourceLock(bool = true);
    ~ScopedReplicatedResourceLock();
    static void run(std::function<void(void)> const&&, bool);
};

class ConditionalExecuteWithMpMemoryAndScopedResourceLock {
    bool m_usingMpMemory;
public:
    ConditionalExecuteWithMpMemoryAndScopedResourceLock(bool);

    ~ConditionalExecuteWithMpMemoryAndScopedResourceLock();
};

} // end namespace voltdb


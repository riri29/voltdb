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

#include "ExecuteWithMpMemory.h"

#include "common/debuglog.h"
#include "common/SynchronizedThreadLock.h"

namespace voltdb {

ExecuteWithMpMemory::ExecuteWithMpMemory(bool f) : m_switch(f) {
    if (m_switch) {
        VOLT_DEBUG("Entering UseMPmemory");
        SynchronizedThreadLock::assumeMpMemoryContext();
    }
}

ExecuteWithMpMemory::~ExecuteWithMpMemory() {
    if (m_switch) {
        VOLT_DEBUG("Exiting UseMPmemory");
        SynchronizedThreadLock::assumeLocalSiteContext();
    }
}

void ExecuteWithMpMemory::run(std::function<void(void)> const&& fun, bool n) {
    if (n) {
        SynchronizedThreadLock::assumeMpMemoryContext();
        try {
            fun();
        } catch (...) {
            SynchronizedThreadLock::assumeLocalSiteContext();
            throw;
        }
        SynchronizedThreadLock::assumeLocalSiteContext();
    } else {
        fun();
    }
}

ConditionalExecuteWithMpMemory::ConditionalExecuteWithMpMemory(bool needMpMemory) :
    m_usingMpMemory(needMpMemory) {
    if (m_usingMpMemory) {
        VOLT_DEBUG("Entering Conditional UseMPmemory");
        SynchronizedThreadLock::assumeMpMemoryContext();
    }
}

ConditionalExecuteWithMpMemory::~ConditionalExecuteWithMpMemory() {
    if (m_usingMpMemory) {
        VOLT_DEBUG("Exiting Conditional UseMPmemory");
        SynchronizedThreadLock::assumeLocalSiteContext();
    }
}

ConditionalExecuteOutsideMpMemory::ConditionalExecuteOutsideMpMemory(bool haveMpMemory) :
    m_notUsingMpMemory(haveMpMemory) {
    if (m_notUsingMpMemory) {
        VOLT_DEBUG("Breaking out of UseMPmemory");
        SynchronizedThreadLock::assumeLocalSiteContext();
    }
}

ConditionalExecuteOutsideMpMemory::~ConditionalExecuteOutsideMpMemory() {
    if (m_notUsingMpMemory) {
        VOLT_DEBUG("Returning to UseMPmemory");
        SynchronizedThreadLock::assumeMpMemoryContext();
    }
}

ConditionalSynchronizedExecuteWithMpMemory::ConditionalSynchronizedExecuteWithMpMemory(
        bool needMpMemoryOnLowestThread, bool isLowestSite, std::function<void()> initiator)
    : m_usingMpMemoryOnLowestThread(needMpMemoryOnLowestThread && isLowestSite),
    m_okToExecute(!needMpMemoryOnLowestThread || m_usingMpMemoryOnLowestThread) {
    if (needMpMemoryOnLowestThread &&
            SynchronizedThreadLock::countDownGlobalTxnStartCount(isLowestSite)) {
        VOLT_DEBUG("Entering Conditional Sync UseMPmemory");
        SynchronizedThreadLock::assumeMpMemoryContext();
        // This must be done in here to avoid a race with the non-MP path.
        initiator();
    }
}

ConditionalSynchronizedExecuteWithMpMemory::~ConditionalSynchronizedExecuteWithMpMemory() {
    if (m_usingMpMemoryOnLowestThread) {
        VOLT_DEBUG("Switching to local site context and waking other threads...");
        SynchronizedThreadLock::assumeLocalSiteContext();
        SynchronizedThreadLock::signalLowestSiteFinished();
    }
}

ExecuteWithAllSitesMemory::ExecuteWithAllSitesMemory()
        : m_engineLocals()
#ifndef NDEBUG
        , m_wasUsingMpMemory(SynchronizedThreadLock::usingMpMemory())
#endif
{
    vassert(SynchronizedThreadLock::isInSingleThreadMode() || SynchronizedThreadLock::isHoldingResourceLock());
    vassert(SynchronizedThreadLock::isLowestSiteContext());
}

ExecuteWithAllSitesMemory::~ExecuteWithAllSitesMemory() {
    ExecutorContext::assignThreadLocals(m_engineLocals);
#ifndef NDEBUG
    SynchronizedThreadLock::setUsingMpMemory(m_wasUsingMpMemory);
#endif
}

SharedEngineLocalsType::iterator ExecuteWithAllSitesMemory::begin() {
    return SynchronizedThreadLock::s_activeEnginesByPartitionId.begin();
}

SharedEngineLocalsType::iterator ExecuteWithAllSitesMemory::end() {
    return SynchronizedThreadLock::s_activeEnginesByPartitionId.end();
}

ScopedReplicatedResourceLock::ScopedReplicatedResourceLock(bool f) : m_lock(f) {
    if (m_lock) {
        SynchronizedThreadLock::lockReplicatedResource();
    }
}

ScopedReplicatedResourceLock::~ScopedReplicatedResourceLock() {
    if (m_lock) {
        SynchronizedThreadLock::unlockReplicatedResource();
    }
}

void ScopedReplicatedResourceLock::run(std::function<void(void)> const&& fun, bool f) {
    if (f) {
        SynchronizedThreadLock::lockReplicatedResource();
        try {
            fun();
        } catch (...) {
            SynchronizedThreadLock::unlockReplicatedResource();
            throw;
        }
        SynchronizedThreadLock::unlockReplicatedResource();
    } else {
        fun();
    }
}

ConditionalExecuteWithMpMemoryAndScopedResourceLock::ConditionalExecuteWithMpMemoryAndScopedResourceLock(
        bool needMpMemory) : m_usingMpMemory(needMpMemory) {
    if (m_usingMpMemory) {
        VOLT_DEBUG("Entering Conditional (locked) UseMPmemory");
        SynchronizedThreadLock::lockReplicatedResource();
        SynchronizedThreadLock::assumeMpMemoryContext();
    }
}

ConditionalExecuteWithMpMemoryAndScopedResourceLock::~ConditionalExecuteWithMpMemoryAndScopedResourceLock() {
    if (m_usingMpMemory) {
        VOLT_DEBUG("Exiting Conditional (locked) UseMPmemory");
        SynchronizedThreadLock::assumeLocalSiteContext();
        SynchronizedThreadLock::unlockReplicatedResource();
    }
}

} // end namespace voltdb

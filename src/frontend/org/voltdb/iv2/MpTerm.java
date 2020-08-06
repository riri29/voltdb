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

package org.voltdb.iv2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.iv2.LeaderCache.LeaderCallBackInfo;
import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSortedSet;

public class MpTerm implements Term
{
    VoltLogger tmLog = new VoltLogger("TM");
    private final String m_whoami;

    private final InitiatorMailbox m_mailbox;
    private final ZooKeeper m_zk;
    private AtomicReference<SortedSet<Long>> m_knownLeaders = new AtomicReference<SortedSet<Long>>();
    private AtomicReference<Map<Integer, Long>> m_cacheCopy = new AtomicReference<Map<Integer, Long>>();
    private boolean m_lastUpdateByMigration;
    public static enum RepairType {
        // Repair process from partition leader changes via LeaderAppointer
        NORMAL(0),
        // Repair process from partition leader changes via migration
        MIGRATE(1),
        // Repair process not from partition leader changes
        TXN_RESTART(2),
        // Do not restart current transactions while repaired.
        SKIP_TXN_RESTART(4);
        final int type;
        RepairType(int type) {
            this.type = type;
        }
        public boolean isMigrate() {
            return this == MIGRATE|| this == SKIP_TXN_RESTART;
        }
        public boolean isTxnRestart() {
            return this == TXN_RESTART;
        }
        public boolean isSkipTxnRestart() {
            return this == SKIP_TXN_RESTART;
        }
    }

    // Initialized in start() -- when the term begins.
    protected LeaderCache m_leaderCache;
    private LeaderCache m_txnTriggerCache;
    // runs on the babysitter thread when a replica changes.
    // simply forward the notice to the initiator mailbox; it controls
    // the Term processing.
    // NOTE: The contract with LeaderCache is that it always
    // returns a full cache (one entry for every partition).  Returning a
    // partially filled cache is NotSoGood(tm).
    LeaderCache.Callback m_leadersChangeHandler = new LeaderCache.Callback()
    {
        @Override
        public void run(ImmutableMap<Integer, LeaderCallBackInfo> cache)
        {
            ImmutableSortedSet.Builder<Long> builder = ImmutableSortedSet.naturalOrder();
            HashMap<Integer, Long> cacheCopy = new HashMap<Integer, Long>();
            boolean migratePartitionLeaderRequested = false;
            for (Entry<Integer, LeaderCallBackInfo> e : cache.entrySet()) {
                long hsid = e.getValue().m_HSId;
                builder.add(hsid);
                cacheCopy.put(e.getKey(), hsid);

                //The master change is triggered via @MigratePartitionLeader
                if (e.getValue().m_isMigratePartitionLeaderRequested && (m_knownLeaders.get() == null || !m_knownLeaders.get().contains(hsid))) {
                    migratePartitionLeaderRequested = true;
                }
            }
            final SortedSet<Long> updatedLeaders = builder.build();
            if (tmLog.isDebugEnabled()) {
                tmLog.debug(m_whoami + "LeaderCache change updating leader list to: "
                        + CoreUtils.hsIdCollectionToString(updatedLeaders) + ". MigratePartitionLeader:" + migratePartitionLeaderRequested);
            }
            m_knownLeaders.set(updatedLeaders);
            RepairType repairType = RepairType.NORMAL;
            if (migratePartitionLeaderRequested) {
                repairType = RepairType.MIGRATE;
            }
            m_lastUpdateByMigration = migratePartitionLeaderRequested;
            m_cacheCopy.set(cacheCopy);
            ((MpInitiatorMailbox)m_mailbox).updateReplicas(new ArrayList<Long>(m_knownLeaders.get()), cacheCopy, repairType);
        }
    };

    // m_leadersChangeHandler and m_txnRestartHandler are running on the same ZK callback thread, one at a time
    // So they won't step on each other. MP transactions are repaired upon partition leader changes via m_leadersChangeHandler.
    // If no partition leader changes occur, MP transactions won't be repaired since ZK won't invoke callback m_leadersChangeHandler.
    // voltadmin stop command may hit the scenario: the host is shutdown after the last partition leader on the host is migrated away
    // If there happens to be a rerouted transaction from the last leader migration, the transaction won't be repaired via m_leadersChangeHandler.
    LeaderCache.Callback m_txnRestartHandler = new LeaderCache.Callback() {
        @Override
        public void run(ImmutableMap<Integer, LeaderCallBackInfo> cache) {
            if (cache == null || cache.isEmpty() || !m_lastUpdateByMigration) {
                VoltZK.removeTxnRestartTrigger(m_zk);
                return;
            }
            // If any partition masters are still on dead host, let m_leadersChangeHandler process transaction repair
            Set<Integer> liveHostIds = m_mailbox.m_messenger.getLiveHostIds();
            if (!m_knownLeaders.get().stream().map(CoreUtils::getHostIdFromHSId).allMatch(liveHostIds::contains)) {
                VoltZK.removeTxnRestartTrigger(m_zk);
                return;
            }
            tmLog.info(m_whoami + "repair transaction after leader migration.");
            ((MpInitiatorMailbox)m_mailbox).updateReplicas(new ArrayList<Long>(m_knownLeaders.get()), m_cacheCopy.get(), RepairType.TXN_RESTART);
        }
    };

    /**
     * Setup a new Term but don't take any action to take responsibility.
     */
    public MpTerm(ZooKeeper zk, long initiatorHSId, InitiatorMailbox mailbox,
            String whoami)
    {
        m_zk = zk;
        m_mailbox = mailbox;
        m_whoami = whoami;
    }

    /**
     * Start a new Term.  This starts watching followers via ZK.  Block on an
     * appropriate repair algorithm to watch final promotion to leader.
     */
    @Override
    public void start()
    {
        try {
            m_leaderCache = new LeaderCache(m_zk, "MpTerm-iv2masters", VoltZK.iv2masters, m_leadersChangeHandler);
            m_leaderCache.start(true);
            m_txnTriggerCache = new LeaderCache(m_zk, "MpTerm-txn-restart",
                    VoltZK.trigger_txn_restart, m_txnRestartHandler);
            m_txnTriggerCache.start(true);
        }
        catch (ExecutionException ee) {
            VoltDB.crashLocalVoltDB("Unable to create babysitter starting term.", true, ee);
        } catch (InterruptedException e) {
            VoltDB.crashLocalVoltDB("Unable to create babysitter starting term.", true, e);
        }
    }

    @Override
    public void shutdown()
    {
        if (m_leaderCache != null) {
            try {
                m_leaderCache.shutdown();
            } catch (InterruptedException e) {
                // We're shutting down...this may just be faster.
            }
        }
        if (m_txnTriggerCache != null) {
            try {
                m_txnTriggerCache.shutdown();
            } catch (InterruptedException e) {
            }
        }
    }

    @Override
    public Supplier<List<Long>> getInterestingHSIds()
    {
        return new Supplier<List<Long>>() {
            @Override
            public List<Long> get() {
                return new ArrayList<Long>(m_knownLeaders.get());
            }
        };
    }
}

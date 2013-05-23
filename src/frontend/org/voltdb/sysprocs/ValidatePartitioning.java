/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.sysprocs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.TheHashinator;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.utils.VoltTableUtil;

import com.google.common.primitives.Longs;

/**
 * A system procedure for validating that the rows at every partition hash correctly when hashed with the
 * hash function stored at the MPI. Optionally you can supply your own hash function which is useful
 * for test where you want to make provide an incorrect hash function to prove
 * that it is actually doing the right thing.
 *
 */
@ProcInfo(singlePartition = false)
public class ValidatePartitioning extends VoltSystemProcedure {
    private static final VoltLogger TRACE_LOG = new VoltLogger(SnapshotDelete.class.getName());

    private static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");

    private static final int DEP_validatePartitioning = (int)
        SysProcFragmentId.PF_validatePartitioning | DtxnConstants.MULTIPARTITION_DEPENDENCY;

    private static final int DEP_validatePartitioningResults = (int)
        SysProcFragmentId.PF_validatePartitioningResults;

    @Override
    public void init() {
        registerPlanFragment(SysProcFragmentId.PF_validatePartitioningResults);
        registerPlanFragment(SysProcFragmentId.PF_validatePartitioning);
    }

    @Override
    public DependencyPair
    executePlanFragment(Map<Integer, List<VoltTable>> dependencies, long fragmentId, ParameterSet params,
                        final SystemProcedureExecutionContext context)
    {
        if (fragmentId == SysProcFragmentId.PF_validatePartitioning)
        {
            final VoltTable results = constructPartitioningResultsTable();
            List<Integer> tableIds = new ArrayList<Integer>();
            List<String> tableNames = new ArrayList<String>();
            for (Table t : context.getDatabase().getTables()) {
                if (!t.getIsreplicated() && t.getMaterializer() == null) {
                    tableIds.add(t.getRelativeIndex());
                    tableNames.add(t.getTypeName());
                }
            }
            long mispartitionedCounts[] = context.getSiteProcedureConnection().validatePartitioning(
                    Longs.toArray(tableIds), (Integer)params.toArray()[0], (byte[])params.toArray()[1]);

            for (int ii = 0; ii < tableNames.size(); ii++) {
                results.addRow(context.getHostId(), CoreUtils.getSiteIdFromHSId(context.getSiteId()), context.getPartitionId(), tableNames.get(ii), mispartitionedCounts[ii]);
            }
            return new DependencyPair( DEP_validatePartitioning, results);
        } else if (fragmentId == SysProcFragmentId.PF_validatePartitioningResults) {
            assert (dependencies.size() > 0);
            final VoltTable results = VoltTableUtil.unionTables(dependencies.get(DEP_validatePartitioning));
            return new DependencyPair( DEP_validatePartitioningResults, results);
        }
        assert (false);
        return null;
    }

    private VoltTable constructPartitioningResultsTable() {
        ColumnInfo[] result_columns = new ColumnInfo[5];
        int ii = 0;
        result_columns[ii++] = new ColumnInfo(CNAME_HOST_ID, CTYPE_ID);
        result_columns[ii++] = new ColumnInfo(CNAME_SITE_ID, CTYPE_ID);
        result_columns[ii++] = new ColumnInfo(CNAME_PARTITION_ID, CTYPE_ID);
        result_columns[ii++] = new ColumnInfo("TABLE", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("MISPARTITIONED_ROWS", VoltType.BIGINT);
        return new VoltTable(result_columns);
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx, int type, byte config[]) throws VoltAbortException
    {
        final long startTime = System.currentTimeMillis();
        VoltTable retval[];
        if (config != null) {
            retval = performValidatePartitioningWork( type, config );
        } else {
            retval = performValidatePartitioningWork(
                    TheHashinator.getCurrentConfig().getFirst().typeId(),
                    TheHashinator.getCurrentConfig().getSecond());
        }
        final long endTime = System.currentTimeMillis();
        final long duration = endTime -startTime;
        SNAP_LOG.info("Validating partitioning took " + duration + " milliseconds");
        return retval;
    }

    private final VoltTable[] performValidatePartitioningWork(int hashinatorType, byte[] config)
    {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_validatePartitioning;
        pfs[0].outputDepId = DEP_validatePartitioning;
        pfs[0].multipartition = true;
        pfs[0].parameters = ParameterSet.fromArrayNoCopy( hashinatorType, config);

        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_validatePartitioningResults;
        pfs[1].outputDepId = DEP_validatePartitioningResults;
        pfs[1].inputDepIds  = new int[] { DEP_validatePartitioning };
        pfs[1].multipartition = false;
        pfs[1].parameters = ParameterSet.emptyParameterSet();


        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_validatePartitioningResults);
        return results;
    }

}

/* This file is part of VoltDB.
 * Copyright (C) 2019 VoltDB Inc.
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

package org.voltdb.task;

import java.time.Duration;

/**
 * {@link Scheduler} implementation which executes a single procedure with a static set of parameters with a fixed delay
 * between execution times. Delay can either be a number of seconds or {@link Duration} string representation
 */
public class DelayScheduler extends DurationScheduler {
    @Override
    public void initialize(TaskHelper helper, int interval, String timeUnit, String procedure,
            String[] procedureParameters) {
        super.initialize(helper, interval, timeUnit, procedure, procedureParameters);
    }

    @Override
    long getNextDelayNs() {
        return m_durationNs;
    }
}

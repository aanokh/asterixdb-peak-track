/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.control.nc.resources.memory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hyracks.api.dataflow.OperatorInstanceId;
import org.apache.hyracks.api.resources.memory.IResourceBroker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ResourceBroker implements IResourceBroker {

    private static final Logger LOGGER = LogManager.getLogger();

    private final Map<OperatorInstanceId, OperatorEntry> registry = new ConcurrentHashMap<>();
    private final int totalMemoryInFrames; // cluster size, used to report the peak as a percentage
    private int grantedFrames;
    private int peakFrames;

    public ResourceBroker(int totalMemoryInFrames) {
        this.totalMemoryInFrames = Math.max(1, totalMemoryInFrames);
        LOGGER.info("ResourceBroker initialized, cluster=" + this.totalMemoryInFrames + " frames");
    }

    @Override
    public synchronized void register(OperatorInstanceId id, String operatorType, int queryPriority,
            int minBudgetInFrames, int initialBudgetInFrames) {
        // call once when an operator starts
        OperatorEntry prev = registry.put(id, new OperatorEntry(operatorType, initialBudgetInFrames));
        if (prev != null) {
            grantedFrames -= prev.assignedBudgetInFrames;
        }
        grantedFrames += initialBudgetInFrames;
        peakFrames = Math.max(peakFrames, grantedFrames);
    }

    @Override
    public synchronized int requestMemory(OperatorInstanceId id, int heldFrames) {
        // call each time an operator needs a frame, always grants 1
        OperatorEntry entry = registry.get(id);
        if (entry == null) {
            return -1;
        }
        entry.assignedBudgetInFrames += 1;
        grantedFrames += 1;
        peakFrames = Math.max(peakFrames, grantedFrames);
        return entry.assignedBudgetInFrames;
    }

    @Override
    public synchronized int checkBudget(OperatorInstanceId id) {
        // call to read an operator's current budget
        OperatorEntry entry = registry.get(id);
        if (entry == null) {
            return -1;
        }
        return entry.assignedBudgetInFrames;
    }

    @Override
    public synchronized void deregister(OperatorInstanceId id) {
        // call when an operator finishes, frees its frames from the live total
        OperatorEntry entry = registry.get(id);
        if (entry != null) {
            grantedFrames -= entry.assignedBudgetInFrames;
            printBreakdown();
        }
    }

    @Override
    public synchronized void signalPressure(OperatorInstanceId id, double budgetUtilization) {
        // no op
    }

    @Override
    public synchronized void reportFreedMemory(OperatorInstanceId id, int numFrames) {
        // no op
    }

    public synchronized int getPeakFrames() {
        return peakFrames;
    }

    public synchronized void printBreakdown() {
        Map<String, Integer> byType = new HashMap<>();
        LOGGER.info("ResourceBroker peak breakdown (" + registry.size() + " operators):");
        for (Map.Entry<OperatorInstanceId, OperatorEntry> e : registry.entrySet()) {
            OperatorEntry op = e.getValue();
            byType.put(op.operatorType, byType.getOrDefault(op.operatorType, 0) + op.assignedBudgetInFrames);
            LOGGER.info(
                    "  " + e.getKey() + " type=" + op.operatorType + " peak=" + op.assignedBudgetInFrames + " frames");
        }
        for (Map.Entry<String, Integer> e : byType.entrySet()) {
            LOGGER.info("  type=" + e.getKey() + " total=" + e.getValue() + " frames");
        }
        LOGGER.info("  GLOBAL peak=" + peakFrames + " frames (" + (100L * peakFrames / totalMemoryInFrames)
                + "% of cluster)");
    }

    private static class OperatorEntry {
        final String operatorType;
        int assignedBudgetInFrames;

        OperatorEntry(String operatorType, int initialBudgetInFrames) {
            this.operatorType = operatorType;
            this.assignedBudgetInFrames = initialBudgetInFrames;
        }
    }
}

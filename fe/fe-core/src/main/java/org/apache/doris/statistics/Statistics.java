// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.statistics;

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;

/**
 * There are the statistics of all of tables.
 * The @Statistics are mainly used to provide input for the Optimizer's cost model.
 *
 * @idToTableStats: <@Long tableId, @TableStats tableStats>
 * Each table will have corresponding @TableStats.
 * Those @TableStats are recorded in @idToTableStats form of MAP.
 * This facilitates the optimizer to quickly find the corresponding
 * @TableStats based on the table id.
 */
public class Statistics {

    private Map<Long, TableStats> idToTableStats = Maps.newConcurrentMap();

    public void updateTableStats(long tableId, Map<String, String> statsNameToValue)
            throws AnalysisException {
        TableStats tableStats = this.idToTableStats.get(tableId);
        if (tableStats == null) {
            tableStats = new TableStats();
            this.idToTableStats.put(tableId, tableStats);
        }
        tableStats.updateTableStats(statsNameToValue);
    }

    public void updateColumnStats(long tableId, String columnName, Type columnType,
                                  Map<String, String> statsNameToValue)
            throws AnalysisException {
        TableStats tableStats = this.idToTableStats.get(tableId);
        if (tableStats == null) {
            tableStats = new TableStats();
            this.idToTableStats.put(tableId, tableStats);
        }
        tableStats.updateColumnStats(columnName, columnType, statsNameToValue);
    }

    public TableStats getTableStats(long tableId) {
        return this.idToTableStats.get(tableId);
    }

    public Map<String, ColumnStats> getColumnStats(long tableId) {
        TableStats tableStats = this.getTableStats(tableId);
        if (tableStats == null) {
            return null;
        }
        return tableStats.getNameToColumnStats();
    }

    public void showStatistics(){
        Set<Map.Entry<Long, TableStats>> entries = this.idToTableStats.entrySet();
        for (Map.Entry<Long, TableStats> entry : entries) {
            TableStats value = entry.getValue();
            long dataSize = value.getDataSize();
            System.out.println("========= data size ========= " + dataSize);
            long rowCount = value.getRowCount();
            System.out.println("========= row count ========= " + rowCount);
            Map<String, ColumnStats> nameToColumnStats = value.getNameToColumnStats();
            Set<Map.Entry<String, ColumnStats>> columnToColumnStats = nameToColumnStats.entrySet();
            for (Map.Entry<String, ColumnStats> columnToColumnStat : columnToColumnStats) {
                ColumnStats cStat = columnToColumnStat.getValue();
                System.out.println("========= stats =========: " + columnToColumnStat.getKey() + " " + cStat.getShowInfo());
            }
        }
    }
}

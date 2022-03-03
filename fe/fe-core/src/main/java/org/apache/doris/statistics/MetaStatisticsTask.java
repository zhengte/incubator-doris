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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A statistics task that directly collects statistics by reading FE meta.
 */
public class MetaStatisticsTask extends StatisticsTask {

    public MetaStatisticsTask(long jobId, StatsGranularityDesc granularityDesc,
                              StatsCategoryDesc categoryDesc, List<StatsType> statsTypeList) {
        super(jobId, granularityDesc, categoryDesc, statsTypeList);
    }

    @Override
    public StatisticsTaskResult call() throws Exception {
        final Map<String, String> statsTypeToValue = new HashMap<>();
        StatsGranularityDesc.StatsGranularity granularity = this.granularityDesc.getGranularity();
        List<StatsType> statsTypeList = this.getStatsTypeList();
        for (StatsType statsType : statsTypeList) {
            switch (statsType) {
                case ROW_COUNT:
                    this.computeTableRowCount(statsType, statsTypeToValue);
                    break;
                case MAX_SIZE:
                case AVG_SIZE:
                    this.getColMaxAndAvgSize(statsType, statsTypeToValue);
                    break;
                case DATA_SIZE:
                    this.computeDataSize(granularity, statsType, statsTypeToValue);
                    break;
                default:
                    throw new DdlException("unsupported type.");
            }
        }
        return new StatisticsTaskResult(this.jobId, this.id, this.granularityDesc, this.categoryDesc, statsTypeToValue);
    }

    private void computeDataSize(StatsGranularityDesc.StatsGranularity granularity, StatsType statsType, Map<String, String> statsTypeToValue) throws DdlException {
        switch (granularity) {
            case TABLE:
                this.computeTableDataSize(statsType, statsTypeToValue);
                break;
            case TABLET:
                this.computeTabletDataSize(statsType, statsTypeToValue);
                break;
            case PARTITION:
                this.computePartitionDataSize(statsType, statsTypeToValue);
                break;
            default:
                throw new DdlException("unsupported type.");
        }
    }

    private void computeTableDataSize(StatsType statsType, Map<String, String> statsTypeToValue) throws DdlException {
        StatsCategoryDesc categoryDesc = this.getCategoryDesc();
        long dbId = categoryDesc.getDbId();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        long tableId = categoryDesc.getTableId();
        Table table = db.getTableOrDdlException(tableId);
        long dataSize = table.getAvgRowLength() * table.getRowCount();
        statsTypeToValue.put(statsType.getValue(), String.valueOf(dataSize));
    }

    private void computeTabletDataSize(StatsType statsType, Map<String, String> statsTypeToValue) throws DdlException {
        StatsCategoryDesc categoryDesc = this.getCategoryDesc();
        long dbId = categoryDesc.getDbId();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        long tableId = categoryDesc.getTableId();
        Table table = db.getTableOrDdlException(tableId);
        // TODO 计算tablet
        long dataSize = table.getAvgRowLength() * table.getRowCount();
        statsTypeToValue.put(statsType.getValue(), String.valueOf(dataSize));
    }

    private void computePartitionDataSize(StatsType statsType, Map<String, String> statsTypeToValue) throws DdlException {
        StatsCategoryDesc categoryDesc = this.getCategoryDesc();
        long dbId = categoryDesc.getDbId();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        long tableId = categoryDesc.getTableId();
        Table table = db.getTableOrDdlException(tableId);
        // TODO 计算partition
        long dataSize = table.getAvgRowLength() * table.getRowCount();
        statsTypeToValue.put(statsType.getValue(), String.valueOf(dataSize));
    }

    private void computeTableRowCount(StatsType statsType, Map<String, String> statsTypeToValue) throws DdlException {
        StatsCategoryDesc categoryDesc = this.getCategoryDesc();
        long dbId = categoryDesc.getDbId();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        long tableId = categoryDesc.getTableId();
        Table table = db.getTableOrDdlException(tableId);
        long rowCount = table.getRowCount();
        statsTypeToValue.put(statsType.getValue(), String.valueOf(rowCount));
    }

    private void getColMaxAndAvgSize(StatsType statsType, Map<String, String> statsTypeToValue) throws DdlException {
        StatsCategoryDesc categoryDesc = this.getCategoryDesc();
        long dbId = categoryDesc.getDbId();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        long tableId = categoryDesc.getTableId();
        Table table = db.getTableOrDdlException(tableId);
        StatsGranularityDesc granularityDesc = this.getGranularityDesc();
        StatsGranularityDesc.StatsGranularity granularity = granularityDesc.getGranularity();
        String columnName = categoryDesc.getColumnName();
        Column column = table.getColumn(columnName);
        //TODO 确认类型大小
        int typeSize = column.getDataType().getSlotSize();
        statsTypeToValue.put(statsType.getValue(), String.valueOf(typeSize));
    }
}

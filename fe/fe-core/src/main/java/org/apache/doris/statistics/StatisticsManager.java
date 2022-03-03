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

import org.apache.doris.analysis.AlterColumnStatsStmt;
import org.apache.doris.analysis.AlterTableStatsStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.clearspring.analytics.util.Lists;

import java.util.List;
import java.util.Map;

public class StatisticsManager {
    private Statistics statistics;

    public StatisticsManager() {
        this.statistics = new Statistics();
    }

    public Statistics getStatistics() {
        return this.statistics;
    }

    public void alterTableStatistics(AlterTableStatsStmt stmt)
            throws AnalysisException {
        Table table = this.validateTableName(stmt.getTableName());
        this.statistics.updateTableStats(table.getId(), stmt.getProperties());
    }

    public void alterTableStatistics(StatisticsTaskResult taskResult)
            throws AnalysisException, DdlException {
        StatsCategoryDesc categoryDesc = taskResult.getCategoryDesc();
        long dbId = categoryDesc.getDbId();
        long tableId = categoryDesc.getTableId();
        // String columnName = categoryDesc.getColumnName();

        // Database database = Catalog.getCurrentCatalog().getDbOrAnalysisException(dbId);
        // Table table = database.getTableOrDdlException(tableId);
        // Type type = table.getColumn(columnName).getType();

        Map<String, String> statsTypeToValue = taskResult.getStatsTypeToValue();
        this.statistics.updateTableStats(tableId, statsTypeToValue);
        // this.statistics.updateColumnStats(tableId,columnName,type,statsTypeToValue);
        // TODO
        // Table table = validateTableName(stmt.getTableName());
        // statistics.updateTableStats(table.getId(), stmt.getProperties());
    }

    public void alterColumnStatistics(AlterColumnStatsStmt stmt) throws AnalysisException {
        Table table = this.validateTableName(stmt.getTableName());
        String columnName = stmt.getColumnName();
        Column column = table.getColumn(columnName);
        if (column == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_FIELD_ERROR, columnName, table.getName());
        }
        // match type and column value
        this.statistics.updateColumnStats(table.getId(), columnName, column.getType(), stmt.getProperties());
    }

    public void alterColumnStatistics(StatisticsTaskResult taskResult) throws AnalysisException {
        // TODO
        StatsCategoryDesc categoryDesc = taskResult.getCategoryDesc();
        long dbId = categoryDesc.getDbId();
        long tableId = categoryDesc.getTableId();
        String columnName = categoryDesc.getColumnName();
        Database db = Catalog.getCurrentCatalog().getDbOrAnalysisException(dbId);
        Table table = db.getTableOrAnalysisException(tableId);
        Type type = table.getColumn(columnName).getType();
        Map<String, String> statsTypeToValue = taskResult.getStatsTypeToValue();
        // (long tableId, String columnName, Type columnType, Map<String, String> statsNameToValue)
        this.statistics.updateColumnStats(tableId, columnName, type, statsTypeToValue);
    }

    public List<List<String>> showTableStatsList(String dbName, String tableName)
            throws AnalysisException {
        Database db = Catalog.getCurrentCatalog().getDbOrAnalysisException(dbName);
        List<List<String>> result = Lists.newArrayList();
        if (tableName != null) {
            Table table = db.getTableOrAnalysisException(tableName);
            // check priv
            if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), dbName, tableName,
                    PrivPredicate.SHOW)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW CREATE TABLE",
                        ConnectContext.get().getQualifiedUser(),
                        ConnectContext.get().getRemoteIP(),
                        dbName + ": " + tableName);
            }
            // get stats
            result.add(this.showTableStats(table));
        } else {
            for (Table table : db.getTables()) {
                if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), dbName, table.getName(),
                        PrivPredicate.SHOW)) {
                    continue;
                }
                try {
                    result.add(this.showTableStats(table));
                } catch (AnalysisException e) {
                    // ignore no stats table
                }
            }
        }
        return result;
    }

    public List<List<String>> showColumnStatsList(TableName tableName) throws AnalysisException {
        // check meta
        Table table = this.validateTableName(tableName);
        // check priv
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), tableName.getDb(),
                tableName.getTbl(), PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW CREATE TABLE",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    tableName.getDb() + ": " + tableName.getTbl());
        }
        // get stats
        List<List<String>> result = Lists.newArrayList();
        Map<String, ColumnStats> nameToColumnStats = this.statistics.getColumnStats(table.getId());
        if (nameToColumnStats == null) {
            throw new AnalysisException("There is no column statistics in this table:" + table.getName());
        }
        for (Map.Entry<String, ColumnStats> entry : nameToColumnStats.entrySet()) {
            List<String> row = Lists.newArrayList();
            row.add(entry.getKey());
            row.addAll(entry.getValue().getShowInfo());
            result.add(row);
        }
        return result;
    }

    private List<String> showTableStats(Table table) throws AnalysisException {
        TableStats tableStats = this.statistics.getTableStats(table.getId());
        if (tableStats == null) {
            throw new AnalysisException("There is no statistics in this table:" + table.getName());
        }
        List<String> row = Lists.newArrayList();
        row.add(table.getName());
        row.addAll(tableStats.getShowInfo());
        return row;
    }

    private Table validateTableName(TableName dbTableName) throws AnalysisException {
        String dbName = dbTableName.getDb();
        String tableName = dbTableName.getTbl();

        Database db = Catalog.getCurrentCatalog().getDbOrAnalysisException(dbName);
        Table table = db.getTableOrAnalysisException(tableName);
        return table;
    }

}

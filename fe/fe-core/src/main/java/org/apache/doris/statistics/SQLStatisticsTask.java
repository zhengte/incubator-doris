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

import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.qe.OriginStatement;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.apache.velocity.VelocityContext;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A statistics task that collects statistics by executing query.
 * The results of the query will be returned as @StatisticsTaskResult.
 */
public class SQLStatisticsTask extends StatisticsTask {

    public static final String ROW_COUNT_SQL = "select count(*) from $tableName;";
    public static final String MAX_MIN_NDV_SQL = "select max($columnName), min($columnName), ndv($columnName) from $tableName;";
    public static final String NUM_NULLS_SQL = "select count($columnName) from $tableName($partition) where $columnName is null;";
    public static final String MAX_AVG_COL_LENS_SQL = "select max(length($columnName)), avg(length($columnName)) from $tableName TABLESAMPLE;";

    private SelectStmt query;

    public SQLStatisticsTask(long jobId, StatsGranularityDesc granularityDesc,
                             StatsCategoryDesc categoryDesc, List<StatsType> statsTypeList) {
        super(jobId, granularityDesc, categoryDesc, statsTypeList);
    }

    @Override
    public StatisticsTaskResult call() throws Exception {
        // TODO
        // step1: construct query by statsDescList
        this.constructQuery();
        // step2: execute query
        // the result should be sequence by @statsTypeList
        // List<String> queryResultList = this.executeQuery(this.query);
        // step3: construct StatisticsTaskResult by query result
        // TODO 模拟数据
        this.statsTypeList = Arrays.asList(StatsType.ROW_COUNT, StatsType.NUM_NULLS, StatsType.NDV, StatsType.MIN_VALUE,
                StatsType.MAX_VALUE, StatsType.MAX_SIZE, StatsType.MAX_COL_LENS, StatsType.DATA_SIZE, StatsType.AVG_SIZE,
                StatsType.AVG_COL_LENS);
        List<String> queryResultList = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
        // queryResultList.size()

        return this.constructTaskResult(queryResultList);
    }

    protected void constructQuery() throws DdlException {
        StatsCategoryDesc categoryDesc = this.getCategoryDesc();
        long dbId = categoryDesc.getDbId();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        long tableId = categoryDesc.getTableId();
        Table table = db.getTableOrDdlException(tableId);
        String dbName = db.getFullName();
        String tableName = table.getName();
        String columnName = categoryDesc.getColumnName();
        VelocityContext context = new VelocityContext();
        context.put("dbName", dbName);
        context.put("columnName", columnName);
        switch (this.getGranularityDesc().getGranularity()) {
            case TABLET:
                long tabletId = this.getGranularityDesc().getTabletId();
                tableName = tableName + "(" + tabletId + ")";
                context.put("tableName", tableName);
                break;
            case PARTITION:
                long partitionId = this.getGranularityDesc().getPartitionId();
                tableName = tableName + "(" + partitionId + ")";
                context.put("tableName", tableName);
                break;
            default:
                context.put("tableName", tableName);
        }

        StringWriter sqlWriter = new StringWriter();
        try {
            for (StatsType type : this.getStatsTypeList()) {
                switch (type) {
                    case NDV:
                    case MAX_SIZE:
                    case MIN_VALUE:
                        StatisticsUtils.buildSql(context, sqlWriter, MAX_MIN_NDV_SQL);
                        break;
                    case MAX_COL_LENS:
                    case AVG_COL_LENS:
                        StatisticsUtils.buildSql(context, sqlWriter, MAX_AVG_COL_LENS_SQL);
                        break;
                    case NUM_NULLS:
                        StatisticsUtils.buildSql(context, sqlWriter, NUM_NULLS_SQL);
                        break;
                    default:
                        StatisticsUtils.buildSql(context, sqlWriter, ROW_COUNT_SQL);
                }
                break;
            }
            SqlParser parser = new SqlParser(new SqlScanner(new StringReader(sqlWriter.toString())));
            this.query = (SelectStmt) SqlParserUtils.getStmt(parser, 0);
            this.query.setOrigStmt(new OriginStatement(sqlWriter.toString(), 0));
            System.out.println(sqlWriter);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // TODO
        // step1: construct FROM by @granularityDesc
        // step2: construct SELECT LIST by @statsTypeList
    }

    protected List<String> executeQuery(SelectStmt query) {
        // TODO (ML)
        return null;
    }

    protected StatisticsTaskResult constructTaskResult(List<String> queryResultList) {
        Preconditions.checkState(this.statsTypeList.size() == queryResultList.size());
        Map<String, String> statsTypeToValue = Maps.newHashMap();
        for (int i = 0; i < this.statsTypeList.size(); i++) {
            statsTypeToValue.put(this.statsTypeList.get(i).getValue(), queryResultList.get(i));
        }
        return new StatisticsTaskResult(this.jobId, this.id, this.granularityDesc, this.categoryDesc, statsTypeToValue);
    }
}

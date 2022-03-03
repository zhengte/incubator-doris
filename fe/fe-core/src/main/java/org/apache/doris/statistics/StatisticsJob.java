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

import org.apache.doris.analysis.AnalyzeStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.internal.guava.Sets;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used to store statistics job info,
 * including job status, progress, etc.
 * <p>
 * AnalyzeStmt: Analyze t1(c1), t2
 * StatisticsJob:
 * tableId [t1, t2]
 * tableIdToColumnName <t1, [c1]> <t2, [c1,c2,c3]>
 */
public class StatisticsJob {

    public enum JobState {
        PENDING,
        SCHEDULING,
        RUNNING,
        FINISHED,
        CANCELLED,
        FAILED
    }

    private long id = -1;
    private JobState jobState = JobState.PENDING;

    private long dbId = -1;

    private AnalyzeStmt analyzeStmt;

    private int progress;

    /**
     * to be collected table stats
     */
    private List<Long> tableIdList = Lists.newArrayList();

    /**
     * to be collected column stats
     */
    private Map<Long, List<String>> tableIdToColumnName = Maps.newHashMap();

    private Map<String, String> properties;

    private List<StatisticsTask> taskList = Lists.newArrayList();

    public StatisticsJob(long id, AnalyzeStmt analyzeStmt) {
        this.id = id;
        this.progress = 0;
        this.analyzeStmt = analyzeStmt;
    }

    public int getProgress() {
        return this.progress;
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }

    public long getId() {
        return this.id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public JobState getJobState() {
        return this.jobState;
    }

    public void setJobState(JobState jobState) {
        this.jobState = jobState;
    }

    public List<Long> getTableIdList() {
        return this.tableIdList;
    }

    public void setTableIdList(List<Long> tableIdList) {
        this.tableIdList = tableIdList;
    }

    public Map<Long, List<String>> getTableIdToColumnName() {
        return this.tableIdToColumnName;
    }

    public void setTableIdToColumnName(Map<Long, List<String>> tableIdToColumnName) {
        this.tableIdToColumnName = tableIdToColumnName;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public List<StatisticsTask> getTaskList() {
        return this.taskList;
    }

    public void setTaskList(List<StatisticsTask> taskList) {
        this.taskList = taskList;
    }

    public void init() throws DdlException {
        this.properties = this.analyzeStmt.getProperties();
        TableName dbTableName = this.analyzeStmt.getTableName();
        List<String> columnNames = this.analyzeStmt.getColumnNames();

        if (dbTableName != null) {
            String dbName = dbTableName.getDb();
            if (StringUtils.isBlank(dbName)) {
                dbName = this.analyzeStmt.getAnalyzer().getDefaultDb();
            } else {
                dbName = this.analyzeStmt.getClusterName() + ":" + dbName;
            }
            Database database = Catalog.getCurrentCatalog().getDbOrDdlException(dbName);
            this.dbId = database.getId();
            Table table = database.getOlapTableOrDdlException(dbTableName.getTbl());
            long tableId = table.getId();
            this.tableIdList.add(tableId);
            if (columnNames == null || columnNames.isEmpty()) {
                List<Column> baseSchema = table.getBaseSchema();
                columnNames = baseSchema.stream().map(Column::getName).collect(Collectors.toList());
            }
            this.tableIdToColumnName.put(tableId, columnNames);
        } else {
            String dbName = this.analyzeStmt.getAnalyzer().getDefaultDb();
            Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbName);
            this.dbId = db.getId();
            List<Table> tables = db.getTables();
            for (Table table : tables) {
                long tableId = table.getId();
                this.tableIdList.add(tableId);
                List<Column> baseSchema = table.getBaseSchema();
                columnNames = baseSchema.stream().map(Column::getName).collect(Collectors.toList());
                this.tableIdToColumnName.put(tableId, columnNames);
            }
        }

        // TODO 如果表 size > 单个 be 最大扫描量 * be 个数，按照 partition 进行分割子任务。

        this.checkPermission();

        // this.checkRestrict();

        this.generateRowCountAndDataSizeTask();

        this.generateMinAndMaxAndNdvTask();

        this.generateMaxColLensAndAvgColLensTask();
    }

    private void checkPermission() {
        // TODO
    }

    // Rule1: The same table cannot have two unfinished statistics jobs
    // Rule2: The unfinished statistics job could not more then Config.max_statistics_job_num
    // Rule3: The job for external table is not supported
    private void checkRestrict() throws DdlException {

        Set<Long> tableIds = this.tableIdToColumnName.keySet();

        // check table type
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(this.dbId);
        for (Long tableId : tableIds) {
            Table table = db.getTableOrDdlException(tableId);
            Table.TableType type = table.getType();
            if (type != Table.TableType.OLAP) {
                throw new DdlException("The job for external table is not supported.");
            }
        }

        StatisticsJobScheduler jobScheduler = Catalog.getCurrentCatalog().getStatisticsJobScheduler();
        Queue<StatisticsJob> jobQueue = jobScheduler.pendingJobQueue;
        int unfinishedJobNum = 0;

        // check table unfinished job
        for (StatisticsJob statisticsJob : jobQueue) {
            JobState jobState = statisticsJob.getJobState();
            List<Long> tableIdList = statisticsJob.getTableIdList();
            if (jobState == JobState.PENDING || jobState == JobState.SCHEDULING || jobState == JobState.RUNNING) {
                for (Long tableId : tableIds) {
                    if (tableIdList.contains(tableId)) {
                        throw new DdlException("The same table cannot have two unfinished statistics jobs.");
                    }
                }
                unfinishedJobNum++;
            }
        }

        // check total unfinished statistics job nums
        if (unfinishedJobNum > Config.cbo_max_statistics_job_num) {
            throw new DdlException("The unfinished statistics job could not more then Config.cbo_max_statistics_job_num.");
        }
    }

    private void generateRowCountAndDataSizeTask() throws DdlException {
        Set<Long> tableIds = this.tableIdToColumnName.keySet();
        for (Long tableId : tableIds) {
            Database db = Catalog.getCurrentCatalog().getDbOrDdlException(this.dbId);
            Table tbl = db.getTableOrDdlException(tableId);
            StatsGranularityDesc statsGranularityDesc = this.getTblStatsGranularityDesc(tableId);

            StatsCategoryDesc statsCategoryDesc = new StatsCategoryDesc();
            statsCategoryDesc.setDbId(this.dbId);
            statsCategoryDesc.setTableId(tableId);
            statsCategoryDesc.setCategory(StatsCategoryDesc.StatsCategory.TABLE);

            final KeysType keysType = ((OlapTable) tbl).getKeysType();
            if (keysType == KeysType.DUP_KEYS) {
                List<StatsType> statsTypeList = new ArrayList<>();
                statsTypeList.add(StatsType.DATA_SIZE);
                statsTypeList.add(StatsType.ROW_COUNT);
                MetaStatisticsTask metaTask = new MetaStatisticsTask(1, statsGranularityDesc, statsCategoryDesc, statsTypeList);
                this.taskList.add(metaTask);
            } else {
                List<StatsType> statsTypeList1 = new ArrayList<>();
                statsTypeList1.add(StatsType.DATA_SIZE);
                MetaStatisticsTask metaTask = new MetaStatisticsTask(1, statsGranularityDesc, statsCategoryDesc, statsTypeList1);
                this.taskList.add(metaTask);

                List<StatsType> statsTypeList2 = new ArrayList<>();
                statsTypeList2.add(StatsType.ROW_COUNT);
                SQLStatisticsTask sqlTask = new SQLStatisticsTask(1, statsGranularityDesc, statsCategoryDesc, statsTypeList2);
                this.taskList.add(sqlTask);
            }
        }
    }

    private void generateMinAndMaxAndNdvTask() {
        Set<Map.Entry<Long, List<String>>> tableIdToColumnNames = this.tableIdToColumnName.entrySet();
        for (Map.Entry<Long, List<String>> tableIdToColumnName : tableIdToColumnNames) {
            final long tableId = tableIdToColumnName.getKey();
            List<String> columnNameList = tableIdToColumnName.getValue();
            for (String columnName : columnNameList) {
                StatsGranularityDesc statsGranularityDesc = this.getTblStatsGranularityDesc(tableId);
                StatsCategoryDesc statsCategoryDesc = this.getColStatsCategoryDesc(this.dbId, tableId, columnName);

                List<StatsType> statsTypeList = new ArrayList<>();
                statsTypeList.add(StatsType.MIN_VALUE);
                statsTypeList.add(StatsType.MAX_VALUE);
                statsTypeList.add(StatsType.NDV);
                SQLStatisticsTask sqlTask = new SQLStatisticsTask(1, statsGranularityDesc, statsCategoryDesc, statsTypeList);
                this.taskList.add(sqlTask);
            }
        }
    }


    private void generateMaxColLensAndAvgColLensTask() throws DdlException {
        Set<Map.Entry<Long, List<String>>> tableIdToColumnNames = this.tableIdToColumnName.entrySet();

        for (Map.Entry<Long, List<String>> tableIdToColumnName : tableIdToColumnNames) {
            Database db = Catalog.getCurrentCatalog().getDbOrDdlException(this.dbId);
            final long tableId = tableIdToColumnName.getKey();
            Table table = db.getTableOrDdlException(tableId);

            List<String> columnNameList = tableIdToColumnName.getValue();
            for (String columnName : columnNameList) {
                StatsGranularityDesc statsGranularityDesc = this.getTblStatsGranularityDesc(tableId);
                StatsCategoryDesc statsCategoryDesc = this.getColStatsCategoryDesc(this.dbId, tableId, columnName);
                List<StatsType> statsTypeList = new ArrayList<>();
                statsTypeList.add(StatsType.MAX_SIZE);
                statsTypeList.add(StatsType.AVG_SIZE);

                Column column = table.getColumn(columnName);
                Type colType = column.getType();
                if (colType.isStringType()) {
                    SQLStatisticsTask sqlTask = new SQLStatisticsTask(1, statsGranularityDesc, statsCategoryDesc, statsTypeList);
                    this.taskList.add(sqlTask);
                } else {
                    MetaStatisticsTask metaTask = new MetaStatisticsTask(1, statsGranularityDesc, statsCategoryDesc, statsTypeList);
                    this.taskList.add(metaTask);
                }
            }
        }
    }

    @NotNull
    private StatsCategoryDesc getColStatsCategoryDesc(long dbId, long tableId, String columnName) {
        StatsCategoryDesc statsCategoryDesc = new StatsCategoryDesc();
        statsCategoryDesc.setDbId(dbId);
        statsCategoryDesc.setTableId(tableId);
        statsCategoryDesc.setCategory(StatsCategoryDesc.StatsCategory.COLUMN);
        statsCategoryDesc.setColumnName(columnName);
        return statsCategoryDesc;
    }

    @NotNull
    private StatsGranularityDesc getTblStatsGranularityDesc(long tableId) {
        StatsGranularityDesc statsGranularityDesc = new StatsGranularityDesc();
        statsGranularityDesc.setTableId(tableId);
        statsGranularityDesc.setGranularity(StatsGranularityDesc.StatsGranularity.TABLE);
        return statsGranularityDesc;
    }

    public Set<Long> relatedTableId() {
        Set<Long> relatedTableId = Sets.newHashSet();
        relatedTableId.addAll(this.tableIdList);
        relatedTableId.addAll(this.tableIdToColumnName.keySet());
        return relatedTableId;
    }
}

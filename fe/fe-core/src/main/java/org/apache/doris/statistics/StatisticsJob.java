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
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.internal.guava.Sets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used to store statistics job info,
 * including job status, progress, etc.
 */
public class StatisticsJob {

    public enum JobState {
        PENDING,
        SCHEDULING,
        RUNNING,
        FINISHED,
        CANCELLED
    }

    private long id = -1;
    private JobState jobState = JobState.PENDING;

    // optional
    // to be collected table stats
    private List<Long> tableIds = Lists.newArrayList();

    // to be collected column stats
    private Map<Long, List<String>> tableIdToColumnName = Maps.newHashMap();

    // nanalyze job properties
    private Map<String, String> properties;
    // end

    private List<StatisticsTask> taskList = Lists.newArrayList();

    public StatisticsJob(long id) {
        this.id = id;
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

    public List<Long> getTableIds() {
        return this.tableIds;
    }

    public void setTableIds(List<Long> tableIds) {
        this.tableIds = tableIds;
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

    public long getId() {
        return this.id;
    }

    /**
     * AnalyzeStmt: Analyze t1(c1), t2
     * StatisticsJob:
     * tableId [t1, t2]
     * tableIdToColumnName <t1, [c1]> <t2, [c1,c2,c3]>
     */
    public static StatisticsJob fromAnalyzeStmt(AnalyzeStmt analyzeStmt) throws DdlException {
        StatisticsJob statisticsJob = new StatisticsJob(1L);

        TableName dbTableName = analyzeStmt.getTableName();
        List<String> columnNames = analyzeStmt.getColumnNames();
        Map<String, String> properties = analyzeStmt.getProperties();

        if (dbTableName != null) {
            String dbName = dbTableName.getDb();
            if (StringUtils.isBlank(dbName)) {
                dbName = analyzeStmt.getAnalyzer().getDefaultDb();
            } else {
                dbName = analyzeStmt.getClusterName() + ":" + dbName;
            }
            String tblName = dbTableName.getTbl();
            Database database = Catalog.getCurrentCatalog().getDbOrDdlException(dbName);
            Table table = database.getOlapTableOrDdlException(tblName);
            if (columnNames == null || columnNames.isEmpty()) {
                List<Column> baseSchema = table.getBaseSchema();
                columnNames = baseSchema.stream().map(Column::getName).collect(Collectors.toList());
            }
            long tableId = table.getId();
            Map<Long, List<String>> tableIdToColumnName = new HashMap<>(1);
            tableIdToColumnName.put(tableId, columnNames);
            statisticsJob.setTableIdToColumnName(tableIdToColumnName);
            statisticsJob.setTableIds(Collections.singletonList(tableId));
        } else {
            String dbName = analyzeStmt.getAnalyzer().getDefaultDb();
            Database database = Catalog.getCurrentCatalog().getDbOrDdlException(dbName);
            List<Table> tables = database.getTables();
            List<Long> tableIds = new ArrayList<>();
            for (int i = tables.size() - 1; i >= 0; i--) {
                long tableId = tables.get(i).getId();
                tableIds.add(tableId);
            }
            statisticsJob.setTableIds(tableIds);
        }

        // TODO
        return new StatisticsJob(1L);
    }

    public Set<Long> relatedTableId() {
        Set<Long> relatedTableId = Sets.newHashSet();
        relatedTableId.addAll(this.tableIds);
        relatedTableId.addAll(this.tableIdToColumnName.keySet());
        return relatedTableId;
    }
}
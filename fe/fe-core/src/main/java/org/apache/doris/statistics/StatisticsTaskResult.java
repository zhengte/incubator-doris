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

import java.util.Map;

public class StatisticsTaskResult {
    private StatsGranularityDesc granularityDesc;
    private StatsCategoryDesc categoryDesc;
    private Map<String, String> statsTypeToValue;

    private long jobId;
    private long taskId;

    public StatisticsTaskResult(long jobId, long taskId,StatsGranularityDesc granularityDesc, StatsCategoryDesc categoryDesc,
                                Map<String, String> statsTypeToValue) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.granularityDesc = granularityDesc;
        this.categoryDesc = categoryDesc;
        this.statsTypeToValue = statsTypeToValue;
    }

    public long getJobId() {
        return this.jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public long getTaskId() {
        return this.taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public StatsGranularityDesc getGranularityDesc() {
        return this.granularityDesc;
    }

    public void setGranularityDesc(StatsGranularityDesc granularityDesc) {
        this.granularityDesc = granularityDesc;
    }

    public StatsCategoryDesc getCategoryDesc() {
        return this.categoryDesc;
    }

    public void setCategoryDesc(StatsCategoryDesc categoryDesc) {
        this.categoryDesc = categoryDesc;
    }

    public Map<String, String> getStatsTypeToValue() {
        return this.statsTypeToValue;
    }

    public void setStatsTypeToValue(Map<String, String> statsTypeToValue) {
        this.statsTypeToValue = statsTypeToValue;
    }
}

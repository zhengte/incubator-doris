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
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.DdlException;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Set;

/**
 * For unified management of statistics job,
 * including job addition, cancellation, scheduling, etc.
 */
public class StatisticsJobManager {
    private static final Logger LOG = LogManager.getLogger(StatisticsJobManager.class);

    // statistics job
    private Map<Long, StatisticsJob> idToStatisticsJob = Maps.newConcurrentMap();

    public void createStatisticsJob(AnalyzeStmt analyzeStmt) throws DdlException {
        StatisticsJob statisticsJob = new StatisticsJob(1L, analyzeStmt);
        statisticsJob.init();
        this.createStatisticsJob(statisticsJob);
    }

    public void createStatisticsJob(StatisticsJob statisticsJob) {
        this.idToStatisticsJob.put(statisticsJob.getId(), statisticsJob);
        try {
            Catalog.getCurrentCatalog().getStatisticsJobScheduler().addPendingJob(statisticsJob);
            statisticsJob.setJobState(StatisticsJob.JobState.SCHEDULING);
            this.idToStatisticsJob.put(statisticsJob.getId(), statisticsJob);
        } catch (IllegalStateException e) {
            LOG.info("The pending statistics job is full. Please submit it again later.");
        }
    }

    public void alterStatisticsJobStats(StatisticsTaskResult taskResult) {
        long jobId = taskResult.getJobId();
        StatisticsJob statisticsJob = this.idToStatisticsJob.get(jobId);
        statisticsJob.setProgress(statisticsJob.getProgress() + 1);
        if (statisticsJob.getTaskList().size() == statisticsJob.getProgress() + 1) {
            statisticsJob.setJobState(StatisticsJob.JobState.FINISHED);
        }
        this.idToStatisticsJob.put(jobId, statisticsJob);
    }

    public Map<Long, StatisticsJob> getIdToStatisticsJob() {
        return this.idToStatisticsJob;
    }

    public void setIdToStatisticsJob(Map<Long, StatisticsJob> idToStatisticsJob) {
        this.idToStatisticsJob = idToStatisticsJob;
    }

    public void showJobsStats(){
        Set<Map.Entry<Long, StatisticsJob>> entries = this.idToStatisticsJob.entrySet();
        for (Map.Entry<Long, StatisticsJob> entry : entries) {
            StatisticsJob value = entry.getValue();
            long id = value.getId();
            StatisticsJob.JobState jobState = value.getJobState();
            System.out.println("========= job state =========: " + id + " " + jobState);
            int progress = value.getProgress();
            int size = value.getTaskList().size();
            System.out.println("========= progress state =========: " + progress + "/" + size);
        }
    }
}

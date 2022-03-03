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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.MasterDaemon;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Queues;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/*
Schedule statistics task
 */
public class StatisticsTaskScheduler extends MasterDaemon {


    private final static Logger LOG = LogManager.getLogger(StatisticsTaskScheduler.class);

    private final Queue<StatisticsTask> queue = Queues.newLinkedBlockingQueue();

    public StatisticsTaskScheduler() {
        super("Statistics task scheduler", 0);
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            Thread.sleep(1000 * 2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        StatisticsJobManager statisticsJobManager = Catalog.getCurrentCatalog().getStatisticsJobManager();
        statisticsJobManager.showJobsStats();

        StatisticsManager statisticsManager = Catalog.getCurrentCatalog().getStatisticsManager();
        statisticsManager.getStatistics().showStatistics();
        // step1: task n concurrent tasks from the queue
        List<StatisticsTask> tasks = this.peek();
        if (!tasks.isEmpty()) {
            Map<Long, Future<StatisticsTaskResult>> futureMap = new HashMap<>(tasks.size());
            // step2: execute tasks
            ThreadPoolExecutor executor = ThreadPoolManager.newDaemonCacheThreadPool(tasks.size(), "statistic-pool", true);
            for (StatisticsTask task : tasks) {
                long jobId = task.getJobId();
                Future<StatisticsTaskResult> taskFuture = executor.submit(task);
                futureMap.put(jobId, taskFuture);
                Map<Long, StatisticsJob> idToStatisticsJob = statisticsJobManager.getIdToStatisticsJob();
                StatisticsJob statisticsJob = idToStatisticsJob.get(jobId);
                if (statisticsJob.getJobState() == StatisticsJob.JobState.SCHEDULING) {
                    statisticsJob.setJobState(StatisticsJob.JobState.RUNNING);
                    idToStatisticsJob.put(jobId, statisticsJob);
                    statisticsJobManager.setIdToStatisticsJob(idToStatisticsJob);
                }
            }
            // step3: update job and statistics
            this.handleTaskResult(futureMap);
            executor.shutdownNow();
        }
        // step4: remove task from queue
        this.remove(tasks.size());
    }

    public void addTasks(List<StatisticsTask> statisticsTaskList) {
        this.queue.addAll(statisticsTaskList);
    }

    private List<StatisticsTask> peek() {
        List<StatisticsTask> tasks = Lists.newArrayList();
        int i = Config.cbo_concurrency_statistics_task_num;
        while (i > 0) {
            StatisticsTask task = this.queue.peek();
            if (task == null) {
                break;
            }
            tasks.add(task);
            i--;
        }
        return tasks;
    }

    private void remove(int size) {
        for (int i = 0; i < size; i++) {
            // StatisticsTask statisticsTask = new StatisticsTask(1, 1);
            // this.queue.remove(statisticsTask);
            this.queue.poll();
        }
    }

    private void handleTaskResult(Map<Long, Future<StatisticsTaskResult>> futureMap) {
        StatisticsManager statisticsManager = Catalog.getCurrentCatalog().getStatisticsManager();
        StatisticsJobManager statisticsJobManager = Catalog.getCurrentCatalog().getStatisticsJobManager();
        Set<Map.Entry<Long, Future<StatisticsTaskResult>>> futures = futureMap.entrySet();
        for (Map.Entry<Long, Future<StatisticsTaskResult>> future : futures) {
            Long jobId = future.getKey();
            try {
                StatisticsTaskResult taskResult = future.getValue().get();
                StatsCategoryDesc categoryDesc = taskResult.getCategoryDesc();
                StatsCategoryDesc.StatsCategory category = categoryDesc.getCategory();
                if (category == StatsCategoryDesc.StatsCategory.TABLE) {
                    // 更新表统计信息
                    statisticsManager.alterTableStatistics(taskResult);
                } else if (category == StatsCategoryDesc.StatsCategory.COLUMN) {
                    // 更新列统计信息
                    statisticsManager.alterColumnStatistics(taskResult);
                }
                // 变更job的task进度--成功
                statisticsJobManager.alterStatisticsJobStats(taskResult);
            } catch (InterruptedException | DdlException | AnalysisException | ExecutionException e) {
                // 变更job的task进度--失败
                Map<Long, StatisticsJob> idToStatisticsJob = statisticsJobManager.getIdToStatisticsJob();
                StatisticsJob statisticsJob = idToStatisticsJob.get(jobId);
                statisticsJob.setJobState(StatisticsJob.JobState.FAILED);
                idToStatisticsJob.put(jobId, statisticsJob);
                statisticsJobManager.setIdToStatisticsJob(idToStatisticsJob);
                LOG.warn("Failed to execute this turn of statistics tasks", e);
            }
        }
    }
}

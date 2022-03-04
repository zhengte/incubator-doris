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

package org.apache.doris.qe;

import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DdlStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.KillStmt;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.SetStmt;
import org.apache.doris.analysis.ShowAuthorStmt;
import org.apache.doris.analysis.ShowStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UseStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.planner.Planner;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicBoolean;

import java_cup.runtime.Symbol;
import mockit.Expectations;
import mockit.Mocked;

public class StmtExecutorTest {
    private ConnectContext ctx;
    private QueryState state;
    private ConnectScheduler scheduler;
    private MysqlChannel channel = null;

    @Mocked
    SocketChannel socketChannel;

    @BeforeClass
    public static void start() {
        MetricRepo.init();
        try {
            FrontendOptions.init();
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Before
    public void setUp() throws IOException {
        this.state = new QueryState();
        this.scheduler = new ConnectScheduler(10);
        this.ctx = new ConnectContext(this.socketChannel);

        SessionVariable sessionVariable = new SessionVariable();
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        Catalog catalog = AccessTestUtil.fetchAdminCatalog();

        this.channel = new MysqlChannel(this.socketChannel);
        new Expectations(this.channel) {
            {
                StmtExecutorTest.this.channel.sendOnePacket((ByteBuffer) this.any);
                this.minTimes = 0;

                StmtExecutorTest.this.channel.reset();
                this.minTimes = 0;
            }
        };

        new Expectations(this.ctx) {
            {
                StmtExecutorTest.this.ctx.getMysqlChannel();
                this.minTimes = 0;
                this.result = StmtExecutorTest.this.channel;

                StmtExecutorTest.this.ctx.getSerializer();
                this.minTimes = 0;
                this.result = serializer;

                StmtExecutorTest.this.ctx.getCatalog();
                this.minTimes = 0;
                this.result = catalog;

                StmtExecutorTest.this.ctx.getState();
                this.minTimes = 0;
                this.result = StmtExecutorTest.this.state;

                StmtExecutorTest.this.ctx.getConnectScheduler();
                this.minTimes = 0;
                this.result = StmtExecutorTest.this.scheduler;

                StmtExecutorTest.this.ctx.getConnectionId();
                this.minTimes = 0;
                this.result = 1;

                StmtExecutorTest.this.ctx.getQualifiedUser();
                this.minTimes = 0;
                this.result = "testUser";

                StmtExecutorTest.this.ctx.getForwardedStmtId();
                this.minTimes = 0;
                this.result = 123L;

                StmtExecutorTest.this.ctx.setKilled();
                this.minTimes = 0;

                StmtExecutorTest.this.ctx.updateReturnRows(this.anyInt);
                this.minTimes = 0;

                StmtExecutorTest.this.ctx.setQueryId((TUniqueId) this.any);
                this.minTimes = 0;

                StmtExecutorTest.this.ctx.queryId();
                this.minTimes = 0;
                this.result = new TUniqueId();

                StmtExecutorTest.this.ctx.getStartTime();
                this.minTimes = 0;
                this.result = 0L;

                StmtExecutorTest.this.ctx.getDatabase();
                this.minTimes = 0;
                this.result = "testCluster:testDb";

                StmtExecutorTest.this.ctx.getSessionVariable();
                this.minTimes = 0;
                this.result = sessionVariable;

                StmtExecutorTest.this.ctx.setStmtId(this.anyLong);
                this.minTimes = 0;

                StmtExecutorTest.this.ctx.getStmtId();
                this.minTimes = 0;
                this.result = 1L;
            }
        };
    }

    @Test
    public void testSelect(@Mocked QueryStmt queryStmt,
                           @Mocked SqlParser parser,
                           @Mocked Planner planner,
                           @Mocked Coordinator coordinator) throws Exception {
        Catalog catalog = Catalog.getCurrentCatalog();
        Deencapsulation.setField(catalog, "canRead", new AtomicBoolean(true));
        Deencapsulation.setField(catalog, "fullNameToDb", new AtomicBoolean(true));

        new Expectations() {
            {
                queryStmt.analyze((Analyzer) this.any);
                this.minTimes = 0;

                queryStmt.getColLabels();
                this.minTimes = 0;
                this.result = Lists.<String>newArrayList();

                queryStmt.getResultExprs();
                this.minTimes = 0;
                this.result = Lists.<Expr>newArrayList();

                queryStmt.isExplain();
                this.minTimes = 0;
                this.result = false;

                queryStmt.getTables((Analyzer) this.any, (SortedMap) this.any, Sets.newHashSet());
                this.minTimes = 0;

                queryStmt.getRedirectStatus();
                this.minTimes = 0;
                this.result = RedirectStatus.NO_FORWARD;

                queryStmt.rewriteExprs((ExprRewriter) this.any);
                this.minTimes = 0;

                Symbol symbol = new Symbol(0, Lists.newArrayList(queryStmt));
                parser.parse();
                this.minTimes = 0;
                this.result = symbol;

                planner.plan((QueryStmt) this.any, (Analyzer) this.any, (TQueryOptions) this.any);
                this.minTimes = 0;

                // mock coordinator
                coordinator.exec();
                this.minTimes = 0;

                coordinator.endProfile();
                this.minTimes = 0;

                coordinator.getQueryProfile();
                this.minTimes = 0;
                this.result = new RuntimeProfile();

                coordinator.getNext();
                this.minTimes = 0;
                this.result = new RowBatch();

                coordinator.getJobId();
                this.minTimes = 0;
                this.result = -1L;

                Catalog.getCurrentCatalog();
                this.minTimes = 0;
                this.result = catalog;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(this.ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.EOF, this.state.getStateType());
    }

    @Test
    public void testShow(@Mocked ShowStmt showStmt, @Mocked SqlParser parser, @Mocked ShowExecutor executor) throws Exception {
        new Expectations() {
            {
                showStmt.analyze((Analyzer) this.any);
                this.minTimes = 0;

                showStmt.getRedirectStatus();
                this.minTimes = 0;
                this.result = RedirectStatus.NO_FORWARD;

                showStmt.toSelectStmt((Analyzer) this.any);
                this.minTimes = 0;
                this.result = null;

                Symbol symbol = new Symbol(0, Lists.newArrayList(showStmt));
                parser.parse();
                this.minTimes = 0;
                this.result = symbol;

                // mock show
                List<List<String>> rows = Lists.newArrayList();
                rows.add(Lists.newArrayList("abc", "bcd"));
                executor.execute();
                this.minTimes = 0;
                this.result = new ShowResultSet(new ShowAuthorStmt().getMetaData(), rows);
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(this.ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.EOF, this.state.getStateType());
    }

    @Test
    public void testShowNull(@Mocked ShowStmt showStmt, @Mocked SqlParser parser, @Mocked ShowExecutor executor) throws Exception {
        new Expectations() {
            {
                showStmt.analyze((Analyzer) this.any);
                this.minTimes = 0;

                showStmt.getRedirectStatus();
                this.minTimes = 0;
                this.result = RedirectStatus.NO_FORWARD;

                showStmt.toSelectStmt((Analyzer) this.any);
                this.minTimes = 0;
                this.result = null;

                Symbol symbol = new Symbol(0, Lists.newArrayList(showStmt));
                parser.parse();
                this.minTimes = 0;
                this.result = symbol;

                // mock show
                List<List<String>> rows = Lists.newArrayList();
                rows.add(Lists.newArrayList("abc", "bcd"));
                executor.execute();
                this.minTimes = 0;
                this.result = null;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(this.ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, this.state.getStateType());
    }

    @Test
    public void testKill(@Mocked KillStmt killStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                killStmt.analyze((Analyzer) this.any);
                this.minTimes = 0;

                killStmt.getConnectionId();
                this.minTimes = 0;
                this.result = 1L;

                killStmt.getRedirectStatus();
                this.minTimes = 0;
                this.result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(killStmt));
                parser.parse();
                this.minTimes = 0;
                this.result = symbol;
            }
        };

        new Expectations(this.scheduler) {
            {
                // suicide
                StmtExecutorTest.this.scheduler.getContext(1);
                this.result = StmtExecutorTest.this.ctx;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(this.ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, this.state.getStateType());
    }

    @Test
    public void testKillOtherFail(@Mocked KillStmt killStmt, @Mocked SqlParser parser, @Mocked ConnectContext killCtx) throws Exception {
        Catalog killCatalog = AccessTestUtil.fetchAdminCatalog();

        new Expectations() {
            {
                killStmt.analyze((Analyzer) this.any);
                this.minTimes = 0;

                killStmt.getConnectionId();
                this.minTimes = 0;
                this.result = 1L;

                killStmt.isConnectionKill();
                this.minTimes = 0;
                this.result = true;

                killStmt.getRedirectStatus();
                this.minTimes = 0;
                this.result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(killStmt));
                parser.parse();
                this.minTimes = 0;
                this.result = symbol;

                killCtx.getCatalog();
                this.minTimes = 0;
                this.result = killCatalog;

                killCtx.getQualifiedUser();
                this.minTimes = 0;
                this.result = "blockUser";

                killCtx.kill(true);
                this.minTimes = 0;

                ConnectContext.get();
                this.minTimes = 0;
                this.result = StmtExecutorTest.this.ctx;
            }
        };

        new Expectations(this.scheduler) {
            {
                // suicide
                StmtExecutorTest.this.scheduler.getContext(1);
                this.result = killCtx;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(this.ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, this.state.getStateType());
    }

    @Test
    public void testKillOther(@Mocked KillStmt killStmt, @Mocked SqlParser parser, @Mocked ConnectContext killCtx) throws Exception {
        Catalog killCatalog = AccessTestUtil.fetchAdminCatalog();
        new Expectations() {
            {
                killStmt.analyze((Analyzer) this.any);
                this.minTimes = 0;

                killStmt.getConnectionId();
                this.minTimes = 0;
                this.result = 1;

                killStmt.isConnectionKill();
                this.minTimes = 0;
                this.result = true;

                killStmt.getRedirectStatus();
                this.minTimes = 0;
                this.result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(killStmt));
                parser.parse();
                this.minTimes = 0;
                this.result = symbol;

                killCtx.getCatalog();
                this.minTimes = 0;
                this.result = killCatalog;

                killCtx.getQualifiedUser();
                this.minTimes = 0;
                this.result = "killUser";

                killCtx.kill(true);
                this.minTimes = 0;

                ConnectContext.get();
                this.minTimes = 0;
                this.result = StmtExecutorTest.this.ctx;
            }
        };

        new Expectations(this.scheduler) {
            {
                // suicide
                StmtExecutorTest.this.scheduler.getContext(1);
                this.result = killCtx;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(this.ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, this.state.getStateType());
    }

    @Test
    public void testKillNoCtx(@Mocked KillStmt killStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                killStmt.analyze((Analyzer) this.any);
                this.minTimes = 0;

                killStmt.getConnectionId();
                this.minTimes = 0;
                this.result = 1;

                killStmt.getRedirectStatus();
                this.minTimes = 0;
                this.result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(killStmt));
                parser.parse();
                this.minTimes = 0;
                this.result = symbol;
            }
        };

        new Expectations(this.scheduler) {
            {
                StmtExecutorTest.this.scheduler.getContext(1);
                this.result = null;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(this.ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, this.state.getStateType());
    }

    @Test
    public void testSet(@Mocked SetStmt setStmt, @Mocked SqlParser parser, @Mocked SetExecutor executor) throws Exception {
        new Expectations() {
            {
                setStmt.analyze((Analyzer) this.any);
                this.minTimes = 0;

                setStmt.getRedirectStatus();
                this.minTimes = 0;
                this.result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(setStmt));
                parser.parse();
                this.minTimes = 0;
                this.result = symbol;

                // Mock set
                executor.execute();
                this.minTimes = 0;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(this.ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, this.state.getStateType());
    }

    @Test
    public void testStmtWithUserInfo(@Mocked StatementBase stmt, @Mocked ConnectContext context) throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(this.ctx, stmt);
        Deencapsulation.setField(stmtExecutor, "parsedStmt", null);
        Deencapsulation.setField(stmtExecutor, "originStmt", new OriginStatement("show databases;", 1));
        stmtExecutor.execute();
        StatementBase newstmt = (StatementBase)Deencapsulation.getField(stmtExecutor, "parsedStmt");
        Assert.assertTrue(newstmt.getUserInfo() != null);
    }

    @Test
    public void testSetFail(@Mocked SetStmt setStmt, @Mocked SqlParser parser, @Mocked SetExecutor executor) throws Exception {
        new Expectations() {
            {
                setStmt.analyze((Analyzer) this.any);
                this.minTimes = 0;

                setStmt.getRedirectStatus();
                this.minTimes = 0;
                this.result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(setStmt));
                parser.parse();
                this.minTimes = 0;
                this.result = symbol;

                // Mock set
                executor.execute();
                this.minTimes = 0;
                this.result = new DdlException("failed");
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(this.ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, this.state.getStateType());
    }

    @Test
    public void testDdl(@Mocked DdlStmt ddlStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                ddlStmt.analyze((Analyzer) this.any);
                this.minTimes = 0;

                ddlStmt.getRedirectStatus();
                this.minTimes = 0;
                this.result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(ddlStmt));
                parser.parse();
                this.minTimes = 0;
                this.result = symbol;
            }
        };

        DdlExecutor ddlExecutor = new DdlExecutor();
        new Expectations(ddlExecutor) {
            {
                // Mock ddl
                DdlExecutor.execute((Catalog) this.any, (DdlStmt) this.any);
                this.minTimes = 0;
            }
        };

        StmtExecutor executor = new StmtExecutor(this.ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, this.state.getStateType());
    }

    @Test
    public void testDdlFail(@Mocked DdlStmt ddlStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                ddlStmt.analyze((Analyzer) this.any);
                this.minTimes = 0;

                ddlStmt.getRedirectStatus();
                this.minTimes = 0;
                this.result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(ddlStmt));
                parser.parse();
                this.minTimes = 0;
                this.result = symbol;
            }
        };

        DdlExecutor ddlExecutor = new DdlExecutor();
        new Expectations(ddlExecutor) {
            {
                // Mock ddl
                DdlExecutor.execute((Catalog) this.any, (DdlStmt) this.any);
                this.minTimes = 0;
                this.result = new DdlException("ddl fail");
            }
        };

        StmtExecutor executor = new StmtExecutor(this.ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, this.state.getStateType());
    }

    @Test
    public void testDdlFail2(@Mocked DdlStmt ddlStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                ddlStmt.analyze((Analyzer) this.any);
                this.minTimes = 0;

                ddlStmt.getRedirectStatus();
                this.minTimes = 0;
                this.result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(ddlStmt));
                parser.parse();
                this.minTimes = 0;
                this.result = symbol;
            }
        };

        DdlExecutor ddlExecutor = new DdlExecutor();
        new Expectations(ddlExecutor) {
            {
                // Mock ddl
                DdlExecutor.execute((Catalog) this.any, (DdlStmt) this.any);
                this.minTimes = 0;
                this.result = new Exception("bug");
            }
        };

        StmtExecutor executor = new StmtExecutor(this.ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, this.state.getStateType());
    }

    @Test
    public void testUse(@Mocked UseStmt useStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                useStmt.analyze((Analyzer) this.any);
                this.minTimes = 0;

                useStmt.getDatabase();
                this.minTimes = 0;
                this.result = "testCluster:testDb";

                useStmt.getRedirectStatus();
                this.minTimes = 0;
                this.result = RedirectStatus.NO_FORWARD;

                useStmt.getClusterName();
                this.minTimes = 0;
                this.result = "testCluster";

                Symbol symbol = new Symbol(0, Lists.newArrayList(useStmt));
                parser.parse();
                this.minTimes = 0;
                this.result = symbol;
            }
        };

        StmtExecutor executor = new StmtExecutor(this.ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, this.state.getStateType());
    }

    @Test
    public void testUseFail(@Mocked UseStmt useStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                useStmt.analyze((Analyzer) this.any);
                this.minTimes = 0;

                useStmt.getDatabase();
                this.minTimes = 0;
                this.result = "blockDb";

                useStmt.getRedirectStatus();
                this.minTimes = 0;
                this.result = RedirectStatus.NO_FORWARD;

                useStmt.getClusterName();
                this.minTimes = 0;
                this.result = "testCluster";

                Symbol symbol = new Symbol(0, Lists.newArrayList(useStmt));
                parser.parse();
                this.minTimes = 0;
                this.result = symbol;
            }
        };

        StmtExecutor executor = new StmtExecutor(this.ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, this.state.getStateType());
    }
}


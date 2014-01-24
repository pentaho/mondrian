/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.server.Execution;
import mondrian.server.Statement;
import mondrian.server.StatementImpl;
import mondrian.spi.Dialect;
import mondrian.spi.StatisticsProvider;
import mondrian.spi.impl.JdbcDialectImpl;

import junit.framework.TestCase;

import javax.sql.DataSource;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

public class RolapSchemaTest extends TestCase {

    public static final int QUERY_CARD_VAL = 1010;
    public static final int TABLE_CARD_VAL = 2010;
    public static final int APPROX_CARD    = 3010;

    public void testGetRelationCardinalityWithTable() {
        StatisticsProvider statsProv = mock(StatisticsProvider.class);
        RolapSchema.PhysTable table = mock(RolapSchema.PhysTable.class);
        RolapSchema.PhysStatistic physStatistic = buildTestPhysStatistic(
            statsProv);
        assertEquals(
            TABLE_CARD_VAL,
            physStatistic.getRelationCardinality(table, "alias", -1));
        assertEquals(
            APPROX_CARD,
            physStatistic.getRelationCardinality(table, "alias", APPROX_CARD));
        verify(statsProv).getTableCardinality(
            any(Dialect.class), any(DataSource.class),
            anyString(), anyString(), anyString(), any(Execution.class));
    }

    public void testGetRelationCardinalityWithQuery() {
        RolapSchema.PhysRelation[] relations = new RolapSchema.PhysRelation[] {
            spy(new RolapSchema.PhysInlineTable(null, "alias")),
            spy(new RolapSchema.PhysView(null, "alias", "sql")) };

        StatisticsProvider statsProv = mock(StatisticsProvider.class);
        RolapSchema.PhysStatistic physStatistic = buildTestPhysStatistic(
            statsProv);
        for (RolapSchema.PhysRelation relation : relations) {
            assertEquals(
                QUERY_CARD_VAL,
                physStatistic.getRelationCardinality(relation, "alias", -1));
            assertEquals(
                APPROX_CARD,
                physStatistic.getRelationCardinality(
                    relation, "alias", APPROX_CARD));
        }
        verify(statsProv, times(relations.length))
            .getQueryCardinality(
                any(Dialect.class), any(DataSource.class),
                anyString(), any(Execution.class));
    }

    private RolapSchema.PhysStatistic buildTestPhysStatistic(
        StatisticsProvider statsProv)
    {
        when(
            statsProv.getTableCardinality(
                any(Dialect.class),
                any(DataSource.class),
                anyString(), anyString(), anyString(), any(Execution.class)))
            .thenReturn(TABLE_CARD_VAL);
        Dialect spyDialect = spy(new JdbcDialectImpl());
        when(statsProv.getQueryCardinality(
            any(Dialect.class), any(DataSource.class),
            anyString(), any(Execution.class))).thenReturn(QUERY_CARD_VAL);
        when(spyDialect.getDatabaseProduct()).thenReturn(
            Dialect.DatabaseProduct.MYSQL);
        List<StatisticsProvider> statsProvs =
            new ArrayList<StatisticsProvider>();
        statsProvs.add(statsProv);
        when(spyDialect.getStatisticsProviders()).thenReturn(statsProvs);
        RolapConnection conn = mock(RolapConnection.class);
        Statement statement = mock(StatementImpl.class);
        when(conn.getInternalStatement()).thenReturn(statement);
        return new RolapSchema.PhysStatistic(spyDialect, conn);
    }
}
// End RolapSchemaTest.java
/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.olap.OlapElement;
import mondrian.rolap.*;
import mondrian.rolap.sql.SqlQuery;
import mondrian.spi.Dialect;

import mondrian.test.TestContext;

import junit.framework.TestCase;

import org.olap4j.OlapConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

public class DrillThroughQuerySpecTest extends TestCase {

  private DrillThroughCellRequest requestMock;
  private StarPredicate starPredicateMock;
  private SqlQuery sqlQueryMock;
  private DrillThroughQuerySpec drillThroughQuerySpec;
  private RolapStar.Column includedColumn;
  private RolapStar.Column excludedColumn;

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    requestMock = mock(DrillThroughCellRequest.class);
    starPredicateMock = mock(StarPredicate.class);
    sqlQueryMock = mock(SqlQuery.class);
    RolapStar.Measure measureMock = mock(RolapStar.Measure.class);
    includedColumn = mock(RolapStar.Column.class);
    excludedColumn = mock(RolapStar.Column.class);
    RolapStar starMock = mock(RolapStar.class);

    when(requestMock.includeInSelect(any(RolapStar.Column.class)))
      .thenReturn(true);
    when(requestMock.getMeasure()).thenReturn(measureMock);
    when(requestMock.getConstrainedColumns())
      .thenReturn(new RolapStar.Column[0]);
    when(measureMock.getStar()).thenReturn(starMock);
    when(starMock.getSqlQueryDialect()).thenReturn(mock(Dialect.class));
    when(starPredicateMock.getConstrainedColumnList())
      .thenReturn(Collections.singletonList(includedColumn));
    when(includedColumn.getTable()).thenReturn(mock(RolapStar.Table.class));
    when(excludedColumn.getTable()).thenReturn(mock(RolapStar.Table.class));
    drillThroughQuerySpec =
      new DrillThroughQuerySpec
        (requestMock, starPredicateMock, new ArrayList<OlapElement> (), false);
  }

  public void testEmptyColumns() {
    List<RolapStar.Column> columns = Collections.emptyList();
    when(starPredicateMock.getConstrainedColumnList())
      .thenReturn(columns);
    drillThroughQuerySpec.extraPredicates(sqlQueryMock);
    verify(sqlQueryMock, times(0))
      .addSelect(anyString(), any(SqlStatement.Type.class), anyString());
  }

  public void testOneColumnExists() {
    drillThroughQuerySpec.extraPredicates(sqlQueryMock);
    verify(sqlQueryMock, times(1))
      .addSelect(anyString(), any(SqlStatement.Type.class), anyString());
  }

  public void testTwoColumnsExist() {
    when(starPredicateMock.getConstrainedColumnList())
      .thenReturn(Arrays.asList(includedColumn, excludedColumn));
    drillThroughQuerySpec.extraPredicates(sqlQueryMock);
    verify(sqlQueryMock, times(2))
      .addSelect(anyString(), any(SqlStatement.Type.class), anyString());
  }

  public void testColumnsNotIncludedInSelect() {
    when(requestMock.includeInSelect(includedColumn)).thenReturn(false);
    drillThroughQuerySpec.extraPredicates(sqlQueryMock);
    verify(sqlQueryMock, times(0))
      .addSelect(anyString(), any(SqlStatement.Type.class), anyString());

    when(starPredicateMock.getConstrainedColumnList())
      .thenReturn(Arrays.asList(includedColumn, excludedColumn));
    verify(sqlQueryMock, times(0))
      .addSelect(anyString(), any(SqlStatement.Type.class), anyString());
  }

  public void testColumnsPartiallyIncludedInSelect() {
    when(requestMock.includeInSelect(excludedColumn)).thenReturn(false);
    when(requestMock.includeInSelect(includedColumn)).thenReturn(true);
    when(starPredicateMock.getConstrainedColumnList())
      .thenReturn(Arrays.asList(includedColumn, excludedColumn));

    drillThroughQuerySpec.extraPredicates(sqlQueryMock);
    verify(sqlQueryMock, times(1))
      .addSelect(anyString(), any(SqlStatement.Type.class), anyString());
  }

  // test that returns correct number of columns
  public void testMdxQuery() throws SQLException {
    String drillThroughMdx = "DRILLTHROUGH WITH "
        + "SET [*NATIVE_CJ_SET_WITH_SLICER] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Product_],[*BASE_MEMBERS__Store Type_])' "
        + "SET [*NATIVE_CJ_SET] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Product].CURRENTMEMBER)})' "
        + "SET [*BASE_MEMBERS__Store Type_] AS 'FILTER([Store Type].[Store Type].MEMBERS,[Store Type].CURRENTMEMBER "
        + "NOT IN {[Store Type].[All Store Types].[Small Grocery]})' "
        + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Product].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Product]"
        + ".CURRENTMEMBER,[Product].[Product Family]).ORDERKEY,BASC)' "
        + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[Warehouse Cost]}' "
        + "SET [*CJ_SLICER_AXIS] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Store Type].CURRENTMEMBER)})' "
        + "SET [*BASE_MEMBERS__Product_] AS '[Product].[Product Department].MEMBERS' "
        + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Product].CURRENTMEMBER)})' "
        + "SELECT "
        + "FILTER([*BASE_MEMBERS__Measures_],([Measures].CurrentMember Is [Measures].[Warehouse Cost])) ON COLUMNS "
        + ",FILTER([*SORTED_ROW_AXIS],([Product].CurrentMember Is [Product].[Drink].[Alcoholic Beverages])) ON ROWS "
        + "FROM [Warehouse] " + "WHERE ([*CJ_SLICER_AXIS]) "
        + "RETURN [Product].[Product Department]";

    OlapConnection olap4jConnection = TestContext.instance()
      .getOlap4jConnection();
    ResultSet resultSet = olap4jConnection.createStatement()
      .executeQuery(drillThroughMdx);

    assertEquals(1, resultSet.getMetaData().getColumnCount());
    assertEquals
      ("product_department", resultSet.getMetaData().getColumnName(1));
  }

}
// End DrillThroughQuerySpecTest.java

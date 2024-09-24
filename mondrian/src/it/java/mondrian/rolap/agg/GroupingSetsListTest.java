/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2018 Hitachi Vantara
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import junit.framework.TestCase;
import mondrian.rolap.BitKey;
import mondrian.rolap.RolapSchema;
import mondrian.rolap.RolapStar;
import mondrian.rolap.StarColumnPredicate;
import mondrian.rolap.StarPredicate;
import mondrian.util.ByteString;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GroupingSetsListTest extends TestCase {

  private List<GroupingSet> groupingSetList;
  private GroupingSetsList testObject;
  private BitKey bitkeyMock = mock(BitKey.class);

  private RolapStar starMock;
  private RolapStar.Measure measureMock = mock(RolapStar.Measure.class);

  private RolapStar.Column col1, col2, col3, col4;
  private RolapStar.Column[] columns;

  protected void setUp() throws Exception {
    super.setUp();
    //Get star mock - just to allow creating Segment
    starMock = getStarMock();
    //Columns mocks
    col1 = getColumnMock("LV1_ID", "Table1");
    col2 = getColumnMock("LV1_ID", "Table2");
    col3 = getColumnMock("LV1_ID", "Table3");
    col4 = getColumnMock("LV1_ID", "Table4");
    columns = new RolapStar.Column[] {col1, col2, col3, col4 };

    groupingSetList = createGroupingSetList();
  }

  public void testNewGroupingSetsList_RollupColumnsFoundCorrectly() {
    testObject = new GroupingSetsList(groupingSetList);
    assertNotNull(testObject);
    assertSame(groupingSetList, testObject.getGroupingSets());
    assertTrue(testObject.useGroupingSets());
    // verify count of grouping sets for columns
    assertEquals(
        expectedGroupingSetsColumns().size(),
        testObject.getGroupingSetsColumns().size());
    // verify columns in each of groups
    for (int i = 0; i < expectedGroupingSetsColumns().size(); i++) {
      assertEquals(
          expectedGroupingSetsColumns().get(i).length,
          testObject.getGroupingSetsColumns().get(i).length);
      for (int j = 0; j < expectedGroupingSetsColumns().get(i).length; j++) {
        assertEquals(
            expectedGroupingSetsColumns().get(i)[j],
            testObject.getGroupingSetsColumns().get(i)[j]);
      }
    }
    assertEquals(2, testObject.getRollupColumns().size());
    assertEquals(Arrays.asList(col3, col4), testObject.getRollupColumns());
    assertEquals(5, testObject.getGroupingBitKeyIndex());
  }

  private List<RolapStar.Column[]> expectedGroupingSetsColumns() {
    List<RolapStar.Column[]> ls = new ArrayList<RolapStar.Column[]>();
    ls.add(columns);
    ls.add(new RolapStar.Column[] {col1, col2, col3});
    ls.add(new RolapStar.Column[] {col1, col2});
    return ls;
  }
  private List<GroupingSet> createGroupingSetList() {
    List<GroupingSet> grList = new ArrayList<GroupingSet>();
    //We have 3 grouping sets for testing
    //detailed grouping set - all columns used
    grList.add(createGroupingSet(4));
    //rolled-up grouping sets
    grList.add(createGroupingSet(3));
    grList.add(createGroupingSet(2));
    return grList;
  }
  private GroupingSet createGroupingSet(int columnCount) {
    StarColumnPredicate[] predicates = new StarColumnPredicate[] {};
    List<StarPredicate> compPredicates = new ArrayList<StarPredicate>();
   RolapStar.Column[] c = new RolapStar.Column[columnCount];
    //Every grouping set will contain different count of columns
    for (int i = 0; i < columnCount; i++) {
      c[i] = columns[i];
    }
    Segment segment =
        new Segment(
            starMock, bitkeyMock, c, measureMock,
            predicates, null, compPredicates);
    GroupingSet grSet =
        new GroupingSet(
            Arrays.asList(segment), bitkeyMock,
            bitkeyMock, predicates, c);
    return grSet;
  }

  private static RolapStar.Column getColumnMock(
      String columnName,
      String tableName)
  {
    RolapStar.Column colMock = mock(RolapStar.Column.class);
    RolapStar.Table tableMock = mock(RolapStar.Table.class);
    when(colMock.getName()).thenReturn(columnName);
    when(colMock.getTable()).thenReturn(tableMock);
    when(tableMock.getAlias()).thenReturn(tableName);
   return colMock;
  }

  private static RolapStar getStarMock() {
    RolapStar mock = mock(RolapStar.class);

    RolapStar.Table tableMock = mock(RolapStar.Table.class);
    RolapSchema schemaMock = mock(RolapSchema.class);
    ByteString md5 = new ByteString("test schema".getBytes());
    when(mock.getSchema()).thenReturn(schemaMock);
    when(schemaMock.getChecksum()).thenReturn(md5);
    when(mock.getFactTable()).thenReturn(tableMock);
    when(tableMock.getAlias()).thenReturn("Table Mock");
    return mock;
  }

}
// End GroupingSetsListTest.java

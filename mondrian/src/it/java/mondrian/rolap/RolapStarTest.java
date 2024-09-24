/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2018 Hitachi Vantara..  All rights reserved.
*/

package mondrian.rolap;

import mondrian.olap.Connection;
import mondrian.olap.MondrianDef;
import mondrian.olap.MondrianDef.SQL;
import mondrian.rolap.RolapStar.Column;
import mondrian.test.FoodMartTestCase;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import junit.framework.Assert;

import javax.sql.DataSource;

/**
 * Unit test for {@link RolapStar}.
 *
 * @author pedrovale
 */
public class RolapStarTest extends FoodMartTestCase {

    static class RolapStarForTests extends RolapStar {
        public RolapStarForTests(
            final RolapSchema schema,
            final DataSource dataSource,
            final MondrianDef.Relation fact)
        {
            super(schema, dataSource, fact);
        }

        public MondrianDef.RelationOrJoin cloneRelationForTests(
            MondrianDef.Relation rel,
            String possibleName)
        {
            return cloneRelation(rel, possibleName);
        }
    }

    RolapStar getStar(Connection con, String starName) {
        RolapCube cube =
            (RolapCube) con.getSchema().lookupCube(starName, true);
        return cube.getStar();
    }

    RolapStarForTests getStar(String starName) {
        Connection con = getTestContext().getConnection();
        RolapStar rs =  getStar(con, starName);

        return new RolapStarForTests(
            rs.getSchema(),
            rs.getDataSource(),
            rs.getFactTable().getRelation());
    }

    /**
     * Tests that given a {@link mondrian.olap.MondrianDef.Table}, cloneRelation
     * respects the existing filters.
     */
    public void testCloneRelationWithFilteredTable() {
      RolapStarForTests rs = getStar("sales");
      MondrianDef.Table original = new MondrianDef.Table();
      original.name = "TestTable";
      original.alias = "Alias";
      original.schema = "Sechema";
      original.filter = new SQL();
      original.filter.dialect = "generic";
      original.filter.cdata = "Alias.clicked = 'true'";

      MondrianDef.Table cloned = (MondrianDef.Table)rs.cloneRelationForTests(
          original,
          "NewAlias");

      Assert.assertEquals("NewAlias", cloned.alias);
      Assert.assertEquals("TestTable", cloned.name);
      Assert.assertNotNull(cloned.filter);
      Assert.assertEquals("NewAlias.clicked = 'true'", cloned.filter.cdata);
  }

   //Below there are tests for mondrian.rolap.RolapStar.ColumnComparator
   public void testTwoColumnsWithDifferentNamesNotEquals() {
     RolapStar.ColumnComparator colComparator =
         RolapStar.ColumnComparator.instance;
     Column column1 = getColumnMock("Column1", "Table1");
     Column column2 = getColumnMock("Column2", "Table1");
     assertNotSame(column1, column2);
     assertEquals(-1, colComparator.compare(column1, column2));
   }

   public void testTwoColumnsWithEqualsNamesButDifferentTablesNotEquals() {
     RolapStar.ColumnComparator colComparator =
         RolapStar.ColumnComparator.instance;
     Column column1 = getColumnMock("Column1", "Table1");
     Column column2 = getColumnMock("Column1", "Table2");
     assertNotSame(column1, column2);
     assertEquals(-1, colComparator.compare(column1, column2));
   }

   public void testTwoColumnsEquals() {
     RolapStar.ColumnComparator colComparator =
         RolapStar.ColumnComparator.instance;
     Column column1 = getColumnMock("Column1", "Table1");
     Column column2 = getColumnMock("Column1", "Table1");
     assertNotSame(column1, column2);
     assertEquals(0, colComparator.compare(column1, column2));
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
}

// End RolapStarTest.java

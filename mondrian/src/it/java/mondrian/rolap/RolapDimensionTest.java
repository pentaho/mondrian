/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/
package mondrian.rolap;

import mondrian.olap.MondrianDef;
import mondrian.spi.Dialect;
import mondrian.test.PropertyRestoringTestCase;

import org.mockito.Mockito;

public class RolapDimensionTest extends PropertyRestoringTestCase {

  private RolapSchema schema;
  private RolapCube cube;
  private MondrianDef.Dimension xmlDimension;
  private MondrianDef.CubeDimension xmlCubeDimension;
  private MondrianDef.Hierarchy hierarchy;

  @Override
  protected void setUp() throws Exception {
    schema = Mockito.mock(RolapSchema.class);
    cube = Mockito.mock(RolapCube.class);
    MondrianDef.Relation fact = Mockito.mock(MondrianDef.Relation.class);

    Mockito.when(cube.getSchema()).thenReturn(schema);
    Mockito.when(cube.getFact()).thenReturn(fact);

    xmlDimension = new MondrianDef.Dimension();
    hierarchy = new MondrianDef.Hierarchy();
    MondrianDef.Level level = new MondrianDef.Level();
    xmlCubeDimension = new MondrianDef.Dimension();

    xmlDimension.name = "dimensionName";
    xmlDimension.visible = true;
    xmlDimension.highCardinality = true;
    xmlDimension.hierarchies = new MondrianDef.Hierarchy[] {hierarchy};


    hierarchy.visible = true;
    hierarchy.hasAll = false;
    hierarchy.levels = new MondrianDef.Level[]{level};

    level.visible = true;
    level.properties = new MondrianDef.Property[0];
    level.uniqueMembers = true;
    level.type = Dialect.Datatype.String.name();
    level.hideMemberIf = "Never";
    level.levelType = "Regular";
  }

  public void testHierarchyRelation() {
    MondrianDef.Relation hierarchyTable = Mockito
            .mock(MondrianDef.Relation.class);
    hierarchy.relation = hierarchyTable;

    new RolapDimension(schema, cube, xmlDimension, xmlCubeDimension);
    assertNotNull(hierarchy);
    assertEquals(hierarchyTable, hierarchy.relation);
  }

  /**
   * Check that hierarchy.relation is not set to cube.fact
   */
  public void testHierarchyRelationNotSet() {
    new RolapDimension(schema, cube, xmlDimension, xmlCubeDimension);

    assertNotNull(hierarchy);
    assertNull(hierarchy.relation);
  }

}

// End RolapDimensionTest.java

/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.metadata.*;

import java.sql.SQLException;
import java.util.List;
import java.util.Random;

/**
 * Tests mondrian's olap4j API.
 *
 * <p>Test cases in this test could, in principle, be moved to olap4j's test.
 *
 * @author jhyde
 * @version $Id$
 */
public class Olap4jTest extends FoodMartTestCase {
    @SuppressWarnings({"UnusedDeclaration"})
    public Olap4jTest() {
        super();
    }

    @SuppressWarnings({"UnusedDeclaration"})
    public Olap4jTest(String name) {
        super(name);
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-920">
     * MONDRIAN-920, "olap4j: inconsistent measure's member type"</a>.
     *
     * @throws java.sql.SQLException on error
     */
    public void testSameMemberByVariousMeans() throws SQLException {
        Random random = new Random();
        for (int i = 0; i < 20; i++) {
            int n = random.nextInt(7);
            Member member = foo(n);
            String s = "source #" + n;
            assertEquals(s, "Unit Sales", member.getName());
            assertEquals(s, Member.Type.MEASURE, member.getMemberType());
        }
    }

    private Member foo(int i) throws SQLException {
        final OlapConnection connection =
            getTestContext().getOlap4jConnection();
        final Cube cube;
        final Hierarchy measuresHierarchy;
        final CellSet cellSet;
        switch (i) {
        case 0:
            cellSet = connection.createStatement().executeOlapQuery(
                "select [Measures].[Unit Sales] on 0\n"
                + "from [Sales]");
            return cellSet.getAxes().get(0).getPositions().get(0)
                .getMembers().get(0);

        case 1:
            cellSet =
                connection.createStatement().executeOlapQuery(
                    "select [Measures].Members on 0\n"
                    + "from [Sales]");
            return cellSet.getAxes().get(0).getPositions().get(0)
                .getMembers().get(0);

        case 2:
            cellSet =
                connection.createStatement().executeOlapQuery(
                    "select [Measures].[Measures].Members on 0\n"
                    + "from [Sales]");
            return cellSet.getAxes().get(0).getPositions().get(0)
                .getMembers().get(0);

        case 3:
            cube = connection.getOlapSchema().getCubes().get("Sales");
            measuresHierarchy = cube.getHierarchies().get("Measures");
            final NamedList<Member> rootMembers =
                measuresHierarchy.getRootMembers();
            return rootMembers.get(0);

        case 4:
            cube = connection.getOlapSchema().getCubes().get("Sales");
            measuresHierarchy = cube.getHierarchies().get("Measures");
            final Level measuresLevel = measuresHierarchy.getLevels().get(0);
            final List<Member> levelMembers = measuresLevel.getMembers();
            return levelMembers.get(0);

        case 5:
            cube = connection.getOlapSchema().getCubes().get("Sales");
            measuresHierarchy = cube.getHierarchies().get("Measures");
            return measuresHierarchy.getDefaultMember();

        case 6:
            cube = connection.getOlapSchema().getCubes().get("Sales");
            return
                cube.lookupMember(
                    IdentifierNode.parseIdentifier("[Measures].[Unit Sales]")
                        .getSegmentList());
        default:
            throw new IllegalArgumentException("bad index " + i);
        }
    }
}

// End Olap4jTest.java

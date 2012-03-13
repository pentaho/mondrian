/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.*;
import mondrian.xmla.XmlaHandler;

import org.olap4j.*;
import org.olap4j.Cell;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.metadata.*;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.Property;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Tests mondrian's olap4j API.
 *
 * <p>Test cases in this test could, in principle, be moved to olap4j's test.
 *
 * @author jhyde
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

    public void testAnnotation() throws SQLException {
        final OlapConnection connection =
            getTestContext().getOlap4jConnection();
        final CellSet cellSet =
            connection.createStatement().executeOlapQuery(
                "select from [Sales]");
        final CellSetMetaData metaData = cellSet.getMetaData();
        final Cube salesCube = metaData.getCube();
        Annotated annotated = ((OlapWrapper) salesCube).unwrap(Annotated.class);
        final Annotation annotation =
            annotated.getAnnotationMap().get("caption.fr_FR");
        assertEquals("Ventes", annotation.getValue());

        final Map<String, Object> map =
            XmlaHandler.getExtra(connection).getAnnotationMap(salesCube);
        assertEquals("Ventes", map.get("caption.fr_FR"));
    }

    public void testFormatString() throws SQLException {
        final OlapConnection connection =
            getTestContext().getOlap4jConnection();
        final CellSet cellSet =
            connection.createStatement().executeOlapQuery(
                "with member [Measures].[Foo] as 1, FORMAT_STRING = Iif(1 < 2, '##.0%', 'foo')\n"
                + "select\n"
                + " [Measures].[Foo] DIMENSION PROPERTIES FORMAT_EXP on 0\n"
                + "from [Sales]");
        final CellSetAxis axis = cellSet.getAxes().get(0);
        final Member member =
            axis.getPositions().get(0).getMembers().get(0);
        Property property = findProperty(axis, "FORMAT_EXP");
        assertNotNull(property);

        // Note that the expression is returned, unevaluated. You can tell from
        // the parentheses and quotes that it has been un-parsed.
        assertEquals(
            "IIf((1 < 2), \"##.0%\", \"foo\")",
            member.getPropertyValue(property));
    }

    /**
     * Tests that a property that is not a standard olap4j property but is a
     * Mondrian-builtin property (viz, "FORMAT_EXP") is included among a level's
     * properties.
     *
     * @throws SQLException on error
     */
    public void testLevelProperties() throws SQLException {
        final OlapConnection connection =
            getTestContext().getOlap4jConnection();
        final CellSet cellSet =
            connection.createStatement().executeOlapQuery(
                "select [Store].[Store Name].Members on 0\n"
                + "from [Sales]");
        final CellSetAxis axis = cellSet.getAxes().get(0);
        final Member member =
            axis.getPositions().get(0).getMembers().get(0);
        final NamedList<Property> properties =
            member.getLevel().getProperties();
        // UNIQUE_NAME is an olap4j standard property.
        assertNotNull(properties.get("MEMBER_UNIQUE_NAME"));
        // FORMAT_EXP is a Mondrian built-in but not olap4j standard property.
        assertNotNull(properties.get("FORMAT_EXP"));
        // [Store Type] is a property of the level.
        assertNotNull(properties.get("Store Type"));
    }

    private Property findProperty(CellSetAxis axis, String name) {
        for (Property property : axis.getAxisMetaData().getProperties()) {
            if (property.getName().equals(name)) {
                return property;
            }
        }
        return null;
    }

    public void testCellProperties() throws SQLException {
        final OlapConnection connection =
            getTestContext().getOlap4jConnection();
        final CellSet cellSet =
            connection.createStatement().executeOlapQuery(
                "with member [Customers].[USA].[CA WA] as\n"
                + " Aggregate({[Customers].[USA].[CA], [Customers].[USA].[WA]})\n"
                + "select [Measures].[Unit Sales] on 0,\n"
                + " {[Customers].[USA].[CA], [Customers].[USA].[CA WA]} on 1\n"
                + "from [Sales]\n"
                + "cell properties ACTION_TYPE, DRILLTHROUGH_COUNT");
        final CellSetMetaData metaData = cellSet.getMetaData();
        final Property actionTypeProperty =
            metaData.getCellProperties().get("ACTION_TYPE");
        final Property drillthroughCountProperty =
            metaData.getCellProperties().get("DRILLTHROUGH_COUNT");

        // Cell [0, 0] is drillable
        final Cell cell0 = cellSet.getCell(0);
        final int actionType0 =
            (Integer) cell0.getPropertyValue(actionTypeProperty);
        assertEquals(0x100, actionType0); // MDACTION_TYPE_DRILLTHROUGH
        final int drill0 =
            (Integer) cell0.getPropertyValue(drillthroughCountProperty);
        assertEquals(24442, drill0);

        // Cell [0, 1] is not drillable
        final Cell cell1 = cellSet.getCell(1);
        final int actionType1 =
            (Integer) cell1.getPropertyValue(actionTypeProperty);
        assertEquals(0x0, actionType1);
        final int drill1 =
            (Integer) cell1.getPropertyValue(drillthroughCountProperty);
        assertEquals(-1, drill1);
    }

    /**
     * Same case as
     * {@link mondrian.test.BasicQueryTest#testQueryIterationLimit()}, but this
     * time, check that the OlapException has the required SQLstate.
     *
     * @throws SQLException on error
     */
    public void testLimit() throws SQLException {
        propSaver.set(MondrianProperties.instance().IterationLimit, 11);
        String queryString =
            "With Set [*NATIVE_CJ_SET] as "
            + "'NonEmptyCrossJoin([*BASE_MEMBERS_Dates], [*BASE_MEMBERS_Stores])' "
            + "Set [*BASE_MEMBERS_Dates] as '{[Time].[1997].[Q1], [Time].[1997].[Q2], [Time].[1997].[Q3], [Time].[1997].[Q4]}' "
            + "Set [*GENERATED_MEMBERS_Dates] as 'Generate([*NATIVE_CJ_SET], {[Time].[Time].CurrentMember})' "
            + "Set [*GENERATED_MEMBERS_Measures] as '{[Measures].[*SUMMARY_METRIC_0]}' "
            + "Set [*BASE_MEMBERS_Stores] as '{[Store].[USA].[CA], [Store].[USA].[WA], [Store].[USA].[OR]}' "
            + "Set [*GENERATED_MEMBERS_Stores] as 'Generate([*NATIVE_CJ_SET], {[Store].CurrentMember})' "
            + "Member [Time].[Time].[*SM_CTX_SEL] as 'Aggregate([*GENERATED_MEMBERS_Dates])' "
            + "Member [Measures].[*SUMMARY_METRIC_0] as '[Measures].[Unit Sales]/([Measures].[Unit Sales],[Time].[*SM_CTX_SEL])' "
            + "Member [Time].[Time].[*SUBTOTAL_MEMBER_SEL~SUM] as 'sum([*GENERATED_MEMBERS_Dates])' "
            + "Member [Store].[*SUBTOTAL_MEMBER_SEL~SUM] as 'sum([*GENERATED_MEMBERS_Stores])' "
            + "select crossjoin({[Time].[*SUBTOTAL_MEMBER_SEL~SUM]}, {[Store].[*SUBTOTAL_MEMBER_SEL~SUM]}) "
            + "on columns from [Sales]";

        final OlapConnection connection =
            getTestContext().getOlap4jConnection();

        try {
            final CellSet cellSet =
                connection.createStatement().executeOlapQuery(queryString);
            fail("expected exception, got " + cellSet);
        } catch (OlapException e) {
            assertEquals("ResourceLimitExceeded", e.getSQLState());
        }
    }

    public void testCloseOnCompletion() throws Exception {
        if (Util.JdbcVersion < 0x0401) {
            // Statement.closeOnCompletion added in JDBC 4.1 / JDK 1.7.
            return;
        }
        final OlapConnection connection =
            getTestContext().getOlap4jConnection();
        for (boolean b : new boolean[] {false, true}) {
            final OlapStatement statement = connection.createStatement();
            final CellSet cellSet = statement.executeOlapQuery(
                "select [Measures].[Unit Sales] on 0\n"
                + "from [Sales]");
            if (b) {
                closeOnCompletion(statement);
            }
            assertFalse(isClosed(cellSet));
            assertFalse(isClosed(statement));
            cellSet.close();
            assertTrue(isClosed(cellSet));
            assertEquals(b, isClosed(statement));
            statement.close(); // not error to close twice
        }
    }

    /**
     * Calls {@link java.sql.Statement#isClosed()} or
     * {@link java.sql.ResultSet#isClosed()} via reflection.
     *
     * @param statement Statement or result set
     * @return Whether statement or result set is closed
     * @throws Exception on error
     */
    static boolean isClosed(Object statement) throws Exception {
        Method method =
            (statement instanceof Statement
                ? java.sql.Statement.class
                : java.sql.ResultSet.class).getMethod("isClosed");
        return (Boolean) method.invoke(statement);
    }

    /**
     * Calls {@link java.sql.Statement#closeOnCompletion()} via reflection.
     *
     * @param statement Statement or result set
     * @throws Exception on error
     */
    static void closeOnCompletion(Object statement) throws Exception {
        Method method = java.sql.Statement.class.getMethod("closeOnCompletion");
        method.invoke(statement);
    }
}

// End Olap4jTest.java

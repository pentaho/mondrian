/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2018 Hitachi Vantara..  All rights reserved.
*/

package mondrian.test;

import mondrian.olap.*;
import mondrian.xmla.XmlaHandler;

import org.olap4j.*;
import org.olap4j.Cell;
import org.olap4j.Position;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.mdx.SelectNode;
import org.olap4j.mdx.parser.*;
import org.olap4j.metadata.*;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.Property;

import java.lang.reflect.Method;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

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
    
    public void testLevelDataType() throws SQLException {
  
        final OlapConnection connection = getTestContext().getOlap4jConnection();
        Cube cube = connection.getOlapSchema().getCubes().get( "Sales" );
        Hierarchy hier = cube.getHierarchies().get( "Customers" );
        
        Level level = hier.getLevels().get( 4 );
        assertEquals( "Name", level.getName());
        assertEquals( "Numeric", XmlaHandler.getExtra(connection).getLevelDataType( level ));
        
        level = hier.getLevels().get( 3 );
        assertEquals( "City", level.getName());
        assertEquals( "String", XmlaHandler.getExtra(connection).getLevelDataType( level ));
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
     * Calls {@link java.sql.Statement}.closeOnCompletion() via reflection.
     * (It cannot be called directly because it only exists from JDK 1.7
     * onwards.)
     *
     * @param statement Statement or result set
     * @throws Exception on error
     */
    static void closeOnCompletion(Object statement) throws Exception {
        Method method = java.sql.Statement.class.getMethod("closeOnCompletion");
        method.invoke(statement);
    }

    public void testDrillThrough() throws Exception {
        final OlapConnection connection =
            getTestContext().getOlap4jConnection();
        final OlapStatement statement = connection.createStatement();
        final ResultSet resultSet =
            statement.executeQuery(
                "DRILLTHROUGH MAXROWS 100\n"
                + "SELECT FROM [Sales] WHERE [Measures].[Unit Sales]");
        final ArrayBlockingQueue<String> results =
            new ArrayBlockingQueue<String>(1);

        // Synchronous. Works fine.
        ResultSetMetaData metaData = resultSet.getMetaData();
        results.add("foreground " + metaData.getColumnCount());
        assertEquals("foreground 29", results.poll(10, TimeUnit.SECONDS));

        // Background. Works fine.
        Executor executor =
            Executors.newSingleThreadExecutor();
        executor.execute(
            new Runnable() {
                public void run() {
                    try {
                        ResultSetMetaData metaData = resultSet.getMetaData();
                        results.add("background " + metaData.getColumnCount());
                    } catch (SQLException e) {
                        System.out.println(e);
                    }
                }
            });
        assertEquals("background 29", results.poll(10, TimeUnit.SECONDS));

        // Background, after result set has been closed. Expect an
        // error saying that the statement has been closed.
        executor.execute(
            new Runnable() {
                public void run() {
                    try {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            results.add(e.toString());
                        }
                        ResultSetMetaData metaData = resultSet.getMetaData();
                        results.add("background2 " + metaData.getColumnCount());
                    } catch (SQLException e) {
                        results.add(e.toString());
                    }
                }
            });
        resultSet.close();
        assertEquals(
            "java.sql.SQLException: Invalid operation. Statement is closed.",
            results.poll(10, TimeUnit.SECONDS));
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1204">MONDRIAN-1204,
     * "Olap4j's method toOlap4j throws NPE if we have a function"</a>.
     */
    public void testBugMondrian1204() throws SQLException {
        final OlapConnection connection =
            TestContext.instance().getOlap4jConnection();
        final String mdx =
            "SELECT\n"
            + "NON EMPTY {Hierarchize({[Measures].[Customer Count]})} ON COLUMNS,\n"
            + "CurrentDateMember([Time], \"\"\"[Time].[Year].[1997]\"\"\") ON ROWS\n"
            + "FROM [Sales 2]";
        try {
            final MdxParserFactory parserFactory =
                connection.getParserFactory();
            MdxParser mdxParser =
                parserFactory.createMdxParser(connection);
            MdxValidator mdxValidator =
                parserFactory.createMdxValidator(connection);

            SelectNode select = mdxParser.parseSelect(mdx);
            SelectNode validatedSelect = mdxValidator.validateSelect(select);
            Util.discard(validatedSelect);
        } finally {
            Util.close(null, null, connection);
        }
    }

    /**
     * Runs a statement repeatedly, flushing cache every 10 iterations and
     * calling cancel at random intervals.
     *
     * <p>Test case for </p>
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1217">MONDRIAN-1217,
     * "Statement.cancel() during fact query leads to permanent segment
     * lock"</a>.
     */
    public void testBugMondrian1217() throws SQLException {
        // The checked-in version does nothing. Uncomment one of the following
        // lines to stress the system in a dev environment.
        if (false) {
            checkBugMondrian1217(10000, 20000);
        }
        if (false) {
            checkBugMondrian1217(1000, 4000);
        }
    }

    private void checkBugMondrian1217(final int cancelMin, final int cancelMax)
        throws SQLException
    {
        assert cancelMin < cancelMax;
        final AtomicBoolean finished = new AtomicBoolean(false);
        final AtomicInteger failCount = new AtomicInteger();
        final AtomicInteger cancelCount = new AtomicInteger();
        final AtomicInteger tryCancelCount = new AtomicInteger();
        final AtomicInteger actualCancelCount = new AtomicInteger();
        final AtomicReference<Statement> stmtRef =
            new AtomicReference<Statement>();
        try {
            TestContext testContext = TestContext.instance();
            final OlapConnection connection =
                testContext.getOlap4jConnection();

            final Thread thread = new Thread(
                new Runnable() {
                    public void run() {
                        final Random random = new Random();
                        while (!finished.get()) {
                            try {
                                Thread.sleep(
                                    random.nextInt(cancelMax - cancelMin)
                                    + cancelMin);
                            } catch (InterruptedException e) {
                                return;
                            }
                            try {
                                Statement statement = stmtRef.get();
                                tryCancelCount.incrementAndGet();
                                if (statement != null) {
                                    actualCancelCount.incrementAndGet();
                                    statement.cancel();
                                }
                            } catch (SQLException e) {
                                failCount.incrementAndGet();
                                e.printStackTrace();
                            }
                        }
                    }
                });
            thread.start();

            CacheControl cacheControl = testContext.getCacheControl();
            Cube cube0 =
                connection.getOlapSchema().getCubes().get("Sales");
            mondrian.olap.Cube cube =
                ((OlapWrapper) cube0).unwrap(mondrian.olap.Cube.class);
            CacheControl.CellRegion cellRegion =
                cacheControl.createMeasuresRegion(cube);
            final Random random = new Random();
            final String[] queries = {
                "select [Product].Members on 0 from [Sales]",
                "select [Product].[Drink].Children on 0 from [Sales]",
                "select [Product].[Food].Children on 0 from [Sales]"
            };
            for (int i = 0;; i++) {
                if (i % 10 == 0) {
                    cacheControl.flush(cellRegion);
                }
                final OlapStatement statement = connection.createStatement();
                stmtRef.set(statement);
                try {
                final CellSet cellSet =
                    statement.executeOlapQuery(
                        queries[i == 0 ? 0 : random.nextInt(3)]);
                    String s = TestContext.toString(cellSet);
                    assertNotNull(s);
                    cellSet.close();
                } catch (OlapException e) {
                    assertEquals(
                        Arrays.toString(Util.convertStackToString(e)),
                        "Query canceled",
                        e.getMessage());
                    cancelCount.incrementAndGet();
                }
                statement.close();
                stmtRef.set(null);

                System.out.println(
                    "i=" + i
                    + ", failCount=" + failCount
                    + ", tryCancelCount=" + tryCancelCount
                    + ", actualCancelCount=" + tryCancelCount
                    + ", cancelCount=" + tryCancelCount);
            }
        } finally {
            finished.set(true);
        }
    }

    /**
     * This is a test for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1353">MONDRIAN-1353</a>
     *
     * <p>An empty stack exception was thrown from the olap4j API if
     * the hierarchy didn't have a all member and the default member
     * was not explicitly set.
     */
    public void testMondrian1353() throws Exception {
        final TestContext testContext = TestContext.instance().create(
            null,
            "<Cube name=\"Mondrian1353\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <Dimension name=\"Cities\" foreignKey=\"customer_id\">\n"
            + "    <Hierarchy hasAll=\"false\" primaryKey=\"customer_id\">\n"
            + "      <Table name=\"customer\"/>\n"
            + "      <Level name=\"City\" column=\"city\" uniqueMembers=\"false\"/> \n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\" visible=\"false\"/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);

        final Member defaultMember =
            testContext.getOlap4jConnection()
                .getOlapSchema()
                .getCubes().get("Mondrian1353")
                .getDimensions().get("Cities")
                .getDefaultHierarchy().getDefaultMember();

        assertNotNull(defaultMember);
        assertEquals("Acapulco", defaultMember.getName());
    }

    /**
     * Same as {@link SchemaTest#testMondrian1390()} but this time
     * with olap4j.
     */
    public void testMondrian1390() throws Exception {
        final List<Member> members =
            getTestContext().getOlap4jConnection()
                .getOlapSchema()
                .getCubes().get("Sales")
                .getDimensions().get("Store Size in SQFT")
                .getDefaultHierarchy()
                .getLevels().get("Store Sqft")
                    .getMembers();
        assertEquals(
            "[[Store Size in SQFT].[#null], "
            + "[Store Size in SQFT].[20319], "
            + "[Store Size in SQFT].[21215], "
            + "[Store Size in SQFT].[22478], "
            + "[Store Size in SQFT].[23112], "
            + "[Store Size in SQFT].[23593], "
            + "[Store Size in SQFT].[23598], "
            + "[Store Size in SQFT].[23688], "
            + "[Store Size in SQFT].[23759], "
            + "[Store Size in SQFT].[24597], "
            + "[Store Size in SQFT].[27694], "
            + "[Store Size in SQFT].[28206], "
            + "[Store Size in SQFT].[30268], "
            + "[Store Size in SQFT].[30584], "
            + "[Store Size in SQFT].[30797], "
            + "[Store Size in SQFT].[33858], "
            + "[Store Size in SQFT].[34452], "
            + "[Store Size in SQFT].[34791], "
            + "[Store Size in SQFT].[36509], "
            + "[Store Size in SQFT].[38382], "
            + "[Store Size in SQFT].[39696]]",
            members.toString());
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-1123">
     * MONDRIAN-1123, "ClassCastException for calculated members that are not
     * part of the measures dimension"</a>.
     *
     * @throws java.sql.SQLException on error
     */
    public void testCalcMemberInCube() throws SQLException {
        final OlapConnection testContext =
            TestContext.instance().createSubstitutingCube(
                "Sales",
                null,
                "<CalculatedMember name='H1 1997' formula='Aggregate([Time].[1997].[Q1]:[Time].[1997].[Q2])' dimension='Time' />")
            .getOlap4jConnection();
        final Cube cube = testContext.getOlapSchema().getCubes().get("Sales");
        final List<Measure> measureList = cube.getMeasures();
        StringBuilder buf = new StringBuilder();
        for (Measure measure : measureList) {
            buf.append(measure.getName()).append(";");
        }
        // Calc member in the Time dimension does not appear in the list.
        // Never did, as far as I can tell.
        assertEquals(
            "Unit Sales;Store Cost;Store Sales;Sales Count;Customer Count;"
            + "Promotion Sales;Profit;Profit last Period;Profit Growth;",
            buf.toString());

        final CellSet cellSet = testContext.createStatement().executeOlapQuery(
            "select AddCalculatedMembers([Time].[Time].Members) on 0 from [Sales]");
        int n = 0, n2 = 0;
        for (Position position : cellSet.getAxes().get(0).getPositions()) {
            if (position.getMembers().get(0).getName().equals("H1 1997")) {
                ++n;
            }
            ++n2;
        }
        assertEquals(1, n);
        assertEquals(35, n2);

        final CellSet cellSet2 = testContext.createStatement().executeOlapQuery(
            "select Filter(\n"
            + " AddCalculatedMembers([Time].[Time].Members),\n"
            + " [Time].[Time].CurrentMember.Properties('MEMBER_TYPE') = 4) on 0\n"
            + "from [Sales]");
        n = 0;
        n2 = 0;
        for (Position position : cellSet2.getAxes().get(0).getPositions()) {
            if (position.getMembers().get(0).getName().equals("H1 1997")) {
                ++n;
            }
            ++n2;
        }
        assertEquals(1, n);
        assertEquals(1, n2);
    }

    /**
     * Test for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1967">MONDRIAN-1967</a>
     *
     * <p>An empty case statement in RolapMemberBase was breaking when the
     * visibility property was asked instead of delegating to isVisible().
     */
    public void testMondrian1967() throws Exception {
        assertTrue(
                getTestContext().getOlap4jConnection()
                        .getOlapSchema()
                        .getCubes().get("Sales")
                        .lookupMember(
                                IdentifierNode.parseIdentifier("[Time].[Time].[1997]")
                                        .getSegmentList())
                        .isVisible());
    }
}

// End Olap4jTest.java

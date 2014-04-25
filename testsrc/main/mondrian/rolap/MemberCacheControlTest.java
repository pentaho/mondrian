/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.olap.CacheControl.MemberEditCommand;
import mondrian.olap.Hierarchy;
import mondrian.rolap.agg.AggregationManager;
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.server.Statement;
import mondrian.test.*;

import org.apache.log4j.*;
import org.apache.log4j.Level;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

/**
 * Unit tests for flushing member cache and editing cached member properties.
 *
 * <p>The purpose of the cache control API is to clear the cache so that
 * changes made to the DBMS can be seen. However, it is difficult to write
 * tests that modify the database. So these tests just check that the relevant
 * caches have been cleared. It is assumed that the updated values will be
 * loaded next time mondrian goes to the database.
 *
 * @author mberkowitz
 * @since Jan 2008
 */
public class MemberCacheControlTest extends FoodMartTestCase {
    private Locus locus;

    // TODO: add multi-thread tests.
    // TODO: test set properties negative: refer to invalid property
    // TODO: test set properties negative: set prop to invalid value
    // TODO: edit a different member not known to be in cache -- will it be
    //       fetched?

    public MemberCacheControlTest() {
    }

    public MemberCacheControlTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
        RolapSchemaPool.instance().clear();

        final RolapConnection conn = (RolapConnection) getConnection();
        final Statement statement = conn.getInternalStatement();
        final Execution execution = new Execution(statement, 0);
        locus = new Locus(execution, getName(), null);
        Locus.push(locus);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        RolapSchemaPool.instance().clear();
        Locus.pop(locus);
        locus = null;
    }

    // ~ Utility methods ------------------------------------------------------

    DiffRepository getDiffRepos() {
        return DiffRepository.lookup(MemberCacheControlTest.class);
    }

    public TestContext getTestContext() {
        return TestContext.instance().legacy().createSubstitutingCube(
            "Sales",
            // Reduced size Store dimension. Omits the 'Store Country' level,
            // and adds properties to non-leaf levels.
            "  <Dimension name=\"Retail\" foreignKey=\"store_id\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n"
            + "      <Table name=\"store\"/>\n"
            + "      <Level name=\"State\" column=\"store_state\" uniqueMembers=\"true\">\n"
            + "        <Property name=\"Country\" column=\"store_country\"/>\n"
            + "      </Level>\n"
            + "      <Level name=\"City\" column=\"store_city\" uniqueMembers=\"true\">\n"
            + "        <Property name=\"Population\" column=\"store_postal_code\"/>\n"
            + "      </Level>\n"
            + "      <Level name=\"Name\" column=\"store_name\" uniqueMembers=\"true\">\n"
            + "        <Property name=\"Store Type\" column=\"store_type\"/>\n"
            + "        <Property name=\"Store Manager\" column=\"store_manager\"/>\n"
            + "        <Property name=\"Store Sqft\" column=\"store_sqft\" type=\"Numeric\"/>\n"
            + "        <Property name=\"Has coffee bar\" column=\"coffee_bar\" type=\"Boolean\"/>\n"
            + "        <Property name=\"Street address\" column=\"store_street_address\" type=\"String\"/>\n"
            + "      </Level>\n"
            + "    </Hierarchy>\n"
            + "   </Dimension>");
    }

    /**
     * Creates a map.
     *
     * @param keys Keys
     * @param values Values
     * @return Map
     */
    private static <K, V> Map<K, V> createMap(List<K> keys, List<V> values) {
        assert keys.size() == values.size();
        final Map<K, V> map = new HashMap<K, V>(keys.size());
        for (int i = 0; i < keys.size(); ++i) {
            map.put(keys.get(i), values.get(i));
        }
        return map;
    }

    /**
     * Finds a Member by its name and the name of its containing cube.
     *
     * @param tc Test context
     * @param cubeName Cube name
     * @param names the full-qualified Member name
     * @return the Member
     * @throws MondrianException when not found.
     */
    protected static RolapMember findMember(
        TestContext tc,
        String cubeName,
        String... names)
    {
        Cube cube = tc.getConnection().getSchema().lookupCube(cubeName, true);
        SchemaReader scr = cube.getSchemaReader(null).withLocus();
        return (RolapMember)
            scr.getMemberByUniqueName(Id.Segment.toList(names), true);
    }

    /**
     * Prints all properties of a Member.
     *
     * @param pw Print writer
     * @param member Member
     * @return the same print writer
     */
    private static PrintWriter printMemberProperties(
        PrintWriter pw,
        Member member)
    {
        pw.print(member.getUniqueName());
        pw.print(" {");
        int k = -1;
        for (Property p : member.getLevel().getProperties()) {
            if (++k > 0) {
                pw.print(",");
            }
            pw.println();
            String name = p.getName();
            Object value = member.getPropertyValue(p);

            // Fixup value for different database representations of boolean and
            // numeric values.
            if (value == null) {
                // no fixup needed
            } else if (name.equals("Has coffee bar")) {
                if (value instanceof Number) {
                    value = ((Number) value).intValue() != 0;
                }
            } else if (name.endsWith(" Sqft")) {
                Number number = (Number) value;
                value =
                    number.equals(number.intValue())
                        ? number.intValue()
                        : Math.round(number.floatValue());
            }
            pw.print("  [");
            pw.print(name);
            pw.print("]=[");
            pw.print(value);
            pw.print("]");
        }
        pw.println("}");
        return pw;
    }

    /**
     * Prints properties of all Members on an Axis.
     *
     * @param pw Print writer
     * @param axis Axis
     * @return the same print writer
     */
    private static PrintWriter printMemberProperties(
        PrintWriter pw,
        Axis axis)
    {
        for (Position pos : axis.getPositions()) {
            for (Member m : pos) {
                printMemberProperties(pw, m).println();
            }
        }
        return pw;
    }

    /**
     * Prints properties of the Row Axis from a Result.
     *
     * @param pw Print writer
     * @param result Result
     * @return the same print writer
     */
    private static PrintWriter printRowMemberProperties(
        PrintWriter pw,
        Result result)
    {
        return printMemberProperties(
            pw,
            result.getAxes()[
                AxisOrdinal.StandardAxisOrdinal.ROWS.logicalOrdinal()]);
    }

    private static String getRowMemberPropertiesAsString(Result r) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        printRowMemberProperties(pw, r);
        pw.flush();
        return sw.toString();
    }

    private CacheControl.MemberSet createInterestingMemberSet(
        TestContext tc, CacheControl cc)
    {
        return cc.createUnionSet(
            // all stores in OR
            cc.createMemberSet(findMember(tc, "Sales", "Retail", "OR"), true),
            // all stores in Hidalgo, Zacatecas
            cc.createMemberSet(
                findMember(tc, "Sales", "Retail", "Zacatecas", "Hidalgo"),
                true),
            // a single store
            cc.createMemberSet(
                findMember(tc, "Sales", "Retail", "CA", "Alameda", "HQ"),
                false),
            // a range of stores
            cc.createMemberSet(
                true, findMember(tc, "Sales", "Retail", "WA", "Bremerton"),
                true, findMember(tc, "Sales", "Retail", "Yucatan", "Merida"),
                false),
            // all stores in a range of states
            cc.createMemberSet(
                true, findMember(tc, "Sales", "Retail", "DF"),
                true, findMember(tc, "Sales", "Retail", "Jalisco"),
                true));
    }

    // ~ Tests ----------------------------------------------------------------

    /**
     * Tests operations on member sets, in particular the
     * {@link mondrian.olap.CacheControl#filter} method.
     */
    public void testFilter() {
        final TestContext tc = getTestContext();
        final Connection conn = tc.getConnection();
        final DiffRepository dr = getDiffRepos();
        final CacheControl cc = conn.getCacheControl(null);

        CacheControl.MemberSet memberSet = createInterestingMemberSet(tc, cc);
        dr.assertEquals("before", "${before}", memberSet.toString());
        final Member orMember = findMember(tc, "Sales", "Retail", "OR");
        final CacheControl.MemberSet filteredMemberSet =
            cc.filter(orMember.getLevel(), memberSet);
        dr.assertEquals("after", "${after}", filteredMemberSet.toString());
    }

    /**
     * Test that edits the properties of a single leaf Member.
     */
    public void testSetPropertyCommandOnLeafMember() {
        final TestContext tc = getTestContext();
        final Connection conn = tc.getConnection();
        final DiffRepository dr = getDiffRepos();
        final CacheControl cc = conn.getCacheControl(null);

        // A query that refers to a single leaf Member fetches the Member.
        // Changing Member properties does not affect Cell boundaries, so we
        // check that the MDX results are invariant.
        String mdx =
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,"
            + " {[Store].[USA].[CA].[San Francisco].[Store 14]}"
            + " ON ROWS FROM [Sales]";
        Query q = conn.parseQuery(mdx);
        Result r = conn.execute(q);
        dr.assertEquals(
            "props before",
            "${props before}",
            getRowMemberPropertiesAsString(r));
        final String resultString = TestContext.toString(r);
        dr.assertEquals(
            "result before",
            "${result before}",
            resultString);

        // Change properties
        Member m =
            findMember(
                tc, "Sales", "Store", "USA", "CA", "San Francisco", "Store 14");
        cc.execute(cc.createSetPropertyCommand(m, "Store Manager", "Higgins"));
        cc.execute(
            cc.createCompoundCommand(
                Arrays.asList(
                    cc.createSetPropertyCommand(
                        m, "Street address", "770 Mission St"),
                    cc.createSetPropertyCommand(m, "Store Sqft", 6000),
                    cc.createSetPropertyCommand(
                        m, "Has coffee bar", "false"))));

        // Repeat same query; verify properties are changed.
        // Changing properties does not affect measures, so results unchanged.
        r = conn.execute(q);
        dr.assertEquals(
            "props after",
            "${props after}",
            getRowMemberPropertiesAsString(r));
        assertEquals(
            resultString,
            TestContext.toString(r));
    }

    /**
     * Test that edits properties of Members at various Levels (use Retail
     * Dimension), but leaves grouping unchanged, so results not changed.
     */
    public void testSetPropertyCommandOnNonLeafMember() {
        final TestContext tc = getTestContext();
        final Connection conn = tc.getConnection();
        final DiffRepository dr = getDiffRepos();
        final CacheControl cc = tc.getConnection().getCacheControl(null);

        String mdx = "SELECT {[Measures].[Unit Sales]} ON COLUMNS,"
            + " {[Retail].Members} ON ROWS "
            + "FROM [Sales]";
        Query q = conn.parseQuery(mdx);
        Result r = conn.execute(q);
        dr.assertEquals(
            "props before",
            "${props before}",
            getRowMemberPropertiesAsString(r));
        final String resultString = TestContext.toString(r);
        dr.assertEquals(
            "result before",
            "${result before}",
            resultString);

        // Change some properties (TODO: change dimension table first)
        // set 2 properties (TODO: set both with one command)

        // try all ways to construct MemberSets
        CacheControl.MemberSet memberSet = createInterestingMemberSet(tc, cc);

        final Map<String, Object> propertyValues =
            createMap(
                Arrays.asList("Has coffee bar", "Store Sqft"),
                Arrays.asList((Object) "true", 123));
        CacheControl.MemberEditCommand command;

        // first, the member set contains members of various levels
        try {
            command = cc.createSetPropertyCommand(memberSet, propertyValues);
            fail("expected exception, got " + command);
        } catch (IllegalArgumentException e) {
            assertEquals(
                "all members in set must belong to same level",
                e.getMessage());
        }

        // after we filter set to just members of store level we're ok
        final Member hqMember =
            findMember(tc, "Sales", "Retail", "CA", "Alameda", "HQ");
        final CacheControl.MemberSet filteredMemberSet =
            cc.filter(hqMember.getLevel(), memberSet);
        command =
            cc.createSetPropertyCommand(filteredMemberSet, propertyValues);
        cc.execute(command);

        // Repeat same query; verify properties were changed.
        // Changing properties does not affect measures, so results unchanged.
        r = conn.execute(q);
        dr.assertEquals(
            "props after",
            "${props after}",
            getRowMemberPropertiesAsString(r));
        assertEquals(
            resultString,
            TestContext.toString(r));
    }

    public void testAddCommand() {
        final TestContext tc = getTestContext();
        final Connection conn = tc.getConnection();
        final CacheControl cc = conn.getCacheControl(null);
        final RolapMember caMember =
            findMember(tc, "Sales", "Retail", "CA");
        final RolapMember rootMember = caMember.getParentMember();
        final RolapCubeHierarchy hierarchy = caMember.getHierarchy();
        final RolapMember berkeleyMember =
            hierarchy.createMember(
                caMember,
                caMember.getLevel().getChildLevel(),
                "Berkeley",
                null);
        final RolapBaseCubeMeasure unitSalesCubeMember =
            (RolapBaseCubeMeasure) findMember(
                tc, "Sales", "Measures", "Unit Sales");
        final RolapMember yearMember =
            findMember(
                tc, "Sales", "Time", "Year", "1997");
        final Member[] cacheRegionMembers =
            new Member[] {
                unitSalesCubeMember,
                caMember,
                yearMember
            };

        tc.assertQueryReturns(
            "select {[Retail].[City].Members} on columns from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Retail].[Retail].[BC].[Vancouver]}\n"
            + "{[Retail].[Retail].[BC].[Victoria]}\n"
            + "{[Retail].[Retail].[CA].[Alameda]}\n"
            + "{[Retail].[Retail].[CA].[Beverly Hills]}\n"
            + "{[Retail].[Retail].[CA].[Los Angeles]}\n"
            + "{[Retail].[Retail].[CA].[San Diego]}\n"
            + "{[Retail].[Retail].[CA].[San Francisco]}\n"
            + "{[Retail].[Retail].[DF].[Mexico City]}\n"
            + "{[Retail].[Retail].[DF].[San Andres]}\n"
            + "{[Retail].[Retail].[Guerrero].[Acapulco]}\n"
            + "{[Retail].[Retail].[Jalisco].[Guadalajara]}\n"
            + "{[Retail].[Retail].[OR].[Portland]}\n"
            + "{[Retail].[Retail].[OR].[Salem]}\n"
            + "{[Retail].[Retail].[Veracruz].[Orizaba]}\n"
            + "{[Retail].[Retail].[WA].[Bellingham]}\n"
            + "{[Retail].[Retail].[WA].[Bremerton]}\n"
            + "{[Retail].[Retail].[WA].[Seattle]}\n"
            + "{[Retail].[Retail].[WA].[Spokane]}\n"
            + "{[Retail].[Retail].[WA].[Tacoma]}\n"
            + "{[Retail].[Retail].[WA].[Walla Walla]}\n"
            + "{[Retail].[Retail].[WA].[Yakima]}\n"
            + "{[Retail].[Retail].[Yucatan].[Merida]}\n"
            + "{[Retail].[Retail].[Zacatecas].[Camacho]}\n"
            + "{[Retail].[Retail].[Zacatecas].[Hidalgo]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 21,333\n"
            + "Row #0: 25,663\n"
            + "Row #0: 25,635\n"
            + "Row #0: 2,117\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 26,079\n"
            + "Row #0: 41,580\n"
            + "Row #0: \n"
            + "Row #0: 2,237\n"
            + "Row #0: 24,576\n"
            + "Row #0: 25,011\n"
            + "Row #0: 23,591\n"
            + "Row #0: 35,257\n"
            + "Row #0: 2,203\n"
            + "Row #0: 11,491\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n");
        tc.assertAxisReturns(
            "[Retail].[CA].Children",
            "[Retail].[Retail].[CA].[Alameda]\n"
            + "[Retail].[Retail].[CA].[Beverly Hills]\n"
            + "[Retail].[Retail].[CA].[Los Angeles]\n"
            + "[Retail].[Retail].[CA].[San Diego]\n"
            + "[Retail].[Retail].[CA].[San Francisco]");
        final MemberReader memberReader = hierarchy.getMemberReader();
        final MemberCache memberCache =
            ((SmartMemberReader) memberReader).getMemberCache();
        List<RolapMember> caChildren =
            memberCache.getChildrenFromCache(caMember, null);
        assertEquals(5, caChildren.size());

        // Load cell data and check it is in cache
        executeQuery(
            "select {[Measures].[Unit Sales]} on columns, {[Retail].[CA]} on rows from [Sales]");
        final AggregationManager aggMgr =
            ((RolapConnection) conn).getServer().getAggregationManager();
        assertEquals(
            Double.valueOf("74748"),
            aggMgr.getCellFromAllCaches(
                AggregationManager.makeRequest(cacheRegionMembers)));

        // Now tell the cache that [CA].[Berkeley] is new
        final CacheControl.MemberEditCommand command =
            cc.createAddCommand(berkeleyMember);
        cc.execute(command);

        // test that cells have been removed
        assertNull(
            aggMgr.getCellFromAllCaches(
                AggregationManager.makeRequest(cacheRegionMembers)));

        tc.assertAxisReturns(
            "[Retail].[CA].Children",
            "[Retail].[Retail].[CA].[Alameda]\n"
            + "[Retail].[Retail].[CA].[Beverly Hills]\n"
            + "[Retail].[Retail].[CA].[Los Angeles]\n"
            + "[Retail].[Retail].[CA].[San Diego]\n"
            + "[Retail].[Retail].[CA].[San Francisco]\n"
            + "[Retail].[Retail].[CA].[Berkeley]");

        tc.assertQueryReturns(
            "select {[Retail].[City].Members} on columns from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Retail].[Retail].[BC].[Vancouver]}\n"
            + "{[Retail].[Retail].[BC].[Victoria]}\n"
            + "{[Retail].[Retail].[CA].[Alameda]}\n"
            + "{[Retail].[Retail].[CA].[Berkeley]}\n"
            + "{[Retail].[Retail].[CA].[Beverly Hills]}\n"
            + "{[Retail].[Retail].[CA].[Los Angeles]}\n"
            + "{[Retail].[Retail].[CA].[San Diego]}\n"
            + "{[Retail].[Retail].[CA].[San Francisco]}\n"
            + "{[Retail].[Retail].[DF].[Mexico City]}\n"
            + "{[Retail].[Retail].[DF].[San Andres]}\n"
            + "{[Retail].[Retail].[Guerrero].[Acapulco]}\n"
            + "{[Retail].[Retail].[Jalisco].[Guadalajara]}\n"
            + "{[Retail].[Retail].[OR].[Portland]}\n"
            + "{[Retail].[Retail].[OR].[Salem]}\n"
            + "{[Retail].[Retail].[Veracruz].[Orizaba]}\n"
            + "{[Retail].[Retail].[WA].[Bellingham]}\n"
            + "{[Retail].[Retail].[WA].[Bremerton]}\n"
            + "{[Retail].[Retail].[WA].[Seattle]}\n"
            + "{[Retail].[Retail].[WA].[Spokane]}\n"
            + "{[Retail].[Retail].[WA].[Tacoma]}\n"
            + "{[Retail].[Retail].[WA].[Walla Walla]}\n"
            + "{[Retail].[Retail].[WA].[Yakima]}\n"
            + "{[Retail].[Retail].[Yucatan].[Merida]}\n"
            + "{[Retail].[Retail].[Zacatecas].[Camacho]}\n"
            + "{[Retail].[Retail].[Zacatecas].[Hidalgo]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 21,333\n"
            + "Row #0: 25,663\n"
            + "Row #0: 25,635\n"
            + "Row #0: 2,117\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 26,079\n"
            + "Row #0: 41,580\n"
            + "Row #0: \n"
            + "Row #0: 2,237\n"
            + "Row #0: 24,576\n"
            + "Row #0: 25,011\n"
            + "Row #0: 23,591\n"
            + "Row #0: 35,257\n"
            + "Row #0: 2,203\n"
            + "Row #0: 11,491\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n");

        tc.assertQueryReturns(
            "select [Retail].Children on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Retail].[Retail].[BC]}\n"
            + "{[Retail].[Retail].[CA]}\n"
            + "{[Retail].[Retail].[DF]}\n"
            + "{[Retail].[Retail].[Guerrero]}\n"
            + "{[Retail].[Retail].[Jalisco]}\n"
            + "{[Retail].[Retail].[OR]}\n"
            + "{[Retail].[Retail].[Veracruz]}\n"
            + "{[Retail].[Retail].[WA]}\n"
            + "{[Retail].[Retail].[Yucatan]}\n"
            + "{[Retail].[Retail].[Zacatecas]}\n"
            + "Row #0: \n"
            + "Row #0: 74,748\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 67,659\n"
            + "Row #0: \n"
            + "Row #0: 124,366\n"
            + "Row #0: \n"
            + "Row #0: \n");

        List<RolapMember> rootChildren =
            memberCache.getChildrenFromCache(rootMember, null);
        if (rootChildren != null) { // might be null due to gc
            assertEquals(
                10, rootChildren.size());
        }
    }

    public void testDeleteCommand() {
        final TestContext tc = getTestContext();
        final Connection conn = tc.getConnection();
        final CacheControl cc = conn.getCacheControl(null);
        final RolapMember sfMember =
            findMember(
                tc, "Sales", "Retail", "CA", "San Francisco");
        final RolapMember caMember = sfMember.getParentMember();
        final RolapCubeHierarchy hierarchy = caMember.getHierarchy();
        final RolapBaseCubeMeasure unitSalesCubeMember =
            (RolapBaseCubeMeasure) findMember(
                tc, "Sales", "Measures", "Unit Sales");
        final RolapMember yearMember =
            findMember(
                tc, "Sales", "Time", "Year", "1997");
        final Member[] cacheRegionMembers =
            new Member[] {
                unitSalesCubeMember,
                sfMember,
                yearMember
            };

        tc.assertAxisReturns(
            "[Retail].[CA].Children",
            "[Retail].[Retail].[CA].[Alameda]\n"
            + "[Retail].[Retail].[CA].[Beverly Hills]\n"
            + "[Retail].[Retail].[CA].[Los Angeles]\n"
            + "[Retail].[Retail].[CA].[San Diego]\n"
            + "[Retail].[Retail].[CA].[San Francisco]");

        final MemberReader memberReader = hierarchy.getMemberReader();
        final MemberCache memberCache =
            ((SmartMemberReader) memberReader).getMemberCache();
        List<RolapMember> caChildren =
            memberCache.getChildrenFromCache(caMember, null);
        assertEquals(5, caChildren.size());

        // Load cell data and check it is in cache
        executeQuery(
            "select {[Measures].[Unit Sales]} on columns, {[Retail].[CA].[Alameda]} on rows from [Sales]");
        final AggregationManager aggMgr =
            ((RolapConnection) conn).getServer().getAggregationManager();
        assertEquals(
            Double.valueOf("2117"),
            aggMgr.getCellFromAllCaches(
                AggregationManager.makeRequest(cacheRegionMembers)));

        // Now tell the cache that [CA].[San Francisco] has been removed.
        final CacheControl.MemberEditCommand command =
            cc.createDeleteCommand(sfMember);
        cc.execute(command);

        // Children of CA should be 4
        assertEquals(
            4,
            memberCache.getChildrenFromCache(caMember, null).size());

        // test that cells have been removed
        assertNull(
            aggMgr.getCellFromAllCaches(
                AggregationManager.makeRequest(cacheRegionMembers)));

        // The list of children should be updated.
        tc.assertAxisReturns(
            "[Retail].[CA].Children",
            "[Retail].[Retail].[CA].[Alameda]\n"
            + "[Retail].[Retail].[CA].[Beverly Hills]\n"
            + "[Retail].[Retail].[CA].[Los Angeles]\n"
            + "[Retail].[Retail].[CA].[San Diego]");
    }

    public void testMoveCommand() {
        final TestContext tc = getTestContext();
        final Connection conn = tc.getConnection();
        final CacheControl cc = conn.getCacheControl(null);
        final RolapMember caMember =
            findMember(tc, "Sales", "Retail", "CA");
        final RolapCubeHierarchy hierarchy = caMember.getHierarchy();
        final MemberReader memberReader = hierarchy.getMemberReader();
        final MemberCache memberCache =
            ((SmartMemberReader) memberReader).getMemberCache();
        final RolapMember alamedaMember =
            hierarchy.createMember(
                caMember,
                caMember.getLevel().getChildLevel(),
                "Alameda",
                null);
        final RolapMember sfMember =
            hierarchy.createMember(
                caMember,
                caMember.getLevel().getChildLevel(),
                "San Francisco",
                null);
        final RolapMember storeMember =
            hierarchy.createMember(
                sfMember,
                sfMember.getLevel().getChildLevel(),
                "Store 14",
                null);

        // test axis contents
        tc.assertAxisReturns(
            "[Retail].[CA].Children",
            "[Retail].[Retail].[CA].[Alameda]\n"
            + "[Retail].[Retail].[CA].[Beverly Hills]\n"
            + "[Retail].[Retail].[CA].[Los Angeles]\n"
            + "[Retail].[Retail].[CA].[San Diego]\n"
            + "[Retail].[Retail].[CA].[San Francisco]");
        tc.assertAxisReturns(
            "[Retail].[CA].[Alameda].Children",
            "[Retail].[Retail].[CA].[Alameda].[HQ]");
        tc.assertAxisReturns(
            "[Retail].[CA].[San Francisco].Children",
            "[Retail].[Retail].[CA].[San Francisco].[Store 14]");

        List<RolapMember> sfChildren =
            memberCache.getChildrenFromCache(sfMember, null);
        assertEquals(1, sfChildren.size());
        List<RolapMember> alamedaChildren =
            memberCache.getChildrenFromCache(alamedaMember, null);
        assertEquals(1, alamedaChildren.size());
        assertTrue(
            storeMember.getParentMember().equals(sfMember));

        // Now tell the cache that Store 14 moved to Alameda
        final MemberEditCommand command =
            cc.createMoveCommand(storeMember, alamedaMember);
        cc.execute(command);

        // The list of SF children should contain 0 elements
        assertEquals(
            0,
            memberCache.getChildrenFromCache(sfMember, null).size());

        // Check Alameda's children. It should be null as the parent's list
        // should be cleared.
        alamedaChildren =
            memberCache.getChildrenFromCache(alamedaMember, null);
        assertEquals(2, alamedaChildren.size());

        // test axis contents
        tc.assertAxisReturns(
            "[Retail].[CA].[San Francisco].Children",
            "");
        tc.assertAxisReturns(
            "[Retail].[CA].[Alameda].Children",
            "[Retail].[Retail].[CA].[Alameda].[HQ]\n"
            + "[Retail].[Retail].[CA].[Alameda].[Store 14]");

        // Test parent object
        assertTrue(
            storeMember.getParentMember().equals(alamedaMember));
    }

    public void testMoveFailBadLevel() {
        final TestContext tc = getTestContext();
        final Connection conn = tc.getConnection();
        final CacheControl cc = conn.getCacheControl(null);
        final RolapMember caMember =
            findMember(tc, "Sales", "Retail", "CA");
        final RolapCubeHierarchy hierarchy = caMember.getHierarchy();
        final MemberReader memberReader = hierarchy.getMemberReader();
        final MemberCache memberCache =
            ((SmartMemberReader) memberReader).getMemberCache();
        final RolapMember sfMember =
            hierarchy.createMember(
                caMember,
                caMember.getLevel().getChildLevel(),
                "San Francisco",
                null);
        final RolapMember storeMember =
            hierarchy.createMember(
                sfMember,
                sfMember.getLevel().getChildLevel(),
                "Store 14",
                null);

        // test axis contents
        tc.assertAxisReturns(
            "[Retail].[CA].Children",
            "[Retail].[Retail].[CA].[Alameda]\n"
            + "[Retail].[Retail].[CA].[Beverly Hills]\n"
            + "[Retail].[Retail].[CA].[Los Angeles]\n"
            + "[Retail].[Retail].[CA].[San Diego]\n"
            + "[Retail].[Retail].[CA].[San Francisco]");
        tc.assertAxisReturns(
            "[Retail].[CA].[San Francisco].Children",
            "[Retail].[Retail].[CA].[San Francisco].[Store 14]");

        List<RolapMember> sfChildren =
            memberCache.getChildrenFromCache(sfMember, null);
        assertEquals(1, sfChildren.size());
        assertTrue(
            storeMember.getParentMember().equals(sfMember));

        // Now tell the cache that Store 14 moved to CA
        final MemberEditCommand command =
            cc.createMoveCommand(storeMember, caMember);
        try {
            cc.execute(command);
            fail("Should have failed due to improper level");
        } catch (MondrianException e) {
            assertEquals(
                "new parent belongs to different level than old",
                e.getCause().getMessage());
        }

        // The list of SF children should still contain 1 element
        assertEquals(
            1,
            memberCache.getChildrenFromCache(sfMember, null).size());

        // test axis contents. should not have been modified
        tc.assertAxisReturns(
            "[Retail].[CA].[San Francisco].Children",
            "[Retail].[Retail].[CA].[San Francisco].[Store 14]");
        tc.assertAxisReturns(
            "[Retail].[CA].Children",
            "[Retail].[Retail].[CA].[Alameda]\n"
            + "[Retail].[Retail].[CA].[Beverly Hills]\n"
            + "[Retail].[Retail].[CA].[Los Angeles]\n"
            + "[Retail].[Retail].[CA].[San Diego]\n"
            + "[Retail].[Retail].[CA].[San Francisco]");

        // Test parent object. should be the same
        assertTrue(
            storeMember.getParentMember().equals(sfMember));
    }

    /**
     * Tests a variety of negative cases including add/delete/move null members
     * add/delete/move members in parent-child hierarchies.
     */
    public void _testAddCommandNegative() {
        final TestContext tc = getTestContext();
        final Connection conn = tc.getConnection();
        final CacheControl cc = conn.getCacheControl(null);

        CacheControl.MemberEditCommand command;
        try {
            command = cc.createAddCommand(null);
            fail("expected exception, got " + command);
        } catch (IllegalArgumentException e) {
            assertEquals("cannot add null member", e.getMessage());
        }

        final RolapMember alamedaMember =
            findMember(
                tc, "Sales", "Retail", "CA", "Alameda");
        final RolapMember caMember = alamedaMember.getParentMember();

        final RolapMember empMember =
            findMember(
                tc, "HR", "Employees", "Sheri Nowmer", "Michael Spence");

        try {
            command = cc.createMoveCommand(null, alamedaMember);
            fail("expected exception, got " + command);
        } catch (IllegalArgumentException e) {
            assertEquals("cannot move null member", e.getMessage());
        }

        try {
            command = cc.createMoveCommand(alamedaMember, null);
            fail("expected exception, got " + command);
        } catch (IllegalArgumentException e) {
            assertEquals("cannot move member to null location", e.getMessage());
        }

        try {
            command = cc.createDeleteCommand((Member) null);
            fail("expected exception, got " + command);
        } catch (IllegalArgumentException e) {
            assertEquals("cannot delete null member", e.getMessage());
        }

        try {
            command = cc.createSetPropertyCommand(null, "foo", 1);
            fail("expected exception, got " + command);
        } catch (IllegalArgumentException e) {
            assertEquals(
                "cannot set properties on null member",
                e.getMessage());
        }

        try {
            command = cc.createAddCommand(empMember);
            fail("expected exception, got " + command);
        } catch (IllegalArgumentException e) {
            assertEquals(
                "add member not supported for parent-child hierarchy",
                e.getMessage());
        }

        try {
            command = cc.createMoveCommand(empMember, null);
            fail("expected exception, got " + command);
        } catch (IllegalArgumentException e) {
            assertEquals(
                "move member not supported for parent-child hierarchy",
                e.getMessage());
        }

        try {
            command = cc.createDeleteCommand(empMember);
            fail("expected exception, got " + command);
        } catch (IllegalArgumentException e) {
            assertEquals(
                "delete member not supported for parent-child hierarchy",
                e.getMessage());
        }

        try {
            command = cc.createSetPropertyCommand(empMember, "foo", "bar");
            fail("expected exception, got " + command);
        } catch (IllegalArgumentException e) {
            assertEquals(
                "set properties not supported for parent-child hierarchy",
                e.getMessage());
        }

        try {
            command = cc.createSetPropertyCommand(
                cc.createUnionSet(
                    cc.createMemberSet(alamedaMember, false),
                    cc.createMemberSet(caMember, false)),
                Collections.<String, Object>emptyMap());
            fail("expected exception, got " + command);
        } catch (IllegalArgumentException e) {
            assertEquals(
                "all members in set must belong to same level",
                e.getMessage());
        }
    }

    /**
     * Test case for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1076">MONDRIAN-1076,
     * "Add CacheControl API to flush members from dimension cache"</a>.
     */
    public void testFlushHierarchy() {
        final TestContext testContext = getTestContext();
        CacheControlTest.flushCache(testContext);
        final CacheControl cacheControl =
            testContext.getConnection().getCacheControl(null);
        final Cube salesCube =
            testContext.getConnection()
                .getSchema().lookupCube("Sales", true);

        final Logger logger = RolapUtil.SQL_LOGGER;
        final Level level = logger.getLevel();
        final StringWriter sw = new StringWriter();
        final WriterAppender appender =
            new WriterAppender(new SimpleLayout(), sw);
        try {
            logger.setLevel(Level.DEBUG);
            logger.addAppender(appender);

            final Hierarchy storeHierarchy =
                salesCube.getDimensionList().get(1).getHierarchyList().get(0);
            assertEquals("Store", storeHierarchy.getName());
            final CacheControl.MemberSet storeMemberSet =
                cacheControl.createMemberSet(
                    storeHierarchy.getAllMember(), true);
            final Runnable storeFlusher =
                new Runnable() {
                    public void run() {
                        cacheControl.flush(storeMemberSet);
                    }
                };

            final Result result =
                testContext.executeQuery(
                    "select [Store].[Mexico].[Yucatan] on 0 from [Sales]");
            final Member storeYucatanMember =
                result.getAxes()[0].getPositions().get(0).get(0);
            final CacheControl.MemberSet storeYucatanMemberSet =
                cacheControl.createMemberSet(
                    storeYucatanMember, true);
            final Runnable storeYucatanFlusher =
                new Runnable() {
                    public void run() {
                        cacheControl.flush(storeYucatanMemberSet);
                    }
                };

            checkFlushHierarchy(
                sw, true, storeFlusher,
                new Runnable() {
                    public void run() {
                        // Check that <Member>.Children uses cache when applied
                        // to an 'all' member.
                        testContext.assertAxisReturns(
                            "[Store].Children",
                            "[Store].[Store].[Canada]\n"
                            + "[Store].[Store].[Mexico]\n"
                            + "[Store].[Store].[USA]");
                    }
                });
            checkFlushHierarchy(
                sw, true, storeFlusher,
                new Runnable() {
                    public void run() {
                        // Check that <Member>.Children uses cache when applied
                        // to regular member.
                        testContext.assertAxisReturns(
                            "[Store].[Store].[USA].[CA].Children",
                            "[Store].[Store].[USA].[CA].[Alameda]\n"
                            + "[Store].[Store].[USA].[CA].[Beverly Hills]\n"
                            + "[Store].[Store].[USA].[CA].[Los Angeles]\n"
                            + "[Store].[Store].[USA].[CA].[San Diego]\n"
                            + "[Store].[Store].[USA].[CA].[San Francisco]");
                    }
                });

            // In contrast to preceding, flushing Yucatan should not affect
            // California.
            checkFlushHierarchy(
                sw, false, storeYucatanFlusher,
                new Runnable() {
                    public void run() {
                        // Check that <Member>.Children uses cache when applied
                        // to regular member.
                        testContext.assertAxisReturns(
                            "[Store].[Store].[USA].[CA].Children",
                            "[Store].[Store].[USA].[CA].[Alameda]\n"
                            + "[Store].[Store].[USA].[CA].[Beverly Hills]\n"
                            + "[Store].[Store].[USA].[CA].[Los Angeles]\n"
                            + "[Store].[Store].[USA].[CA].[San Diego]\n"
                            + "[Store].[Store].[USA].[CA].[San Francisco]");
                    }
                });

            checkFlushHierarchy(
                sw, true, storeFlusher, new Runnable() {
                    public void run() {
                        // Check that <Hierarchy>.Members uses cache.
                        testContext.assertExprReturns(
                            "Count([Store].Members)", "63");
                    }
                });
            checkFlushHierarchy(
                sw, true, storeFlusher, new Runnable() {
                    public void run() {
                        // Check that <Level>.Members uses cache.
                        testContext.assertExprReturns(
                            "Count([Store].[Store Name].Members)", "25");
                    }
                });


            // Time hierarchy is interesting because it has public 'all' member.
            // But you can still use the private all member for purposes like
            // flushing.
            final Hierarchy timeHierarchy =
                salesCube.getDimensionList().get(4).getHierarchyList().get(0);
            assertEquals("Time", timeHierarchy.getName());
            final CacheControl.MemberSet timeMemberSet =
                cacheControl.createMemberSet(
                    timeHierarchy.getAllMember(), true);
            final Runnable timeFlusher =
                new Runnable() {
                    public void run() {
                        cacheControl.flush(timeMemberSet);
                    }
                };

            checkFlushHierarchy(
                sw, true, timeFlusher,
                new Runnable() {
                    public void run() {
                        // Check that <Level>.Members uses cache.
                        testContext.assertExprReturns(
                            "Count([Time].[Month].Members)",
                            "24");
                    }
                });
            checkFlushHierarchy(
                sw, true, timeFlusher,
                new Runnable() {
                    public void run() {
                        // Check that <Level>.Members uses cache.
                        testContext.assertAxisReturns(
                            "[Time].[Time].[1997].[Q2].Children",
                            "[Time].[Time].[1997].[Q2].[4]\n"
                            + "[Time].[Time].[1997].[Q2].[5]\n"
                            + "[Time].[Time].[1997].[Q2].[6]");
                    }
                });
        } finally {
            logger.setLevel(level);
            logger.removeAppender(appender);
        }
    }

    /**
     * Runs the same command ({@code foo(testContext, k)}) three times. Between
     * the 2nd and the 3rd, flushes the cache, and makes sure that the 3rd time
     * causes SQL to be executed.
     *
     * @param writer Writer, written into each time a SQL statement is executed
     * @param affected Whether the cache flush affects the command
     * @param flusher Functor that performs cache flushing action to be tested
     * @param command Command to execute that requires cache contents
     */
    private void checkFlushHierarchy(
        StringWriter writer,
        boolean affected,
        Runnable flusher,
        Runnable command)
    {
        // Run command for first time.
        command.run();

        // Now cache is primed, running the command for second time should not
        // require any additional SQL. (There is a small chance that GC will
        // kick in and we'll lose the cache, but we've never seen that happen
        // in the wild.)
        int length1 = writer.getBuffer().length();
        command.run();
        final String since1 = writer.getBuffer().substring(length1);
        assertEquals("", since1);
        flusher.run();

        // Now cache has been flushed, it should be impossible to execute the
        // command without running additional SQL.
        int length2 = writer.getBuffer().length();
        command.run();
        final String since2 = writer.getBuffer().substring(length2);
        if (affected) {
            assertNotSame("", since2);
        }
    }
}

// End MemberCacheControlTest.java

/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2008 Julian Hyde and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.test.*;

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
 * @version $Id$
 * @since Jan 2008
 */
public class MemberCacheControlTest extends FoodMartTestCase {
    private final boolean rolapCubeMemberCacheEnabled =
        MondrianProperties.instance().EnableRolapCubeMemberCache.get();

    // TODO: add multi-thread tests.
    // TODO: test moveMember (execute source.children and target.children before and after)
    // TODO: test moveMember negaitve case previous parent not in correct level
    // TODO: test set properties negative: refer to invalid property
    // TODO: test set properties negative: set prop to invalid value
    // TODO: edit a different member not known to be in cache -- will it be fetched?

    public MemberCacheControlTest() {
    }

    public MemberCacheControlTest(String name) {
        super(name);
    }

    // ~ Utility methods ------------------------------------------------------

    DiffRepository getDiffRepos() {
        return DiffRepository.lookup(MemberCacheControlTest.class);
    }

    public TestContext getTestContext() {
        return TestContext.createSubstitutingCube(
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
    static protected RolapMember findMember(
        TestContext tc,
        String cubeName,
        String ... names)
    {
        Cube cube = tc.getConnection().getSchema().lookupCube(cubeName, true);
        SchemaReader scr = cube.getSchemaReader(null);
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
            pw.printf("  [%s]=[%s]", name, member.getPropertyValue(name));
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
            result.getAxes()[AxisOrdinal.ROWS.logicalOrdinal()]);
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
            cc.createMemberSet(findMember(tc, "Sales", "Retail","OR"), true),
            // all stores in Hidalgo, Zacatecas
            cc.createMemberSet(findMember(tc, "Sales", "Retail","Zacatecas","Hidalgo"), true),
            // a single store
            cc.createMemberSet(findMember(tc, "Sales", "Retail","CA","Alameda","HQ"), false),
            // a range of stores
            cc.createMemberSet(
                true, findMember(tc, "Sales", "Retail","WA","Bremerton"),
                true, findMember(tc, "Sales", "Retail","Yucatan","Merida"),
                false),
            // all stores in a range of states
            cc.createMemberSet(
                true, findMember(tc, "Sales", "Retail","DF"),
                true, findMember(tc, "Sales", "Retail","Jalisco"),
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
     * Tests that member operations fail if cache is enabled.
     */
    public void testMemberOpsFailIfCacheEnabled() {
        if (!rolapCubeMemberCacheEnabled) {
            return;
        }
        final TestContext tc = getTestContext();
        final Connection conn = tc.getConnection();
        final CacheControl cc = conn.getCacheControl(null);
        final CacheControl.MemberEditCommand command =
            cc.createDeleteCommand(findMember(tc, "Sales", "Retail", "OR"));
        try {
            cc.execute(command);
            fail("expected exception");
        } catch (IllegalArgumentException e) {
            assertEquals(
                "Member cache control operations are not allowed unless "
                    + "property mondrian.rolap.EnableRolapCubeMemberCache is "
                    + "false",
                e.getMessage());
        }
    }

    /**
     * Test that edits the properties of a single leaf Member.
     */
    public void testSetPropertyCommandOnLeafMember() {
        // This feature is not supported if cache is enabled.
        if (rolapCubeMemberCacheEnabled) {
            return;
        }
        final TestContext tc = getTestContext();
        final Connection conn = tc.getConnection();
        final DiffRepository dr = getDiffRepos();
        final CacheControl cc = conn.getCacheControl(null);

        // A query that refers to a single leaf Member fetches the Member.
        // Changing Member properties does not affect Cell boundaries, so we
        // check that the MDX results are invariant.
        String mdx = "SELECT {[Measures].[Unit Sales]} ON COLUMNS," +
            " {[Store].[USA].[CA].[San Francisco].[Store 14]}" +
            " ON ROWS FROM [Sales]";
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
        Member m = findMember(tc, "Sales", "Store","USA","CA","San Francisco","Store 14");
        cc.execute(cc.createSetPropertyCommand(m, "Store Manager", "Higgins"));
        cc.execute(
            cc.createCompoundCommand(
                Arrays.asList(
                    cc.createSetPropertyCommand(m, "Street address", "770 Mission St"),
                    cc.createSetPropertyCommand(m, "Store Sqft", "6000"),
                    cc.createSetPropertyCommand(m, "Has coffee bar", "false"))));

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
        // This feature is not supported if cache is enabled.
        if (rolapCubeMemberCacheEnabled) {
            return;
        }
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
                Arrays.asList((Object) "true", "123"));
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
            findMember(tc, "Sales", "Retail","CA","Alameda","HQ");
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
        // This feature is not supported if cache is enabled.
        if (rolapCubeMemberCacheEnabled) {
            return;
        }
        final TestContext tc = getTestContext();
        final Connection conn = tc.getConnection();
        final CacheControl cc = conn.getCacheControl(null);
        final RolapCubeMember caCubeMember =
            (RolapCubeMember) findMember(tc, "Sales", "Retail", "CA");
        final RolapMember caMember = caCubeMember.rolapMember;
        final RolapMember rootMember = caMember.getParentMember();
        final RolapHierarchy hierarchy = caMember.getHierarchy();
        final RolapMember berkeleyMember =
            (RolapMember) hierarchy.createMember(
                caMember,
                caMember.getLevel().getChildLevel(),
                "Berkeley",
                null);
        final RolapMember store987Member =
            (RolapMember) hierarchy.createMember(
                berkeleyMember,
                berkeleyMember.getLevel().getChildLevel(),
                "Store 987",
                null);
        tc.assertAxisReturns(
            "[Retail].[CA].Children",
            fold("[Retail].[All Retails].[CA].[Alameda]\n" +
                "[Retail].[All Retails].[CA].[Beverly Hills]\n" +
                "[Retail].[All Retails].[CA].[Los Angeles]\n" +
                "[Retail].[All Retails].[CA].[San Diego]\n" +
                "[Retail].[All Retails].[CA].[San Francisco]"));
        final MemberReader memberReader = hierarchy.getMemberReader();
        final MemberCache memberCache =
            ((SmartMemberReader) memberReader).getMemberCache();
        List<RolapMember> caChildren =
            memberCache.getChildrenFromCache(caMember, null);
        assertEquals(5, caChildren.size());

        // Now tell the cache that [CA].[Berkeley] and
        // [CA].[Berkeley].[Store 987] are new.
        final CacheControl.MemberEditCommand command =
            cc.createCompoundCommand(
                cc.createAddCommand(berkeleyMember),
                cc.createAddCommand(store987Member));
        cc.execute(command);

        // Make sure that the new members' parents have been removed from the
        // cache.
        caChildren =
            memberCache.getChildrenFromCache(caMember, null);
        assertNull(caChildren);
        final List<RolapMember> berkeleyChildren =
            memberCache.getChildrenFromCache(berkeleyMember, null);
        assertNull(berkeleyChildren);

        List<RolapMember> rootChildren =
            memberCache.getChildrenFromCache(rootMember, null);
        if (rootChildren != null) { // might be null due to gc
            assertEquals(3, rootChildren.size());
        }

        // TODO test that cells have been removed
    }

    public void testDeleteCommand() {
        // This feature is not supported if cache is enabled.
        if (rolapCubeMemberCacheEnabled) {
            return;
        }
        final TestContext tc = getTestContext();
        final Connection conn = tc.getConnection();
        final CacheControl cc = conn.getCacheControl(null);
        final RolapCubeMember alamedaCubeMember =
            (RolapCubeMember) findMember(tc, "Sales", "Retail", "CA", "Alameda");
        final RolapMember alamedaMember = alamedaCubeMember.rolapMember;
        final RolapMember caMember = alamedaMember.getParentMember();
        final RolapMember rootMember = caMember.getParentMember();
        final RolapHierarchy hierarchy = caMember.getHierarchy();

        tc.assertAxisReturns(
            "[Retail].[CA].Children",
            fold("[Retail].[All Retails].[CA].[Alameda]\n" +
                "[Retail].[All Retails].[CA].[Beverly Hills]\n" +
                "[Retail].[All Retails].[CA].[Los Angeles]\n" +
                "[Retail].[All Retails].[CA].[San Diego]\n" +
                "[Retail].[All Retails].[CA].[San Francisco]"));
        final MemberReader memberReader = hierarchy.getMemberReader();
        final MemberCache memberCache =
            ((SmartMemberReader) memberReader).getMemberCache();
        List<RolapMember> caChildren =
            memberCache.getChildrenFromCache(caMember, null);
        assertEquals(5, caChildren.size());

        // Now tell the cache that [CA].[Alameda] has been removed.
        final CacheControl.MemberEditCommand command =
            cc.createDeleteCommand(alamedaMember);
        cc.execute(command);

        // Make sure that the children of [CA] are either removed from the
        // cache or reduced from 5 to 4.
        caChildren =
            memberCache.getChildrenFromCache(caMember, null);
        if (caChildren != null) {
            assertEquals(4, caChildren.size());
        }

        List<RolapMember> rootChildren =
            memberCache.getChildrenFromCache(rootMember, null);
        if (rootChildren != null) {
            assertEquals(9999, rootChildren.size());
        }

        // TODO test that cells have been removed
    }

    /**
     * Tests a variety of negative cases including add/delete/move null members
     * add/delete/move members in parent-child hierarchies.
     */
    public void testAddCommandNegative() {
        // This feature is not supported if cache is enabled.
        if (rolapCubeMemberCacheEnabled) {
            return;
        }
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

        final RolapCubeMember alamedaCubeMember =
            (RolapCubeMember) findMember(tc, "Sales", "Retail", "CA", "Alameda");
        final RolapMember alamedaMember = alamedaCubeMember.rolapMember;
        final RolapMember caMember = alamedaMember.getParentMember();

        final RolapCubeMember empCubeMember =
            (RolapCubeMember) findMember(tc, "HR", "Employees", "Sheri Nowmer", "Michael Spence");
        final RolapMember empMember = empCubeMember.rolapMember;

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
            assertEquals("cannot set properties on null member", e.getMessage());
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
}

// End MemberCacheControlTest.java

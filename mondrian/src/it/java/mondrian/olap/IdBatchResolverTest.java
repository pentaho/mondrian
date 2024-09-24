/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2017 Hitachi Vantara and others
// All Rights Reserved.
 */
package mondrian.olap;

import mondrian.parser.*;
import mondrian.rolap.*;
import mondrian.server.*;

import org.apache.commons.collections.*;

import org.mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.*;

public class IdBatchResolverTest  extends BatchTestCase {

    private Query query;

    @Captor
    private ArgumentCaptor<List<Id.NameSegment>> childNames;

    @Captor
    private ArgumentCaptor<Member> parentMember;

    @Captor
    private ArgumentCaptor<MatchType> matchType;

    @Override
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    public void testSimpleEnum() {
        assertContains(
            "Resolved map omitted one or more members",
            batchResolve(
                "SELECT "
                + "{[Product].[Food].[Dairy],"
                + "[Product].[Food].[Deli],"
                + "[Product].[Food].[Eggs],"
                + "[Product].[Food].[Produce],"
                + "[Product].[Food].[Starchy Foods]}"
                + "on 0 FROM SALES"),
            list(
                "[Product].[Food].[Dairy]",
                "[Product].[Food].[Deli]",
                "[Product].[Food].[Eggs]",
                "[Product].[Food].[Produce]",
                "[Product].[Food].[Starchy Foods]"));

        // verify lookupMemberChildrenByNames is called as expected with
        // batched children's names.
        verify(
            query.getSchemaReader(true), times(2))
            .lookupMemberChildrenByNames(
                parentMember.capture(),
                childNames.capture(),
                matchType.capture());

        assertEquals(
            "[Product].[All Products]",
            parentMember.getAllValues().get(0).getUniqueName());
        assertTrue(childNames.getAllValues().get(0).size() == 1);
        assertEquals(
            "Food",
            childNames.getAllValues().get(0).get(0).getName());

        assertEquals(
            "[Product].[Food]",
            parentMember.getAllValues().get(1).getUniqueName());
        assertTrue(childNames.getAllValues().get(1).size() == 5);

        assertEquals(
            "[[Dairy], [Deli], [Eggs], [Produce], [Starchy Foods]]",
            sortedNames(childNames.getAllValues().get(1)));
    }

    public void testCalcMemsNotResolved() {
        assertFalse(
            "Resolved map should not contain calc members",
            batchResolve(
                "with member time.foo as '1' member time.bar as '2' "
                + " select "
                + " {[Time].[foo], [Time].[bar], "
                + "  [Time].[1997],"
                + "  [Time].[1997].[Q1], [Time].[1997].[Q2]} "
                + " on 0 from sales ")
                .removeAll(list("[Time].[foo]", "[Time].[bar]")));
        // .removeAll will only return true if the set has changed, i.e. if
        // one ore more of the members were present.
    }

    public void testLevelReferenceHandled() {
        // make sure ["Week", 1997] don't get batched as children of
        // [Time.Weekly].[All]
        batchResolve(
            "with member Gender.levelRef as "
            + "'Sum(Descendants([Time.Weekly].CurrentMember, [Time.Weekly].Week))' "
            + "select Gender.levelRef on 0 from sales where [Time.Weekly].[1997]");
        verify(
            query.getSchemaReader(true), times(1))
            .lookupMemberChildrenByNames(
                parentMember.capture(),
                childNames.capture(),
                matchType.capture());
        assertEquals(
            "[Time.Weekly].[All Time.Weeklys]",
            parentMember.getAllValues().get(0).getUniqueName());
        assertEquals(
            "[[1997]]",
            sortedNames(childNames.getAllValues().get(0)));
    }


    public void testPhysMemsResolvedWhenCalcsMixedIn() {
        assertContains(
            "Resolved map omitted one or more members",
            batchResolve(
                "with member time.foo as '1' member time.bar as '2' "
                + " select "
                + " {[Time].[foo], [Time].[bar], "
                + "  [Time].[1997],"
                + "  [Time].[1997].[Q1], [Time].[1997].[Q2]} "
                + " on 0 from sales "),
            list(
                "[Time].[1997]",
                "[Time].[1997].[Q1]",
                "[Time].[1997].[Q2]"));
        verify(
            query.getSchemaReader(true), times(1))
            .lookupMemberChildrenByNames(
                parentMember.capture(),
                childNames.capture(),
                matchType.capture());
        assertEquals(
            "[Time].[1997]",
            parentMember.getAllValues().get(0).getUniqueName());
        assertTrue(childNames.getAllValues().get(0).size() == 2);
        assertEquals(
            "[[Q1], [Q2]]",
            sortedNames(childNames.getAllValues().get(0)));
    }


    public void testAnalyzerFilterMdx() {
        assertContains(
            "Resolved map omitted one or more members",
            batchResolve(
                "WITH\n"
                + "SET [*NATIVE_CJ_SET] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Promotions_],[*BASE_MEMBERS__Store_])'\n"
                + "SET [*BASE_MEMBERS__Store_] AS '{[Store].[USA].[WA].[Bellingham],[Store].[USA].[CA].[Beverly Hills],[Store].[USA].[WA].[Bremerton],[Store].[USA].[CA].[Los Angeles]}'\n"
                + "SET [*SORTED_COL_AXIS] AS 'ORDER([*CJ_COL_AXIS],[Promotions].CURRENTMEMBER.ORDERKEY,BASC)'\n"
                + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
                + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Store].CURRENTMEMBER)})'\n"
                + "SET [*BASE_MEMBERS__Promotions_] AS '{[Promotions].[Bag Stuffers],[Promotions].[Best Savings],[Promotions].[Big Promo],[Promotions].[Big Time Discounts],[Promotions].[Big Time Savings],[Promotions].[Bye Bye Baby]}'\n"
                + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Store].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Store].CURRENTMEMBER,[Store].[Store State]).ORDERKEY,BASC)'\n"
                + "SET [*CJ_COL_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Promotions].CURRENTMEMBER)})'\n"
                + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\n"
                + "SELECT\n"
                + "CROSSJOIN([*SORTED_COL_AXIS],[*BASE_MEMBERS__Measures_]) ON COLUMNS\n"
                + ",NON EMPTY\n"
                + "[*SORTED_ROW_AXIS] ON ROWS\n"
                + "FROM [Sales]"),
            list(
                "[Store].[USA].[WA].[Bellingham]",
                "[Store].[USA].[CA].[Beverly Hills]",
                "[Store].[USA].[WA].[Bremerton]",
                "[Store].[USA].[CA].[Los Angeles]",
                "[Promotions].[Bag Stuffers]", "[Promotions].[Best Savings]",
                "[Promotions].[Big Promo]", "[Promotions].[Big Time Discounts]",
                "[Promotions].[Big Time Savings]",
                "[Promotions].[Bye Bye Baby]"));

        verify(
            query.getSchemaReader(true), times(5))
            .lookupMemberChildrenByNames(
                parentMember.capture(),
                childNames.capture(),
                matchType.capture());

        assertEquals(
            "[Promotions].[All Promotions]",
            parentMember.getAllValues().get(0).getUniqueName());
        assertTrue(childNames.getAllValues().get(0).size() == 6);
        assertEquals(
            "[[Bag Stuffers], [Best Savings], [Big Promo], "
            + "[Big Time Discounts], [Big Time Savings], [Bye Bye Baby]]",
            sortedNames(childNames.getAllValues().get(0)));

        assertEquals(
            "[Store].[USA].[CA]",
            parentMember.getAllValues().get(3).getUniqueName());
        assertTrue(childNames.getAllValues().get(3).size() == 2);
        assertEquals(
            "[[Beverly Hills], [Los Angeles]]",
            sortedNames(childNames.getAllValues().get(3)));
    }

    public void testSetWithNullMember() {
        assertContains(
            "Resolved map omitted one or more members",
            batchResolve(
                "WITH\n"
                + "SET [*NATIVE_CJ_SET] AS 'FILTER([*BASE_MEMBERS__Store Size in SQFT_], NOT ISEMPTY ([Measures].[Unit Sales]))'\n"
                + "SET [*BASE_MEMBERS__Store Size in SQFT_] AS '{[Store Size in SQFT].[#null],[Store Size in SQFT].[20319],[Store Size in SQFT].[21215],[Store Size in SQFT].[22478],[Store Size in SQFT].[23598]}'\n"
                + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
                + "SET [*CJ_SLICER_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Store Size in SQFT].CURRENTMEMBER)})'\n"
                + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\n"
                + "SELECT\n"
                + "[*BASE_MEMBERS__Measures_] ON COLUMNS\n"
                + "FROM [Sales]\n"
                + "WHERE ([*CJ_SLICER_AXIS])"),
            list(
                "[Store Size in SQFT].[#null]",
                "[Store Size in SQFT].[20319]",
                "[Store Size in SQFT].[21215]",
                "[Store Size in SQFT].[22478]",
                "[Store Size in SQFT].[23598]"));

        verify(
            query.getSchemaReader(true), times(1))
            .lookupMemberChildrenByNames(
                parentMember.capture(),
                childNames.capture(),
                matchType.capture());

        assertEquals(
            "[Store Size in SQFT].[All Store Size in SQFTs]",
            parentMember.getAllValues().get(0).getUniqueName());
        assertTrue(childNames.getAllValues().get(0).size() == 5);
        assertEquals(
            "[[#null], [20319], [21215], [22478], [23598]]",
            sortedNames(childNames.getAllValues().get(0)));
    }

    public void testMultiHierarchyNonSSAS() {
        propSaver.set(propSaver.properties.SsasCompatibleNaming, false);
        assertContains(
            "Resolved map omitted one or more members",
            batchResolve(
                "WITH\n"
                + "SET [*NATIVE_CJ_SET] AS 'FILTER([*BASE_MEMBERS__Time.Weekly_], NOT ISEMPTY ([Measures].[Unit Sales]))'\n"
                + "SET [*BASE_MEMBERS__Time.Weekly_] AS '{[Time.Weekly].[1997].[4],[Time.Weekly].[1997].[5],[Time.Weekly].[1997].[6]}'\n"
                + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
                + "SET [*CJ_SLICER_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Time.Weekly].CURRENTMEMBER)})'\n"
                + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\n"
                + "SELECT\n"
                + "[*BASE_MEMBERS__Measures_] ON COLUMNS\n"
                + "FROM [Sales]\n"
                + "WHERE ([*CJ_SLICER_AXIS])"),
            list(
                "[Time.Weekly].[1997].[4]",
                "[Time.Weekly].[1997].[5]",
                "[Time.Weekly].[1997].[6]"));

        verify(
            query.getSchemaReader(true), times(2))
            .lookupMemberChildrenByNames(
                parentMember.capture(),
                childNames.capture(),
                matchType.capture());
        assertEquals(
            "[Time.Weekly].[All Time.Weeklys]",
            parentMember.getAllValues().get(0).getUniqueName());
        assertTrue(childNames.getAllValues().get(0).size() == 1);
        assertEquals(
            "1997",
            childNames.getAllValues().get(0).get(0).getName());
        assertEquals(
            "[[4], [5], [6]]",
            sortedNames(childNames.getAllValues().get(1)));
    }

    public void testMultiHierarchySSAS() {
        propSaver.set(propSaver.properties.SsasCompatibleNaming, true);
        assertContains(
            "Resolved map omitted one or more members",
            batchResolve(
                "WITH\n"
                + "SET [*NATIVE_CJ_SET] AS 'FILTER([*BASE_MEMBERS__Time.Weekly_], NOT ISEMPTY ([Measures].[Unit Sales]))'\n"
                + "SET [*BASE_MEMBERS__Time.Weekly_] AS '{[Time].[Weekly].[1997].[4],[Time].[Weekly].[1997].[5],[Time].[Weekly].[1997].[6]}'\n"
                + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
                + "SET [*CJ_SLICER_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Time].[Weekly].CURRENTMEMBER)})'\n"
                + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\n"
                + "SELECT\n"
                + "[*BASE_MEMBERS__Measures_] ON COLUMNS\n"
                + "FROM [Sales]\n"
                + "WHERE ([*CJ_SLICER_AXIS])"),
            list(
                "[Time].[Weekly].[1997].[4]",
                "[Time].[Weekly].[1997].[5]",
                "[Time].[Weekly].[1997].[6]"));

        verify(
            query.getSchemaReader(true), times(2))
            .lookupMemberChildrenByNames(
                parentMember.capture(),
                childNames.capture(),
                matchType.capture());
        assertEquals(
            "[Time].[Weekly].[All Weeklys]",
            parentMember.getAllValues().get(0).getUniqueName());
        assertTrue(
            childNames.getAllValues().get(0).size() == 1);
        assertEquals(
            "1997",
            childNames.getAllValues().get(0).get(0).getName());
        assertEquals(
            "[[4], [5], [6]]",
            sortedNames(childNames.getAllValues().get(1)));
    }

    public void testParentChild() {
        // P-C resolution will not result in consolidated SQL, but it should
        // still correctly identify children and attempt to resolve them
        // together.
        assertContains(
            "Resolved map omitted one or more members",
            batchResolve(
                "WITH\n"
                + "SET [*NATIVE_CJ_SET] AS 'FILTER([*BASE_MEMBERS__Employees_], NOT ISEMPTY ([Measures].[Number of Employees]))'\n"
                + "SET [*BASE_MEMBERS__Employees_] AS '{[Employees].[Sheri Nowmer].[Derrick Whelply],[Employees].[Sheri Nowmer].[Michael Spence]}'\n"
                + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
                + "SET [*CJ_SLICER_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Employees].CURRENTMEMBER)})'\n"
                + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Number of Employees]', FORMAT_STRING = '#,#', SOLVE_ORDER=500\n"
                + "SELECT\n"
                + "[*BASE_MEMBERS__Measures_] ON COLUMNS\n"
                + "FROM [HR]\n"
                + "WHERE ([*CJ_SLICER_AXIS])"),
                list(
                    "[Employees].[Sheri Nowmer].[Derrick Whelply]",
                    "[Employees].[Sheri Nowmer].[Michael Spence]"));

        verify(
            query.getSchemaReader(true), times(2))
            .lookupMemberChildrenByNames(
                parentMember.capture(),
                childNames.capture(),
                matchType.capture());
        assertEquals(
            "[Employees].[Sheri Nowmer]",
            parentMember.getAllValues().get(1).getUniqueName());
        assertTrue(childNames.getAllValues().get(1).size() == 2);
    }


    private void assertContains(
        String msg, Collection<String> strings, Collection<String> list)
    {
        if (!strings.containsAll(list)) {
            List<String> copy = new ArrayList<String>(list);
            copy.removeAll(strings);
            fail(
                String.format(
                    "%s\nMissing: %s", msg,
                Arrays.toString(copy.toArray())));
        }
    }

    public Set<String> batchResolve(String mdx) {
        IdBatchResolver batchResolver = makeTestBatchResolver(mdx);
        Map<QueryPart, QueryPart> resolvedIdents = batchResolver.resolve();
        Set<String> resolvedNames = getResolvedNames(resolvedIdents);
        return resolvedNames;
    }

    private String sortedNames(List<Id.NameSegment> items) {
        Collections.sort(
            items, new Comparator<Id.NameSegment>()
        {
            public int compare(Id.NameSegment o1, Id.NameSegment o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });
        return Arrays.toString(items.toArray());
    }

    private Collection<String> list(String... items) {
        return Arrays.asList(items);
    }

    private Set<String> getResolvedNames(
        Map<QueryPart, QueryPart> resolvedIdents)
    {
        return new HashSet(
            CollectionUtils
            .collect(
                resolvedIdents.keySet(),
                new Transformer()
                {
                    public Object transform(Object o) {
                        return o.toString();
                    }
                }));
    }

    public IdBatchResolver makeTestBatchResolver(String mdx) {
        getTestContext().flushSchemaCache();
        Parser.FactoryImpl factoryImpl = new FactoryImplTestWrapper();
        MdxParserValidator parser = new JavaccParserValidatorImpl(factoryImpl);

        RolapConnection conn = (RolapConnection) spy(
            getTestContext().withFreshConnection().getConnection());
        when(conn.createParser()).thenReturn(parser);

        query = conn.parseQuery(mdx);
        Locus.push(new Locus(new Execution(
            query.getStatement(), Integer.MAX_VALUE),
            "batchResolveTest", "batchResolveTest"));

        return new IdBatchResolver(query);
    }

    private class QueryTestWrapper extends Query {
        private SchemaReader spyReader;

        public QueryTestWrapper(
            Statement statement,
            Formula[] formulas,
            QueryAxis[] axes,
            String cube,
            QueryAxis slicerAxis,
            QueryPart[] cellProps,
            boolean strictValidation)
        {
            super(
                statement,
                Util.lookupCube(statement.getSchemaReader(), cube, true),
                formulas,
                axes,
                slicerAxis,
                cellProps,
                new Parameter[0],
                strictValidation);
        }

        @Override
        public void resolve() {
            // for testing purposes we want to defer resolution till after
            //  Query init (resolve is called w/in constructor).
            // We do still need formulas to be created, though.
            if (getFormulas() != null) {
                for (Formula formula : getFormulas()) {
                    formula.createElement(this);
                }
            }
        }

        @Override
        public synchronized SchemaReader getSchemaReader(
            boolean accessControlled)
        {
            if (spyReader == null) {
                spyReader = spy(super.getSchemaReader(accessControlled));
            }
            return spyReader;
        }
    }

    public class FactoryImplTestWrapper extends Parser.FactoryImpl {

        @Override
        public Query makeQuery(
            Statement statement,
            Formula[] formulae,
            QueryAxis[] axes,
            String cube,
            Exp slicer,
            QueryPart[] cellProps,
            boolean strictValidation)
        {
            final QueryAxis slicerAxis =
                slicer == null
                    ? null
                    : new QueryAxis(
                        false, slicer, AxisOrdinal.StandardAxisOrdinal.SLICER,
                        QueryAxis.SubtotalVisibility.Undefined, new Id[0]);
            return new QueryTestWrapper(
                statement, formulae, axes, cube, slicerAxis, cellProps,
                strictValidation);
        }
    }
}

// End IdBatchResolverTest.java
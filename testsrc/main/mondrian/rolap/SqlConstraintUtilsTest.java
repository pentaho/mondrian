/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2016 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.impl.UnaryTupleList;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Category;
import mondrian.olap.Connection;
import mondrian.olap.Evaluator;
import mondrian.olap.Evaluator.SetEvaluator;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Member;
import mondrian.olap.Query;
import mondrian.olap.QueryAxis;
import mondrian.olap.Syntax;
import mondrian.olap.fun.AggregateFunDef;
import mondrian.olap.fun.CrossJoinTest.NullFunDef;
import mondrian.olap.fun.ParenthesesFunDef;
import mondrian.olap.type.DecimalType;
import mondrian.olap.type.NullType;
import mondrian.olap.type.Type;
import mondrian.server.Execution;
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;

import junit.framework.Assert;

import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * <code>SqlConstraintUtilsTest</code> tests the functions defined in
 * {@link SqlConstraintUtils}.
 *
 */
public class SqlConstraintUtilsTest extends FoodMartTestCase {

    // ~ Constructors ----------------------------------------------------------

    /**
     * Creates a FunctionTest.
     */
    public SqlConstraintUtilsTest() {
    }

    /**
     * Creates a FuncionTest with an explicit name.
     *
     * @param s Test name
     */
    public SqlConstraintUtilsTest(String s) {
        super(s);
    }

    // ~ Methods ---------------------------------------------------------------


    private void assertSameContent(
        String msg, Collection<Member> expected, Collection<Member> actual)
    {
        if (expected == null) {
            Assert.assertEquals(msg,  expected, actual);
        }
        Assert.assertEquals(msg + " size", expected.size(), actual.size());
        Iterator<Member> itExpected = expected.iterator();
        Iterator<Member> itActual = actual.iterator();
        for (int i = 0; itExpected.hasNext(); i++) {
            Assert.assertEquals(
                msg + " [" + i + "]",
                itActual.next(), itExpected.next());
        }
    }

    private void assertSameContent(
        String msg, List<Member> expected, Member[] actual)
    {
      assertSameContent(msg, expected, Arrays.asList(actual));
    }

    /**
    * Used to suppress a series of asserts on
    * {@code SqlConstraintUtils.expandSupportedCalculatedMembers}
    * when they are supposed to result identically.
    * @param msg message for asserts
    * @param expectedMembersArray expected result
    * @param argMembersArray passed to the tested method
    * @param evaluator passed to the tested method
    */

    private void assertEveryExpandSupportedCalculatedMembers(
        String msg, Member[] expectedMembersArray, Member[] argMembersArray,
        Evaluator evaluator)
    {
      final List<Member> expectedMembersList =
          Collections.unmodifiableList(Arrays.asList(expectedMembersArray));
      final List<Member> argMembersList =
          Collections.unmodifiableList(Arrays.asList(argMembersArray));
      assertSameContent(
          msg + " - (list, eval)",
          expectedMembersList,
          SqlConstraintUtils.expandSupportedCalculatedMembers(
              argMembersList, evaluator));
      assertSameContent(
          msg + " - (array, eval)",
          expectedMembersList,
          SqlConstraintUtils.expandSupportedCalculatedMembers(
              argMembersArray, evaluator));
      assertSameContent(
          msg + " - (array, eval, false)",
          expectedMembersList,
          SqlConstraintUtils.expandSupportedCalculatedMembers(
              argMembersArray, evaluator, false));
      assertSameContent(
          msg + " - (array, eval, true)",
          expectedMembersList,
          SqlConstraintUtils.expandSupportedCalculatedMembers(
              argMembersArray, evaluator, true));
    }

    private void assertApartExpandSupportedCalculatedMembers(
        String msg,
        Member[] expectedByDefault,
        Member[] expectedOnDisjoint,
        Member[] argMembersArray,
        Evaluator evaluator)
    {
      final List<Member> expectedListOnDisjoin =
          Collections.unmodifiableList(Arrays.asList(expectedOnDisjoint));
      final List<Member> expectedListByDefault =
          Collections.unmodifiableList(Arrays.asList(expectedByDefault));
      final List<Member> argMembersList =
          Collections.unmodifiableList(Arrays.asList(argMembersArray));
      assertSameContent(
          msg + " - (list, eval)",
          expectedListByDefault,
          SqlConstraintUtils.expandSupportedCalculatedMembers(
              argMembersList, evaluator));
      assertSameContent(
          msg + " - (array, eval)",
          expectedListByDefault,
          SqlConstraintUtils.expandSupportedCalculatedMembers(
              argMembersArray, evaluator));
      assertSameContent(
          msg + " - (array, eval, false)",
          expectedListByDefault,
          SqlConstraintUtils.expandSupportedCalculatedMembers(
              argMembersArray, evaluator, false));
      assertSameContent(
          msg + " - (array, eval, true)",
          expectedListOnDisjoin,
          SqlConstraintUtils.expandSupportedCalculatedMembers(
              argMembersArray, evaluator, true));
    }

    private Member makeNoncalculatedMember(String toString) {
        Member member = Mockito.mock(Member.class);
        Assert.assertEquals(false, member.isCalculated());
        Mockito.doReturn("mock[" + toString + "]").when(member).toString();
        return member;
    }

    private Exp makeSupportedExpressionForCalculatedMember() {
        Exp memberExpr = new MemberExpr(Mockito.mock(Member.class));
        Assert.assertEquals(
            true,
            SqlConstraintUtils.isSupportedExpressionForCalculatedMember(
                memberExpr));
        return memberExpr;
    }

    private Exp makeUnsupportedExpressionForCalculatedMember() {
        Exp nullFunDefExpr = new ResolvedFunCall(
            new NullFunDef(), new Exp[]{}, new NullType());
        Assert.assertEquals(
            false,
            SqlConstraintUtils.isSupportedExpressionForCalculatedMember(
                nullFunDefExpr));
        return nullFunDefExpr;
    }

    private Member makeUnsupportedCalculatedMember(String toString) {
        Exp memberExp = makeUnsupportedExpressionForCalculatedMember();
        Member member = Mockito.mock(Member.class);
        Mockito.doReturn("mock[" + toString + "]").when(member).toString();
        Mockito.doReturn(true).when(member).isCalculated();
        Mockito.doReturn(memberExp).when(member).getExpression();

        Assert.assertEquals(true, member.isCalculated());
        Assert.assertEquals(
            false, SqlConstraintUtils.isSupportedCalculatedMember(member));

        return member;
    }

    private Member makeMemberExprMember(Member resultMember) {
        Exp memberExp = new MemberExpr(resultMember);
        Member member = Mockito.mock(Member.class);
        Mockito.doReturn(true).when(member).isCalculated();
        Mockito.doReturn(memberExp).when(member).getExpression();
        return member;
    }

    private Member makeAggregateExprMember(
        Evaluator mockEvaluator, List<Member> endMembers)
    {
        Member member = Mockito.mock(Member.class);
        Mockito.doReturn(true).when(member).isCalculated();

        Member aggregatedMember0 = Mockito.mock(Member.class);
        Exp aggregateArg0 = new MemberExpr(aggregatedMember0);

        FunDef dummy = Mockito.mock(FunDef.class);
        Mockito.doReturn(Syntax.Function).when(dummy).getSyntax();
        Mockito.doReturn("dummy").when(dummy).getName();

        FunDef funDef = new AggregateFunDef(dummy);
        Exp[] args = new Exp[]{aggregateArg0};
        Type returnType = new DecimalType(1, 1);
        Exp memberExp = new ResolvedFunCall(funDef, args, returnType);

        Mockito.doReturn(memberExp).when(member).getExpression();

        SetEvaluator setEvaluator = Mockito.mock(SetEvaluator.class);
        Mockito.doReturn(setEvaluator)
            .when(mockEvaluator).getSetEvaluator(aggregateArg0, true);
        Mockito.doReturn(
            new UnaryTupleList(endMembers))
            .when(setEvaluator).evaluateTupleIterable();

        Assert.assertEquals(true, member.isCalculated());
        Assert.assertEquals(
            true, SqlConstraintUtils.isSupportedCalculatedMember(member));

        return member;
    }

    private Member makeParenthesesExprMember(
        Evaluator evaluator, Member parenthesesInnerMember, String toString)
    {
        Member member = Mockito.mock(Member.class);
        Mockito.doReturn("mock[" + toString + "]").when(member).toString();
        Mockito.doReturn(true).when(member).isCalculated();

        Exp parenthesesArg = new MemberExpr(parenthesesInnerMember);

        FunDef funDef = new ParenthesesFunDef(Category.Member);
        Exp[] args = new Exp[]{parenthesesArg};
        Type returnType = new DecimalType(1, 1);
        Exp memberExp = new ResolvedFunCall(funDef, args, returnType);

        Mockito.doReturn(memberExp).when(member).getExpression();

        Assert.assertEquals(true, member.isCalculated());
        Assert.assertEquals(
            true, SqlConstraintUtils.isSupportedCalculatedMember(member));

        return member;
    }

    // ~ Test methods ----------------------------------------------------------

    public void testIsSupportedExpressionForCalculatedMember() {
        Assert.assertEquals(
            "null expression",
            false,
            SqlConstraintUtils.isSupportedExpressionForCalculatedMember(null));

        Exp memberExpr = new MemberExpr(Mockito.mock(Member.class));
        Assert.assertEquals(
            "MemberExpr",
            true,
            SqlConstraintUtils.isSupportedExpressionForCalculatedMember(
                memberExpr));

        Exp nullFunDefExpr = new ResolvedFunCall(
            new NullFunDef(), new Exp[]{}, new NullType());
        Assert.assertEquals(
            "ResolvedFunCall-NullFunDef",
            false,
            SqlConstraintUtils.isSupportedExpressionForCalculatedMember(
                nullFunDefExpr));

        // ResolvedFunCall arguments
        final Exp argUnsupported = new ResolvedFunCall(
            new NullFunDef(), new Exp[]{}, new NullType());
        final Exp argSupported = new MemberExpr(Mockito.mock(Member.class));
        Assert.assertEquals(
            false,
            SqlConstraintUtils.isSupportedExpressionForCalculatedMember(
                argUnsupported));
        Assert.assertEquals(
            true,
            SqlConstraintUtils.isSupportedExpressionForCalculatedMember(
                argSupported));
        final Exp[] noArgs = new Exp[]{};
        final Exp[] args1Unsupported = new Exp[]{argUnsupported};
        final Exp[] args1Supported = new Exp[]{argSupported};
        final Exp[] args2Different = new Exp[]{argUnsupported, argSupported};

        final ParenthesesFunDef parenthesesFunDef =
            new ParenthesesFunDef(Category.Member);
        Type parenthesesReturnType = new DecimalType(1, 1);
        Exp parenthesesExpr = new ResolvedFunCall(
            parenthesesFunDef, noArgs, parenthesesReturnType);
        Assert.assertEquals(
            "ResolvedFunCall-Parentheses()",
            true,
            SqlConstraintUtils.isSupportedExpressionForCalculatedMember(
                parenthesesExpr));

        parenthesesExpr = new ResolvedFunCall(
            parenthesesFunDef, args1Unsupported, parenthesesReturnType);
        Assert.assertEquals(
            "ResolvedFunCall-Parentheses(N)",
            false, SqlConstraintUtils.isSupportedExpressionForCalculatedMember(
                parenthesesExpr));

        parenthesesExpr = new ResolvedFunCall(
            parenthesesFunDef, args1Supported, parenthesesReturnType);
        Assert.assertEquals(
            "ResolvedFunCall-Parentheses(Y)",
            true, SqlConstraintUtils.isSupportedExpressionForCalculatedMember(
                parenthesesExpr));

        parenthesesExpr = new ResolvedFunCall(
            parenthesesFunDef, args2Different, parenthesesReturnType);
        Assert.assertEquals(
            "ResolvedFunCall-Parentheses(N,Y)",
            true, SqlConstraintUtils.isSupportedExpressionForCalculatedMember(
                parenthesesExpr));

        FunDef dummy = Mockito.mock(FunDef.class);
        Mockito.doReturn(Syntax.Function).when(dummy).getSyntax();
        Mockito.doReturn("dummy").when(dummy).getName();
        FunDef aggregateFunDef = new AggregateFunDef(dummy);
        Type aggregateReturnType = new DecimalType(1, 1);

        Exp aggregateExpr = new ResolvedFunCall(
            aggregateFunDef, noArgs, aggregateReturnType);
        Assert.assertEquals(
            "ResolvedFunCall-Aggregate()",
            true, SqlConstraintUtils.isSupportedExpressionForCalculatedMember(
                aggregateExpr));

        aggregateExpr = new ResolvedFunCall(
            aggregateFunDef, args1Unsupported, aggregateReturnType);
        Assert.assertEquals(
            "ResolvedFunCall-Aggregate(N)",
            true, SqlConstraintUtils.isSupportedExpressionForCalculatedMember(
                aggregateExpr));

        aggregateExpr = new ResolvedFunCall(
            aggregateFunDef, args1Supported, aggregateReturnType);
        Assert.assertEquals(
            "ResolvedFunCall-Aggregate(Y)",
            true, SqlConstraintUtils.isSupportedExpressionForCalculatedMember(
                aggregateExpr));

        aggregateExpr = new ResolvedFunCall(
            aggregateFunDef, args2Different, aggregateReturnType);
        Assert.assertEquals(
            "ResolvedFunCall-Aggregate(N,Y)",
            true,
            SqlConstraintUtils.isSupportedExpressionForCalculatedMember(
                aggregateExpr));
    }

    public void testIsSupportedCalculatedMember() {
        Member member = Mockito.mock(Member.class);
        Assert.assertEquals(false, member.isCalculated());
        Assert.assertEquals(
            false, SqlConstraintUtils.isSupportedCalculatedMember(member));

        Mockito.doReturn(true).when(member).isCalculated();

        Assert.assertEquals(null, member.getExpression());
        Assert.assertEquals(
            false, SqlConstraintUtils.isSupportedCalculatedMember(member));

        Mockito.doReturn(makeUnsupportedExpressionForCalculatedMember())
        .when(member).getExpression();
        Assert.assertEquals(
            false, SqlConstraintUtils.isSupportedCalculatedMember(member));

        Mockito.doReturn(makeSupportedExpressionForCalculatedMember())
        .when(member).getExpression();
        Assert.assertEquals(
            true, SqlConstraintUtils.isSupportedCalculatedMember(member));
    }

    public void testReplaceCompoundSlicerPlaceholder() {
        final TestContext testContext = TestContext.instance();
        final Connection connection = testContext.getConnection();

        final String queryText =
            "SELECT {[Measures].[Customer Count]} ON 0 "
            + "FROM [Sales] "
            + "WHERE [Time].[1997]";

        final Query query = connection.parseQuery(queryText);
        final QueryAxis querySlicerAxis = query.getSlicerAxis();
        final Member slicerMember =
            ((MemberExpr)querySlicerAxis.getSet()).getMember();
        final RolapHierarchy slicerHierarchy =
            ((RolapCube)query.getCube()).getTimeHierarchy(null);

        final Execution execution = new Execution(query.getStatement(), 0L);
        final RolapEvaluatorRoot rolapEvaluatorRoot =
            new RolapEvaluatorRoot(execution);
        final RolapEvaluator rolapEvaluator =
            new RolapEvaluator(rolapEvaluatorRoot);
        final Member expectedMember = slicerMember;
        rolapEvaluator.setSlicerContext(expectedMember);

        RolapResult.CompoundSlicerRolapMember placeHolderMember =
            Mockito.mock(RolapResult.CompoundSlicerRolapMember.class);
        Mockito.doReturn(slicerHierarchy)
        .when(placeHolderMember).getHierarchy();
        // tested call
        Member r = SqlConstraintUtils.replaceCompoundSlicerPlaceholder(
            placeHolderMember, rolapEvaluator);
        // test
        Assert.assertSame(expectedMember, r);
    }

    public void testExpandSupportedCalculatedMembers() {
        Evaluator evaluator = Mockito.mock(Evaluator.class);

        Member endMember0 = Mockito.mock(Member.class);
        Member endMember1 = Mockito.mock(Member.class);
        Member endMember2 = Mockito.mock(Member.class);
        Member endMember3 = Mockito.mock(Member.class);

        Member argMember0 = null;
        Member argMember1 = null;

        Member[] argMembers = null;
        Member[] expectedMembers = null;

        // ()
        argMembers = new Member[] {};
        expectedMembers = new Member[] {};
        assertEveryExpandSupportedCalculatedMembers(
            "()", expectedMembers, argMembers, evaluator);

        // (0, 2)
        argMember0 = endMember0;
        argMember1 = endMember2;
        argMembers = new Member[] {argMember0, argMember1};
        expectedMembers = new Member[] {endMember0, endMember2};
        assertEveryExpandSupportedCalculatedMembers(
            "(0, 2)", expectedMembers, argMembers, evaluator);

        // (Aggr(0, 1), 2)
        argMember0 = makeAggregateExprMember(
            evaluator,
            Arrays.asList(new Member[] {endMember0, endMember1}));
        argMember1 = endMember2;
        argMembers = new Member[] {argMember0, argMember1};
        expectedMembers = new Member[] {endMember0, endMember1, endMember2};
        assertEveryExpandSupportedCalculatedMembers(
            "(Aggr(0, 1), 2)", expectedMembers, argMembers, evaluator);

        // (Aggr(0, 1), Aggr(3, 2))
        argMember0 = makeAggregateExprMember(
            evaluator,
            Arrays.asList(new Member[] {endMember0, endMember1}));
        argMember1 = makeAggregateExprMember(
            evaluator,
            Arrays.asList(new Member[] {endMember3, endMember2}));
        argMember1 = endMember2;
        argMembers = new Member[] {argMember0, argMember1};
        expectedMembers = new Member[] {endMember0, endMember1, endMember2};
        assertEveryExpandSupportedCalculatedMembers(
            "(Aggr(0, 1), Aggr(3, 2))", expectedMembers, argMembers, evaluator);
    }

    // test with a placeholder member
    public void testExpandSupportedCalculatedMembers2() {
      final TestContext testContext = TestContext.instance();
      final Connection connection = testContext.getConnection();

      final String queryText =
          "SELECT {[Measures].[Customer Count]} ON 0 "
          + "FROM [Sales] "
          + "WHERE [Time].[1997]";

      final Query query = connection.parseQuery(queryText);
      final QueryAxis querySlicerAxis = query.getSlicerAxis();
      final Member slicerMember =
          ((MemberExpr)querySlicerAxis.getSet()).getMember();
      final RolapHierarchy slicerHierarchy =
          ((RolapCube)query.getCube()).getTimeHierarchy(null);

      final Execution execution = new Execution(query.getStatement(), 0L);
      final RolapEvaluatorRoot rolapEvaluatorRoot =
          new RolapEvaluatorRoot(execution);
      final RolapEvaluator rolapEvaluator =
          new RolapEvaluator(rolapEvaluatorRoot);
      final Member expectedMember = slicerMember;
      rolapEvaluator.setSlicerContext(expectedMember);

      RolapResult.CompoundSlicerRolapMember placeHolderMember =
          Mockito.mock(RolapResult.CompoundSlicerRolapMember.class);
      Mockito.doReturn(slicerHierarchy)
      .when(placeHolderMember).getHierarchy();

      Member endMember0 = makeNoncalculatedMember("0");

      // (0, placeholder)
      Member[] argMembers = new Member[] {endMember0, placeHolderMember};
      Member[] expectedMembers = new Member[] {endMember0, slicerMember};
      Member[] expectedMembersOnDisjoin = new Member[] {endMember0};
      assertApartExpandSupportedCalculatedMembers(
          "(0, placeholder)",
          expectedMembers, expectedMembersOnDisjoin, argMembers,
          rolapEvaluator);
  }

}

// End SqlConstraintUtilsTest.java
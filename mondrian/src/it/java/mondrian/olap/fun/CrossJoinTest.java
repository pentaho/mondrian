/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2015 Pentaho Corporation..  All rights reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.ArrayTupleList;
import mondrian.calc.impl.UnaryTupleList;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.rolap.RolapCube;
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.test.FoodMartTestCase;

import junit.framework.Assert;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.PrintWriter;
import java.util.*;

/**
 * <code>CrossJoint</code> tests the collation order of positive and negative
 * infinity, and {@link Double#NaN}.
 *
 * @author <a>Richard M. Emberson</a>
 * @since Jan 14, 2007
 */

public class CrossJoinTest extends FoodMartTestCase {

    private static final String SELECT_GENDER_MEMBERS =
        "select Gender.members on 0 from sales";

    private static final String SALES_CUBE = "Sales";

    private Execution excMock = mock(Execution.class);

    static List<List<Member>> m3 = Arrays.asList(
        Arrays.<Member>asList(new TestMember("k"), new TestMember("l")),
        Arrays.<Member>asList(new TestMember("m"), new TestMember("n")));

    static List<List<Member>> m4 = Arrays.asList(
        Arrays.<Member>asList(new TestMember("U"), new TestMember("V")),
        Arrays.<Member>asList(new TestMember("W"), new TestMember("X")),
        Arrays.<Member>asList(new TestMember("Y"), new TestMember("Z")));

    static final Comparator<List<Member>> memberComparator =
        new Comparator<List<Member>>() {
            public int compare(List<Member> ma1, List<Member> ma2) {
                for (int i = 0; i < ma1.size(); i++) {
                    int c = ma1.get(i).compareTo(ma2.get(i));
                    if (c < 0) {
                        return c;
                    } else if (c > 0) {
                        return c;
                    }
                }
                return 0;
            }
        };

    private CrossJoinFunDef crossJoinFunDef;

    public CrossJoinTest() {
    }

    public CrossJoinTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
        crossJoinFunDef = new CrossJoinFunDef(new NullFunDef());
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    ////////////////////////////////////////////////////////////////////////
    // Iterable
    ////////////////////////////////////////////////////////////////////////

    public void testListTupleListTupleIterCalc() {
      if (!Util.Retrowoven) {
        propSaver.set(propSaver.properties.CheckCancelOrTimeoutInterval, 0);
        CrossJoinFunDef.CrossJoinIterCalc calc =
            crossJoinFunDef.new CrossJoinIterCalc(getResolvedFunCall(), null);

        doTupleTupleIterTest(calc, excMock);
      }
    }

    private void doTupleTupleIterTest(
        CrossJoinFunDef.CrossJoinIterCalc calc, Execution execution)
    {
      TupleList l4 = makeListTuple(m4);
      String s4 = toString(l4);
      String e4 = "{[U, V], [W, X], [Y, Z]}";
      Assert.assertEquals(e4, s4);

      TupleList l3 = makeListTuple(m3);
      String s3 = toString(l3);
      String e3 = "{[k, l], [m, n]}";
      Assert.assertEquals(e3, s3);

      String s = Locus.execute(
          execution, "CrossJoinTest", new Locus.Action<String>()
      {
        public String execute() {
          TupleIterable iterable = calc.makeIterable(l4, l3);
          return CrossJoinTest.this.toString(iterable);
        }
      });
      String e =
          "{[U, V, k, l], [U, V, m, n], [W, X, k, l], "
          + "[W, X, m, n], [Y, Z, k, l], [Y, Z, m, n]}";
      Assert.assertEquals(e, s);
    }

    // The test to verify that cancellation/timeout is checked
    // in CrossJoinFunDef$CrossJoinIterCalc$1$1.forward()
    public void testCrossJoinIterCalc_IterationCancellationOnForward() {
      propSaver.set(propSaver.properties.CheckCancelOrTimeoutInterval, 1);
      // Get product members as TupleList
      RolapCube salesCube =
          (RolapCube) cubeByName(getTestContext().getConnection(), SALES_CUBE);
      SchemaReader salesCubeSchemaReader =
          salesCube.getSchemaReader(getTestContext().getConnection().getRole())
          .withLocus();
      TupleList productMembers =
          productMembersPotScrubbersPotsAndPans(salesCubeSchemaReader);
      // Get genders members as TupleList
      Result genders = executeQuery(SELECT_GENDER_MEMBERS);
      TupleList genderMembers = getGenderMembers(genders);

      // Test execution to track cancellation/timeout calls
      Execution execution =
          spy(new Execution(genders.getQuery().getStatement(), 0));
      // check no execution of checkCancelOrTimeout has been yet
      verify(execution, times(0)).checkCancelOrTimeout();
      Integer crossJoinIterCalc =
          crossJoinIterCalcIterate(productMembers, genderMembers, execution);

      // checkCancelOrTimeout should be called once for the left tuple
      //from CrossJoinIterCalc$1$1.forward() since phase
      // interval is 1
      verify(execution, times(productMembers.size())).checkCancelOrTimeout();
      assertEquals(
          productMembers.size() * genderMembers.size(),
          crossJoinIterCalc.intValue());
    }

    private static TupleList getGenderMembers(Result genders) {
      TupleList genderMembers = new UnaryTupleList();
      for (Position pos : genders.getAxes()[0].getPositions()) {
        genderMembers.add(pos);
      }
      return genderMembers;
    }

    private Integer crossJoinIterCalcIterate(
        final TupleList list1, final TupleList list2,
        final Execution execution)
    {
      return Locus.execute(
          execution, "CrossJoinTest", new Locus.Action<Integer>() {
        public Integer execute() {
          TupleIterable iterable =
              crossJoinFunDef.new CrossJoinIterCalc(
                  getResolvedFunCall(), null).makeIterable(list1, list2);
          TupleCursor tupleCursor = iterable.tupleCursor();
          // total count of all iterations
          int counter = 0;
          while (tupleCursor.forward()) {
            counter++;
          }
          return Integer.valueOf(counter);
        }
      });
    }

    ////////////////////////////////////////////////////////////////////////
    // Immutable List
    ////////////////////////////////////////////////////////////////////////

    public void testImmutableListTupleListTupleListCalc() {
        CrossJoinFunDef.ImmutableListCalc calc =
            crossJoinFunDef.new ImmutableListCalc(
                getResolvedFunCall(), null);

        doTupleTupleListTest(calc);
    }

    protected void doTupleTupleListTest(
        CrossJoinFunDef.BaseListCalc calc)
    {
        TupleList l4 = makeListTuple(m4);
        String s4 = toString(l4);
        String e4 = "{[U, V], [W, X], [Y, Z]}";
        Assert.assertEquals(e4, s4);

        TupleList l3 = makeListTuple(m3);
        String s3 = toString(l3);
        String e3 = "{[k, l], [m, n]}";
        Assert.assertEquals(e3, s3);

        TupleList list = calc.makeList(l4, l3);
        String s = toString(list);
        String e =
            "{[U, V, k, l], [U, V, m, n], [W, X, k, l], "
            + "[W, X, m, n], [Y, Z, k, l], [Y, Z, m, n]}";
        Assert.assertEquals(e, s);

        TupleList subList = list.subList(0, 6);
        s = toString(subList);
        Assert.assertEquals(6, subList.size());
        Assert.assertEquals(e, s);

        subList = subList.subList(0, 6);
        s = toString(subList);
        Assert.assertEquals(6, subList.size());
        Assert.assertEquals(e, s);

        subList = subList.subList(1, 5);
        s = toString(subList);
        e = "{[U, V, m, n], [W, X, k, l], [W, X, m, n], [Y, Z, k, l]}";
        Assert.assertEquals(4, subList.size());
        Assert.assertEquals(e, s);

        subList = subList.subList(2, 4);
        s = toString(subList);
        e = "{[W, X, m, n], [Y, Z, k, l]}";
        Assert.assertEquals(2, subList.size());
        Assert.assertEquals(e, s);

        subList = subList.subList(1, 2);
        s = toString(subList);
        e = "{[Y, Z, k, l]}";
        Assert.assertEquals(1, subList.size());
        Assert.assertEquals(e, s);

        subList = list.subList(1, 4);
        s = toString(subList);
        e = "{[U, V, m, n], [W, X, k, l], [W, X, m, n]}";
        Assert.assertEquals(3, subList.size());
        Assert.assertEquals(e, s);

        subList = list.subList(2, 4);
        s = toString(subList);
        e = "{[W, X, k, l], [W, X, m, n]}";
        Assert.assertEquals(2, subList.size());
        Assert.assertEquals(e, s);

        subList = list.subList(2, 3);
        s = toString(subList);
        e = "{[W, X, k, l]}";
        Assert.assertEquals(1, subList.size());
        Assert.assertEquals(e, s);

        subList = list.subList(4, 4);
        s = toString(subList);
        e = "{}";
        Assert.assertEquals(0, subList.size());
        Assert.assertEquals(e, s);
    }



    ////////////////////////////////////////////////////////////////////////
    // Mutable List
    ////////////////////////////////////////////////////////////////////////

    public void testMutableListTupleListTupleListCalc() {
        CrossJoinFunDef.MutableListCalc calc =
            crossJoinFunDef.new MutableListCalc(
                getResolvedFunCall(), null);

        doMTupleTupleListTest(calc);
    }

    protected void doMTupleTupleListTest(
        CrossJoinFunDef.BaseListCalc calc)
    {
        TupleList l1 = makeListTuple(m3);
        String s1 = toString(l1);
        String e1 = "{[k, l], [m, n]}";
        Assert.assertEquals(e1, s1);

        TupleList l2 = makeListTuple(m4);
        String s2 = toString(l2);
        String e2 = "{[U, V], [W, X], [Y, Z]}";
        Assert.assertEquals(e2, s2);

        TupleList list = calc.makeList(l1, l2);
        String s = toString(list);
        String e = "{[k, l, U, V], [k, l, W, X], [k, l, Y, Z], "
            + "[m, n, U, V], [m, n, W, X], [m, n, Y, Z]}";
        Assert.assertEquals(e, s);

        if (false) {
            // Cannot apply Collections.reverse to TupleList
            // because TupleList.set always returns null.
            // (This is a violation of the List contract, but it is inefficient
            // to construct a list to return.)
            Collections.reverse(list);
            s = toString(list);
            e = "{[m, n, Y, Z], [m, n, W, X], [m, n, U, V], "
                + "[k, l, Y, Z], [k, l, W, X], [k, l, U, V]}";
            Assert.assertEquals(e, s);
        }

        // sort
        Collections.sort(list, memberComparator);
        s = toString(list);
        e = "{[k, l, U, V], [k, l, W, X], [k, l, Y, Z], "
            + "[m, n, U, V], [m, n, W, X], [m, n, Y, Z]}";
        Assert.assertEquals(e, s);

        List<Member> members = list.remove(1);
        s = toString(list);
        e = "{[k, l, U, V], [k, l, Y, Z], [m, n, U, V], "
            + "[m, n, W, X], [m, n, Y, Z]}";
        Assert.assertEquals(e, s);
    }

    ////////////////////////////////////////////////////////////////////////
    // Helper methods
    ////////////////////////////////////////////////////////////////////////
    protected String toString(TupleIterable l) {
        StringBuffer buf = new StringBuffer(100);
        buf.append('{');
        int j = 0;
        for (List<Member> o : l) {
            if (j++ > 0) {
                buf.append(", ");
            }
            buf.append(o);
        }
        buf.append('}');
        return buf.toString();
    }

    protected TupleList makeListTuple(List<List<Member>> ms) {
        final TupleList list = new ArrayTupleList(ms.get(0).size());
        for (List<Member> m : ms) {
            list.add(m);
        }
        return list;
    }

    protected ResolvedFunCall getResolvedFunCall() {
        FunDef funDef = new TestFunDef();
        Exp[] args = new Exp[0];
        Type returnType =
            new SetType(
                new TupleType(
                    new Type[] {
                    new MemberType(null, null, null, null),
                    new MemberType(null, null, null, null)}));
        return new ResolvedFunCall(funDef, args, returnType);
    }

    ////////////////////////////////////////////////////////////////////////
    // Helper classes
    ////////////////////////////////////////////////////////////////////////
    public static class TestFunDef implements FunDef {
        TestFunDef() {
        }
        public Syntax getSyntax() {
            throw new UnsupportedOperationException();
        }
        public String getName() {
            throw new UnsupportedOperationException();
        }
        public String getDescription() {
            throw new UnsupportedOperationException();
        }
        public int getReturnCategory() {
            throw new UnsupportedOperationException();
        }
        public int[] getParameterCategories() {
            throw new UnsupportedOperationException();
        }
        public Exp createCall(Validator validator, Exp[] args) {
            throw new UnsupportedOperationException();
        }
        public String getSignature() {
            throw new UnsupportedOperationException();
        }
        public void unparse(Exp[] args, PrintWriter pw) {
            throw new UnsupportedOperationException();
        }
        public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
            throw new UnsupportedOperationException();
        }
    }

    public static class NullFunDef implements FunDef {
        public NullFunDef() {
        }
        public Syntax getSyntax() {
            return Syntax.Function;
        }
        public String getName() {
            return "";
        }
        public String getDescription() {
            return "";
        }
        public int getReturnCategory() {
            return 0;
        }
        public int[] getParameterCategories() {
            return new int[0];
        }
        public Exp createCall(Validator validator, Exp[] args) {
            return null;
        }
        public String getSignature() {
            return "";
        }
        public void unparse(Exp[] args, PrintWriter pw) {
           //
        }
        public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
            return null;
        }
    }
}

// End CrossJoinTest.java

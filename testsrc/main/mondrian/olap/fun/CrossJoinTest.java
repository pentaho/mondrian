/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import junit.framework.Assert;
import mondrian.test.FoodMartTestCase;
import mondrian.calc.*;
import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.mdx.*;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.io.PrintWriter;

/**
 * <code>CrossJoint</code> tests the collation order of positive and negative
 * infinity, and {@link Double#NaN}.
 *
 * @author <a>Richard M. Emberson</a>
 * @version $Id$
 * @since Jan 14, 2007
 */

public class CrossJoinTest extends FoodMartTestCase {

    static Member[] m1 = new Member[] {
        new TestMember("a"),
        new TestMember("b"),
        new TestMember("c"),
    };
    static Member[] m2 = new Member[] {
        new TestMember("A"),
        new TestMember("B"),
        new TestMember("C"),
        new TestMember("D"),
    };
    static Member[][] m3 = new Member[][] {
        { new TestMember("k"), new TestMember("l") },
        { new TestMember("m"), new TestMember("n") },
    };
    static Member[][] m4 = new Member[][] {
        { new TestMember("U"), new TestMember("V") },
        { new TestMember("W"), new TestMember("X") },
        { new TestMember("Y"), new TestMember("Z") },
    };

    static final Comparator<Member[]> memberComparator =
        new Comparator<Member[]>() {
            public int compare(Member[] ma1, Member[] ma2) {
                for (int i = 0; i < ma1.length; i++) {
                    int c = ma1[i].compareTo(ma2[i]);
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
    ////////////////////////////////////////////////////////////////////////
    // Iterable
    ////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////
    // Member Member
    ////////////////////////////////////////////////////////////////////////
    public void testIterMemberIterMemberIterCalc() {
if (! Util.Retrowoven) {
        CrossJoinFunDef.IterMemberIterMemberIterCalc calc =
            crossJoinFunDef.new IterMemberIterMemberIterCalc(
                getResolvedFunCall(), null);

//System.out.println("testIterMemberIterMemberIterCalc");
        doMemberMemberIterTest(calc);
}
    }
    public void testIterMemberListMemberIterCalc() {
if (! Util.Retrowoven) {
        CrossJoinFunDef.IterMemberListMemberIterCalc calc =
            crossJoinFunDef.new IterMemberListMemberIterCalc(
                getResolvedFunCall(), null);

//System.out.println("testIterMemberListMemberIterCalc");
        doMemberMemberIterTest(calc);
}
    }
    public void testListMemberIterMemberIterCalc() {
if (! Util.Retrowoven) {
        CrossJoinFunDef.ListMemberIterMemberIterCalc calc =
            crossJoinFunDef.new ListMemberIterMemberIterCalc(
                getResolvedFunCall(), null);

//System.out.println("testListMemberIterMemberIterCalc");
        doMemberMemberIterTest(calc);
}
    }
    public void testListMemberListMemberIterCalc() {
if (! Util.Retrowoven) {
        CrossJoinFunDef.ListMemberListMemberIterCalc calc =
            crossJoinFunDef.new ListMemberListMemberIterCalc(
                getResolvedFunCall(), null);

//System.out.println("testListMemberListMemberIterCalc");
        doMemberMemberIterTest(calc);
}
    }

    protected void doMemberMemberIterTest(
            CrossJoinFunDef.BaseMemberMemberIterCalc calc) {

        List<Member> l1 = makeListMember(m1);
        String s1 = toString(l1);
        String e1 = "{a,b,c}";
//System.out.println("l1="+s1);
        Assert.assertEquals(s1, e1);

        List<Member> l2 = makeListMember(m2);
        String s2 = toString(l2);
        String e2 = "{A,B,C,D}";
//System.out.println("l2="+s2);
        Assert.assertEquals(s2, e2);

        Iterable<Member[]> iter = calc.makeIterable(l1, l2);
        String s = toString(iter);
        String e = "{[a,A],[a,B],[a,C],[a,D],[b,A],[b,B],[b,C],[b,D],[c,A],[c,B],[c,C],[c,D]}";
//System.out.println("iter="+s);
        Assert.assertEquals(s, e);
    }
    ////////////////////////////////////////////////////////////////////////
    // Member Member[]
    ////////////////////////////////////////////////////////////////////////

    public void testIterMemberIterMemberArrayIterCalc() {
if (! Util.Retrowoven) {
        CrossJoinFunDef.IterMemberIterMemberArrayIterCalc calc =
            crossJoinFunDef.new IterMemberIterMemberArrayIterCalc(
                getResolvedFunCall(), null);

//System.out.println("testIterMemberIterMemberArrayIterCalc");
        doMemberMemberArrayIterTest(calc);
}
    }

    public void testIterMemberListMemberArrayIterCalc() {
if (! Util.Retrowoven) {
        CrossJoinFunDef.IterMemberListMemberArrayIterCalc calc =
            crossJoinFunDef.new IterMemberListMemberArrayIterCalc(
                getResolvedFunCall(), null);

//System.out.println("testIterMemberListMemberArrayIterCalc");
        doMemberMemberArrayIterTest(calc);
}
    }
    public void testListMemberIterMemberArrayIterCalc() {
if (! Util.Retrowoven) {
        CrossJoinFunDef.ListMemberIterMemberArrayIterCalc calc =
            crossJoinFunDef.new ListMemberIterMemberArrayIterCalc(
                getResolvedFunCall(), null);

//System.out.println("testListMemberIterMemberArrayIterCalc");
        doMemberMemberArrayIterTest(calc);
}
    }
    public void testListMemberListMemberArrayIterCalc() {
if (! Util.Retrowoven) {
        CrossJoinFunDef.ListMemberListMemberArrayIterCalc calc =
            crossJoinFunDef.new ListMemberListMemberArrayIterCalc(
                getResolvedFunCall(), null);

//System.out.println("testListMemberListMemberArrayIterCalc");
        doMemberMemberArrayIterTest(calc);
}
    }

    protected void doMemberMemberArrayIterTest(
            CrossJoinFunDef.BaseMemberMemberArrayIterCalc calc) {

        List<Member> l1 = makeListMember(m1);
        String s1 = toString(l1);
        String e1 = "{a,b,c}";
//System.out.println("l1="+s1);
        Assert.assertEquals(s1, e1);

        List<Member[]> l3 = makeListMemberArray(m3);
        String s3 = toString(l3);
        String e3 = "{[k,l],[m,n]}";
//System.out.println("l3="+s3);
        Assert.assertEquals(s3, e3);

        Iterable<Member[]> iter = calc.makeIterable(l1, l3);
        String s = toString(iter);
        String e = "{[a,k,l],[a,m,n],[b,k,l],[b,m,n],[c,k,l],[c,m,n]}";
//System.out.println("iter="+s);
        Assert.assertEquals(s, e);
    }
    ////////////////////////////////////////////////////////////////////////
    // Member[] Member
    ////////////////////////////////////////////////////////////////////////
    public void testIterMemberArrayIterMemberIterCalc() {
if (! Util.Retrowoven) {
        CrossJoinFunDef.IterMemberArrayIterMemberIterCalc calc =
            crossJoinFunDef.new IterMemberArrayIterMemberIterCalc(
                getResolvedFunCall(), null);

//System.out.println("testIterMemberArrayIterMemberIterCalc");
        doMemberArrayMemberIterTest(calc);
}
    }
    public void testIterMemberArrayListMemberIterCalc() {
if (! Util.Retrowoven) {
        CrossJoinFunDef.IterMemberArrayListMemberIterCalc calc =
            crossJoinFunDef.new IterMemberArrayListMemberIterCalc(
                getResolvedFunCall(), null);

//System.out.println("testIterMemberArrayListMemberIterCalc");
        doMemberArrayMemberIterTest(calc);
}
    }
    public void testListMemberArrayIterMemberIterCalc() {
if (! Util.Retrowoven) {
        CrossJoinFunDef.ListMemberArrayIterMemberIterCalc calc =
            crossJoinFunDef.new ListMemberArrayIterMemberIterCalc(
                getResolvedFunCall(), null);

//System.out.println("testListMemberArrayIterMemberIterCalc");
        doMemberArrayMemberIterTest(calc);
}
    }
    public void testListMemberArrayListMemberIterCalc() {
if (! Util.Retrowoven) {
        CrossJoinFunDef.ListMemberArrayListMemberIterCalc calc =
            crossJoinFunDef.new ListMemberArrayListMemberIterCalc(
                getResolvedFunCall(), null);

//System.out.println("testListMemberArrayListMemberIterCalc");
        doMemberArrayMemberIterTest(calc);
}
    }

    protected void doMemberArrayMemberIterTest(
            CrossJoinFunDef.BaseMemberArrayMemberIterCalc calc) {

        List<Member[]> l3 = makeListMemberArray(m3);
        String s3 = toString(l3);
        String e3 = "{[k,l],[m,n]}";
//System.out.println("l3="+s3);
        Assert.assertEquals(s3, e3);

        List<Member> l2 = makeListMember(m2);
        String s2 = toString(l2);
        String e2 = "{A,B,C,D}";
//System.out.println("l2="+s2);
        Assert.assertEquals(s2, e2);

        Iterable<Member[]> iter = calc.makeIterable(l3, l2);
        String s = toString(iter);
        String e = "{[k,l,A],[k,l,B],[k,l,C],[k,l,D],[m,n,A],[m,n,B],[m,n,C],[m,n,D]}";
        Assert.assertEquals(s, e);
//System.out.println("iter="+s);
    }
    ////////////////////////////////////////////////////////////////////////
    // Member[] Member[]
    ////////////////////////////////////////////////////////////////////////
    public void testIterMemberArrayIterMemberArrayIterCalc() {
if (! Util.Retrowoven) {
        CrossJoinFunDef.IterMemberArrayIterMemberArrayIterCalc calc =
            crossJoinFunDef.new IterMemberArrayIterMemberArrayIterCalc(
                getResolvedFunCall(), null);

//System.out.println("testIterMemberArrayIterMemberArrayIterCalc");
        doMemberArrayMemberArrayIterTest(calc);
}
    }
    public void testIterMemberArrayListMemberArrayIterCalc() {
if (! Util.Retrowoven) {
        CrossJoinFunDef.IterMemberArrayListMemberArrayIterCalc calc =
            crossJoinFunDef.new IterMemberArrayListMemberArrayIterCalc(
                getResolvedFunCall(), null);

//System.out.println("testIterMemberArrayListMemberArrayIterCalc");
        doMemberArrayMemberArrayIterTest(calc);
}
    }
    public void testListMemberArrayIterMemberArrayIterCalc() {
if (! Util.Retrowoven) {
        CrossJoinFunDef.ListMemberArrayIterMemberArrayIterCalc calc =
            crossJoinFunDef.new ListMemberArrayIterMemberArrayIterCalc(
                getResolvedFunCall(), null);

//System.out.println("testListMemberArrayIterMemberArrayIterCalc");
        doMemberArrayMemberArrayIterTest(calc);
}
    }
    public void testListMemberArrayListMemberArrayIterCalc() {
if (! Util.Retrowoven) {
        CrossJoinFunDef.ListMemberArrayListMemberArrayIterCalc calc =
            crossJoinFunDef.new ListMemberArrayListMemberArrayIterCalc(
                getResolvedFunCall(), null);

//System.out.println("testListMemberArrayListMemberArrayIterCalc");
        doMemberArrayMemberArrayIterTest(calc);
}
    }

    protected void doMemberArrayMemberArrayIterTest(
            CrossJoinFunDef.BaseMemberArrayMemberArrayIterCalc calc) {

        List<Member[]> l4 = makeListMemberArray(m4);
        String s4 = toString(l4);
        String e4 = "{[U,V],[W,X],[Y,Z]}";
//System.out.println("l4="+s4);
        Assert.assertEquals(s4, e4);

        List<Member[]> l3 = makeListMemberArray(m3);
        String s3 = toString(l3);
        String e3 = "{[k,l],[m,n]}";
//System.out.println("l3="+s3);
        Assert.assertEquals(s3, e3);

        Iterable<Member[]> iter = calc.makeIterable(l4, l3);
        String s = toString(iter);
        String e = "{[U,V,k,l],[U,V,m,n],[W,X,k,l],[W,X,m,n],[Y,Z,k,l],[Y,Z,m,n]}";
//System.out.println("iter="+s);
        Assert.assertEquals(s, e);
    }

    ////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////
    // Immutable List
    ////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////
    // Member Member
    ////////////////////////////////////////////////////////////////////////
    public void testImmutableListMemberListMemberListCalc() {
        CrossJoinFunDef.ImmutableListMemberListMemberListCalc calc =
            crossJoinFunDef.new ImmutableListMemberListMemberListCalc(
                getResolvedFunCall(), null);

//System.out.println("testImmutableListMemberListMemberListCalc");
        doMemberMemberListTest(calc);
    }

    protected void doMemberMemberListTest(
            CrossJoinFunDef.BaseListCalc calc) {

        List<Member> l1 = makeListMember(m1);
        String s1 = toString(l1);
        String e1 = "{a,b,c}";
//System.out.println("l1="+s1);
        Assert.assertEquals(s1, e1);

        List<Member> l2 = makeListMember(m2);
        String s2 = toString(l2);
        String e2 = "{A,B,C,D}";
//System.out.println("l2="+s2);
        Assert.assertEquals(s2, e2);

        List<Member[]> list = calc.makeList(l1, l2);
        String s = toString(list);
        String e = "{[a,A],[a,B],[a,C],[a,D],[b,A],[b,B],[b,C],[b,D],[c,A],[c,B],[c,C],[c,D]}";
//System.out.println("list="+s);
        Assert.assertEquals(s, e);

        List<Member[]> subList = list.subList(0, 12);
        s = toString(subList);
//System.out.println("list="+s);
        Assert.assertEquals(s, e);

        subList = list.subList(5, 12);
        s = toString(subList);
//System.out.println("list="+s);
        e = "{[b,B],[b,C],[b,D],[c,A],[c,B],[c,C],[c,D]}";
        Assert.assertEquals(s, e);

        subList = list.subList(0, 8);
//System.out.println("subList.size="+subList.size());
        Assert.assertEquals(8, subList.size());
        s = toString(subList);
//System.out.println("subList="+s);
        e = "{[a,A],[a,B],[a,C],[a,D],[b,A],[b,B],[b,C],[b,D]}";
//System.out.println("expect ="+e);
        Assert.assertEquals(s, e);

        subList = list.subList(0, 12);
//System.out.println("subList.size="+subList.size());
        Assert.assertEquals(12, subList.size());
        subList = subList.subList(4, 10);
        Assert.assertEquals(6, subList.size());
//System.out.println("subList.size="+subList.size());
        s = toString(subList);
//System.out.println("subList="+s);
        e = "{[b,A],[b,B],[b,C],[b,D],[c,A],[c,B]}";
//System.out.println("expect ="+e);
        Assert.assertEquals(s, e);

        subList = subList.subList(1, 4);
//System.out.println("subList.size="+subList.size());
        Assert.assertEquals(3, subList.size());
        s = toString(subList);
//System.out.println("subList="+s);
        e = "{[b,B],[b,C],[b,D]}";
//System.out.println("expect ="+e);
        Assert.assertEquals(s, e);

        subList = subList.subList(1, 2);
//System.out.println("subList.size="+subList.size());
        Assert.assertEquals(1, subList.size());
        s = toString(subList);
//System.out.println("subList="+s);
        e = "{[b,C]}";
//System.out.println("expect ="+e);
        Assert.assertEquals(s, e);

        subList = subList.subList(0, 1);
//System.out.println("subList.size="+subList.size());
        Assert.assertEquals(1, subList.size());
        s = toString(subList);
//System.out.println("subList="+s);
        e = "{[b,C]}";
//System.out.println("expect ="+e);
        Assert.assertEquals(s, e);

        subList = subList.subList(0, 0);
//System.out.println("subList.size="+subList.size());
        Assert.assertEquals(0, subList.size());
        s = toString(subList);
//System.out.println("subList="+s);
        e = "{}";
//System.out.println("expect ="+e);
        Assert.assertEquals(s, e);
    }
    ////////////////////////////////////////////////////////////////////////
    // Member Member[]
    ////////////////////////////////////////////////////////////////////////
    public void testImmutableListMemberListMemberArrayListCalc() {
        CrossJoinFunDef.ImmutableListMemberListMemberArrayListCalc calc =
            crossJoinFunDef.new ImmutableListMemberListMemberArrayListCalc(
                getResolvedFunCall(), null);

//System.out.println("testImmutableListMemberListMemberArrayListCalc");
        doMemberMemberArrayListTest(calc);
    }

    protected void doMemberMemberArrayListTest(
            CrossJoinFunDef.BaseListCalc calc) {

        List<Member> l1 = makeListMember(m1);
        String s1 = toString(l1);
        String e1 = "{a,b,c}";
//System.out.println("l1="+s1);
        Assert.assertEquals(s1, e1);

        List<Member[]> l3 = makeListMemberArray(m3);
        String s3 = toString(l3);
        String e3 = "{[k,l],[m,n]}";
//System.out.println("l3="+s3);
        Assert.assertEquals(s3, e3);

        List<Member[]> list = calc.makeList(l1, l3);
        String s = toString(list);
        String e = "{[a,k,l],[a,m,n],[b,k,l],[b,m,n],[c,k,l],[c,m,n]}";
//System.out.println("list="+s);
        Assert.assertEquals(s, e);

        List<Member[]> subList = list.subList(0, 6);
        s = toString(subList);
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(6, subList.size());
        Assert.assertEquals(s, e);

        subList = subList.subList(0, 6);
        s = toString(subList);
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(6, subList.size());
        Assert.assertEquals(s, e);

        subList = list.subList(1, 5);
        s = toString(subList);
        e = "{[a,m,n],[b,k,l],[b,m,n],[c,k,l]}";
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(4, subList.size());
        Assert.assertEquals(s, e);

        subList = subList.subList(1, 3);
        s = toString(subList);
        e = "{[b,k,l],[b,m,n]}";
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(2, subList.size());
        Assert.assertEquals(s, e);

        subList = subList.subList(1, 2);
        s = toString(subList);
        e = "{[b,m,n]}";
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(1, subList.size());
        Assert.assertEquals(s, e);

        subList = list.subList(4, 4);
        s = toString(subList);
        e = "{}";
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(0, subList.size());
        Assert.assertEquals(s, e);
    }

    ////////////////////////////////////////////////////////////////////////
    // Member[] Member
    ////////////////////////////////////////////////////////////////////////
    public void testImmutableListMemberArrayListMemberListCalc() {
        CrossJoinFunDef.ImmutableListMemberArrayListMemberListCalc calc =
            crossJoinFunDef.new ImmutableListMemberArrayListMemberListCalc(
                getResolvedFunCall(), null);

//System.out.println("testImmutableListMemberArrayListMemberListCalc");
        doMemberArrayMemberListTest(calc);
    }

    protected void doMemberArrayMemberListTest(
            CrossJoinFunDef.BaseListCalc calc) {

        List<Member[]> l3 = makeListMemberArray(m3);
        String s3 = toString(l3);
        String e3 = "{[k,l],[m,n]}";
//System.out.println("l3="+s3);
        Assert.assertEquals(s3, e3);

        List<Member> l2 = makeListMember(m2);
        String s2 = toString(l2);
        String e2 = "{A,B,C,D}";
//System.out.println("l2="+s2);
        Assert.assertEquals(s2, e2);

        List<Member[]> list = calc.makeList(l3, l2);
        String s = toString(list);
        String e = "{[k,l,A],[k,l,B],[k,l,C],[k,l,D],[m,n,A],[m,n,B],[m,n,C],[m,n,D]}";
//System.out.println("list="+s);
        Assert.assertEquals(s, e);

        List<Member[]> subList = list.subList(0, 8);
        s = toString(subList);
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(8, subList.size());
        Assert.assertEquals(s, e);

        subList = list.subList(1, 7);
        s = toString(subList);
        e = "{[k,l,B],[k,l,C],[k,l,D],[m,n,A],[m,n,B],[m,n,C]}";
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(6, subList.size());
        Assert.assertEquals(s, e);

        subList = list.subList(4, 5);
        s = toString(subList);
        e = "{[m,n,A]}";
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(1, subList.size());
        Assert.assertEquals(s, e);

        subList = subList.subList(0, 1);
        s = toString(subList);
        e = "{[m,n,A]}";
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(1, subList.size());
        Assert.assertEquals(s, e);

        subList = subList.subList(0, 0);
        s = toString(subList);
        e = "{}";
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(0, subList.size());
        Assert.assertEquals(s, e);

        subList = list.subList(0, 8);
        s = toString(subList);
        e = "{[k,l,A],[k,l,B],[k,l,C],[k,l,D],[m,n,A],[m,n,B],[m,n,C],[m,n,D]}";
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(8, subList.size());
        Assert.assertEquals(s, e);

        subList = subList.subList(0, 8);
        s = toString(subList);
        e = "{[k,l,A],[k,l,B],[k,l,C],[k,l,D],[m,n,A],[m,n,B],[m,n,C],[m,n,D]}";
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(8, subList.size());
        Assert.assertEquals(s, e);

        subList = subList.subList(1, 7);
        s = toString(subList);
        e = "{[k,l,B],[k,l,C],[k,l,D],[m,n,A],[m,n,B],[m,n,C]}";
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(6, subList.size());
        Assert.assertEquals(s, e);

        subList = subList.subList(1, 5);
        s = toString(subList);
        e = "{[k,l,C],[k,l,D],[m,n,A],[m,n,B]}";
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(4, subList.size());
        Assert.assertEquals(s, e);

        subList = subList.subList(1, 3);
        s = toString(subList);
        e = "{[k,l,D],[m,n,A]}";
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(2, subList.size());
        Assert.assertEquals(s, e);
    }

    ////////////////////////////////////////////////////////////////////////
    // Member[] Member[]
    ////////////////////////////////////////////////////////////////////////

    public void testImmutableListMemberArrayListMemberArrayListCalc() {
        CrossJoinFunDef.ImmutableListMemberArrayListMemberArrayListCalc calc =
            crossJoinFunDef.new ImmutableListMemberArrayListMemberArrayListCalc(
                getResolvedFunCall(), null);

//System.out.println("testImmutableListMemberArrayListMemberArrayIterCalc");
        doMemberArrayMemberArrayListTest(calc);
    }

    protected void doMemberArrayMemberArrayListTest(
            CrossJoinFunDef.BaseListCalc calc) {

        List<Member[]> l4 = makeListMemberArray(m4);
        String s4 = toString(l4);
        String e4 = "{[U,V],[W,X],[Y,Z]}";
//System.out.println("l4="+s4);
        Assert.assertEquals(s4, e4);

        List<Member[]> l3 = makeListMemberArray(m3);
        String s3 = toString(l3);
        String e3 = "{[k,l],[m,n]}";
//System.out.println("l3="+s3);
        Assert.assertEquals(s3, e3);

        List<Member[]> list = calc.makeList(l4, l3);
        String s = toString(list);
        String e = "{[U,V,k,l],[U,V,m,n],[W,X,k,l],[W,X,m,n],[Y,Z,k,l],[Y,Z,m,n]}";
//System.out.println("list="+s);
        Assert.assertEquals(s, e);

        List<Member[]> subList = list.subList(0, 6);
        s = toString(subList);
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(6, subList.size());
        Assert.assertEquals(s, e);

        subList = subList.subList(0, 6);
        s = toString(subList);
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(6, subList.size());
        Assert.assertEquals(s, e);

        subList = subList.subList(1, 5);
        s = toString(subList);
        e = "{[U,V,m,n],[W,X,k,l],[W,X,m,n],[Y,Z,k,l]}";
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(4, subList.size());
        Assert.assertEquals(s, e);

        subList = subList.subList(2, 4);
        s = toString(subList);
        e = "{[W,X,m,n],[Y,Z,k,l]}";
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(2, subList.size());
        Assert.assertEquals(s, e);

        subList = subList.subList(1, 2);
        s = toString(subList);
        e = "{[Y,Z,k,l]}";
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(1, subList.size());
        Assert.assertEquals(s, e);

        subList = list.subList(1, 4);
        s = toString(subList);
        e = "{[U,V,m,n],[W,X,k,l],[W,X,m,n]}";
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(3, subList.size());
        Assert.assertEquals(s, e);

        subList = list.subList(2, 4);
        s = toString(subList);
        e = "{[W,X,k,l],[W,X,m,n]}";
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(2, subList.size());
        Assert.assertEquals(s, e);

        subList = list.subList(2, 3);
        s = toString(subList);
        e = "{[W,X,k,l]}";
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(1, subList.size());
        Assert.assertEquals(s, e);

        subList = list.subList(4, 4);
        s = toString(subList);
        e = "{}";
//System.out.println("subList="+s);
//System.out.println("expect ="+e);
        Assert.assertEquals(0, subList.size());
        Assert.assertEquals(s, e);
    }



    ////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////
    // Mutable List
    ////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////
    // Member Member
    ////////////////////////////////////////////////////////////////////////
    public void testMutableListMemberListMemberListCalc() {
        CrossJoinFunDef.MutableListMemberListMemberListCalc calc =
            crossJoinFunDef.new MutableListMemberListMemberListCalc(
                getResolvedFunCall(), null);

//System.out.println("testMutableListMemberListMemberListCalc");
        doMMemberMemberListTest(calc);
    }

    protected void doMMemberMemberListTest(
            CrossJoinFunDef.BaseListCalc calc) {

        List<Member> l1 = makeListMember(m1);
        String s1 = toString(l1);
        String e1 = "{a,b,c}";
//System.out.println("l1="+s1);
        Assert.assertEquals(s1, e1);

        List<Member> l2 = makeListMember(m2);
        String s2 = toString(l2);
        String e2 = "{A,B,C,D}";
//System.out.println("l2="+s2);
        Assert.assertEquals(s2, e2);

        List<Member[]> list = calc.makeList(l1, l2);
        String s = toString(list);
        String e = "{[a,A],[a,B],[a,C],[a,D],[b,A],[b,B],[b,C],[b,D],[c,A],[c,B],[c,C],[c,D]}";
//System.out.println("list="+s);
        Assert.assertEquals(s, e);

        Collections.reverse(list);
        s = toString(list);
        e = "{[c,D],[c,C],[c,B],[c,A],[b,D],[b,C],[b,B],[b,A],[a,D],[a,C],[a,B],[a,A]}";
//System.out.println("list="+s);
        Assert.assertEquals(s, e);

        // sort
        Comparator<Member[]> c = new Comparator<Member[]>() {
            public int compare(Member[] ma1, Member[] ma2) {
                int c = ma1[0].compareTo(ma2[0]);
                if (c < 0) {
                    return c;
                } else if (c > 0) {
                    return c;
                } else {
                    return ma1[1].compareTo(ma2[1]);
                }
            }
        };
        Collections.sort(list, c);
        s = toString(list);
        e = "{[a,A],[a,B],[a,C],[a,D],[b,A],[b,B],[b,C],[b,D],[c,A],[c,B],[c,C],[c,D]}";
//System.out.println("list="+s);
        Assert.assertEquals(s, e);

        Member[] members = list.remove(1);
        String m = toString(members);
//System.out.println("members="+m);
        s = toString(list);
        e = "{[a,A],[a,C],[a,D],[b,A],[b,B],[b,C],[b,D],[c,A],[c,B],[c,C],[c,D]}";
//System.out.println("list="+s);
        Assert.assertEquals(s, e);
    }

    ////////////////////////////////////////////////////////////////////////
    // Member Member[]
    ////////////////////////////////////////////////////////////////////////
    public void testMutableListMemberListMemberArrayListCalc() {
        CrossJoinFunDef.MutableListMemberListMemberArrayListCalc calc =
            crossJoinFunDef.new MutableListMemberListMemberArrayListCalc(
                getResolvedFunCall(), null);

//System.out.println("testMutableListMemberListMemberArrayListCalc");
        doMMemberMemberArrayListTest(calc);
    }

    protected void doMMemberMemberArrayListTest(
            CrossJoinFunDef.BaseListCalc calc) {

        List<Member> l1 = makeListMember(m1);
        String s1 = toString(l1);
        String e1 = "{a,b,c}";
//System.out.println("l1="+s1);
        Assert.assertEquals(s1, e1);

        List<Member[]> l3 = makeListMemberArray(m3);
        String s3 = toString(l3);
        String e3 = "{[k,l],[m,n]}";
//System.out.println("l3="+s3);
        Assert.assertEquals(s3, e3);

        List<Member[]> list = calc.makeList(l1, l3);
        String s = toString(list);
        String e = "{[a,k,l],[a,m,n],[b,k,l],[b,m,n],[c,k,l],[c,m,n]}";
//System.out.println("list="+s);
        Assert.assertEquals(s, e);

        Collections.reverse(list);
        s = toString(list);
        e = "{[c,m,n],[c,k,l],[b,m,n],[b,k,l],[a,m,n],[a,k,l]}";
//System.out.println("list="+s);
        Assert.assertEquals(s, e);

        // sort
        Collections.sort(list, memberComparator);
        s = toString(list);
        e = "{[a,k,l],[a,m,n],[b,k,l],[b,m,n],[c,k,l],[c,m,n]}";
//System.out.println("list="+s);
        Assert.assertEquals(s, e);

        Member[] members = list.remove(1);
//        String m = toString(members);
//System.out.println("members="+m);
        s = toString(list);
        e = "{[a,k,l],[b,k,l],[b,m,n],[c,k,l],[c,m,n]}";
//System.out.println("list="+s);
        Assert.assertEquals(s, e);
    }

    ////////////////////////////////////////////////////////////////////////
    // Member[] Member
    ////////////////////////////////////////////////////////////////////////
    public void testMutableListMemberArrayListMemberListCalc() {
        CrossJoinFunDef.MutableListMemberArrayListMemberListCalc calc =
            crossJoinFunDef.new MutableListMemberArrayListMemberListCalc(
                getResolvedFunCall(), null);

//System.out.println("testMutableListMemberArrayListMemberListCalc");
        doMMemberArrayMemberListTest(calc);
    }

    protected void doMMemberArrayMemberListTest(
            CrossJoinFunDef.BaseListCalc calc) {

        List<Member[]> l1 = makeListMemberArray(m3);
        String s1 = toString(l1);
        String e1 = "{[k,l],[m,n]}";
//System.out.println("l1="+s1);
        Assert.assertEquals(s1, e1);

        List<Member> l2 = makeListMember(m1);
        String s2 = toString(l2);
        String e2 = "{a,b,c}";
//System.out.println("l2="+s2);
        Assert.assertEquals(s2, e2);


        List<Member[]> list = calc.makeList(l1, l2);
        String s = toString(list);
        String e = "{[k,l,a],[k,l,b],[k,l,c],[m,n,a],[m,n,b],[m,n,c]}";
//System.out.println("list="+s);
        Assert.assertEquals(s, e);

        Collections.reverse(list);
        s = toString(list);
        e = "{[m,n,c],[m,n,b],[m,n,a],[k,l,c],[k,l,b],[k,l,a]}";
//System.out.println("list="+s);
        Assert.assertEquals(s, e);

        // sort
        Collections.sort(list, memberComparator);
        s = toString(list);
        e = "{[k,l,a],[k,l,b],[k,l,c],[m,n,a],[m,n,b],[m,n,c]}";
//System.out.println("list="+s);
        Assert.assertEquals(s, e);

        Member[] members = list.remove(1);
//        String m = toString(members);
//System.out.println("members="+m);
        s = toString(list);
        e = "{[k,l,a],[k,l,c],[m,n,a],[m,n,b],[m,n,c]}";
//System.out.println("list="+s);
        Assert.assertEquals(s, e);
    }

    ////////////////////////////////////////////////////////////////////////
    // Member[] Member[]
    ////////////////////////////////////////////////////////////////////////

    public void testMutableListMemberArrayListMemberArrayListCalc() {
        CrossJoinFunDef.MutableListMemberArrayListMemberArrayListCalc calc =
            crossJoinFunDef.new MutableListMemberArrayListMemberArrayListCalc(
                getResolvedFunCall(), null);

//System.out.println("testMutableListMemberArrayListMemberArrayListCalc");
        doMMemberArrayMemberArrayListTest(calc);
    }

    protected void doMMemberArrayMemberArrayListTest(
            CrossJoinFunDef.BaseListCalc calc) {

        List<Member[]> l1 = makeListMemberArray(m3);
        String s1 = toString(l1);
        String e1 = "{[k,l],[m,n]}";
//System.out.println("l1="+s1);
        Assert.assertEquals(s1, e1);

        List<Member[]> l2 = makeListMemberArray(m4);
        String s2 = toString(l2);
        String e2 = "{[U,V],[W,X],[Y,Z]}";
//System.out.println("l2="+s2);
        Assert.assertEquals(s2, e2);


        List<Member[]> list = calc.makeList(l1, l2);
        String s = toString(list);
        String e = "{[k,l,U,V],[k,l,W,X],[k,l,Y,Z],[m,n,U,V],[m,n,W,X],[m,n,Y,Z]}";
//System.out.println("list="+s);
        Assert.assertEquals(s, e);

        Collections.reverse(list);
        s = toString(list);
        e = "{[m,n,Y,Z],[m,n,W,X],[m,n,U,V],[k,l,Y,Z],[k,l,W,X],[k,l,U,V]}";
//System.out.println("list="+s);
        Assert.assertEquals(s, e);

        // sort
        Collections.sort(list, memberComparator);
        s = toString(list);
        e = "{[k,l,U,V],[k,l,W,X],[k,l,Y,Z],[m,n,U,V],[m,n,W,X],[m,n,Y,Z]}";
//System.out.println("list="+s);
        Assert.assertEquals(s, e);

        Member[] members = list.remove(1);
//        String m = toString(members);
//System.out.println("members="+m);
        s = toString(list);
        e = "{[k,l,U,V],[k,l,Y,Z],[m,n,U,V],[m,n,W,X],[m,n,Y,Z]}";
//System.out.println("list="+s);
        Assert.assertEquals(s, e);
    }
    ////////////////////////////////////////////////////////////////////////
    // Helper methods
    ////////////////////////////////////////////////////////////////////////
    protected String toString(Iterable l) {
        StringBuffer buf = new StringBuffer(100);
        buf.append('{');
        int j = 0;
        for (Object o: l) {
            if (j++ > 0) {
                buf.append(',');
            }
            if (o instanceof Member) {
                buf.append(o);
            } else {
                Member[] members = (Member[]) o;
                buf.append('[');
                int k = 0;
                for (Member m : members) {
                    if (k++ > 0) {
                        buf.append(',');
                    }
                    buf.append(m);
                }
                buf.append(']');
            }
        }
        buf.append('}');
        return buf.toString();
    }
    protected String toString(Member[] members) {
        StringBuffer buf = new StringBuffer(100);
        buf.append('[');
        boolean firstTimeInner = true;
        for (Member m: members) {
            if (! firstTimeInner) {
                buf.append(',');
            }
            buf.append(m);
            firstTimeInner = false;
        }
        buf.append(']');
        return buf.toString();
    }
    protected List<Member> makeListMember(Member[] ms) {
        return new ArrayList<Member>(Arrays.asList(ms));
    }
    protected List<Member[]> makeListMemberArray(Member[][] ms) {
        return new ArrayList<Member[]>(Arrays.asList(ms));
    }
    protected ResolvedFunCall getResolvedFunCall() {
        FunDef funDef = new TestFunDef();
        Exp[] args = new Exp[0];
        Type returnType =
            new SetType(new MemberType(null, null, null, null));
        ResolvedFunCall call = new ResolvedFunCall(funDef, args, returnType);
        return call;
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
        NullFunDef() {
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

    public static class TestExp implements Exp {
        Type type;
        TestExp() {
            this.type = new SetType(new MemberType(null, null, null, null));
        }
        public Exp clone() {
            throw new UnsupportedOperationException();
        }
        public int getCategory() {
            throw new UnsupportedOperationException();
        }
        public Type getType() {
            return type;
        }
        public void unparse(PrintWriter pw) {
            throw new UnsupportedOperationException();
        }
        public Exp accept(Validator validator) {
            throw new UnsupportedOperationException();
        }
        public Calc accept(ExpCompiler compiler) {
            throw new UnsupportedOperationException();
        }
        public Object accept(MdxVisitor visitor) {
            throw new UnsupportedOperationException();
        }
    }

}

// End CrossJoinTest.java

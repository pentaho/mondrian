/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import junit.framework.Assert;
import mondrian.test.FoodMartTestCase;
import mondrian.olap.fun.TestMember;
import mondrian.olap.Member;
import mondrian.olap.Position;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

public class RolapAxisTest extends FoodMartTestCase {
    public RolapAxisTest() {
        super();
    }
    public RolapAxisTest(String name) {
        super(name);
    }
    public void testMemberList() {
        List<Member> list = new ArrayList<Member>();
        list.add(new TestMember("a"));
        list.add(new TestMember("b"));
        list.add(new TestMember("c"));
        list.add(new TestMember("d"));

        StringBuffer buf = new StringBuffer(100);

        RolapAxis axis = new RolapAxis.MemberList(list);
        List<Position> positions = axis.getPositions();
        boolean firstTimeInner = true;
        for (Position position : positions) {
            if (! firstTimeInner) {
                buf.append(',');
            }
            buf.append(toString(position));
            firstTimeInner = false;
        }
        String s = buf.toString();
        String e = "{a},{b},{c},{d}";
//System.out.println("s=" +s);
        Assert.assertEquals(s, e);

        positions = axis.getPositions();
        int size = positions.size();
//System.out.println("size=" +size);
        Assert.assertEquals(size, 4);

        buf.setLength(0);
        for (int i = 0; i < size; i++) {
            Position position = positions.get(i);
            if (i > 0) {
                buf.append(',');
            }
            buf.append(toString(position));
        }
        s = buf.toString();
        e = "{a},{b},{c},{d}";
//System.out.println("s=" +s);
        Assert.assertEquals(s, e);
    }
    public void testMemberArrayList() {
        List<Member[]> list = new ArrayList<Member[]>();
        list.add(
            new Member[] {
                new TestMember("a"),
                new TestMember("b"),
                new TestMember("c")
            });
        list.add(
            new Member[] {
                new TestMember("d"),
                new TestMember("e"),
                new TestMember("f")
            });
        list.add(
            new Member[] {
                new TestMember("g"),
                new TestMember("h"),
                new TestMember("i")
            });

        StringBuffer buf = new StringBuffer(100);

        RolapAxis axis = new RolapAxis.MemberArrayList(list);
        List<Position> positions = axis.getPositions();
        boolean firstTimeInner = true;
        for (Position position : positions) {
            if (! firstTimeInner) {
                buf.append(',');
            }
            buf.append(toString(position));
            firstTimeInner = false;
        }
        String s = buf.toString();
        String e = "{a,b,c},{d,e,f},{g,h,i}";
//System.out.println("s=" +s);
        Assert.assertEquals(s, e);

        positions = axis.getPositions();
        int size = positions.size();
//System.out.println("size=" +size);
        Assert.assertEquals(size, 3);

        buf.setLength(0);
        for (int i = 0; i < size; i++) {
            Position position = positions.get(i);
            if (i > 0) {
                buf.append(',');
            }
            buf.append(toString(position));
        }
        s = buf.toString();
        e = "{a,b,c},{d,e,f},{g,h,i}";
//System.out.println("s=" +s);
        Assert.assertEquals(s, e);
    }

    public void testMemberIterable() {
        List<Member> list = new ArrayList<Member>();
        list.add(new TestMember("a"));
        list.add(new TestMember("b"));
        list.add(new TestMember("c"));
        list.add(new TestMember("d"));

        StringBuffer buf = new StringBuffer(100);

        RolapAxis axis = new RolapAxis.MemberIterable(list);
        List<Position> positions = axis.getPositions();
        boolean firstTimeInner = true;
        for (Position position : positions) {
            if (! firstTimeInner) {
                buf.append(',');
            }
            buf.append(toString(position));
            firstTimeInner = false;
        }
        String s = buf.toString();
        String e = "{a},{b},{c},{d}";
//System.out.println("s=" +s);
        Assert.assertEquals(s, e);

        positions = axis.getPositions();
        int size = positions.size();
//System.out.println("size=" +size);
        Assert.assertEquals(size, 4);

        buf.setLength(0);
        for (int i = 0; i < size; i++) {
            Position position = positions.get(i);
            if (i > 0) {
                buf.append(',');
            }
            buf.append(toString(position));
        }
        s = buf.toString();
        e = "{a},{b},{c},{d}";
//System.out.println("s=" +s);
        Assert.assertEquals(s, e);
    }
    public void testMemberArrayIterable() {
        List<Member[]> list = new ArrayList<Member[]>();
        list.add(
            new Member[] {
                new TestMember("a"),
                new TestMember("b"),
                new TestMember("c")
            });
        list.add(
            new Member[] {
                new TestMember("d"),
                new TestMember("e"),
                new TestMember("f")
            });
        list.add(
            new Member[] {
                new TestMember("g"),
                new TestMember("h"),
                new TestMember("i")
            });

        StringBuffer buf = new StringBuffer(100);

        RolapAxis axis = new RolapAxis.MemberArrayIterable(list);
        List<Position> positions = axis.getPositions();
        boolean firstTimeInner = true;
        for (Position position : positions) {
            if (! firstTimeInner) {
                buf.append(',');
            }
            buf.append(toString(position));
            firstTimeInner = false;
        }
        String s = buf.toString();
        String e = "{a,b,c},{d,e,f},{g,h,i}";
//System.out.println("s=" +s);
        Assert.assertEquals(s, e);

        positions = axis.getPositions();
        int size = positions.size();
//System.out.println("size=" +size);
        Assert.assertEquals(size, 3);

        buf.setLength(0);
        for (int i = 0; i < size; i++) {
            Position position = positions.get(i);
            if (i > 0) {
                buf.append(',');
            }
            buf.append(toString(position));
        }
        s = buf.toString();
        e = "{a,b,c},{d,e,f},{g,h,i}";
//System.out.println("s=" +s);
        Assert.assertEquals(s, e);
    }

    protected String toString(List<Member> position) {
        StringBuffer buf = new StringBuffer(100);
        buf.append('{');
        boolean firstTimeInner = true;
        for (Member m : position) {
            if (! firstTimeInner) {
                buf.append(',');
            }
            buf.append(m);
            firstTimeInner = false;
        }
        buf.append('}');
        return buf.toString();
    }
}

// End RolapAxisTest.java

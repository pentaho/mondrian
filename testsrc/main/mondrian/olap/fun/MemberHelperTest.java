/*
 * Copyright 2003 by Alphablox Corp. All rights reserved.
 *
 * Created by gjohnson
 * Last change: $Modtime: $
 * Last author: $Author$
 * Revision: $Revision$
 */

package mondrian.olap.fun;

import java.io.PrintWriter;

import junit.framework.TestCase;
import mondrian.olap.*;

public class MemberHelperTest extends TestCase {
    public MemberHelperTest(String name) {
        super(name);
    }

    public void testEqualsMembers() {
        MemberHelper mh0 = new MemberHelper(null);
        assertFalse(mh0.equals(null));

        assertEquals(mh0, mh0);

        assertFalse(mh0.equals(""));

        MemberHelper mh1 = new MemberHelper(null);
        assertEquals(mh0, mh1);

        mh0 = new MemberHelper(new TestMember("foo"));
        assertFalse(mh0.equals(mh1));

        mh1 = new MemberHelper(new TestMember("baz"));
        MemberHelper mh2 = new MemberHelper(new TestMember("foo"));

        assertEquals(mh0, mh2);
        assertFalse(mh1.equals(mh2));

    }


    public void testEqualsMemberArray() {
        Member[] a1 = new Member[]{
            new TestMember("blah"),
            new TestMember("foo"),
            new TestMember("bar"),
        };
        Member[] a2 = new Member[]{
            new TestMember("blah"),
            new TestMember("foo"),
            new TestMember("bar"),
        };
        Member[] a3 = new Member[]{
            new TestMember("blah"),
            new TestMember("bar"),
        };

        MemberHelper mh1 = new MemberHelper(a1);
        MemberHelper mh2 = new MemberHelper(a2);
        MemberHelper mh3 = new MemberHelper(a3);

        assertEquals(mh1, mh2);
        assertFalse(mh1.equals(mh3));
        assertFalse(mh3.equals(mh1));

    }

    public void testConstructor() {
        try {
            new MemberHelper("blah");
            fail("Should not be able to create a MemberHelper with a string");
        }
        catch(IllegalArgumentException iae) {
        }
    }

    static class TestMember implements Member {
        String name;

        public TestMember(String name) {
            this.name = name;
        }

        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TestMember)) {
                return false;
            }

            final TestMember testMember = (TestMember) o;

            return name.equals(testMember.name);
        }

        public int hashCode() {
            return name.hashCode();
        }

        public Member[] getAncestorMembers() {
            return new Member[0];  //To change body of implemented methods use File | Settings | File Templates.
        }

        public String getCaption() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public Hierarchy getHierarchy() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public Level getLevel() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public int getMemberType() {
            return 0;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public int getOrdinal() {
            return 0;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public Member getParentMember() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public String getParentUniqueName() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public Property[] getProperties() {
            return new Property[0];  //To change body of implemented methods use File | Settings | File Templates.
        }

        public Object getPropertyValue(String propertyName) {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public boolean isAll() {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public boolean isCalculated() {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public boolean isCalculatedInQuery() {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public boolean isChildOrEqualTo(Member member) {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public boolean isMeasure() {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public boolean isNull() {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public void setName(String name) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        public void setProperty(String name, Object value) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        public void accept(Visitor visitor) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        public void childrenAccept(Visitor visitor) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        public String getDescription() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public String getName() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public String getQualifiedName() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public String getUniqueName() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public OlapElement lookupChild(SchemaReader schemaReader, String s) {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public void unparse(PrintWriter pw) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        public int addAtPosition(Exp e, int iPosition) {
            return 0;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public Object clone() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public boolean dependsOn(Dimension dimension) {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public Object evaluate(Evaluator evaluator) {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public Object evaluateScalar(Evaluator evaluator) {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public Dimension getDimension() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public int getType() {
            return 0;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public boolean isElement() {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public boolean isEmptySet() {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public boolean isMember() {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public boolean isSet() {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public Exp resolve(Exp.Resolver resolver) {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public boolean usesDimension(Dimension dimension) {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public int compareTo(Object o) {
            return 0;  //To change body of implemented methods use File | Settings | File Templates.
        }
    }
}

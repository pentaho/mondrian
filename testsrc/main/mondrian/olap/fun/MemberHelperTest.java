/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2004-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.olap.fun;

import junit.framework.TestCase;
import mondrian.olap.*;
import mondrian.olap.Validator;
import mondrian.olap.type.Type;

import java.io.PrintWriter;

/**
 * <code>MemberHelperTest</code> tests {@link MemberHelper}.
 *
 * @author gjohnson
 * @version $Id$
 */
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
            return new Member[0];
        }

        public String getCaption() {
            return null;
        }

        public Hierarchy getHierarchy() {
            return null;
        }

        public Level getLevel() {
            return null;
        }

        public int getMemberType() {
            return 0;
        }

        public int getOrdinal() {
            return 0;
        }

        public Member getParentMember() {
            return null;
        }

        public String getParentUniqueName() {
            return null;
        }

        public Property[] getProperties() {
            return new Property[0];
        }

        public Object getPropertyValue(String propertyName) {
            return null;
        }

        public boolean isAll() {
            return false;
        }

        public boolean isCalculated() {
            return false;
        }

        public boolean isCalculatedInQuery() {
            return false;
        }

        public boolean isChildOrEqualTo(Member member) {
            return false;
        }

        public boolean isMeasure() {
            return false;
        }

        public boolean isNull() {
            return false;
        }

        public void setName(String name) {

        }

        public void setProperty(String name, Object value) {

        }

        public void accept(Visitor visitor) {

        }

        public void childrenAccept(Visitor visitor) {

        }

        public String getDescription() {
            return null;
        }

        public String getName() {
            return null;
        }

        public String getQualifiedName() {
            return null;
        }

        public String getUniqueName() {
            return null;
        }

        public OlapElement lookupChild(SchemaReader schemaReader, String s) {
            return null;
        }

        public void unparse(PrintWriter pw) {

        }

        public int addAtPosition(Exp e, int iPosition) {
            return 0;
        }

        public Object clone() {
            return null;
        }

        public boolean dependsOn(Dimension dimension) {
            return false;
        }

        public Object evaluate(Evaluator evaluator) {
            return null;
        }

        public Object evaluateScalar(Evaluator evaluator) {
            return null;
        }

        public Dimension getDimension() {
            return null;
        }

        public int getType() {
            return getCategory();
        }

        public int getCategory() {
            return Category.Unknown;
        }

        public Type getTypeX() {
            throw new UnsupportedOperationException();
        }

        public boolean isElement() {
            return false;
        }

        public boolean isEmptySet() {
            return false;
        }

        public boolean isMember() {
            return false;
        }

        public boolean isSet() {
            return false;
        }

        public Exp accept(Validator validator) {
            return null;
        }

        public int compareTo(Object o) {
            return 0;
        }

        public boolean isHidden() {
            return false;
        }

        public int getDepth() {
            return 0;
        }

        public String getPropertyFormattedValue(String propertyName) {
            return "";
        }

        public Member getDataMember() {
            return null;
        }
        public Exp getExpression() {
            return null;
        }
        public int getSolveOrder() {
            return -1;
        }

    }
}

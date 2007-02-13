/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
/**
 *
 *
 * @author <a>Richard M. Emberson</a>
 * @version $Id$
 */
public class TestMember implements Member {
    private final String identifer;
    public TestMember(String identifer) {
        this.identifer = identifer;
    }
    public String toString() {
        return identifer;
    }
    public int compareTo(Object o) {
        TestMember other = (TestMember) o;
        return this.identifer.compareTo(other.identifer);
    }
    public boolean equals(Object o) {
        return (this == o);
    }
    public int hashCode() {
        return super.hashCode();
    }

    public Member getParentMember() {
        throw new UnsupportedOperationException();
    }

    public Level getLevel() {
        throw new UnsupportedOperationException();
    }

    public Hierarchy getHierarchy() {
        throw new UnsupportedOperationException();
    }

    public String getParentUniqueName() {
        throw new UnsupportedOperationException();
    }

    public MemberType getMemberType() {
        throw new UnsupportedOperationException();
    }
    public void setName(String name) {
        throw new UnsupportedOperationException();
    }

    public boolean isAll() {
        return false;
    }

    public boolean isMeasure() {
        throw new UnsupportedOperationException();
    }

    public boolean isNull() {
        return true;
    }

    public boolean isChildOrEqualTo(Member member) {
        throw new UnsupportedOperationException();
    }

    public boolean isCalculated() {
        throw new UnsupportedOperationException();
    }
    public int getSolveOrder() {
        throw new UnsupportedOperationException();
    }

    public Exp getExpression() {
        throw new UnsupportedOperationException();
    }

    public Member[] getAncestorMembers() {
        throw new UnsupportedOperationException();
    }

    public boolean isCalculatedInQuery() {
        throw new UnsupportedOperationException();
    }

    public Object getPropertyValue(String propertyName) {
        throw new UnsupportedOperationException();
    }

    public Object getPropertyValue(String propertyName, boolean matchCase) {
        throw new UnsupportedOperationException();
    }

    public String getPropertyFormattedValue(String propertyName) {
        throw new UnsupportedOperationException();
    }

    public void setProperty(String name, Object value) {
        throw new UnsupportedOperationException();
    }

    public Property[] getProperties() {
        throw new UnsupportedOperationException();
    }

    public int getOrdinal() {
        throw new UnsupportedOperationException();
    }

    public boolean isHidden() {
        throw new UnsupportedOperationException();
    }

    public int getDepth() {
        throw new UnsupportedOperationException();
    }

    public Member getDataMember() {
        throw new UnsupportedOperationException();
    }

    public String getUniqueName() {
        throw new UnsupportedOperationException();
    }

    public String getName() {
        throw new UnsupportedOperationException();
    }

    public String getDescription() {
        throw new UnsupportedOperationException();
    }

    public OlapElement lookupChild(SchemaReader schemaReader, String s) {
        throw new UnsupportedOperationException();
    }

    public OlapElement lookupChild(
        SchemaReader schemaReader, String s, MatchType matchType) {
        throw new UnsupportedOperationException();
    }

    public String getQualifiedName() {
        throw new UnsupportedOperationException();
    }

    public String getCaption() {
        throw new UnsupportedOperationException();
    }

    public Dimension getDimension() {
        throw new UnsupportedOperationException();
    }
}

/*
//$Id$
//This software is subject to the terms of the Common Public License
//Agreement, available at the following URL:
//http://www.opensource.org/licenses/cpl.html.
//Copyright (C) 2004-2005 TONBELLER AG
//All Rights Reserved.
//You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.rolap.RolapMember;

/**
 * A <code>ColumnConstraint</code> is an Object to constraining a
 * column (WHERE-Clause) when a segment is loaded.
 */
public class ColumnConstraint {

    private Object value;
    private RolapMember member = null;

    public ColumnConstraint(Object o) {
        if ( o instanceof RolapMember ) {
            member = (RolapMember) o;
            value = member.getSqlKey();
        } else {
            value = o;
        }
    }

    public Object getValue() {
        return value;
    }

    public RolapMember getMember() {
        return member;
    }

    public boolean isMember() {
        return (member != null);
    }

    public boolean equals(Object other){
        if (!(other instanceof ColumnConstraint))
            return false;
        if (member!= null)
            return (member.equals(((ColumnConstraint)other).getMember()));
        if (value != null)
            return (value.equals(((ColumnConstraint)other).getValue()));
        return (null == ((ColumnConstraint)other).getValue());
    }

    public int hashCode() {
        if (member!= null)
            return member.hashCode();
        if (value != null)
            return value.hashCode();
        return 0;
    }

}

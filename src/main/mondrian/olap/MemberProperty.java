/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2000-2002 Kana Software, Inc.
// Copyright (C) 2001-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 1 March, 2000
*/

package mondrian.olap;
import java.io.PrintWriter;

/**
 * Member property or solve order specification.
 */
public class MemberProperty extends QueryPart {

    private final String name;
    private Exp exp;

    public MemberProperty(String name, Exp exp) {
        this.name = name;
        this.exp = exp;
    }

    protected Object clone() {
        return new MemberProperty(name, (Exp) exp.clone());
    }

    static MemberProperty[] cloneArray(MemberProperty[] x) {
        MemberProperty[] x2 = new MemberProperty[x.length];
        for (int i = 0; i < x.length; i++) {
            x2[i] = (MemberProperty) x[i].clone();
        }
        return x2;
    }

    void resolve(Validator validator) {
        exp = validator.validate(exp, false);
    }

    public Object[] getChildren() {
        return new Exp[] {exp};
    }

    public void replaceChild(int ordinal, QueryPart with) {
        Util.assertTrue(ordinal == 0);
        exp = (Exp) with;
    }

    public void unparse(PrintWriter pw) {
        pw.print(name + " = ");
        exp.unparse(pw);
    }

    /**
     * Retrieves a property by name from an array.
     */
    static Exp get(MemberProperty[] a, String name) {
        // TODO: Linear search may be a performance problem.
        for (int i = 0; i < a.length; i++) {
            if (Util.equalName(a[i].name, name)) {
                return a[i].exp;
            }
        }
        return null;
    }
}


// End MemberProperty.java

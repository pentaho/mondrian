/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2000-2005 Julian Hyde
// Copyright (C) 2005-2006 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

import java.io.PrintWriter;

/**
 * Member property or solve order specification.
 *
 * @author jhyde, 1 March, 2000
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

    public Exp getExp() {
        return exp;
    }

    public String getName() {
        return name;
    }

    public Object[] getChildren() {
        return new Exp[] {exp};
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

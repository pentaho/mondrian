/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

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

/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package mondrian.calc;

import mondrian.mdx.MdxVisitor;
import mondrian.olap.Exp;
import mondrian.olap.Validator;
import mondrian.olap.type.Type;

import java.io.PrintWriter;

/**
 * Dummy expression which exists only to wrap a
 * {@link mondrian.olap.type.Type}.
 *
 * @author jhyde
 * @since Sep 26, 2005
 */
public class DummyExp implements Exp {
    private final Type type;

    public DummyExp(Type type) {
        this.type = type;
    }

    public DummyExp clone() {
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

// End DummyExp.java

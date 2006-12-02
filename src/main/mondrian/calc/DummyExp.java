/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc;

import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.mdx.MdxVisitor;

import java.io.PrintWriter;

/**
 * Dummy expression which exists only to wrap a
 * {@link mondrian.olap.type.Type}.
 *
 * @author jhyde
 * @version $Id$
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

/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 7 August, 2001
*/

package mondrian.olap;

/**
 * <code>Visitor</code> is the Element role in the Visitor pattern.  See
 * also {@link OlapElement#accept}.
 *
 * @author jhyde
 * @since 7 August, 2001
 * @version $Id$
 **/
public abstract class Visitor {
    public boolean returnMeasures;

    public void visit(OlapElement element) {
        element.accept(this);
    }
    public abstract void visit(Cube cube);
    public abstract void visit(Dimension dimension);
    public abstract void visit(Hierarchy hierarchy);
    public abstract void visit(Level level);
    public abstract void visit(Member member);
}


// End Visitor.java

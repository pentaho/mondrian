/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.mdx;

import mondrian.olap.*;

/**
 * Default implementation of the visitor interface, {@link MdxVisitor}.
 *
 * <p>The method implementations just ask the child nodes to
 * {@link Exp#accept(MdxVisitor)} this visitor.
 *
 * @author jhyde
 * @version $Id$
 * @since Jul 21, 2006
 */
public class MdxVisitorImpl implements MdxVisitor {
    public Object visit(Query query) {
        return null;
    }

    public Object visit(QueryAxis queryAxis) {
        return null;
    }

    public Object visit(Formula formula) {
        return null;
    }

    public Object visit(UnresolvedFunCall call) {
        return null;
    }

    public Object visit(ResolvedFunCall call) {
        return null;
    }

    public Object visit(Id id) {
        return null;
    }

    public Object visit(ParameterExpr parameterExpr) {
        return null;
    }

    public Object visit(DimensionExpr dimensionExpr) {
        // do nothing
        return null;
    }

    public Object visit(HierarchyExpr hierarchyExpr) {
        // do nothing
        return null;
    }

    public Object visit(LevelExpr levelExpr) {
        // do nothing
        return null;
    }

    public Object visit(MemberExpr memberExpr) {
        // do nothing
        return null;
    }

    public Object visit(NamedSetExpr namedSetExpr) {
        // do nothing
        return null;
    }

    public Object visit(Literal literal) {
        // do nothing
        return null;
    }
}

// End MdxVisitorImpl.java

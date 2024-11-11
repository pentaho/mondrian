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


package mondrian.mdx;

import mondrian.olap.*;

/**
 * Default implementation of the visitor interface, {@link MdxVisitor}.
 *
 * <p>The method implementations just ask the child nodes to
 * {@link Exp#accept(MdxVisitor)} this visitor.
 *
 * @author jhyde
 * @since Jul 21, 2006
 */
public class MdxVisitorImpl implements MdxVisitor {
    private boolean shouldVisitChildren = true;

    public boolean shouldVisitChildren() {
        boolean returnValue = shouldVisitChildren;
        turnOnVisitChildren();
        return returnValue;
    }

    public void turnOnVisitChildren() {
        shouldVisitChildren = true;
    }

    public void turnOffVisitChildren() {
        shouldVisitChildren = false;
    }

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

    /**
     * Visits an array of expressions. Returns the same array if none of the
     * expressions are changed, otherwise a new array.
     *
     * @param args Array of expressions
     * @return Array of visited expressions; same as {@code args} iff none of
     * the expressions are changed.
     */
    protected Exp[] visitArray(Exp[] args) {
        Exp[] newArgs = args;
        for (int i = 0; i < args.length; i++) {
            Exp arg = args[i];
            Exp newArg = (Exp) arg.accept(this);
            if (newArg != arg) {
                if (newArgs == args) {
                    newArgs = args.clone();
                }
                newArgs[i] = newArg;
            }
        }
        return newArgs;
    }
}

// End MdxVisitorImpl.java

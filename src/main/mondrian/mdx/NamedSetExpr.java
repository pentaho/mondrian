/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.mdx;

import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;

import java.util.List;

/**
 * Usage of a {@link mondrian.olap.NamedSet} in an MDX expression.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 26, 2005
 */
public class NamedSetExpr extends ExpBase implements Exp {
    private final NamedSet namedSet;

    /**
     * Creates a usage of a named set.
     *
     * @param namedSet namedSet
     * @pre NamedSet != null
     */
    public NamedSetExpr(NamedSet namedSet) {
        Util.assertPrecondition(namedSet != null, "namedSet != null");
        this.namedSet = namedSet;
    }

    /**
     * Returns the named set.
     *
     * @post return != null
     */
    public NamedSet getNamedSet() {
        return namedSet;
    }

    public String toString() {
        return namedSet.getUniqueName();
    }

    public Object clone() {
        return new NamedSetExpr(namedSet);
    }

    public int getCategory() {
        return Category.Set;
    }

    public Exp accept(Validator validator) {
        // A set is sometimes used in more than one cube. So, clone the
        // expression and re-validate every time it is used.
        //
        // But keep the expression wrapped in a NamedSet, so that the
        // expression is evaluated once per query. (We don't want the
        // expression to be evaluated context-sensitive.)
        NamedSet namedSet2 = namedSet.validate(validator);
        if (namedSet2 == namedSet) {
            return this;
        }
        return new NamedSetExpr(namedSet2);
    }

    public Calc accept(ExpCompiler compiler) {
        return new AbstractListCalc(this, new Calc[] { /* todo: compile namedSet.getExp() */ }) {
            public List evaluateList(Evaluator evaluator) {
                return (List) evaluator.evaluateNamedSet(
                        namedSet.getName(), namedSet.getExp());
            }

            public boolean dependsOn(Dimension dimension) {
                return true;
                // TODO: namedSet.getExp().dependsOn(dimension);
            }
        };
    }

    public Object evaluate(Evaluator evaluator) {
        Util.deprecated("remove Exp.evaluate");
        final String name = namedSet.getName();
        final Exp exp = namedSet.getExp();
        return evaluator.evaluateNamedSet(name, exp);
    }

    public Type getType() {
        return namedSet.getType();
    }
}

// End NamedSetExpr.java

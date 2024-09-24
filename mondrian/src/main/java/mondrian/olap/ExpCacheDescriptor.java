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

import mondrian.calc.*;
import mondrian.calc.impl.BetterExpCompiler;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds information necessary to add an expression to the expression result
 * cache (see {@link Evaluator#getCachedResult(ExpCacheDescriptor)}).
 *
 * @author jhyde
 * @since Aug 16, 2005
 */
public class ExpCacheDescriptor {
    private final Exp exp;
    private int[] dependentHierarchyOrdinals;
    private final Calc calc;

    /**
     * Creates a descriptor with a given compiled expression.
     *
     * @param exp Expression
     * @param calc Compiled expression
     * @param evaluator Evaluator
     */
    public ExpCacheDescriptor(Exp exp, Calc calc, Evaluator evaluator) {
        this.calc = calc;
        this.exp = exp;
        computeDepends(calc, evaluator);
    }

    /**
     * Creates a descriptor.
     *
     * @param exp Expression
     * @param evaluator Evaluator
     */
    public ExpCacheDescriptor(Exp exp, Evaluator evaluator) {
        this(exp, new BetterExpCompiler(evaluator, null));
    }

    /**
     * Creates a descriptor.
     *
     * @param exp Expression
     * @param compiler Compiler
     */
    public ExpCacheDescriptor(Exp exp, ExpCompiler compiler) {
        this.exp = exp;

        // Compile expression.
        Calc calc = compiler.compile(exp);
        if (calc == null) {
            // now allow conversions
            calc = compiler.compileAs(exp, null, ResultStyle.ANY_ONLY);
        }
        this.calc = calc;

        // Compute list of dependent dimensions.
        computeDepends(calc, compiler.getEvaluator());
    }

    private void computeDepends(Calc calc, Evaluator evaluator) {
        final List<Integer> ordinalList = new ArrayList<Integer>();
        final Member[] members = evaluator.getMembers();
        for (int i = 0; i < members.length; i++) {
            Hierarchy hierarchy = members[i].getHierarchy();
            if (calc.dependsOn(hierarchy)) {
                ordinalList.add(i);
            }
        }
        dependentHierarchyOrdinals = new int[ordinalList.size()];
        for (int i = 0; i < dependentHierarchyOrdinals.length; i++) {
            dependentHierarchyOrdinals[i] = ordinalList.get(i);
        }
    }

    public Exp getExp() {
        return exp;
    }

    public Calc getCalc() {
        return calc;
    }

    public Object evaluate(Evaluator evaluator) {
        return calc.evaluate(evaluator);
    }

    /**
     * Returns the ordinals of the hierarchies which this expression is
     * dependent upon. When the cache descriptor is used to generate a cache
     * key, the key will consist of a member from each of these hierarchies.
     */
    public int[] getDependentHierarchyOrdinals() {
        return dependentHierarchyOrdinals;
    }

}

// End ExpCacheDescriptor.java

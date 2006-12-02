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
import mondrian.olap.type.DimensionType;
import mondrian.olap.type.Type;
import mondrian.calc.*;
import mondrian.calc.impl.ConstantCalc;

/**
 * Usage of a {@link mondrian.olap.Dimension} as an MDX expression.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 26, 2005
 */
public class DimensionExpr extends ExpBase implements Exp {
    private final Dimension dimension;

    /**
     * Creates a dimension expression.
     *
     * @param dimension Dimension
     * @pre dimension != null
     */
    public DimensionExpr(Dimension dimension) {
        Util.assertPrecondition(dimension != null, "dimension != null");
        this.dimension = dimension;
    }

    /**
     * Returns the dimension.
     *
     * @post return != null
     */
    public Dimension getDimension() {
        return dimension;
    }

    public String toString() {
        return dimension.getUniqueName();
    }

    public Type getType() {
        return DimensionType.forDimension(dimension);
    }

    public DimensionExpr clone() {
        return new DimensionExpr(dimension);
    }

    public int getCategory() {
        return Category.Dimension;
    }

    public Exp accept(Validator validator) {
        return this;
    }

    public Calc accept(ExpCompiler compiler) {
        return ConstantCalc.constantDimension(dimension);
    }

    public Object accept(MdxVisitor visitor) {
        return visitor.visit(this);
    }

}

// End DimensionExpr.java

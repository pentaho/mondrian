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


package mondrian.mdx;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.impl.ConstantCalc;
import mondrian.olap.*;
import mondrian.olap.type.DimensionType;
import mondrian.olap.type.Type;

/**
 * Usage of a {@link mondrian.olap.Dimension} as an MDX expression.
 *
 * @author jhyde
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

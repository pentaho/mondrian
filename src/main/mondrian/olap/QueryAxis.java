/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 1998-2002 Kana Software, Inc.
// Copyright (C) 2001-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 20 January, 1999
*/

package mondrian.olap;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ResultStyle;
import mondrian.mdx.*;
import mondrian.olap.type.Type;
import mondrian.olap.type.TypeUtil;
import mondrian.resource.MondrianResource;

import java.io.PrintWriter;
import java.util.List;

/**
 * An axis in an MDX query. For example, the typical MDX query has two axes,
 * which appear as the "ON COLUMNS" and "ON ROWS" clauses.
 *
 * @version $Id$
 */
public class QueryAxis extends QueryPart {

    private boolean nonEmpty;
    private boolean ordered;
    private Exp exp;
    private final AxisOrdinal axisOrdinal;

    /**
     * Whether to show subtotals on this axis.
     * The "(show\hide)Subtotals" operation changes its valud.
     */
    private SubtotalVisibility subtotalVisibility;
    private final Id[] dimensionProperties;

    /**
     * Creates an axis.
     *
     * @param nonEmpty Whether to filter out members of this axis whose cells
     *    are all empty
     * @param set Expression to populate the axis
     * @param axisDef Which axis (ROWS, COLUMNS, etc.)
     * @param subtotalVisibility Whether to show subtotals
     * @param dimensionProperties List of dimension properties
     */
    public QueryAxis(
            boolean nonEmpty,
            Exp set,
            AxisOrdinal axisDef,
            SubtotalVisibility subtotalVisibility,
            Id[] dimensionProperties) {
        assert dimensionProperties != null;
        this.nonEmpty = nonEmpty ||
            MondrianProperties.instance().EnableNonEmptyOnAllAxis.get();
        this.exp = set;
        this.axisOrdinal = axisDef;
        this.subtotalVisibility = subtotalVisibility;
        this.dimensionProperties = dimensionProperties;
        this.ordered = false;
    }

    /**
     * Creates an axis with no dimension properties.
     *
     * @see #QueryAxis(boolean,Exp,AxisOrdinal,mondrian.olap.QueryAxis.SubtotalVisibility,Id[])
     */
    public QueryAxis(
            boolean nonEmpty,
            Exp set,
            AxisOrdinal axisDef,
            SubtotalVisibility subtotalVisibility) {
        this(nonEmpty, set, axisDef, subtotalVisibility, new Id[0]);
    }

    public Object clone() {
        return new QueryAxis(nonEmpty, exp.clone(), axisOrdinal,
            subtotalVisibility, dimensionProperties.clone());
    }

    static QueryAxis[] cloneArray(QueryAxis[] a) {
        QueryAxis[] a2 = new QueryAxis[a.length];
        for (int i = 0; i < a.length; i++) {
            a2[i] = (QueryAxis) a[i].clone();
        }
        return a2;
    }

    public Object accept(MdxVisitor visitor) {
        final Object o = visitor.visit(this);

        // visit the expression which forms the axis
        exp.accept(visitor);

        return o;
    }

    public Calc compile(ExpCompiler compiler, List<ResultStyle> resultStyles) {
        Exp exp = this.exp;
        if (axisOrdinal == AxisOrdinal.SLICER) {
            exp = normalizeSlicerExpression(exp);
            exp = exp.accept(compiler.getValidator());
        }
        return compiler.compileAs(exp, null, resultStyles);
    }

    private static Exp normalizeSlicerExpression(Exp exp) {
        Exp slicer = exp;
        if (slicer instanceof LevelExpr ||
            slicer instanceof HierarchyExpr ||
            slicer instanceof DimensionExpr) {

            slicer = new UnresolvedFunCall(
                    "DefaultMember", Syntax.Property, new Exp[] {
                        slicer});
        }
        if (slicer == null) {
            ;
        } else if (slicer instanceof FunCall &&
                   ((FunCall) slicer).getSyntax() == Syntax.Parentheses) {
            slicer = new UnresolvedFunCall(
                    "{}", Syntax.Braces, new Exp[] {
                        slicer});
        } else {
            slicer = new UnresolvedFunCall(
                    "{}", Syntax.Braces, new Exp[] {
                        new UnresolvedFunCall(
                                "()", Syntax.Parentheses, new Exp[] {
                                    slicer})});
        }

        return slicer;
    }

    public String getAxisName() {
        return axisOrdinal.name();
    }

    /**
     * Returns the ordinal of this axis, for example {@link AxisOrdinal#ROWS}.
     */
    public AxisOrdinal getAxisOrdinal() {
        return axisOrdinal;
    }

    /**
     * Returns whether the axis has the <code>NON EMPTY</code> property set.
     */
    public boolean isNonEmpty() {
        return nonEmpty;
    }

    /**
     * Sets whether the axis has the <code>NON EMPTY</code> property set.
     * See {@link #isNonEmpty()}.
     */
    public void setNonEmpty(boolean nonEmpty) {
        this.nonEmpty = nonEmpty;
    }

     /**
     * Returns whether the axis has the <code>ORDER</code> property set.
     */
    public boolean isOrdered() {
        return ordered;
    }

    /**
     * Sets whether the axis has the <code>ORDER</code> property set.
     */
    public void setOrdered(boolean ordered) {
        this.ordered = ordered;
    }
    
    /**
     * Returns the expression which is used to compute the value of this axis.
     */
    public Exp getSet() {
        return exp;
    }

    /**
     * Sets the expression which is used to compute the value of this axis.
     * See {@link #getSet()}.
     */
    public void setSet(Exp set) {
        this.exp = set;
    }

    public void resolve(Validator validator) {
        exp = validator.validate(exp, false);
        final Type type = exp.getType();
        if (!TypeUtil.isSet(type)) {
            throw MondrianResource.instance().MdxAxisIsNotSet.ex(axisOrdinal.name());
        }
    }

    public Object[] getChildren() {
        return new Object[] {exp};
    }

    public void unparse(PrintWriter pw) {
        if (nonEmpty) {
            pw.print("NON EMPTY ");
        }
        if (exp != null) {
            exp.unparse(pw);
        }
        if (dimensionProperties.length > 0) {
            pw.print(" DIMENSION PROPERTIES ");
            for (int i = 0; i < dimensionProperties.length; i++) {
                Id dimensionProperty = dimensionProperties[i];
                if (i > 0) {
                    pw.print(", ");
                }
                dimensionProperty.unparse(pw);
            }
        }
        if (axisOrdinal != AxisOrdinal.SLICER) {
            pw.print(" ON " + axisOrdinal);
        }
    }

    public void addLevel(Level level) {
        Util.assertTrue(level != null, "addLevel needs level");
        exp = new UnresolvedFunCall("Crossjoin", Syntax.Function, new Exp[]{
            exp,
            new UnresolvedFunCall("Members", Syntax.Property, new Exp[]{
                new LevelExpr(level)})});
    }

    void setSubtotalVisibility(boolean bShowSubtotals) {
        subtotalVisibility = bShowSubtotals ?
            SubtotalVisibility.Show :
            SubtotalVisibility.Hide;
    }

    public SubtotalVisibility getSubtotalVisibility() {
        return subtotalVisibility;
    }

    public void resetSubtotalVisibility() {
        this.subtotalVisibility = SubtotalVisibility.Undefined;
    }

    public void validate(Validator validator) {
        if (axisOrdinal == AxisOrdinal.SLICER) {
            if (exp != null) {
                exp = validator.validate(exp, false);
            }
        }
    }

    public Id[] getDimensionProperties() {
        return dimensionProperties;
    }

    /**
     * <code>SubtotalVisibility</code> enumerates the allowed values of
     * whether subtotals are visible.
     */
    public enum SubtotalVisibility {
        Undefined,
        Hide,
        Show;
    }

}

// End QueryAxis.java

/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 1998-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho and others
// All Rights Reserved.
*/

package mondrian.olap;

import mondrian.calc.*;
import mondrian.mdx.*;
import mondrian.olap.type.DimensionType;
import mondrian.olap.type.*;
import mondrian.resource.MondrianResource;

import java.io.PrintWriter;

/**
 * An axis in an MDX query. For example, the typical MDX query has two axes,
 * which appear as the "ON COLUMNS" and "ON ROWS" clauses.
 *
 * @author jhyde, 20 January, 1999
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
     * @param axisOrdinal Which axis (ROWS, COLUMNS, etc.)
     * @param subtotalVisibility Whether to show subtotals
     * @param dimensionProperties List of dimension properties
     */
    public QueryAxis(
        boolean nonEmpty,
        Exp set,
        AxisOrdinal axisOrdinal,
        SubtotalVisibility subtotalVisibility,
        Id[] dimensionProperties)
    {
        assert dimensionProperties != null;
        assert axisOrdinal != null;
        this.nonEmpty = nonEmpty
            || (MondrianProperties.instance().EnableNonEmptyOnAllAxis.get()
            && !axisOrdinal.isFilter());
        this.exp = set;
        this.axisOrdinal = axisOrdinal;
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
        AxisOrdinal axisOrdinal,
        SubtotalVisibility subtotalVisibility)
    {
        this(nonEmpty, set, axisOrdinal, subtotalVisibility, new Id[0]);
    }

    public Object clone() {
        return new QueryAxis(
            nonEmpty, exp.clone(), axisOrdinal,
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

        if (visitor.shouldVisitChildren()) {
            // visit the expression which forms the axis
            exp.accept(visitor);
        }
        return o;
    }

    public Calc compile(ExpCompiler compiler, ResultStyle resultStyle) {
        Exp exp = this.exp;
        if (axisOrdinal.isFilter()) {
            exp = normalizeSlicerExpression(exp);
            exp = exp.accept(compiler.getValidator());
        }
        switch (resultStyle) {
        case LIST:
            return compiler.compileList(exp, false);
        case MUTABLE_LIST:
            return compiler.compileList(exp, true);
        case ITERABLE:
            return compiler.compileIter(exp);
        default:
            throw Util.unexpected(resultStyle);
        }
    }

    private static Exp normalizeSlicerExpression(Exp exp) {
        Exp slicer = exp;
        if (slicer instanceof LevelExpr
            || slicer instanceof HierarchyExpr
            || slicer instanceof DimensionExpr)
        {
            slicer = new UnresolvedFunCall(
                "DefaultMember", Syntax.Property, new Exp[] {
                    slicer});
        }
        if (slicer == null) {
            ;
        } else if (slicer instanceof FunCall
            && ((FunCall) slicer).getSyntax() == Syntax.Parentheses)
        {
            slicer =
                new UnresolvedFunCall(
                    "{}", Syntax.Braces, new Exp[] {slicer});
        } else {
            slicer =
                new UnresolvedFunCall(
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
     * Returns the ordinal of this axis, for example
     * {@link mondrian.olap.AxisOrdinal.StandardAxisOrdinal#ROWS}.
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
            // If expression is a member or a tuple, implicitly convert it
            // into a set. Dimensions and hierarchies can be converted to
            // members, thence to sets.
            if (type instanceof MemberType
                || type instanceof TupleType
                || type instanceof DimensionType
                || type instanceof HierarchyType)
            {
                exp =
                    new UnresolvedFunCall(
                        "{}",
                        Syntax.Braces,
                        new Exp[] {exp});
                exp = validator.validate(exp, false);
            } else {
                throw MondrianResource.instance().MdxAxisIsNotSet.ex(
                    axisOrdinal.name());
            }
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
        if (!axisOrdinal.isFilter()) {
            pw.print(" ON " + axisOrdinal.name());
        }
    }

    public void addLevel(Level level) {
        Util.assertTrue(level != null, "addLevel needs level");
        exp = new UnresolvedFunCall(
            "Crossjoin", Syntax.Function, new Exp[] {
                exp,
                new UnresolvedFunCall(
                    "Members", Syntax.Property, new Exp[] {
                        new LevelExpr(level)})});
    }

    void setSubtotalVisibility(boolean bShowSubtotals) {
        subtotalVisibility =
            bShowSubtotals
            ? SubtotalVisibility.Show
            : SubtotalVisibility.Hide;
    }

    public SubtotalVisibility getSubtotalVisibility() {
        return subtotalVisibility;
    }

    public void resetSubtotalVisibility() {
        this.subtotalVisibility = SubtotalVisibility.Undefined;
    }

    public void validate(Validator validator) {
        if (axisOrdinal.isFilter()) {
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

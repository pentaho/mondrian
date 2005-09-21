/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1998-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 20 January, 1999
*/

package mondrian.olap;
import mondrian.olap.type.Type;
import mondrian.olap.type.TypeUtil;
import mondrian.resource.MondrianResource;

import java.io.PrintWriter;

/**
 * An axis in an MDX query. For example, the typical MDX query has two axes,
 * which appear as the "ON COLUMNS" and "ON ROWS" clauses.
 *
 * @version $Id$
 */
public class QueryAxis extends QueryPart {

    static QueryAxis[] cloneArray(QueryAxis[] a) {
        QueryAxis[] a2 = new QueryAxis[a.length];
        for (int i = 0; i < a.length; i++) {
            a2[i] = (QueryAxis) a[i].clone();
        }
        return a2;
    }

    /**
     * <code>SubtotalVisibility</code> enumerates the allowed values of
     * whether subtotals are visible.
     **/
    public static class SubtotalVisibility extends EnumeratedValues {
        /** The singleton instance of <code>SubtotalVisibility</code>. **/
        public static final SubtotalVisibility instance = new SubtotalVisibility();

        private SubtotalVisibility() {
            super(new String[] {"undefined", "hide", "show"},
                  new int[] {Undefined, Hide, Show});
        }
        public static final int Undefined = -1;
        public static final int Hide = 0;
        public static final int Show = 1;
    }

    /**
     * public-private: This must be public because it is accessed in olap.Query
     */
    public boolean nonEmpty;

    /**
     * public-private: This must be public because it is accessed in olap.Query
     */
    public Exp set;

    private final AxisOrdinal axisOrdinal;

    /** <code>showSubtotals</code> indicates if "(show\hide)Subtotals"
     * operation has been applied to axis*/
    private int  showSubtotals;

    public QueryAxis(
            boolean nonEmpty,
            Exp set,
            AxisOrdinal axisDef,
            int showSubtotals) {
        this.nonEmpty = nonEmpty;
        this.set = set;
        this.axisOrdinal = axisDef;
        this.showSubtotals = showSubtotals;
    }

    public Object clone() {
        return new QueryAxis(nonEmpty, (Exp) set.clone(), axisOrdinal, showSubtotals);
    }

    public String getAxisName() {
        return axisOrdinal.getName();
    }

    /**
     * Returns the ordinal of this axis, for example {@link AxisOrdinal#Rows}.
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
     * Returns the expression which is used to compute the value of this axis.
     */
    public Exp getSet() {
        return set;
    }

    /**
     * Sets the expression which is used to compute the value of this axis.
     * See {@link #getSet()}.
     */
    public void setSet(Exp set) {
        this.set = set;
    }

    public void resolve(Validator validator) {
        set = validator.validate(set, false);
        final Type type = set.getTypeX();
        if (!TypeUtil.isSet(type)) {
            throw MondrianResource.instance().MdxAxisIsNotSet.ex(axisOrdinal.getName());
        }
    }

    public Object[] getChildren() {
        return new Object[] {set};
    }

    public void replaceChild(int ordinal, QueryPart with) {
        Util.assertTrue(ordinal == 0);
        set = (Exp) with;
    }

    public void unparse(PrintWriter pw) {
        if (nonEmpty) {
            pw.print("NON EMPTY ");
        }
        if (set != null) {
            set.unparse(pw);
        }
        pw.print(" ON " + axisOrdinal);
    }

    public void addLevel(Level level) {
        Util.assertTrue(level != null, "addLevel needs level");
        set = new FunCall("Crossjoin",
                Syntax.Function,
                new Exp[]{
                    set,
                    new FunCall("Members",
                            Syntax.Property,
                            new Exp[]{level})
                });
    }

    void setShowSubtotals(boolean bShowSubtotals) {
        showSubtotals = bShowSubtotals ?
            SubtotalVisibility.Show :
            SubtotalVisibility.Hide;
    }

    public int getShowSubtotals() {
        return showSubtotals;
    }

    public void resetShowHideSubtotals() {
        this.showSubtotals = SubtotalVisibility.Undefined;
    }

}

// End QueryAxis.java

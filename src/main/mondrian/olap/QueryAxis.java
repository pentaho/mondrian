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
import java.io.PrintWriter;

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
     * NOTE: This must be public because JPivoi directly accesses this instance
     * variable.
     * Currently, the JPivoi usages are:
     * if (qAxis.nonEmpty)
     * qAxis.nonEmpty = nonEmpty;
     *
     * This usage is deprecated: please use the nonEmpty's getter and setter
     * methods:
     *   public boolean isNonEmpty()
     *   public void setNonEmpty(boolean nonEmpty)
     */
    public boolean nonEmpty;

    /** 
     * NOTE: This must be public because JPivoi directly accesses this instance
     * variable.
     * Currently, the JPivoi usages are:
     * axis.set  = ...
     *  = axis.set;
     *
     * This usage is deprecated: please use the set's getter and setter
     * methods:
     *   public Exp getSet()
     *   public void setSet(Exp set)
     */
    public Exp set;

    private final String axisName;

    /** <code>showSubtotals</code> indicates if "(show\hide)Subtotals"
     * operation has been applied to axis*/
    private int  showSubtotals;

    public QueryAxis(boolean nonEmpty, 
                     Exp set, 
                     String axisName, 
                     int showSubtotals) {
        this.nonEmpty = nonEmpty;
        this.set = set;
        this.axisName = axisName;
        this.showSubtotals = showSubtotals;
    }

    public Object clone() {
        return new QueryAxis(nonEmpty, (Exp) set.clone(), axisName, showSubtotals);
    }
    public String getAxisName() {
        return axisName;
    }

    public boolean isNonEmpty() {
        return nonEmpty;
    }
    public void setNonEmpty(boolean nonEmpty) {
        this.nonEmpty = nonEmpty;
    }
    public Exp getSet() {
        return set;
    }
    public void setSet(Exp set) {
        this.set = set;
    }

    public void resolve(Validator resolver) {
        set = resolver.resolveChild(set);
        if (!set.isSet()) {
            throw Util.getRes().newMdxAxisIsNotSet( axisName );
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
        pw.print(" ON " + axisName);
    }

    public void addLevel(Level level) {
        Util.assertTrue(level != null, "addLevel needs level");
        set = new FunCall("Crossjoin", 
                          Syntax.Function, 
                          new Exp[] {
                            set,
                            new FunCall("Members", 
                                        Syntax.Property, 
                                        new Exp[] {level})
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

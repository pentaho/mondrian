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
import mondrian.olap.fun.BuiltinFunTable;
import mondrian.olap.fun.ParameterFunDef;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

/**
 * <code>Query</code> is an MDX query.
 *
 * <p>It is created by calling {@link Connection#parseQuery},
 * and executed by calling {@link Connection#execute},
 * to return a {@link Result}.
 **/
public class Query extends QueryPart {

    //hidden string
    public static final String HIDDEN = "hidden_";

    /** 
     * NOTE: This must be public because JPivoi directly accesses this instance
     * variable.
     * Currently, the JPivoi usages are:
     * mondrian.olap.Formula[] formulas = q.formulas;
     *
     * This usage is deprecated: please use the formulas's getter methods:
     *   public Formula[] getFormulas()
     */
    public Formula[] formulas;

    /** 
     * NOTE: This must be public because JPivoi directly accesses this instance
     * variable.
     * Currently, the JPivoi usages are:
     * monQuery.axes.length
     * mondrian.olap.QueryAxis qAxis = monQuery.axes[i];
     *
     * This usage is deprecated: please use the axes's getter methods:
     *   public QueryAxis[] getAxes()
     */
    public QueryAxis[] axes;
    
    /** 
     * NOTE: This must be public because JPivoi directly accesses this instance
     * variable. 
     * Currently, the JPivoi usages are:
     * monQuery.slicer = null;
     * monQuery.slicer = f;
     *
     * This usage is deprecated: please use the slicer's getter and setter
     * methods:
     *   public Exp getSlicer()
     *   public void setSlicer(Exp exp)
     */
    public Exp slicer;

    private Parameter[] parameters; // stores definitions of parameters
    private final QueryPart[] cellProps;
    private final Cube mdxCube;
    private final Connection connection;

/*
    public Query() {
    }
*/

    /** Constructs a Query. */
    public Query(Connection connection, 
                 Formula[] formulas, 
                 QueryAxis[] axes,
                 String cube, 
                 Exp slicer, 
                 QueryPart[] cellProps) {
        this(connection, 
             connection.getSchema().lookupCube(cube, true),
             formulas, 
             axes, 
             slicer, 
             cellProps, 
             new Parameter[0]);
    }

    /** Construct a Query; called from clone(). */
    public Query(Connection connection, 
                 Cube mdxCube,
                 Formula[] formulas, 
                 QueryAxis[] axes, 
                 Exp slicer,
                 QueryPart[] cellProps, 
                 Parameter[] parameters) {
        this.connection = connection;
        this.mdxCube = mdxCube;
        this.formulas = formulas;
        this.axes = axes;
        normalizeAxes();
        setSlicer(slicer);
        this.cellProps = cellProps;
        this.parameters = parameters;
        resolve();
    }

    /**
     * add a new formula specifying a set
     *  to an existing query
     */
    public void addFormula(String[] names, Exp exp) {
        Formula newFormula = new Formula(names, exp);
        int nFor = 0;
        if (formulas.length > 0) {
            nFor = formulas.length;
        }
        Formula[] newFormulas = new Formula[nFor + 1];
        for (int i = 0; i < nFor; i++ ) {
            newFormulas[i] = formulas[i];
        }
        newFormulas[nFor] = newFormula;
        formulas = newFormulas;
        resolve();
    }

    /**
     * add a new formula specifying a member
     *  to an existing query
     */
    public void addFormula(String[] names, 
                           Exp exp, 
                           MemberProperty[] memberProperties) {
        Formula newFormula = new Formula(names, exp, memberProperties);
        int nFor = 0;
        if (formulas.length > 0) {
            nFor = formulas.length;
        }
        Formula[] newFormulas = new Formula[nFor + 1];
        for (int i = 0; i < nFor; i++ ) {
            newFormulas[i] = formulas[i];
        }
        newFormulas[nFor] = newFormula;
        formulas = newFormulas;
        resolve();
    }


    public Exp.Resolver createResolver() {
        return new StackResolver(BuiltinFunTable.instance());
    }

    public Object clone() throws CloneNotSupportedException {
        return new Query(connection,  
                         mdxCube,
                         Formula.cloneArray(formulas), 
                         QueryAxis.cloneArray(axes),
                         (slicer == null) ? null : (Exp) slicer.clone(), 
                         null,
                         Parameter.cloneArray(parameters));
    }

    public Query safeClone() {
        try {
            return (Query) clone();
        } catch (CloneNotSupportedException e) {
            throw Util.getRes().newInternal("Query.clone() failed", e);
        }
    }

    public Connection getConnection() {
        return connection;
    }

    /**
     * Returns the MDX query string. If the query was created by parsing an
     * MDX string, the string returned by this method may not be identical, but
     * it will have the same meaning. If the query's parse tree has been
     * manipulated (for instance, the rows and columns axes have been
     * interchanged) the returned string represents the current parse tree.
     */
    public String getQueryString() {
        return toWebUIMdx();
    }

    private void normalizeAxes() {
        for (int i = 0; i < axes.length; i++) {
            String correctName = AxisOrdinal.instance.getName(i);
            if (!axes[i].getAxisName().equalsIgnoreCase(correctName)) {
                for (int j = i + 1; j < axes.length; j++) {
                    if (axes[j].getAxisName().equalsIgnoreCase(correctName)) {
                        // swap axes
                        QueryAxis temp = axes[i];
                        axes[i] = axes[j];
                        axes[j] = temp;
                        break;
                    }
                }
            }
        }
    }

    /**
     * Performs type-checking and validates internal consistency of a query,
     * using the default resolver.
     *
     * <p>This method is called automatically when a query is created; you need
     * to call this method manually if you have modified the query's expression
     * tree in any way.
     */
    public void resolve() {
        resolve(createResolver()); // resolve self and children
    }

    /**
     * Performs type-checking and validates internal consistency of a query,
     * using a custom resolver.
     *
     * @param resolver Custom resolver
     */
    void resolve(Exp.Resolver resolver) {
        if (formulas != null) {
            //resolving of formulas should be done in two parts
            //because formulas might depend on each other, so all calculated
            //mdx elements have to be defined during resolve
            for (int i = 0; i < formulas.length; i++) {
                formulas[i].createElement(resolver.getQuery());
            }
            for (int i = 0; i < formulas.length; i++) {
                resolver.resolveChild(formulas[i]);
            }
        }

        if (axes != null) {
            for (int i = 0; i < axes.length; i++) {
                resolver.resolveChild(axes[i]);
            }
        }
        if (slicer != null) {
            setSlicer(resolver.resolveChild(slicer));
        }

        // Now that out Parameters have been created (from FunCall's to
        // Parameter() and ParamRef()), resolve them.
        for (int i = 0; i < parameters.length; i++) {
            parameters[i] = resolver.resolveChild(parameters[i]);
        }
        resolveParameters();
    }

    public void unparse(PrintWriter pw) {
        if (formulas != null) {
            for (int i = 0; i < formulas.length; i++) {
                if (i == 0) {
                    pw.print("with ");
                } else {
                    pw.print("  ");
                }
                formulas[i].unparse(pw);
                pw.println();
            }
        }
        pw.print("select ");
        if (axes != null) {
            for (int i = 0; i < axes.length; i++) {
                axes[i].unparse(pw);
                if (i < axes.length - 1) {
                    pw.println(",");
                    pw.print("  ");
                } else {
                    pw.println();
                }
            }
        }
        if (mdxCube != null) {
            pw.println("from [" + mdxCube.getName() + "]");
        }
        if (slicer != null) {
            pw.print("where ");
            slicer.unparse(pw);
            pw.println();
        }
    }

    public String toPlatoMdx() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        unparse(pw);
        return sw.toString();
    }

    public String toWebUIMdx() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        unparse(pw);
        resetParametersPrintProperty();
        return sw.toString();
    }

    /**
     * Returns the axis which the result axis is based on, taking into account
     * any axis re-ordering.
     *
     * <p>Suppose that they've written
     * <pre>select {} on rows, {} on pages from Sales</pre>
     *
     * <p>Then we will execute
     * <pre>select {} on columns, {} on rows from Sales</pre>
     *
     * getLogicalAxis(0) = 1, meaning that axis 0 of the Plato cellset matches
     * the rows (1) axis of their query; likewise, getLogicalAxis(1) = 2.
     *
     * @param iPhysicalAxis ordinal of axis in cellset
     * @return axis label in original query (0 = columns, 1 = rows, etc.)
     */
    public int getLogicalAxis(int iPhysicalAxis) {
        if ((iPhysicalAxis == AxisOrdinal.SLICER) || 
                (iPhysicalAxis == axes.length)) {
            return AxisOrdinal.SLICER; // slicer is never permuted
        }
        String axisName = axes[iPhysicalAxis].getAxisName();
        final EnumeratedValues.Value value = 
            AxisOrdinal.instance.getValue(axisName);

        return (value != null)
            ? value.getOrdinal()
            : AxisOrdinal.NONE;
    }

    /** The inverse of {@link #getLogicalAxis}. */
    public int getPhysicalAxis(int iLogicalAxis) {
        if (iLogicalAxis < 0) {
            return iLogicalAxis;
        }
        String axisName = AxisOrdinal.instance.getName(iLogicalAxis);
        for (int i = 0; i < axes.length; i++) {
            if (axes[i].getAxisName().equalsIgnoreCase(axisName)) {
                return i;
            }
        }
        return AxisOrdinal.NONE;
    }

    /** Constructs hidden unique name based on given uName. It is used for
     * formatting existing measures. */
    public static String getHiddenMemberUniqueName(String uName) {
        int i = uName.lastIndexOf("].[");
        return uName.substring(0, i + 3) + HIDDEN + uName.substring(i+3);
    }

    /** checks for hidden string in name and strips it out. It looks only for
     * first occurence */
    public static String stripHiddenName(String name) {
        final int i = name.indexOf(HIDDEN);
        return (i >= 0)
            ? name.substring(0, i) + name.substring(i + HIDDEN.length())
            : name;
    }

    public static String getHiddenMemberFormulaDefinition(String uName) {
        return uName;
    }

    /** Returns the MDX query string. */
    public String toString() {
        resolve();
        return toWebUIMdx();
    }

    public Object[] getChildren() {
        // Chidren are axes, slicer, and formulas (in that order, to be
        // consistent with replaceChild).
        List list = new ArrayList();
        for (int i = 0; i < axes.length; i++) {
            list.add(axes[i]);
        }
        if (slicer != null) {
            list.add(slicer);
        }
        for (int i = 0; i < formulas.length; i++) {
            list.add(formulas[i]);
        }
        return list.toArray();
    }

    public void replaceChild(int i, QueryPart with) {
        int i0 = i;
        if (i < axes.length) {
            if (with == null) {
                // We need to remove the axis.  Copy the array, omitting
                // element i.
                QueryAxis[] oldAxes = axes;
                axes = new QueryAxis[oldAxes.length - 1];
                for (int j = 0; j < axes.length; j++) {
                    axes[j] = oldAxes[j < i ? j : j + 1];
                }
            } else {
                axes[i] = (QueryAxis) with;
            }
            return;
        }

        i -= axes.length;
        if (i == 0) {
            setSlicer((Exp) with); // replace slicer
            return;
        }

        i -= 1;
        if (i < formulas.length) {
            if (with == null) {
                // We need to remove the formula.  Copy the array, omitting
                // element i.
                Formula[] oldFormulas = formulas;
                formulas = new Formula[oldFormulas.length - 1];
                for (int j = 0; j < formulas.length; j++) {
                    formulas[j] = oldFormulas[j < i ? j : j + 1];
                }
            } else {
                formulas[i] = (Formula) with;
            }
            return;
        }
        throw Util.getRes().newInternal(
            "Query child ordinal " + i0 + " out of range (there are " +
            axes.length + " axes, " + formulas.length + " formula)");
    }

    /** Normalize slicer into a tuple of members; for example, '[Time]' becomes
     * '([Time].DefaultMember)'.  todo: Make slicer an Axis, not an Exp, and
     * put this code inside Axis.  */
    public void setSlicer(Exp exp) {
        slicer = exp;
        if (slicer instanceof Level ||
            slicer instanceof Hierarchy ||
            slicer instanceof Dimension) {

            slicer = new FunCall("DefaultMember", 
                                 Syntax.Property, 
                                 new Exp[] {slicer});
        }
        if (slicer == null) {
            ;
        } else if (slicer instanceof FunCall &&
                   ((FunCall) slicer).isCallToTuple()) {
            ;
        } else {
            slicer = new FunCall(
                "()", Syntax.Parentheses, new Exp[] {slicer});
        }
    }

    public Exp getSlicer() {
        return slicer;
    }

    /** Returns an enumeration, each item of which is an Ob containing a
     * dimension which does not appear in any Axis or in the slicer. */
    public Iterator unusedDimensions() {
        Dimension[] mdxDimensions = mdxCube.getDimensions();
        return Arrays.asList(mdxDimensions).iterator();
    }

    /**
     * Adds a level to an axis expression.
     *
     * @pre AxisOrdinal.instance().isValid(axis)
     * @pre axis &lt; axes.length
     */
    public void addLevelToAxis(int axis, Level level) {
        Util.assertPrecondition(AxisOrdinal.instance.isValid(axis), "AxisOrdinal.instance.isValid(axis)");
        Util.assertPrecondition(axis < axes.length, "axis < axes.length");
        axes[axis].addLevel(level);
    }

    /**
     * Walk over the tree looking for an expression of a particular hierarchy
     * If there is one, return the walker pointing at it (from which we can get
     * its parent); otherwise, return null.
     **/
    private Walker findHierarchy(Hierarchy hierarchy) {
        Walker walker = new Walker(this);
        while (walker.hasMoreElements()) {
            Object o = walker.nextElement();
            if (o instanceof Formula) {
                walker.prune();
                continue; // ignore expressions in formula
            } else if (o instanceof Exp) {

                // if object's parent is a function (except a tuple/parentheses
                // or CrossJoin), algorithm shall look only at the first child
                Object parent =  walker.getParent();
                if (parent instanceof FunCall) {
                    FunCall funCall = (FunCall) parent;
                    if (!funCall.isCallToTuple() &&
                        !funCall.isCallToCrossJoin() &&
                        !funCall.isCallTo("Generate") &&
                        funCall.getArg(0) != o) {
                        walker.prune();
                        continue;
                    }
                }

                Exp e = (Exp) o;
                // expression must represent a set or be mdx element
                if (!e.isSet() && !e.isElement()) {
                    continue;
                }

                Hierarchy obExpHierarchy = e.getHierarchy();
                if (obExpHierarchy == null) {
                    // set must have a dimension (e.g. disallow CrossJoin)
                    continue;
                }

                if (obExpHierarchy.equals(hierarchy)) {
                    return walker; // success!
                } else if (e instanceof FunCall && 
                        ((FunCall) e).isCallToFilter()){
                    // tell walker not to look at any more children of Filter
                    walker.prune();
                }
            }
        }
        return null; // no expression of that dimension found
    }

    /**
     * Returns the hierarchies in an expression
     * @see #findHierarchy
     */
    private Hierarchy[] collectHierarchies(QueryPart queryPart) {
        Walker walker = new Walker(queryPart);
        List hierList = new ArrayList();
        while (walker.hasMoreElements()) {
            Object o = walker.nextElement();
            if (o instanceof Exp) {
                // if object's parent is a function (except tuple/parentheses
                // or CrossJoin), algorithm shall look only at the first child
                Object parent =  walker.getParent();
                if (parent instanceof FunCall) {
                    FunCall funCall = (FunCall) parent;
                    if (!funCall.isCallToTuple() &&
                        !funCall.isCallToCrossJoin() &&
                        !funCall.isCallTo("Generate") &&
                        funCall.getArg(0) != o) {
                        walker.prune();
                        continue;
                    }
                }

                Exp e = (Exp) o;
                if (!e.isSet() && !e.isMember()) {
                    continue; // expression must represent a set or be a member
                }

                Hierarchy obExpHierarchy = e.getHierarchy();
                if (obExpHierarchy == null) {
                    // set must have a dimension (e.g. disallow CrossJoin)
                    continue;
                }
                if(!hierList.contains(obExpHierarchy)) {
                  hierList.add(obExpHierarchy);
                }
            }
        }

        return (Hierarchy[]) hierList.toArray(new Hierarchy[0]);
    }

    /** Place expression 'exp' at position 'iPositionOnAxis' on axis 'axis'. */
    private void putInAxisPosition(Exp exp, int axis, int iPositionOnAxis) {
        switch (axis) {
        case AxisOrdinal.SLICER:
            // slicer shall contain at most one tuple
            if (slicer == null) {
                setSlicer(exp);
            } else {
                slicer.addAtPosition(exp, iPositionOnAxis);
            }
            break;

        default:
            Util.assertTrue(axis >= 0);
            if (axis >= axes.length) {
                Util.assertTrue(axis == axes.length);
                QueryAxis[] oldAxes = axes;
                axes = new QueryAxis[oldAxes.length + 1];
                for (int i = 0; i < oldAxes.length; i++) {
                    axes[i] = oldAxes[i];
                }
                axes[oldAxes.length] = new QueryAxis(
                    false, null, AxisOrdinal.instance.getName(axis),
                    QueryAxis.SubtotalVisibility.Undefined);
            }

            Exp axisExp = axes[axis].set;
            if (axisExp == null || axisExp.isEmptySet()) {
                // Axis is empty, so just put expression there.
                axes[axis].set = exp;
            } else {
                if (iPositionOnAxis == 0) {
                    // 'exp' has to go first:
                    //   axisExp
                    // becomes
                    //   CrossJoin(exp, axisExp)
                    FunCall funCrossJoin = new FunCall("CrossJoin",
                            Syntax.Function, new Exp[] {exp, axisExp});
                    axes[axis].set = funCrossJoin;
                } else if (iPositionOnAxis < 0) {
                    // 'exp' has to go last:
                    //   axisExp
                    // becomes
                    //   CrossJoin(axisExp, exp)
                    FunCall funCrossJoin = new FunCall("CrossJoin",
                            Syntax.Function, new Exp[] {axisExp, exp});
                    axes[axis].set = funCrossJoin;
                } else {
                    int i = axes[axis].set.addAtPosition(exp, iPositionOnAxis);
                    if (i != -1) {
                        // The expression was not added, because the position
                        // equalled or exceded the number of hierarchies. Add
                        // it on the end.
                        FunCall funCrossJoin = new FunCall("CrossJoin",
                                Syntax.Function, new Exp[] {axisExp, exp});
                        axes[axis].set = funCrossJoin;
                    }
                }
            }
            break;
        }
    }

    /**
     * Restrict the axis which contains "level" to only return members
     * between "startMember" and "endMember", inclusive.
     */
    public void crop(Level level, Member startMember, Member endMember) {
        // Form the cropping expression.  If we have a range, include all
        // descendents of the ends of the range, because ':' only includes
        // members at the same level.
        Hierarchy hierarchy = level.getHierarchy();
        Exp expCrop = startMember.equals(endMember)
                // e.g. {[Beverages]}
                ?  new FunCall("{}", Syntax.Braces, new Exp[] {startMember})
                // e.g.
                // Generate([Beverages]:[Breakfast Foods],
                //          Descendants([Products].CurrentMember,
                //                      [Products].[(All)],
                //                      SELF_BEFORE_AFTER))
                : new FunCall("Generate", Syntax.Function, new Exp[] {
                    new FunCall(":", Syntax.Infix, new Exp[] {
                        startMember, endMember}),
                    new FunCall("Descendants", Syntax.Function, new Exp[] {
                        new FunCall("CurrentMember", Syntax.Property, new Exp[] {
                            hierarchy}),
                        Util.lookupHierarchyLevel(hierarchy, "(All)"),
                        Literal.createSymbol("SELF_BEFORE_AFTER")
                    })
                });

        crop(level, expCrop);
    }

    /**
     *
     * The technique is to find the expression which generates the set of
     * members for that dimension, then intersect it with the cropping set.
     *
     * For example,
     *
     * select
     *    {[Measures].[Unit Sales], [Measures].[Sales Count]} on columns,
     *    CROSSJOIN(
     *        [Product].[Product Department].MEMBERS,
     *        [Gender].[Gender].MEMBERS) on rows
     * from Sales
     *
     * when cropped with {[Beverages], [Breakfast Foods]} becomes
     *
     * select
     *    {[Measures].[Unit Sales], [Measures].[Sales Count]} on columns,
     *    CROSSJOIN(
     *        INTERSECT(
     *            [Product].[Product Department].MEMBERS,
     *            {[Beverages], [Breakfast Foods]}),
     *        [Gender].[Gender].MEMBERS) on rows
     * from Sales
     */
    private void crop(Level level, Exp expCrop) {
        boolean found = false;
        Walker walker = new Walker(this);
        while (walker.hasMoreElements()) {
            Object o = walker.nextElement();
            if (o instanceof Exp) {
                Exp e = (Exp) o;
                if (!e.isSet()) {
                    continue;   // expression must represent a set
                }

                Dimension dim = e.getDimension();
                if (dim == null) {
                    // set must have a dimension (e.g. disallow Crossjoin)
                    continue;
                }

                if (!dim.equals(level.getDimension())) {
                    continue; // set must be of right dimension
                }

                FunCall funIntersect = new FunCall("Intersect", 
                                                   Syntax.Function, 
                                                   new Exp[] {e, expCrop});

                QueryPart parent = (QueryPart) walker.getParent();
                parent.replaceChild(walker.getOrdinal(), funIntersect);
                found = true;
                break;
            }
        }
        Util.assertTrue(
            found,
            "could not find expression of dimension " +
            level.getDimension());
    }

    /**
     * Assigns a value to the parameter with a given name.
     *
     * @throws RuntimeException if there is not parameter with the given name
     */
    public void setParameter(String parameterName, String value) {
        Parameter param = lookupParam(parameterName);
        if (param == null) {
            throw Util.getRes().newMdxParamNotFound(parameterName);
        }
        param.setValue(value, this);
    }

    /**
     * Moves <code>hierarchy</code> from <code>fromAxis</code>, to
     * <code>toAxis</code> at <code>position</code> (-1 means last position).
     * The hierarchy is added if <code>fromAxis</code> is {@link AxisOrdinal#NONE},
     * and removed if <code>toAxis</code> is {@link AxisOrdinal#NONE}.
     *
     * <p>If the target axis is the slicer, selects the [All] member;
     * otherwise, if the hierarchy is already on an axis, keep the same
     * drill-state; otherwise, select the first level (children), if expand =
     * true, else put default member.</p>
     **/
    public void moveHierarchy(Hierarchy hierarchy, 
                              int fromAxis, 
                              int toAxis,
                              int iPositionOnAxis, 
                              boolean bExpand) {
        Exp e;

        // Find the hierarchy in its current position.
        Walker walker = findHierarchy(hierarchy.getHierarchy());
        if (fromAxis == AxisOrdinal.NONE) {
            if (walker != null) {
                throw Util.getRes().newMdxHierarchyUsed(hierarchy.getUniqueName());
            }
            e = null;
        } else {
            if (walker == null) {
                throw Util.getRes().newMdxHierarchyNotUsed(hierarchy.getUniqueName());
            }

            // Remove from current position.
            e = (Exp) walker.currentElement();
            QueryPart parent = (QueryPart) walker.getParent();
            Util.assertTrue(parent != null, "hierarchy must have parent");
            if (parent instanceof QueryAxis) {
                // Axis only contains this hierarchy; remove it.
                Util.assertTrue(walker.getAncestor(2) == this);
                int iAxis = walker.getAncestorOrdinal(1);
                replaceChild(iAxis, null);
                if (toAxis > iAxis) {
                    --toAxis;
                }
            } else if (parent instanceof Query && fromAxis == AxisOrdinal.SLICER) {
                // Hierachy sits on the slicer and it's the only hierachy on
                // the slicer (otherwise the parent would be _Tuple with at
                // least 2 children) and it is being removed - Simply delete
                // the slicer
                slicer = null;
            } else if (parent instanceof FunCall &&
                       ((FunCall) parent).isCallToCrossJoin()) {
                // Function must be CrossJoin.  If 'e' is our expression, then
                //   f(..., CrossJoin(e, other), ...)
                // becomes
                //   f(..., other, ...).
                int iOrdinal = walker.getOrdinal();
                int iOtherOrdinal = 1 - iOrdinal;
                Exp otherExp = ((FunCall) parent).getArg(iOtherOrdinal);
                QueryPart grandparent = (QueryPart) walker.getAncestor(2);
                int iParentOrdinal = walker.getAncestorOrdinal(1);
                grandparent.replaceChild(iParentOrdinal, (QueryPart) otherExp);
            } else if (parent instanceof FunCall &&
                       ((FunCall)parent).isCallToTuple() &&
                       fromAxis == AxisOrdinal.SLICER) {
                int iOrdinal = walker.getOrdinal();
                ((FunCall)slicer).removeChild( iOrdinal );
            } else if (parent instanceof Parameter) {
                // The hierarchy is a child of parameter, so we need to remove
                // the parameter itself.
                QueryPart grandparent = (QueryPart) walker.getAncestor(2);
                int iParentOrdinal = walker.getAncestorOrdinal(1);
                if (grandparent instanceof FunCall &&
                       ((FunCall)grandparent).isCallToTuple() &&
                       fromAxis == AxisOrdinal.SLICER) {
                    ((FunCall)slicer).removeChild( iParentOrdinal );
                    if (((FunCall)slicer).getArgLength() == 0) {
                        // the slicer is empty now
                        slicer = null;
                    }
                } else if (grandparent instanceof FunCall &&
                       ((FunCall) grandparent).isCallToCrossJoin()) {
                    // Function must be CrossJoin.  If 'e' is our expression,
                    // then
                    //   f(..., CrossJoin(e, other), ...)
                    // becomes
                    //   f(..., other, ...).
                    int iOtherOrdinal = 1 - iParentOrdinal;
                    Exp otherExp = 
                        ((FunCall) grandparent).getArg(iOtherOrdinal);
                    QueryPart grandGrandparent = (QueryPart)
                        walker.getAncestor(3);
                    int iGrandParentOrdinal = walker.getAncestorOrdinal(2);
                    grandGrandparent.replaceChild(
                        iGrandParentOrdinal, (QueryPart) otherExp);
                }
            } else {
                throw Util.getRes().newInternal(
                    "hierarchy starts under " + parent.toString());
            }
        }

        // Move to slicer?
        switch (toAxis) {
        case AxisOrdinal.SLICER:
            // we do not care of expression is already a Member, because it's a
            // very rare case; we have to make a new expression containing
            // default
            e = new FunCall("DefaultMember", 
                            Syntax.Property, 
                            new Exp[] {hierarchy});
            putInAxisPosition(e, toAxis, iPositionOnAxis);
            break;
        case AxisOrdinal.COLUMNS:
        case AxisOrdinal.ROWS:
            // If this hierarchy is new, create an expression to display the
            // children of the default member (which is, we hope, the root
            // member).
            if (e == null) {
                if (bExpand) {
                    e = new FunCall("Children", 
                                    Syntax.Property, 
                                    new Exp[] {hierarchy});
                } else {
                    Exp tmpExp = new FunCall("DefaultMember", 
                                             Syntax.Property, 
                                             new Exp[] {hierarchy}
                    );
                    e = new FunCall("{}", 
                                    Syntax.Braces, 
                                    new Exp[] {tmpExp});
                }
            } else if (fromAxis == AxisOrdinal.SLICER) {
                // Expressions on slicers are stored as DefaultMember.  We need
                // to convert it to $Brace expression first (curly braces
                // needed).
                e = new FunCall("{}", Syntax.Braces, new Exp[] {e});
            }

            // Move to regular axis.
            putInAxisPosition(e, toAxis, iPositionOnAxis);
            break;

        case AxisOrdinal.NONE:
            // Discard hierarchy.  Nothing to do.
            break;

        default:
            throw Util.getRes().newInternal("bad axis code: " + toAxis);
        }
    }

    /**
     * Filters the set of elements which are returned from a hierarchy.  If
     * hierarchy is in the slicer, the set must contain exactly one element.
     * (Hierarchy must be on the axis specified.)
     *
     * 'members' are the members to be displayed.  They may be from different
     * levels - for example, {[USA], [USA].[California]} - and their order is
     * important.
     *
     * @pre AxisOrdinal.instance().isValid(axis)
     * @pre axis &lt; axes.length
     **/
    public void filterHierarchy(Hierarchy hierarchy, 
                                int /*axisType*/ axis, 
                                Member[] members) {
        Util.assertPrecondition(AxisOrdinal.instance.isValid(axis), 
            "AxisOrdinal.instance.isValid(axis)");
        Util.assertPrecondition(axis < axes.length, "axis < axes.length");

        // Check that there can be only one filter per hierarchy applied on
        // slicer.
        if (axis == AxisOrdinal.SLICER && members.length > 1) {
            throw Util.getRes().newInternal(
                "there can be only one filter per hierarchy on slicer");
        }
        // Check that members are all in the right hierarchy.
        for (int iMember = 0; iMember < members.length; iMember++) {
            if (!members[iMember].getHierarchy().equals(hierarchy)) {
                throw Util.getRes().newInternal(
                    "member " + members[iMember] +
                    " is not in hierarchy " + hierarchy);
            }
        }

        Walker walker = findHierarchy(hierarchy.getHierarchy());
        if (walker == null) {
            // Hierarchy is not currently used.  Put it at the last position on
            // the desired axis, then filter it.
            moveHierarchy(hierarchy, AxisOrdinal.NONE, axis, -1, true);
            walker = findHierarchy(hierarchy.getHierarchy());
            Util.assertTrue(walker != null, "hierarchy wasn't added");
        }

        // The expression we find may be either:
        // a) a member, for example '[Gender].[M]' in
        //      ([Gender].[M], [Marital Status].[S])
        //    or in
        //      CrossJoin([Marital Status].Members, {[Gender].[M]}); or
        // b) a set, for example '[Gender].Members' in
        //      CrossJoin([Store].Members, [Gender].Members).
        // We replace a set with a set, and a member with a member.
        QueryPart parent = (QueryPart) walker.getParent();
        int iOrdinal = walker.getOrdinal();
        Exp e = (Exp) walker.currentElement();
        if (e.isMember()) {
            Util.assertTrue(
                members.length == 1,
                "filterHierarchy cannot replace member with set");

            parent.replaceChild(iOrdinal, (QueryPart) members[0]);

        } else if (e.isSet()) {
            // Build a set out of the members supplied using the "{}" operator.
            // If there are no members, revert to the default member (for bug
            // 13728).
            Exp[] exps = members;
            if (members.length == 0) {
                exps = new Exp[] {new FunCall(
                    "DefaultMember", Syntax.Property, new Exp[] {hierarchy})};
            }
            // Neither slicer nor the tuple function (which is likely to occur
            // in a slicer) can have a set as a child, so reduce a singleton
            // set to a member in these cases.
            Exp exp = (exps.length == 1) &&
                      (parent instanceof Query || // because e is slicer
                      parent instanceof FunCall &&
                      ((FunCall) parent).isCallToTuple())
                ? exps[0]
                : new FunCall("{}", Syntax.Braces, exps);

            parent.replaceChild(iOrdinal, (QueryPart) exp);

        } else {
            throw Util.newInternal("findHierarchy returned a " +
                    Category.instance.getName(e.getType()));
        }
    }

    /** ToggleDrillState. */
    public void toggleDrillState(Member member) {
        Walker walker = findHierarchy(member.getHierarchy());
        if (walker == null) {
            throw Util.getRes().newInternal(
                "member's dimension is not used: " + member.toString());
        }

        // If 'e' is our expression, then
        //    f(..., e, ...)
        // becomes
        //    f(..., ToggleDrillState(e, {member}), ...)
        Exp e = (Exp) walker.currentElement();
        FunCall funToggle = new FunCall("ToggleDrillState", 
                                        Syntax.Function, 
                                        new Exp[] {
                                            e, 
                                            new FunCall("{}", 
                                                        Syntax.Braces, 
                                                        new Exp[] {member})
                                        }
                                    );
        QueryPart parent = (QueryPart) walker.getParent();
        int iOrdinal = walker.getOrdinal();
        parent.replaceChild(iOrdinal, funToggle);
    }


    /**
     * Sort.
     *
     * <p>This function always removes previous sort on <code>axis</code>.
     * If <code>direction</code> is "none" then axis becomes sorted in natural
     * order (no explicit sorting).
     *
     * @param axis is the axis to sort, a member of {@link AxisOrdinal}
     * @param direction is the direction to sort, a member of {@link SortDirection}
     * @param members is tuple of members to sort on.  For
     *   example, the y-axis can be sorted by [Time].[Quarter] (its name), or by
     *   {[Measures].[Unit Sales], [Stores].[California]} (Unit Sales in
     *   California).  In general, the latter specification identifies a single
     *   column (or row, for x-axis sorting) for each hierarchy on the other
     *   axis.
     *
     * @pre AxisOrdinal.instance().isValid(axis)
     * @pre axis &lt; axes.length
     * @pre SortDirection.instance.isValid(direction)
     */
    public void sort(int axis, int direction, Member[] members) {
        Util.assertPrecondition(AxisOrdinal.instance.isValid(axis), 
            "AxisOrdinal.instance.isValid(axis)");
        Util.assertPrecondition(axis < axes.length, "axis < axes.length");
        Util.assertPrecondition(SortDirection.instance().isValid(direction), 
            "SortDirection.instance().isValid(direction)");

        // Find and remove any existing sorts on this axis.
        removeSortFromAxis(axis);

        //apply new sort
        if (direction == SortDirection.NONE) {
            return; // we already removed the sort
        }
        String sDirection = SortDirection.instance().getName(direction);
        Exp e = axes[axis].set;

        if (members.length == 0) {
            // No members to sort on means use default sort order.  As
            // we've already removed any sorters, we're done.
            return;
        } else {
            Exp membersExp = (members.length == 0)
                             ? null 
                             // handled above
                             : (members.length == 1) 
                                ? (Exp) members[0] 
                                : (Exp) new FunCall("()", 
                                                    Syntax.Parentheses, 
                                                    members);
            FunCall funOrder = new FunCall("Order", 
                                           Syntax.Function, 
                                           new Exp[] {
                                            e,
                                            membersExp,
                                            Literal.createSymbol(sDirection)
                                          }
                                     );
/*
XXXXXXXXXXXXX
            FunCall funOrder = new FunCall("Order", 
                                           Syntax.Function, 
                                           new Exp[] {
                                            e,
                                            (members.length == 0)
                                                ? null 
                                                // handled above
                                                : (members.length == 1) 
                                                    ? (Exp) members[0] 
                                                    : (Exp) new FunCall("()", 
                                                      Syntax.Parentheses, 
                                                      members),
                                Literal.createSymbol(sDirection)
                                          }
                                     );
*/

            axes[axis].set = funOrder;
        }
    }

    /**
     * Finds and removes existing sorts and top/bottom functions from axis.
     *
     * @pre AxisOrdinal.instance().isValid(axis)
     * @pre axis &lt; axes.length
     */
    public void removeSortFromAxis(int axis) {
        Util.assertPrecondition(AxisOrdinal.instance.isValid(axis), 
            "AxisOrdinal.instance.isValid(axis)");
        Util.assertPrecondition(axis < axes.length, "axis < axes.length");

        Walker walker = new Walker((QueryPart) axes[axis].set);
        while (walker.hasMoreElements()) {
            Object o = walker.nextElement();
            if (o instanceof FunCall) {
                FunCall funCall = (FunCall) o;
                if (!funCall.isCallTo("Order") &&
                    !isValidTopBottomNName(funCall.getFunName()))
                    continue;

                Exp e = funCall.getArg(0);
                QueryPart parent = (QueryPart) walker.getParent();
                if (parent == null) {
                    axes[axis].set = e;
                } else {
                    parent.replaceChild(walker.getOrdinal(), (QueryPart) e);
                }
            }
        }
    }


    /**
     * Calls {@link #removeSortFromAxis} first and then applies top/bottom
     * function to the axis.
     *
     * @param axis Axis ordinal
     * @param fName Name of function
     * @param n Number of members top/bottom should return
     * @param members Members to sort on
     *
     * @pre AxisOrdinal.instance().isValid(axis)
     * @pre axis &lt; axes.length
     * @pre fName != null
     * @pre isValidTopBottomNName(fName)
     * @pre members != null
     * @pre members.length > 0
     */
    public void applyTopBottomN(
            int axis, String fName, Integer n, Member[] members) {
        Util.assertPrecondition(fName != null, "fName != null");
        Util.assertPrecondition(AxisOrdinal.instance.isValid(axis), 
            "AxisOrdinal.instance.isValid(axis)");
        Util.assertPrecondition(axis < axes.length, "axis < axes.length");
        Util.assertPrecondition(members != null, "members != null");
        Util.assertPrecondition(members.length > 0, "members.length > 0");
        Util.assertPrecondition(isValidTopBottomNName(fName), 
            "isValidTopBottomNName(fName)");

        if (!isValidTopBottomNName(fName)) {
            throw Util.getRes().newMdxTopBottomInvalidFunctionName(fName);
        }

        // Find and remove any existing sorts on this axis.
        removeSortFromAxis(axis);

        Exp e = axes[axis].set;

        Exp membersExp = (members.length == 1)
            ? (Exp) members[0] 
            : (Exp) new FunCall("()", Syntax.Parentheses, members);

        FunCall funOrder = new FunCall(fName, 
                                       Syntax.Function, 
                                       new Exp[] {
                                           e,
                                           Literal.create(n),
                                           membersExp
                                        });
        axes[axis].set = funOrder;
    }

    public static boolean isValidTopBottomNName(String fName) {
        return fName.equalsIgnoreCase("TopCount") ||
            fName.equalsIgnoreCase("BottomCount") ||
            fName.equalsIgnoreCase("TopPercent") ||
            fName.equalsIgnoreCase("BottomPercent");
    }

    /**
     * Swaps the x- and y- axes.
     * Does nothing if the number of axes != 2.
     */
    public void swapAxes() {
        if (axes.length == 2) {
            Exp e0 = axes[0].set;
            boolean nonEmpty0 = axes[0].nonEmpty;
            Exp e1 = axes[1].set;
            boolean nonEmpty1 = axes[1].nonEmpty;
            axes[1].set = e0;
            axes[1].nonEmpty = nonEmpty0;
            axes[0].set = e1;
            axes[0].nonEmpty = nonEmpty1;
            // showSubtotals ???
        }
    }

    /**
     * Returns a parameter with a given name, or <code>null</code> if there is
     * no such parameter.
     */
    public Parameter lookupParam(String parameterName) {
        for (int i = 0; i < parameters.length; i++) {
            if (parameters[i].getName().equals(parameterName)) {
                return parameters[i];
            }
        }
        return null;
    }

    /**
     * Validates each parameter, calculates their usage, and removes unused
     * parameters.
     * @return the array of parameter usage counts
     */
    private int[] resolveParameters() {
        //validate definitions
        for (int i = 0; i < parameters.length; i++) {
            parameters[i].validate(this);
        }
        int[] usageCount = new int[parameters.length];
        Walker queryElements = new Walker(this);
        while (queryElements.hasMoreElements()) {
            Object queryElement = queryElements.nextElement();
            if (queryElement instanceof Parameter) {
                boolean found = false;
                for (int i = 0; i < parameters.length; i++) {
                    if (parameters[i].equals(queryElement)) {
                        usageCount[i]++;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw Util.getRes().newMdxParamNotFound(
                        ((Parameter) queryElement).getName());
                }
            }
        }
        return usageCount;
    }

    /**
     * Returns the parameters used in this query.
     **/
    public Parameter[] getParameters() {
        int[] usageCount = resolveParameters();
        // count the parameters which are currently used
        int nUsed = 0;
        for (int i = 0; i < usageCount.length; i++) {
            if (usageCount[i] > 0) {
              nUsed++;
            }
        }
        Parameter[] usedParameters = new Parameter[nUsed];
        nUsed = 0;
            for (int i = 0; i < parameters.length; i++) {
                if (usageCount[i] > 0) {
                usedParameters[nUsed++] = parameters[i];
                }
        }
        return usedParameters;
    }

    void resetParametersPrintProperty() {
        for (int i = 0; i < parameters.length; i++) {
            parameters[i].resetPrintProperty();
        }
    }

    // implement NameResolver
    public Cube getCube() {
        return mdxCube;
    }

    public SchemaReader getSchemaReader(boolean accessControlled) {
        final Role role = accessControlled 
            ? getConnection().getRole() 
            : null;
        final SchemaReader cubeSchemaReader = mdxCube.getSchemaReader(role);
        return new QuerySchemaReader(cubeSchemaReader);
    }

    /**
     * Looks up a member whose unique name is <code>s</code> from cache.
     * If the member is not in cache, returns null.
     **/
    public Member lookupMemberFromCache(String s) {
        // first look in defined members
        Iterator definedMembers = getDefinedMembers().iterator();
        while (definedMembers.hasNext()) {
            Member mdxMember = (Member) definedMembers.next();
            if (mdxMember.getUniqueName().equals(s)) {
                return mdxMember;
            }
        }
        return null;
    }

    /** Return an array of the formulas used in this query. */
    public Formula[] getFormulas() {
        return formulas;
    }
    public QueryAxis[] getAxes() {
        return axes;
    }

    /** Remove a formula from the query. If <code>failIfUsedInQuery</code> is
     * true, checks and throws an error if formula is used somewhere in the
     * query; otherwise, what??? */
    public void removeFormula(String uniqueName, boolean failIfUsedInQuery) {
        Formula formula = findFormula(uniqueName);
        if (failIfUsedInQuery && formula != null) {
            OlapElement mdxElement = formula.getElement();
            //search the query tree to see if this formula expression is used
            //anywhere (on the axes or in another formula)
            Walker walker = new Walker(this);
            while (walker.hasMoreElements()) {
                Object queryElement = walker.nextElement();
                if (!queryElement.equals(mdxElement)) {
                    continue;
                }
                // mdxElement is used in the query. lets find on on which axis
                // or formula
                String formulaType = formula.isMember() 
                    ? Util.getRes().getCalculatedMember() 
                    : Util.getRes().getCalculatedSet();

                int i = 0;
                Object parent = walker.getAncestor(i);
                Object grandParent = walker.getAncestor(i+1);
                while ((parent != null) && (grandParent != null)) {
                    if (grandParent instanceof Query) {
                        if (parent instanceof Axis) {
                            throw Util.getRes().newMdxCalculatedFormulaUsedOnAxis(
                                formulaType, 
                                uniqueName,
                                ((QueryAxis) parent).getAxisName());

                        } else if (parent instanceof Formula) {
                            String parentFormulaType = 
                                ((Formula) parent).isMember() 
                                    ? Util.getRes().getCalculatedMember() 
                                    : Util.getRes().getCalculatedSet();
                            throw Util.getRes().newMdxCalculatedFormulaUsedInFormula(
                                formulaType, uniqueName, parentFormulaType,
                                ((Formula) parent).getUniqueName());

                        } else {
                            throw Util.getRes().newMdxCalculatedFormulaUsedOnSlicer(
                                formulaType, uniqueName);
                        }
                    }
                    ++i;
                    parent = walker.getAncestor(i);
                    grandParent = walker.getAncestor(i+1);
                }
                throw Util.getRes().newMdxCalculatedFormulaUsedInQuery(
                    formulaType, uniqueName, this.toWebUIMdx());
            }
        }

        //remove formula from query
        List formulaList = new ArrayList();
        for (int i = 0; i < formulas.length; i++) {
            if (!formulas[i].getUniqueName().equalsIgnoreCase(uniqueName)) {
                formulaList.add(formulas[i]);
            }
        }

        // it has been found and removed
        this.formulas = (Formula[]) formulaList.toArray(new Formula[0]);
    }

    /**
     * Check, whether a formula can be removed from the query.
     */
    public boolean canRemoveFormula(String uniqueName) {
        Formula formula = findFormula(uniqueName);
        if (formula == null) {
            return false;
        }

        OlapElement mdxElement = formula.getElement();
        //search the query tree to see if this formula expression is used
        //anywhere (on the axes or in another formula)
        Walker walker = new Walker(this);
        while (walker.hasMoreElements()) {
            Object queryElement = walker.nextElement();
            if (!queryElement.equals(mdxElement)) {
                continue;
            }
            return false;
        }
        return true;
    }

    /** finds calculated member or set in array of formulas */
    public Formula findFormula(String uniqueName) {
        for (int i = 0; i < formulas.length; i++) {
            if (formulas[i].getUniqueName().equalsIgnoreCase(uniqueName)) {
                return formulas[i];
            }
        }
        return null;
    }

    /** finds formula by name and renames it to new name */
    public void renameFormula(String uniqueName, String newName) {
        Formula formula = findFormula(uniqueName);
        if (formula == null) {
            throw Util.getRes().newMdxFormulaNotFound(
                "formula", uniqueName, toWebUIMdx());
        }
        formula.rename(newName);
    }

    List getDefinedMembers() {
        List definedMembers = new ArrayList();
        for (int i = 0; i < formulas.length; i++) {
            if (formulas[i].isMember() && formulas[i].getElement() != null) {
                definedMembers.add(formulas[i].getElement());
            }
        }
        return definedMembers;
    }

    /** finds axis by index and sets flag to show empty cells on that axis*/
    public void setAxisShowEmptyCells(int axis, boolean showEmpty) {
        if (axis >= axes.length) {
            throw Util.getRes().newMdxAxisShowSubtotalsNotSupported(
                    new Integer(axis));
        }
        axes[axis].nonEmpty = !showEmpty;
    }

    /** finds axis by index and adds/removes subtotals. It finds all
     * hierarchies used on axis, then for every hierarchy it finds the
     * expression, where it's used. Using that expression, it executes mdx
     * query to generate array of mdxMembers. Based on
     * <code>showSubtotals</code> it modifies array of mdxMembers and
     * substitutes expression with set, which is created based on array of
     * mdxMembers */
    public void setAxisShowSubtotals(int axis, boolean showSubtotals) {
        if (axis >= axes.length || axis < 0) {
            //based on Prashant request: don't throw error-just return
            return;
        }

        String sCalculatedMembers = null;
        if (formulas != null) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            for (int i = 0; i < formulas.length; i++) {
                if (i == 0) {
                    pw.print("with ");
                } else {
                    pw.print("  ");
                }
                formulas[i].unparse(pw);
                pw.println();
            }
            sCalculatedMembers = sw.toString();
        }

        Hierarchy[] mdxHierarchies = collectHierarchies(axes[axis]);
        for (int j = 0; j < mdxHierarchies.length; j++) {
            Walker walker = findHierarchy(mdxHierarchies[j]);
            Exp e = (Exp) walker.currentElement();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.unparse(pw);
            String sExp = sw.toString();
            String sQuery = "";
            if (sCalculatedMembers != null)
                sQuery = sCalculatedMembers;
            sQuery += "select {" + sExp + "} on columns from [" +
                mdxCube.getUniqueName() + "]";
            Member[] mdxMembers = mdxCube.getMembersForQuery(
                sQuery, getDefinedMembers());
            java.util.Set set = new HashSet();
            if (showSubtotals) {
                // we need to put all those members plus all their parent
                // members
                for (int i = 0; i < mdxMembers.length; i++){
                    if (!set.contains(mdxMembers[i])) {
                        Member[] parentMembers =
                            mdxMembers[i].getAncestorMembers();
                        for (int k = parentMembers.length - 1; k >= 0; k--) {
                            if (!set.contains(parentMembers[k])) {
                                set.add(parentMembers[k]);
                            }
                        }
                        set.add(mdxMembers[i]);
                    }
                }
            } else {
                //we need to put only members with biggest depth
                int nMaxDepth = 0;
                for (int i = 0; i < mdxMembers.length; i++){
                    if (nMaxDepth < mdxMembers[i].getLevel().getDepth()) {
                        nMaxDepth = mdxMembers[i].getLevel().getDepth();
                    }
                }
                for (int i = 0; i < mdxMembers.length; i++){
                    if (nMaxDepth == mdxMembers[i].getLevel().getDepth()) {
                        set.add(mdxMembers[i]);
                    }
                }
            }
            Member[] goodMembers = (Member[]) set.toArray(new Member[0]);
            filterHierarchy(mdxHierarchies[j], axis, goodMembers);
        }
        axes[axis].setShowSubtotals(showSubtotals);
    }

    /** returns <code>Hierarchy[]</code> used on <code>axis</code>. It calls
     * collectHierarchies() */
    public Hierarchy[] getMdxHierarchiesOnAxis(int axis) {
        if (axis >= axes.length) {
            throw Util.getRes().newMdxAxisShowSubtotalsNotSupported(new Integer(axis));
        }
        return (axis == AxisOrdinal.SLICER)
            ? collectHierarchies((QueryPart) slicer)
            : collectHierarchies(axes[axis]);
    }

    /**
     * Default implementation of {@link Exp.Resolver}.
     *
     * <p>Uses a stack to help us guess the type of our parent expression
     * before we've completely resolved our children -- necessary,
     * unfortunately, when figuring out whether the "*" operator denotes
     * multiplication or crossjoin.
     *
     * <p>Keeps track of which nodes have already been resolved, so we don't
     * try to resolve nodes which have already been resolved. (That would not
     * be wrong, but can cause resolution to be an <code>O(2^N)</code>
     * operation.)
     */
    private class StackResolver implements Exp.Resolver {
        private final Stack stack = new Stack();
        private final FunTable funTable;
        private boolean haveCollectedParameters;
        private java.util.Set resolvedNodes = new HashSet();

        public StackResolver(FunTable funTable) {
            this.funTable = funTable;
        }

        public Query getQuery() {
            return Query.this;
        }

        public Exp resolveChild(Exp exp) {
            if (!resolvedNodes.add(exp)) {
                return exp; // already resolved
            }
            stack.push(exp);
            try {
                final Exp resolved = exp.resolve(this);
                resolvedNodes.add(resolved);
                return resolved;
            } finally {
                stack.pop();
            }
        }

        public Parameter resolveChild(Parameter parameter) {
            if (!resolvedNodes.add(parameter)) {
                return parameter; // already resolved
            }
            stack.push(parameter);
            try {
                final Parameter resolved = (Parameter) parameter.resolve(this);
                resolvedNodes.add(resolved);
                return resolved;
            } finally {
                stack.pop();
            }
        }

        public void resolveChild(MemberProperty memberProperty) {
            if (!resolvedNodes.add(memberProperty)) {
                return; // already resolved
            }
            stack.push(memberProperty);
            try {
                memberProperty.resolve(this);
            } finally {
                stack.pop();
            }
        }

        public void resolveChild(QueryAxis axis) {
            if (!resolvedNodes.add(axis)) {
                return; // already resolved
            }
            stack.push(axis);
            try {
                axis.resolve(this);
            } finally {
                stack.pop();
            }
        }

        public void resolveChild(Formula formula) {
            if (!resolvedNodes.add(formula)) {
                return; // already resolved
            }
            stack.push(formula);
            try {
                formula.resolve(this);
            } finally {
                stack.pop();
            }
        }

        public boolean requiresExpression() {
            return requiresExpression(stack.size() - 1);
        }

        private boolean requiresExpression(int n) {
            if (n < 1) {
                return false;
            }
            final Object parent = stack.get(n - 1);
            if (parent instanceof Formula) {
                return true;
            } else if (parent instanceof FunCall) {
                final FunCall funCall = (FunCall) parent;
                if (funCall.isCallToTuple()) {
                    return requiresExpression(n - 1);
                } else {
                    int k = whichArg(funCall, stack.get(n));
                    Util.assertTrue(k >= 0);
                    return funTable.requiresExpression(funCall, k, this);
                }
            } else {
                return false;
            }
        }

        public FunTable getFunTable() {
            return funTable;
        }

        public Parameter createOrLookupParam(FunCall funCall) {
            Util.assertTrue(funCall.getArg(0) instanceof Literal,
                "The name of parameter must be a quoted string");
            String name = (String) ((Literal) funCall.getArg(0)).getValue();
            Parameter param = lookupParam(name);
            ParameterFunDef funDef = (ParameterFunDef) funCall.getFunDef();
            if (funDef.isDefinition()) {
                if (param != null) {
                    if (param.getDefineCount() > 0) {
                        throw Util.newInternal("Parameter '" + name +
                                "' is defined more than once");
                    } else {
                        param.incrementDefineCount();
                        return param;
                    }
                }
                param = new Parameter(funDef.parameterName, funCall.getType(),
                        funCall.getHierarchy(), funDef.exp,
                        funDef.parameterDescription);

                // Append it to the array of known parameters.
                Parameter[] old = parameters;
                final int count = old.length;
                parameters = new Parameter[count + 1];
                System.arraycopy(old, 0, parameters, 0, count);
                parameters[count] = param;
                return param;
            } else {
                if (param != null) {
                    return param;
                }
                // We're looking at a ParamRef("p"), and its defining
                // Parameter("p") hasn't been seen yet. Just this once, walk
                // over the entire query finding parameter definitions.
                if (!haveCollectedParameters) {
                    haveCollectedParameters = true;
                    collectParameters();
                    param = lookupParam(name);
                    if (param != null) {
                        return param;
                    }
                }
                throw Util.newInternal("Parameter '" + name +
                        "' is referenced but never defined");
            }
        }

        private void collectParameters() {
            final Walker walker = new Walker(Query.this);
            while (walker.hasMoreElements()) {
                final Object o = walker.nextElement();
                if (o instanceof Parameter) {
                    // Parameter has already been resolved.
                    ;
                } else if (o instanceof FunCall) {
                    final FunCall call = (FunCall) o;
                    if (call.getFunName().equalsIgnoreCase("Parameter")) {
                        // Parameter definition which has not been resolved
                        // yet. Resolve it and add it to the list of parameters.
                        // Because we're resolving out out of the proper order,
                        // the resolver doesn't hold the correct ancestral
                        // context, but it should be OK.
                        final Parameter param = (Parameter) resolveChild(call);
                        param.resetDefineCount();
                    }
                }
            }
        }

        /*
        private void replace(QueryPart oldChild, QueryPart newChild) {
            QueryPart parent = (QueryPart) stack.get(stack.size() - 2);
            int childIndex = whichArg(parent, oldChild);
            Util.assertTrue(childIndex >= 0);
            parent.replaceChild(childIndex, newChild);
        }
*/

        private int whichArg(final Object node, final Object arg) {
            if (node instanceof Walkable) {
                final Object[] children = ((Walkable) node).getChildren();
                for (int i = 0; i < children.length; i++) {
                    if (children[i] == arg) {
                        return i;
                    }
                }
            }
            return -1;
        }
    }

    /**
     * Source of metadata within the scope of a query.
     *
     * <p>Note especially that {@link #getCalculatedMember(String[])}
     * returns the calculated members defined in this query.
     */
    private class QuerySchemaReader extends DelegatingSchemaReader {
        public QuerySchemaReader(SchemaReader cubeSchemaReader) {
            super(cubeSchemaReader);
        }

        public Member getMemberByUniqueName(String[] uniqueNameParts,
                                            boolean failIfNotFound) {
            final String uniqueName = Util.implode(uniqueNameParts);
            Member member = lookupMemberFromCache(uniqueName);
            if (member == null) {
                // Not a calculated member in the query, so go to the cube.
                member = schemaReader.getMemberByUniqueName(uniqueNameParts,
                    failIfNotFound);
            }
            return member;
        }

        public Member getCalculatedMember(String[] nameParts) {
            final String uniqueName = Util.implode(nameParts);
            return lookupMemberFromCache(uniqueName);
        }

        public List getCalculatedMembers(Hierarchy hierarchy) {
            List definedMembers = getDefinedMembers();
            List result = new ArrayList();
            for (int i = 0; i < definedMembers.size(); i++) {
                Member member = (Member) definedMembers.get(i);
                if (member.getHierarchy().equals(hierarchy)) {
                    result.add(member);
                }
            }
            return result;
        }

        public OlapElement getElementChild(OlapElement parent, String s) {
            // first look in cube
            OlapElement mdxElement = schemaReader.getElementChild(parent, s);
            if (mdxElement != null) {
                return mdxElement;
            }
            // then look in defined members (removed sf#1084651)

            // then in defined sets
            for (int i = 0; i < formulas.length; i++) {
                Formula formula = formulas[i];
                if (formula.isMember()) {
                    continue;       // have already done these
                }
                if (formula.getNames()[0].equals(s)) {
                    return formula.getMDXSet();
                }
            }

            return mdxElement;
        }

        public OlapElement lookupCompound(OlapElement parent, 
                                          String[] names,
                                          boolean failIfNotFound, 
                                          int category) {
            // First look to ourselves.
            switch (category) {
            case Category.Unknown:
            case Category.Member:
                if (parent == mdxCube) {
                    final Member calculatedMember = getCalculatedMember(names);
                    if (calculatedMember != null) {
                        return calculatedMember;
                    }
                }
            }
            // Then delegate to the next reader.
            OlapElement olapElement = super.lookupCompound(parent, 
                                                           names,
                                                           failIfNotFound, 
                                                           category);
            if (olapElement instanceof Member) {
                Member member = (Member) olapElement;
                final Formula formula = (Formula)
                    member.getPropertyValue(Property.PROPERTY_FORMULA);
                if (formula != null) {
                    // This is a calculated member defined against the cube.
                    // Create a free-standing formula using the same
                    // expression, then use the member defined in that formula.
                    final Formula formulaClone = (Formula) formula.clone();
                    formulaClone.createElement(Query.this);
                    formulaClone.resolve(createResolver());
                    olapElement = formulaClone.getMdxMember();
                }
            }
            return olapElement;
        }
    }
}

// End Query.java

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
import mondrian.olap.type.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
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
     * public-private: This must be public because it is still accessed in rolap.RolapCube
     */
    public Formula[] formulas;

    /**
     * public-private: This must be public because it is still accessed in rolap.RolapConnection
     */
    public QueryAxis[] axes;

    /**
     * public-private: This must be public because it is still accessed in rolap.RolapResult
     */
    public Exp slicer;

    /**
     * Definitions of all parameters used in this query.
     */
    private Parameter[] parameters;

    /**
     * Cell properties. Not currently used.
     */
    private final QueryPart[] cellProps;

    /**
     * Cube this query belongs to.
     */
    private final Cube cube;

    private final Connection connection;

    /** Constructs a Query. */
    public Query(
            Connection connection,
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
    public Query(
            Connection connection,
            Cube mdxCube,
            Formula[] formulas,
            QueryAxis[] axes,
            Exp slicer,
            QueryPart[] cellProps,
            Parameter[] parameters) {
        this.connection = connection;
        this.cube = mdxCube;
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


    public Validator createValidator() {
        return new StackValidator(BuiltinFunTable.instance());
    }

    public Object clone() throws CloneNotSupportedException {
        return new Query(connection,
                cube,
                Formula.cloneArray(formulas),
                QueryAxis.cloneArray(axes),
                (slicer == null) ? null : (Exp) slicer.clone(),
                cellProps,
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
        return toMdx();
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
        resolve(createValidator()); // resolve self and children
    }

    /**
     * Performs type-checking and validates internal consistency of a query,
     * using a custom resolver.
     *
     * @param resolver Custom resolver
     */
    void resolve(Validator resolver) {
        if (formulas != null) {
            //resolving of formulas should be done in two parts
            //because formulas might depend on each other, so all calculated
            //mdx elements have to be defined during resolve
            for (int i = 0; i < formulas.length; i++) {
                formulas[i].createElement(resolver.getQuery());
            }
            for (int i = 0; i < formulas.length; i++) {
                resolver.validate(formulas[i]);
            }
        }

        if (axes != null) {
            for (int i = 0; i < axes.length; i++) {
                resolver.validate(axes[i]);
            }
        }
        if (slicer != null) {
            setSlicer(resolver.validate(slicer));
        }

        // Now that out Parameters have been created (from FunCall's to
        // Parameter() and ParamRef()), resolve them.
        for (int i = 0; i < parameters.length; i++) {
            parameters[i] = resolver.validate(parameters[i]);
        }
        resolveParameters();

        // Make sure that no dimension is used on more than one axis.
        final Dimension[] dimensions = getCube().getDimensions();
        for (int i = 0; i < dimensions.length; i++) {
            Dimension dimension = dimensions[i];
            int useCount = 0;
            for (int j = -1; j < axes.length; j++) {
                final Exp axisExp;
                if (j < 0) {
                    if (slicer == null) {
                        continue;
                    }
                    axisExp = slicer;
                } else {
                    axisExp = axes[j].set;
                }
                if (axisExp.getTypeX().usesDimension(dimension)) {
                    ++useCount;
                }
            }
            if (useCount > 1) {
                throw MondrianResource.instance()
                        .newDimensionInIndependentAxes(
                                dimension.getUniqueName());
            }
        }
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
        if (cube != null) {
            pw.println("from [" + cube.getName() + "]");
        }
        if (slicer != null) {
            pw.print("where ");
            slicer.unparse(pw);
            pw.println();
        }
    }

    public String toMdx() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new QueryPrintWriter(sw);
        unparse(pw);
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
        return toMdx();
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

    /**
     * Normalize slicer into a tuple of members. For example, '[Time]' becomes
     * '([Time].DefaultMember)'.
     *
     * <p>todo: Make slicer an Axis, not an Exp, and
     * put this code inside Axis.
     */
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
        Dimension[] mdxDimensions = cube.getDimensions();
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
     * Returns the hierarchies in an expression.
     */
    private Hierarchy[] collectHierarchies(Exp queryPart) {
        Type type = queryPart.getTypeX();
        if (type instanceof SetType) {
            type = ((SetType) type).getElementType();
        }
        if (type instanceof TupleType) {
            final Type[] types = ((TupleType) type).elementTypes;
            ArrayList hierarchyList = new ArrayList();
            for (int i = 0; i < types.length; i++) {
                final Hierarchy hierarchy = types[i].getHierarchy();
                hierarchyList.add(hierarchy);
            }
            return (Hierarchy[])
                    hierarchyList.toArray(new Hierarchy[hierarchyList.size()]);
        }
        return new Hierarchy[] {type.getHierarchy()};
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
        final Exp exp = param.quickParse(value, this);
        param.setValue(exp);
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

    public Cube getCube() {
        return cube;
    }

    public SchemaReader getSchemaReader(boolean accessControlled) {
        final Role role = accessControlled
            ? getConnection().getRole()
            : null;
        final SchemaReader cubeSchemaReader = cube.getSchemaReader(role);
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
                    formulaType, uniqueName, this.toMdx());
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
                "formula", uniqueName, toMdx());
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

    /**
     * Returns <code>Hierarchy[]</code> used on <code>axis</code>. It calls
     * {@link #collectHierarchies}.
     */
    public Hierarchy[] getMdxHierarchiesOnAxis(int axis) {
        if (axis >= axes.length) {
            throw Util.getRes().newMdxAxisShowSubtotalsNotSupported(new Integer(axis));
        }
        return collectHierarchies(
                (axis == AxisOrdinal.SLICER) ?
                slicer :
                axes[axis].set);
    }

    /**
     * Default implementation of {@link Validator}.
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
    private class StackValidator implements Validator {
        private final Stack stack = new Stack();
        private final FunTable funTable;
        private boolean haveCollectedParameters;
        private java.util.Set resolvedNodes = new HashSet();

        public StackValidator(FunTable funTable) {
            this.funTable = funTable;
        }

        public Query getQuery() {
            return Query.this;
        }

        public Exp validate(Exp exp) {
            if (!resolvedNodes.add(exp)) {
                return exp; // already resolved
            }
            stack.push(exp);
            try {
                final Exp resolved = exp.accept(this);
                resolvedNodes.add(resolved);
                return resolved;
            } finally {
                stack.pop();
            }
        }

        public Parameter validate(Parameter parameter) {
            if (!resolvedNodes.add(parameter)) {
                return parameter; // already resolved
            }
            stack.push(parameter);
            try {
                final Parameter resolved = (Parameter) parameter.accept(this);
                resolvedNodes.add(resolved);
                return resolved;
            } finally {
                stack.pop();
            }
        }

        public void validate(MemberProperty memberProperty) {
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

        public void validate(QueryAxis axis) {
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

        public void validate(Formula formula) {
            if (!resolvedNodes.add(formula)) {
                return; // already resolved
            }
            stack.push(formula);
            try {
                formula.accept(this);
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
            if (funDef.getName().equals("Parameter")) {
                if (param != null) {
                    if (param.getDefineCount() > 0) {
                        throw Util.newInternal("Parameter '" + name +
                                "' is defined more than once");
                    } else {
                        param.incrementDefineCount();
                        return param;
                    }
                }
                final Exp arg1 = funCall.getArg(1);
                final Type type;
                final int category = funCall.getCategory();
                switch (category) {
                case Category.Member:
                    Dimension dimension = (Dimension) arg1;
                    type = new MemberType(dimension.getHierarchy(), null, null);
                    break;
                case Category.String:
                    type = new StringType();
                    break;
                case Category.Numeric:
                    type = new NumericType();
                    break;
                default:
                    throw Category.instance.badValue(category);
                }
                param = new Parameter(
                        funDef.parameterName, category,
                        funDef.exp, funDef.parameterDescription, type);

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
                        final Parameter param = (Parameter) validate(call);
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
                if (parent == cube) {
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
                    formulaClone.accept(createValidator());
                    olapElement = formulaClone.getMdxMember();
                }
            }
            return olapElement;
        }
    }

    /**
     * PrintWriter used for unparsing queries. Remembers which parameters have
     * been printed. The first time, they print themselves as "Parameter";
     * subsequent times as "ParamRef".
     */
    static class QueryPrintWriter extends PrintWriter {
        final HashSet parameters = new HashSet();

        QueryPrintWriter(Writer writer) {
            super(writer);
        }
    }
}

// End Query.java

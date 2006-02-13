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
import mondrian.calc.*;
import mondrian.calc.impl.BetterExpCompiler;
import mondrian.mdx.*;
import mondrian.olap.fun.FunUtil;
import mondrian.olap.fun.ParameterFunDef;
import mondrian.olap.type.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.*;

import java.io.*;
import java.util.*;

/**
 * <code>Query</code> is an MDX query.
 *
 * <p>It is created by calling {@link Connection#parseQuery},
 * and executed by calling {@link Connection#execute},
 * to return a {@link Result}.
 **/
public class Query extends QueryPart {

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
    public QueryAxis slicerAxis;

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
    public Calc[] axisCalcs;
    public Calc slicerCalc;

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

    /**
     * Creates a Query.
     */
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
        if (slicer == null) {
            this.slicerAxis = null;
        } else {
            this.slicerAxis =
                    new QueryAxis(
                            false, slicer, AxisOrdinal.Slicer,
                            QueryAxis.SubtotalVisibility.Undefined);
        }
        this.cellProps = cellProps;
        this.parameters = parameters;
        resolve();
    }

    /**
     * Adds a new formula specifying a set
     * to an existing query.
     */
    public void addFormula(String[] names, Exp exp) {
        Formula newFormula = new Formula(names, exp);
        int formulaCount = 0;
        if (formulas.length > 0) {
            formulaCount = formulas.length;
        }
        Formula[] newFormulas = new Formula[formulaCount + 1];
        for (int i = 0; i < formulaCount; i++ ) {
            newFormulas[i] = formulas[i];
        }
        newFormulas[formulaCount] = newFormula;
        formulas = newFormulas;
        resolve();
    }

    /**
     * Adds a new formula specifying a member
     * to an existing query.
     */
    public void addFormula(
            String[] names,
            Exp exp,
            MemberProperty[] memberProperties) {
        Formula newFormula = new Formula(names, exp, memberProperties);
        int formulaCount = 0;
        if (formulas.length > 0) {
            formulaCount = formulas.length;
        }
        Formula[] newFormulas = new Formula[formulaCount + 1];
        for (int i = 0; i < formulaCount; i++ ) {
            newFormulas[i] = formulas[i];
        }
        newFormulas[formulaCount] = newFormula;
        formulas = newFormulas;
        resolve();
    }

    public Validator createValidator() {
        return new StackValidator(connection.getSchema().getFunTable());
    }

    public Object clone() throws CloneNotSupportedException {
        return new Query(connection,
                cube,
                Formula.cloneArray(formulas),
                QueryAxis.cloneArray(axes),
                (slicerAxis == null) ? null : (Exp) slicerAxis.clone(),
                cellProps,
                Parameter.cloneArray(parameters));
    }

    public Query safeClone() {
        try {
            return (Query) clone();
        } catch (CloneNotSupportedException e) {
            throw Util.newInternal(e, "Query.clone() failed");
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
            AxisOrdinal correctOrdinal = AxisOrdinal.get(i);
            if (axes[i].getAxisOrdinal() != correctOrdinal) {
                for (int j = i + 1; j < axes.length; j++) {
                    if (axes[j].getAxisOrdinal() == correctOrdinal) {
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
        final Validator validator = createValidator();
        resolve(validator); // resolve self and children
        // Create a dummy result so we can use its evaluator
        final Evaluator evaluator = RolapUtil.createEvaluator(this);
        ExpCompiler compiler = createCompiler(evaluator, validator);
        compile(compiler);
    }

    /**
     * Generates compiled forms of all expressions.
     *
     * @param compiler Compiler
     */
    private void compile(ExpCompiler compiler) {
        if (formulas != null) {
            for (int i = 0; i < formulas.length; i++) {
                formulas[i].compile();
            }
        }

        if (axes != null) {
            axisCalcs = new Calc[axes.length];
            for (int i = 0; i < axes.length; i++) {
                axisCalcs[i] = axes[i].compile(compiler);
            }
        }
        if (slicerAxis != null) {
            slicerCalc = slicerAxis.compile(compiler);
        }
    }

    /**
     * Performs type-checking and validates internal consistency of a query.
     *
     * @param validator Validator
     */
    void resolve(Validator validator) {
        if (formulas != null) {
            //resolving of formulas should be done in two parts
            //because formulas might depend on each other, so all calculated
            //mdx elements have to be defined during resolve
            for (int i = 0; i < formulas.length; i++) {
                formulas[i].createElement(validator.getQuery());
            }
            for (int i = 0; i < formulas.length; i++) {
                validator.validate(formulas[i]);
            }
        }

        if (axes != null) {
            for (int i = 0; i < axes.length; i++) {
                validator.validate(axes[i]);
            }
        }
        if (slicerAxis != null) {
            slicerAxis.validate(validator);
        }

        // Now that out Parameters have been created (from FunCall's to
        // Parameter() and ParamRef()), resolve them.
        for (int i = 0; i < parameters.length; i++) {
            parameters[i] = validator.validate(parameters[i]);
        }
        resolveParameters();

        // Make sure that no dimension is used on more than one axis.
        final Dimension[] dimensions = getCube().getDimensions();
        for (int i = 0; i < dimensions.length; i++) {
            Dimension dimension = dimensions[i];
            int useCount = 0;
            for (int j = -1; j < axes.length; j++) {
                final QueryAxis axisExp;
                if (j < 0) {
                    if (slicerAxis == null) {
                        continue;
                    }
                    axisExp = slicerAxis;
                } else {
                    axisExp = axes[j];
                }
                if (axisExp.exp.getType().usesDimension(dimension, false)) {
                    ++useCount;
                }
            }
            if (useCount > 1) {
                throw MondrianResource.instance().DimensionInIndependentAxes.ex(
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
        if (slicerAxis != null) {
            pw.print("where ");
            slicerAxis.unparse(pw);
            pw.println();
        }
    }

    public String toMdx() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new QueryPrintWriter(sw);
        unparse(pw);
        return sw.toString();
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
        if (slicerAxis != null) {
            list.add(slicerAxis);
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
            slicerAxis = (QueryAxis) with; // replace slicer
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
        throw Util.newInternal(
                "Query child ordinal " + i0 + " out of range (there are " +
                axes.length + " axes, " + formulas.length + " formula)");
    }

    public QueryAxis getSlicerAxis() {
        return slicerAxis;
    }

    public void setSlicerAxis(QueryAxis axis) {
        this.slicerAxis = axis;
    }

    /**
     * Adds a level to an axis expression.
     *
     * @pre AxisOrdinal.enumeration.isValid(axis)
     * @pre axis &lt; axes.length
     */
    public void addLevelToAxis(int axis, Level level) {
        Util.assertPrecondition(AxisOrdinal.enumeration.isValid(axis),
                "AxisOrdinal.enumeration.isValid(axis)");
        Util.assertPrecondition(axis < axes.length, "axis < axes.length");
        axes[axis].addLevel(level);
    }

    /**
     * Returns the hierarchies in an expression.
     */
    private Hierarchy[] collectHierarchies(Exp queryPart) {
        Type type = queryPart.getType();
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
            throw MondrianResource.instance().MdxParamNotFound.ex(parameterName);
        }
        final Exp exp = quickParse(param.getCategory(), value, this);
        param.setValue(exp);
    }

    private static Exp quickParse(int category, String value, Query query) {
        switch (category) {
        case Category.Numeric:
            return Literal.create(new Double(value));
        case Category.String:
            return Literal.createString(value);
        case Category.Member:
            Member member = (Member) Util.lookup(query, Util.explode(value));
            return new MemberExpr(member);
        default:
            throw Category.instance.badValue(category);
        }
    }
    
    /**
     * Swaps the x- and y- axes.
     * Does nothing if the number of axes != 2.
     */
    public void swapAxes() {
        if (axes.length == 2) {
            Exp e0 = axes[0].exp;
            boolean nonEmpty0 = axes[0].nonEmpty;
            Exp e1 = axes[1].exp;
            boolean nonEmpty1 = axes[1].nonEmpty;
            axes[1].exp = e0;
            axes[1].nonEmpty = nonEmpty0;
            axes[0].exp = e1;
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
                    throw MondrianResource.instance().MdxParamNotFound.ex(
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
            if (Util.equalName(mdxMember.getUniqueName(), s)) {
                return mdxMember;
            }
        }
        return null;
    }

    /**
     * Looks up a named set.
     */
    private NamedSet lookupNamedSet(String name) {
        for (int i = 0; i < formulas.length; i++) {
            Formula formula = formulas[i];
            if (!formula.isMember() &&
                    formula.getElement() != null &&
                    formula.getName().equals(name)) {
                return (NamedSet) formula.getElement();
            }
        }
        return null;
    }

    /**
     * Returns an array of the formulas used in this query.
     */
    public Formula[] getFormulas() {
        return formulas;
    }

    /**
     * Returns an array of this query's axes.
     */
    public QueryAxis[] getAxes() {
        return axes;
    }

    /**
     * Remove a formula from the query. If <code>failIfUsedInQuery</code> is
     * true, checks and throws an error if formula is used somewhere in the
     * query.
     */
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
                    ? MondrianResource.instance().CalculatedMember.str()
                    : MondrianResource.instance().CalculatedSet.str();

                int i = 0;
                Object parent = walker.getAncestor(i);
                Object grandParent = walker.getAncestor(i+1);
                while ((parent != null) && (grandParent != null)) {
                    if (grandParent instanceof Query) {
                        if (parent instanceof Axis) {
                            throw MondrianResource.instance().MdxCalculatedFormulaUsedOnAxis.ex(
                                formulaType,
                                uniqueName,
                                ((QueryAxis) parent).getAxisName());

                        } else if (parent instanceof Formula) {
                            String parentFormulaType =
                                ((Formula) parent).isMember()
                                    ? MondrianResource.instance().CalculatedMember.str()
                                    : MondrianResource.instance().CalculatedSet.str();
                            throw MondrianResource.instance().MdxCalculatedFormulaUsedInFormula.ex(
                                formulaType, uniqueName, parentFormulaType,
                                ((Formula) parent).getUniqueName());

                        } else {
                            throw MondrianResource.instance().MdxCalculatedFormulaUsedOnSlicer.ex(
                                formulaType, uniqueName);
                        }
                    }
                    ++i;
                    parent = walker.getAncestor(i);
                    grandParent = walker.getAncestor(i+1);
                }
                throw MondrianResource.instance().MdxCalculatedFormulaUsedInQuery.ex(
                    formulaType, uniqueName, this.toMdx());
            }
        }

        // remove formula from query
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
            throw MondrianResource.instance().MdxFormulaNotFound.ex(
                "formula", uniqueName, toMdx());
        }
        formula.rename(newName);
    }

    List getDefinedMembers() {
        List definedMembers = new ArrayList();
        for (int i = 0; i < formulas.length; i++) {
            final Formula formula = formulas[i];
            if (formula.isMember() &&
                    formula.getElement() != null &&
                    getConnection().getRole().canAccess(formula.getElement())) {
                definedMembers.add(formula.getElement());
            }
        }
        return definedMembers;
    }

    /** finds axis by index and sets flag to show empty cells on that axis*/
    public void setAxisShowEmptyCells(int axis, boolean showEmpty) {
        if (axis >= axes.length) {
            throw MondrianResource.instance().MdxAxisShowSubtotalsNotSupported.ex(
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
            throw MondrianResource.instance().MdxAxisShowSubtotalsNotSupported.ex(
                    new Integer(axis));
        }
        QueryAxis queryAxis = (axis == AxisOrdinal.SlicerOrdinal) ?
                slicerAxis :
                axes[axis];
        return collectHierarchies(queryAxis.exp);
    }

    public Calc compileExpression(Exp exp, boolean scalar) {
        Evaluator evaluator = RolapEvaluator.create(this);
        final Validator validator = createValidator();
        final ExpCompiler compiler = createCompiler(evaluator, validator);
        Calc calc;
        if (scalar) {
            calc = compiler.compileScalar(exp, false);
        } else {
            calc = exp.accept(compiler);
        }
        return calc;
    }

    private ExpCompiler createCompiler(
            Evaluator evaluator, final Validator validator) {
        ExpCompiler compiler = new BetterExpCompiler(evaluator, validator);
        final int expDeps = MondrianProperties.instance().TestExpDependencies.get();
        if (expDeps > 0) {
            compiler = RolapUtil.createDependencyTestingCompiler(compiler);
        }
        return compiler;
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
        private final Map resolvedNodes = new HashMap();
        private final Object placeHolder = "dummy";

        /**
         * Creates a StackValidator.
         *
         * @pre funTable != null
         */
        public StackValidator(FunTable funTable) {
            Util.assertPrecondition(funTable != null, "funTable != null");
            this.funTable = funTable;
        }

        public Query getQuery() {
            return Query.this;
        }

        public Exp validate(Exp exp, boolean scalar) {
            Exp resolved;
            try {
                resolved = (Exp) resolvedNodes.get(exp);
            } catch (ClassCastException e) {
                // A classcast exception will occur if there is a String
                // placeholder in the map.
                throw Util.newInternal(e,
                        "Infinite recursion encountered while validating " +
                        exp);
            }
            if (resolved == null) {
                try {
                    stack.push(exp);
                    // Put in a placeholder while we're resolving to prevent
                    // recursion.
                    resolvedNodes.put(exp, placeHolder);
                    resolved = exp.accept(this);
                    Util.assertTrue(resolved != null);
                    resolvedNodes.put(exp, resolved);
                } finally {
                    stack.pop();
                }
            }

            if (scalar) {
                final Type type = resolved.getType();
                if (!TypeUtil.canEvaluate(type)) {
                    String exprString = Util.unparse(resolved);
                    throw MondrianResource.instance().MdxMemberExpIsSet.ex(exprString);
                }
            }

            return resolved;
        }

        public Parameter validate(Parameter parameter) {
            Parameter resolved = (Parameter) resolvedNodes.get(parameter);
            if (resolved != null) {
                return parameter; // already resolved
            }
            try {
                stack.push(parameter);
                resolvedNodes.put(parameter, placeHolder);
                resolved = (Parameter) parameter.accept(this);
                resolvedNodes.put(parameter, resolved);
                return resolved;
            } finally {
                stack.pop();
            }
        }

        public void validate(MemberProperty memberProperty) {
            MemberProperty resolved =
                    (MemberProperty) resolvedNodes.get(memberProperty);
            if (resolved != null) {
                return; // already resolved
            }
            try {
                stack.push(memberProperty);
                resolvedNodes.put(memberProperty, placeHolder);
                memberProperty.resolve(this);
                resolvedNodes.put(memberProperty, memberProperty);
            } finally {
                stack.pop();
            }
        }

        public void validate(QueryAxis axis) {
            final QueryAxis resolved = (QueryAxis) resolvedNodes.get(axis);
            if (resolved != null) {
                return; // already resolved
            }
            try {
                stack.push(axis);
                resolvedNodes.put(axis, placeHolder);
                axis.resolve(this);
                resolvedNodes.put(axis, axis);
            } finally {
                stack.pop();
            }
        }

        public void validate(Formula formula) {
            final Formula resolved = (Formula) resolvedNodes.get(formula);
            if (resolved != null) {
                return; // already resolved
            }
            try {
                stack.push(formula);
                resolvedNodes.put(formula, placeHolder);
                formula.accept(this);
                resolvedNodes.put(formula, formula);
            } finally {
                stack.pop();
            }
        }

        public boolean canConvert(Exp fromExp, int to, int[] conversionCount) {
            return FunUtil.canConvert(fromExp, to, conversionCount);
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
                return ((Formula) parent).isMember();
            } else if (parent instanceof ResolvedFunCall) {
                final ResolvedFunCall funCall = (ResolvedFunCall) parent;
                if (funCall.getFunDef().getSyntax() == Syntax.Parentheses) {
                    return requiresExpression(n - 1);
                } else {
                    int k = whichArg(funCall, stack.get(n));
                    if (k < 0) {
                        // Arguments of call have mutated since call was placed
                        // on stack. Presumably the call has already been
                        // resolved correctly, so the answer we give here is
                        // irrelevant.
                        return false;
                    }
                    final FunDef funDef = funCall.getFunDef();
                    final int[] parameterTypes = funDef.getParameterCategories();
                    return parameterTypes[k] != Category.Set;
                }
            } else if (parent instanceof UnresolvedFunCall) {
                final UnresolvedFunCall funCall = (UnresolvedFunCall) parent;
                if (funCall.getSyntax() == Syntax.Parentheses) {
                    return requiresExpression(n - 1);
                } else {
                    int k = whichArg(funCall, stack.get(n));
                    if (k < 0) {
                        // Arguments of call have mutated since call was placed
                        // on stack. Presumably the call has already been
                        // resolved correctly, so the answer we give here is
                        // irrelevant.
                        return false;
                    }
                    return funTable.requiresExpression(funCall, k, this);
                }
            } else {
                return false;
            }
        }

        public FunTable getFunTable() {
            return funTable;
        }

        public Parameter createOrLookupParam(
                ParameterFunDef funDef,
                Exp[] args) {
            Util.assertTrue(args[0] instanceof Literal,
                "The name of parameter must be a quoted string");
            String name = (String) ((Literal) args[0]).getValue();
            Parameter param = lookupParam(name);
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
                final int category = funDef.getReturnCategory();
                final Type type = getParameterType(args[1], category);
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

        private Type getParameterType(final Exp exp, int category) {
            switch (category) {
            case Category.Member:
            case Category.Dimension:
            case Category.Hierarchy:
                assert exp instanceof DimensionExpr ||
                        exp instanceof HierarchyExpr;
                return TypeUtil.toMemberType(exp.getType());
            case Category.String:
                return new StringType();
            case Category.Numeric:
                return new NumericType();
            case Category.Integer:
                return new DecimalType(Integer.MAX_VALUE, 0);
            default:
                throw Category.instance.badValue(category);
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
                        final Parameter param = (Parameter) validate(call, false);
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

        public Member getMemberByUniqueName(
                String[] uniqueNameParts,
                boolean failIfNotFound) {
            final String uniqueName = Util.implode(uniqueNameParts);
            Member member = lookupMemberFromCache(uniqueName);
            if (member == null) {
                // Not a calculated member in the query, so go to the cube.
                member = schemaReader.getMemberByUniqueName(uniqueNameParts,
                    failIfNotFound);
            }
            if (getRole().canAccess(member)) {
                return member;
            } else {
                return null;
            }
        }

        public Member[] getLevelMembers(
                Level level, boolean includeCalculated) {
            Member[] members = super.getLevelMembers(level, false);
            if (includeCalculated) {
                members = Util.addLevelCalculatedMembers(this, level, members);
            }
            return members;
        }

        public Member getCalculatedMember(String[] nameParts) {
            final String uniqueName = Util.implode(nameParts);
            return lookupMemberFromCache(uniqueName);
        }

        public List getCalculatedMembers(Hierarchy hierarchy) {
            List result = new ArrayList();
            // Add calculated members in the cube.
            final List calculatedMembers = super.getCalculatedMembers(hierarchy);
            result.addAll(calculatedMembers);
            // Add calculated members defined in the query.
            List definedMembers = getDefinedMembers();
            for (int i = 0; i < definedMembers.size(); i++) {
                Member member = (Member) definedMembers.get(i);
                if (member.getHierarchy().equals(hierarchy)) {
                    result.add(member);
                }
            }
            return result;
        }

        public List getCalculatedMembers(Level level) {
            List hierarchyMembers = getCalculatedMembers(level.getHierarchy());
            List result = new ArrayList();
            for (int i = 0; i < hierarchyMembers.size(); i++) {
                Member member = (Member) hierarchyMembers.get(i);
                if (member.getLevel().equals(level)) {
                    result.add(member);
                }
            }
            return result;
        }

        public List getCalculatedMembers() {
            return getDefinedMembers();
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
                if (Util.equalName(formula.getNames()[0], s)) {
                    return formula.getNamedSet();
                }
            }

            return mdxElement;
        }

        public OlapElement lookupCompound(
                OlapElement parent,
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
            switch (category) {
            case Category.Unknown:
            case Category.Set:
                if (parent == cube) {
                    final NamedSet namedSet = getNamedSet(names);
                    if (namedSet != null) {
                        return namedSet;
                    }
                }
            }
            // Then delegate to the next reader.
            OlapElement olapElement = super.lookupCompound(
                    parent, names, failIfNotFound, category);
            if (olapElement instanceof Member) {
                Member member = (Member) olapElement;
                final Formula formula = (Formula)
                    member.getPropertyValue(Property.FORMULA.name);
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

        public NamedSet getNamedSet(String[] nameParts) {
            if (nameParts.length != 1) {
                return null;
            }
            return lookupNamedSet(nameParts[0]);
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

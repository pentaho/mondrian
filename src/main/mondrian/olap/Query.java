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
import mondrian.calc.ExpCompiler.ResultStyle;
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
 * to return a {@link Result}.</p>
 *
 * <h3>Query control</h3>
 *
 * <p>Most queries are model citizens, executing quickly (often using cached
 * results from previous queries), but some queries take more time, or more
 * database resources, or more results, than is reasonable. Mondrian offers
 * three ways to control rogue queries:<ul>
 *
 * <li>You can set a query timeout by setting the
 *     {@link MondrianProperties#QueryTimeout} parameter. If the query
 *     takes longer to execute than the value of this parameter, the system
 *     will kill it.</li>
 *
 * <li>The {@link MondrianProperties#QueryLimit} parameter limits the number
 *     of cells returned by a query.</li>
 *
 * <li>At any time while a query is executing, another thread can call the
 *     {@link #cancel()} method. The call to {@link Connection#execute(Query)}
 *     will throw an exception.</li>
 *
 * </ul>
 *
 * @author jhyde
 * @version $Id$
 */
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
    private final List<Parameter> parameters = new ArrayList<Parameter>();

    private final Map<String, Parameter> parametersByName =
        new HashMap<String, Parameter>();

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

    /**
     * Set of FunDefs for which alerts about non-native evaluation
     * have already been posted.
     */
    Set<FunDef> alertedNonNativeFunDefs;

    /**
     * Start time of query execution
     */
    private long startTime;

    /**
     * Query timeout, in milliseconds
     */
    private final int queryTimeout;

    /**
     * If true, cancel this query
     */
    private boolean isCanceled;

    /**
     * If not <code>null</code>, this query was notified that it
     * might cause an OutOfMemoryError.
     */
    private String outOfMemoryMsg;

    /**
     * If true, query is in the middle of execution
     */
    private boolean isExecuting;

    /**
     * Unique list of members referenced from the measures dimension.
     * Will be used to determine if cross joins can be processed natively
     * for virtual cubes.
     */
    private Set<Member> measuresMembers;

    /**
     * If true, virtual cubes can be processed using native cross joins.
     * It defaults to true, unless functions are applied on measures.
     */
    private boolean nativeCrossJoinVirtualCube;

    /**
     * Used for virtual cubes.  Contains the set of maps used to map the
     * levels referenced in a virtual cube to the columns in the underlying
     * base cubes.
     */
    private Set<Map<RolapLevel, RolapStar.Column>> virtualCubeBaseCubeMaps;

    /**
     * Maps one of the level-to-column maps stored in
     * virtualCubeBaseCubeMaps to a measure corresponding to the underlying
     * cube that the level-to-column map corrsponds to
     */
    private Map<Map<RolapLevel, RolapStar.Column>, RolapMember> levelMapToMeasureMap;

    /**
     * If true, loading schema
     */
    private boolean load;

    /**
     * How should the query be returned? Valid values are:
     *    ResultStyle.ITERABLE
     *    ResultStyle.LIST
     *    ResultStyle.MUTABLE_LIST
     * For java4, use LIST
     */
    private ResultStyle resultStyle = (Util.PreJdk15)
                ? ResultStyle.LIST : ResultStyle.ITERABLE;


    private Map<String, Object> evalCache = new HashMap<String, Object>();

    /**
     * Creates a Query.
     */
    public Query(
            Connection connection,
            Formula[] formulas,
            QueryAxis[] axes,
            String cube,
            QueryAxis slicerAxis,
            QueryPart[] cellProps,
            boolean load) {
        this(
            connection,
            Util.lookupCube(connection.getSchemaReader(), cube, true),
            formulas,
            axes,
            slicerAxis,
            cellProps,
            new Parameter[0],
            load);
    }

    /**
     * Creates a Query.
     */
    public Query(
            Connection connection,
            Cube mdxCube,
            Formula[] formulas,
            QueryAxis[] axes,
            QueryAxis slicerAxis,
            QueryPart[] cellProps,
            Parameter[] parameters,
            boolean load) {
        this.connection = connection;
        this.cube = mdxCube;
        this.formulas = formulas;
        this.axes = axes;
        normalizeAxes();
        this.slicerAxis = slicerAxis;
        this.cellProps = cellProps;
        this.parameters.addAll(Arrays.asList(parameters));
        this.isExecuting = false;
        this.queryTimeout =
            MondrianProperties.instance().QueryTimeout.get() * 1000;
        this.measuresMembers = new HashSet<Member>();
        // assume, for now, that cross joins on virtual cubes can be
        // processed natively; as we parse the query, we'll know otherwise
        this.nativeCrossJoinVirtualCube = true;
        this.load = load;
        this.alertedNonNativeFunDefs = new HashSet<FunDef>();
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
        System.arraycopy(formulas, 0, newFormulas, 0, formulaCount);
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
        System.arraycopy(formulas, 0, newFormulas, 0, formulaCount);
        newFormulas[formulaCount] = newFormula;
        formulas = newFormulas;
        resolve();
    }

    public Validator createValidator() {
        return new StackValidator(connection.getSchema().getFunTable());
    }

    public Object clone() {
        return new Query(
                connection,
                cube,
                Formula.cloneArray(formulas),
                QueryAxis.cloneArray(axes),
                (slicerAxis == null) ? null : (QueryAxis) slicerAxis.clone(),
                cellProps,
                parameters.toArray(new Parameter[parameters.size()]),
                load);
    }

    public Query safeClone() {
        return (Query) clone();
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
     *
     * @deprecated Use {@link Util#unparse(Query)}; deprecated since 2.1.2
     */
    public String getQueryString() {
        return toMdx();
    }

    /**
     * Issues a cancel request on this Query object.  Once the thread
     * running the query detects the cancel request, the query execution will
     * throw an exception. See <code>BasicQueryTest.testCancel</code> for an
     * example of usage of this method.
     */
    public void cancel() {
        isCanceled = true;
    }

    void setOutOfMemory(String msg) {
        outOfMemoryMsg = msg;
    }

    /**
     * Checks if either a cancel request has been issued on the query or
     * the execution time has exceeded the timeout value (if one has been
     * set).  Exceptions are raised if either of these two conditions are
     * met.  This method should be called periodically during query execution
     * to ensure timely detection of these events, particularly before/after
     * any potentially long running operations.
     */
    public void checkCancelOrTimeout() {
        if (!isExecuting) {
            return;
        }
        if (isCanceled) {
            throw MondrianResource.instance().QueryCanceled.ex();
        }
        if (queryTimeout > 0) {
            long currTime = System.currentTimeMillis();
            if ((currTime - startTime) >= queryTimeout) {
                throw MondrianResource.instance().QueryTimeout.ex(
                    (long) queryTimeout / 1000);
            }
        }
        if (outOfMemoryMsg != null) {
            throw new MemoryLimitExceededException(outOfMemoryMsg);
        }
    }

    /**
     * Sets the start time of query execution.  Used to detect timeout for
     * queries.
     */
    public void setQueryStartTime() {
        startTime = System.currentTimeMillis();
        isExecuting = true;
    }

    /**
     * Called when query execution has completed.  Once query execution has
     * ended, it is not possible to cancel or timeout the query until it
     * starts executing again.
     */
    public void setQueryEndExecution() {
        isExecuting = false;
    }

    /**
     * Determines whether an alert for non-native evaluation needs
     * to be posted.
     *
     * @param funDef function type to alert for
     *
     * @return true if alert should be raised
     */
    public boolean shouldAlertForNonNative(FunDef funDef) {
        return alertedNonNativeFunDefs.add(funDef);
    }

    private void normalizeAxes() {
        for (int i = 0; i < axes.length; i++) {
            AxisOrdinal correctOrdinal = AxisOrdinal.forLogicalOrdinal(i);
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
     * @return true if Query object is being accessed during schema load
     * and the property to ignore invalid members is set to true
     */
    public boolean ignoreInvalidMembers()
    {
        return load &&
            MondrianProperties.instance().IgnoreInvalidMembers.get();
    }

    /**
     * A Query's ResultStyle can only be one of the following:
     *   ResultStyle.ITERABLE
     *   ResultStyle.LIST
     *   ResultStyle.MUTABLE_LIST
     *
     * @param resultStyle
     */
    public void setResultStyle(ResultStyle resultStyle) {
        switch (resultStyle) {
        case ITERABLE :
            // For java4, use LIST
            resultStyle = (Util.PreJdk15)
                ? ResultStyle.LIST : ResultStyle.ITERABLE;
            break;
        case LIST :
        case MUTABLE_LIST:
            this.resultStyle = resultStyle;
            break;
        default :
            throw ResultStyleException.generateBadType(
                    new ResultStyle[] {
                        ResultStyle.ITERABLE,
                        ResultStyle.LIST,
                        ResultStyle.MUTABLE_LIST
                    },
                    resultStyle
                );
        }
    }
    public ResultStyle getResultStyle() {
        return resultStyle;
    }

    /**
     * Generates compiled forms of all expressions.
     *
     * @param compiler Compiler
     */
    private void compile(ExpCompiler compiler) {
        if (formulas != null) {
            for (Formula formula : formulas) {
                formula.compile();
            }
        }

        if (axes != null) {
            axisCalcs = new Calc[axes.length];
            for (int i = 0; i < axes.length; i++) {
                axisCalcs[i] = axes[i].compile(compiler,
                                    new ResultStyle[] { resultStyle });
            }
        }
        if (slicerAxis != null) {
            slicerCalc = slicerAxis.compile(compiler,
                                new ResultStyle[] { resultStyle });
        }
    }

    /**
     * Performs type-checking and validates internal consistency of a query.
     *
     * @param validator Validator
     */
    void resolve(Validator validator) {
        // Before commencing validation, create all calculated members,
        // calculated sets, and parameters.
        if (formulas != null) {
            // Resolving of formulas should be done in two parts
            // because formulas might depend on each other, so all calculated
            // mdx elements have to be defined during resolve.
            for (Formula formula : formulas) {
                formula.createElement(validator.getQuery());
            }
        }

        // Register all parameters.
        parameters.clear();
        parametersByName.clear();
        accept(
            new MdxVisitorImpl() {
                public Object visit(ParameterExpr parameterExpr) {
                    Parameter parameter = parameterExpr.getParameter();
                    if (!parameters.contains(parameter)) {
                        parameters.add(parameter);
                        parametersByName.put(parameter.getName(), parameter);
                    }
                    return null;
                }

                public Object visit(UnresolvedFunCall call) {
                    if (call.getFunName().equals("Parameter")) {
                        // Is there already a parameter with this name?
                        String parameterName =
                            ParameterFunDef.getParameterName(call.getArgs());
                        if (parametersByName.get(parameterName) != null) {
                            throw MondrianResource.instance().
                                ParameterDefinedMoreThanOnce.ex(parameterName);
                        }

                        Type type =
                            ParameterFunDef.getParameterType(call.getArgs());

                        // Create a temporary parameter. We don't know its
                        // type yet. The default of NULL is temporary.
                        Parameter parameter = new ParameterImpl(
                            parameterName, Literal.nullValue, null, type);
                        parameters.add(parameter);
                        parametersByName.put(parameterName, parameter);
                    }
                    return null;
                }
            }
        );

        // Validate formulas.
        if (formulas != null) {
            for (Formula formula : formulas) {
                validator.validate(formula);
            }
        }

        // Validate axes.
        if (axes != null) {
            for (QueryAxis axis : axes) {
                validator.validate(axis);
            }
        }
        if (slicerAxis != null) {
            slicerAxis.validate(validator);
        }

        // Make sure that no dimension is used on more than one axis.
        final Dimension[] dimensions = getCube().getDimensions();
        for (Dimension dimension : dimensions) {
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
                if (axisExp.getSet().getType().usesDimension(dimension,
                    false)) {
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
        return Util.unparse(this);
    }

    public Object[] getChildren() {
        // Chidren are axes, slicer, and formulas (in that order, to be
        // consistent with replaceChild).
        List<QueryPart> list = new ArrayList<QueryPart>();
        for (QueryAxis axis : axes) {
            list.add(axis);
        }
        if (slicerAxis != null) {
            list.add(slicerAxis);
        }
        for (Formula formula : formulas) {
            list.add(formula);
        }
        return list.toArray();
    }

    public QueryAxis getSlicerAxis() {
        return slicerAxis;
    }

    public void setSlicerAxis(QueryAxis axis) {
        this.slicerAxis = axis;
    }

    /**
     * Adds a level to an axis expression.
     */
    public void addLevelToAxis(AxisOrdinal axis, Level level) {
        assert axis != null;
        axes[axis.logicalOrdinal()].addLevel(level);
    }

    /**
     * Returns the hierarchies in an expression.
     *
     * <p>If the expression's type is a dimension with several hierarchies,
     * assumes that the expression yields a member of the first (default)
     * hierarchy of the dimension.
     *
     * <p>For example, the expression
     * <blockquote><code>Crossjoin(
     *   Hierarchize(
     *     Union(
     *       {[Time].LastSibling}, [Time].LastSibling.Children)),
     *       {[Measures].[Unit Sales], [Measures].[Store Cost]})</code>
     * </blockquote>
     *
     * has type <code>{[Time.Monthly], [Measures]}</code> even though
     * <code>[Time].LastSibling</code> might return a member of either
     * [Time.Monthly] or [Time.Weekly].
     */
    private Hierarchy[] collectHierarchies(Exp queryPart) {
        Type exprType = queryPart.getType();
        if (exprType instanceof SetType) {
            exprType = ((SetType) exprType).getElementType();
        }
        if (exprType instanceof TupleType) {
            final Type[] types = ((TupleType) exprType).elementTypes;
            ArrayList<Hierarchy> hierarchyList = new ArrayList<Hierarchy>();
            for (Type type : types) {
                hierarchyList.add(getTypeHierarchy(type));
            }
            return hierarchyList.toArray(new Hierarchy[hierarchyList.size()]);
        }
        return new Hierarchy[] {getTypeHierarchy(exprType)};
    }

    private Hierarchy getTypeHierarchy(final Type type) {
        Hierarchy hierarchy = type.getHierarchy();
        if (hierarchy != null) {
            return hierarchy;
        }
        final Dimension dimension = type.getDimension();
        if (dimension != null) {
            return dimension.getHierarchy();
        }
        return null;
    }

    /**
     * Assigns a value to the parameter with a given name.
     *
     * @throws RuntimeException if there is not parameter with the given name
     */
    public void setParameter(String parameterName, String value) {
        // Need to resolve query before we set parameters, in order to create
        // slots to store them in. (This code will go away when parameters
        // belong to prepared statements.)
        if (parameters.isEmpty()) {
            resolve();
        }

        Parameter param = getSchemaReader(false).getParameter(parameterName);
        if (param == null) {
            throw MondrianResource.instance().UnknownParameter.ex(parameterName);
        }
        if (!param.isModifiable()) {
            throw MondrianResource.instance().ParameterIsNotModifiable.ex(
                parameterName, param.getScope().name());
        }
        final Exp exp = quickParse(
            TypeUtil.typeToCategory(param.getType()), value, this);
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
            Exp e0 = axes[0].getSet();
            boolean nonEmpty0 = axes[0].isNonEmpty();
            Exp e1 = axes[1].getSet();
            boolean nonEmpty1 = axes[1].isNonEmpty();
            axes[1].setSet(e0);
            axes[1].setNonEmpty(nonEmpty0);
            axes[0].setSet(e1);
            axes[0].setNonEmpty(nonEmpty1);
            // showSubtotals ???
        }
    }

    /**
     * Returns the parameters defined in this query.
     */
    public Parameter[] getParameters() {
        return parameters.toArray(new Parameter[parameters.size()]);
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
     */
    public Member lookupMemberFromCache(String s) {
        // first look in defined members
        for (Member member : getDefinedMembers()) {
            if (Util.equalName(member.getUniqueName(), s)) {
                return member;
            }
        }
        return null;
    }

    /**
     * Looks up a named set.
     */
    private NamedSet lookupNamedSet(String name) {
        for (Formula formula : formulas) {
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
                            throw MondrianResource.instance().
                                MdxCalculatedFormulaUsedOnAxis.ex(
                                formulaType,
                                uniqueName,
                                ((QueryAxis) parent).getAxisName());

                        } else if (parent instanceof Formula) {
                            String parentFormulaType =
                                ((Formula) parent).isMember()
                                    ? MondrianResource.instance().CalculatedMember.str()
                                    : MondrianResource.instance().CalculatedSet.str();
                            throw MondrianResource.instance().
                                MdxCalculatedFormulaUsedInFormula.ex(
                                formulaType, uniqueName, parentFormulaType,
                                ((Formula) parent).getUniqueName());

                        } else {
                            throw MondrianResource.instance().
                                MdxCalculatedFormulaUsedOnSlicer.ex(
                                formulaType, uniqueName);
                        }
                    }
                    ++i;
                    parent = walker.getAncestor(i);
                    grandParent = walker.getAncestor(i+1);
                }
                throw MondrianResource.instance().
                    MdxCalculatedFormulaUsedInQuery.ex(
                    formulaType, uniqueName, Util.unparse(this));
            }
        }

        // remove formula from query
        List<Formula> formulaList = new ArrayList<Formula>();
        for (Formula formula1 : formulas) {
            if (!formula1.getUniqueName().equalsIgnoreCase(uniqueName)) {
                formulaList.add(formula1);
            }
        }

        // it has been found and removed
        this.formulas = formulaList.toArray(new Formula[0]);
    }

    /**
     * Returns whether a formula can safely be removed from the query. It can be
     * removed if the member or set it defines it not used anywhere else in the
     * query, including in another formula.
     *
     * @param uniqueName Unique name of the member or set defined by the formula
     * @return whether the formula can safely be removed
     */
    public boolean canRemoveFormula(String uniqueName) {
        Formula formula = findFormula(uniqueName);
        if (formula == null) {
            return false;
        }

        OlapElement mdxElement = formula.getElement();
        // Search the query tree to see if this formula expression is used
        // anywhere (on the axes or in another formula).
        Walker walker = new Walker(this);
        while (walker.hasMoreElements()) {
            Object queryElement = walker.nextElement();
            if (queryElement instanceof MemberExpr &&
                ((MemberExpr) queryElement).getMember().equals(mdxElement)) {
                return false;
            }
            if (queryElement instanceof NamedSetExpr &&
                ((NamedSetExpr) queryElement).getNamedSet().equals(mdxElement)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Looks up a calculated member or set defined in this Query.
     *
     * @param uniqueName Unique name of calculated member or set
     * @return formula defining calculated member, or null if not found
     */
    public Formula findFormula(String uniqueName) {
        for (Formula formula : formulas) {
            if (formula.getUniqueName().equalsIgnoreCase(uniqueName)) {
                return formula;
            }
        }
        return null;
    }

    /**
     * Finds formula by name and renames it to new name.
     */
    public void renameFormula(String uniqueName, String newName) {
        Formula formula = findFormula(uniqueName);
        if (formula == null) {
            throw MondrianResource.instance().MdxFormulaNotFound.ex(
                "formula", uniqueName, Util.unparse(this));
        }
        formula.rename(newName);
    }

    List<Member> getDefinedMembers() {
        List<Member> definedMembers = new ArrayList<Member>();
        for (final Formula formula : formulas) {
            if (formula.isMember() &&
                formula.getElement() != null &&
                getConnection().getRole().canAccess(formula.getElement())) {
                definedMembers.add((Member) formula.getElement());
            }
        }
        return definedMembers;
    }

    /**
     * Finds axis by index and sets flag to show empty cells on that axis.
     */
    public void setAxisShowEmptyCells(int axis, boolean showEmpty) {
        if (axis >= axes.length) {
            throw MondrianResource.instance().MdxAxisShowSubtotalsNotSupported.
                ex(axis);
        }
        axes[axis].setNonEmpty(!showEmpty);
    }

    /**
     * Returns <code>Hierarchy[]</code> used on <code>axis</code>. It calls
     * {@link #collectHierarchies}.
     */
    public Hierarchy[] getMdxHierarchiesOnAxis(AxisOrdinal axis) {
        if (axis.logicalOrdinal() >= axes.length) {
            throw MondrianResource.instance().MdxAxisShowSubtotalsNotSupported.
                ex(axis.logicalOrdinal());
        }
        QueryAxis queryAxis = (axis == AxisOrdinal.SLICER) ?
                slicerAxis :
                axes[axis.logicalOrdinal()];
        return collectHierarchies(queryAxis.getSet());
    }

    public Calc compileExpression(Exp exp, boolean scalar) {
        Evaluator evaluator = RolapEvaluator.create(this);
        final Validator validator = createValidator();
        final ExpCompiler compiler = createCompiler(evaluator, validator);
        Calc calc = (scalar)
            ? compiler.compileScalar(exp, false)
            : compiler.compile(exp);
        return calc;
    }

    private ExpCompiler createCompiler(
            final Evaluator evaluator, final Validator validator) {

        ExpCompiler compiler =
            ExpCompiler.Factory.getExpCompiler(
                evaluator,
                validator,
                new ResultStyle[] { resultStyle });

        final int expDeps = MondrianProperties.instance().TestExpDependencies.get();
        if (expDeps > 0) {
            compiler = RolapUtil.createDependencyTestingCompiler(compiler);
        }
        return compiler;
    }

    /**
     * Keeps track of references to members of the measures dimension
     *
     * @param olapElement potential measure member
     */
    public void addMeasuresMembers(OlapElement olapElement)
    {
        if (olapElement instanceof Member) {
            Member member = (Member) olapElement;
            if (member.getDimension().getOrdinal(getCube()) == 0) {
                measuresMembers.add(member);
            }
        }
    }

    /**
     * @return set of members from the measures dimension referenced within
     * this query
     */
    public Set<Member> getMeasuresMembers() {
        return Collections.unmodifiableSet(measuresMembers);
    }

    /**
     * Indicates that the query cannot use native cross joins to process
     * this virtual cube
     */
    public void setVirtualCubeNonNativeCrossJoin() {
        nativeCrossJoinVirtualCube = false;
    }

    /**
     * @return true if the query can use native cross joins on a virtual
     * cube
     */
    public boolean nativeCrossJoinVirtualCube() {
        return nativeCrossJoinVirtualCube;
    }

    /**
     * Saves away the level to column maps for the underlying cubes that make
     * up the virtual cube referenced in this query
     *
     * @param maps the set of maps to be saved
     */
    public void setVirtualCubeBaseCubeMaps(Set<Map<RolapLevel, RolapStar.Column>> maps)
    {
        virtualCubeBaseCubeMaps = maps;
    }

    /**
     * @return the set of level to column maps associated with the virtual
     * cube this query references
     */
    public Set<Map<RolapLevel, RolapStar.Column>> getVirtualCubeBaseCubeMaps()
    {
        return virtualCubeBaseCubeMaps;
    }

    /**
     * Saves away the map that maps a level-to-column map to a measure
     *
     * @param map map to be saved
     */
    public void setLevelMapToMeasureMap(
        Map<Map<RolapLevel, RolapStar.Column>, RolapMember> map)
    {
        levelMapToMeasureMap = map;
    }

    /**
     * @return the level-to-column-to-measure map
     */
    public Map<Map<RolapLevel, RolapStar.Column>, RolapMember> getLevelMapToMeasureMap()
    {
        return levelMapToMeasureMap;
    }

    public Object accept(MdxVisitor visitor) {
        Object o = visitor.visit(this);

        // visit formulas
        for (Formula formula : formulas) {
            formula.accept(visitor);
        }
        // visit axes
        for (QueryAxis axis : axes) {
            axis.accept(visitor);
        }
        if (slicerAxis != null) {
            slicerAxis.accept(visitor);
        }

        return o;
    }

    /**
     * Put an Object value into the evaluation cache with given key.
     * This is used by Calc's to store information between iterations
     * (rather than re-generate each time).
     *
     * @param key the cache key
     * @param value the cache value
     */
    public void putEvalCache(String key, Object value) {
        evalCache.put(key, value);
    }

    /**
     * Gets the Object associated with the value.
     *
     * @param key the cache key
     * @return the cached value or null.
     */
    public Object getEvalCache(String key) {
        return evalCache.get(key);
    }

    /**
     * Remove all entries in the evaluation cache
     */
    public void clearEvalCache() {
        evalCache.clear();
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
        private final Stack<QueryPart> stack = new Stack<QueryPart>();
        private final FunTable funTable;
        private final Map<QueryPart, QueryPart> resolvedNodes =
            new HashMap<QueryPart, QueryPart>();
        private final QueryPart placeHolder = Literal.zero;

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
                // placeholder in the map. This is an internal error -- should
                // not occur for any query, valid or invalid.
                throw Util.newInternal(
                    e,
                    "Infinite recursion encountered while validating '" +
                        Util.unparse(exp) + "'");
            }
            if (resolved == null) {
                try {
                    stack.push((QueryPart) exp);
                    // To prevent recursion, put in a placeholder while we're
                    // resolving.
                    resolvedNodes.put((QueryPart) exp, placeHolder);
                    resolved = exp.accept(this);
                    Util.assertTrue(resolved != null);
                    resolvedNodes.put((QueryPart) exp, (QueryPart) resolved);
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

        public void validate(ParameterExpr parameterExpr) {
            ParameterExpr resolved =
                (ParameterExpr) resolvedNodes.get(parameterExpr);
            if (resolved != null) {
                return; // already resolved
            }
            try {
                stack.push(parameterExpr);
                resolvedNodes.put(parameterExpr, placeHolder);
                resolved = (ParameterExpr) parameterExpr.accept(this);
                assert resolved != null;
                resolvedNodes.put(parameterExpr, resolved);
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
                    int k = whichArg(funCall, (Exp) stack.get(n));
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
                    int k = whichArg(funCall, (Exp) stack.get(n));
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
            boolean definition,
            String name,
            Type type,
            Exp defaultExp,
            String description)
        {
            final SchemaReader schemaReader = getSchemaReader(false);
            Parameter param = schemaReader.getParameter(name);

            if (definition) {
                if (param != null) {
                    if (param.getScope() == Parameter.Scope.Statement) {
                        ParameterImpl paramImpl = (ParameterImpl) param;
                        paramImpl.setDescription(description);
                        paramImpl.setDefaultExp(defaultExp);
                        paramImpl.setType(type);
                    }
                    return param;
                }
                param = new ParameterImpl(
                    name,
                    defaultExp, description, type);

                // Append it to the list of known parameters.
                parameters.add(param);
                parametersByName.put(name, param);
                return param;
            } else {
                if (param != null) {
                    return param;
                }
                throw MondrianResource.instance().UnknownParameter.ex(name);
            }
        }

        private int whichArg(final FunCall node, final Exp arg) {
            final Exp[] children = node.getArgs();
            for (int i = 0; i < children.length; i++) {
                if (children[i] == arg) {
                    return i;
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
            boolean failIfNotFound)
        {
            return getMemberByUniqueName(
                uniqueNameParts, failIfNotFound, MatchType.EXACT);
        }

        public Member getMemberByUniqueName(
                String[] uniqueNameParts,
                boolean failIfNotFound,
                MatchType matchType)
        {
            final String uniqueName = Util.implode(uniqueNameParts);
            Member member = lookupMemberFromCache(uniqueName);
            if (member == null) {
                // Not a calculated member in the query, so go to the cube.
                member = schemaReader.getMemberByUniqueName(
                    uniqueNameParts, failIfNotFound, matchType);
            }
            if (!failIfNotFound && member == null) {
                return null;
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

        public List<Member> getCalculatedMembers(Hierarchy hierarchy) {
            List<Member> result = new ArrayList<Member>();
            // Add calculated members in the cube.
            final List<Member> calculatedMembers =
                super.getCalculatedMembers(hierarchy);
            result.addAll(calculatedMembers);
            // Add calculated members defined in the query.
            for (Member member : getDefinedMembers()) {
                if (member.getHierarchy().equals(hierarchy)) {
                    result.add(member);
                }
            }
            return result;
        }

        public List<Member> getCalculatedMembers(Level level) {
            List<Member> hierarchyMembers =
                getCalculatedMembers(level.getHierarchy());
            List<Member> result = new ArrayList<Member>();
            for (Member member : hierarchyMembers) {
                if (member.getLevel().equals(level)) {
                    result.add(member);
                }
            }
            return result;
        }

        public List<Member> getCalculatedMembers() {
            return getDefinedMembers();
        }

        public OlapElement getElementChild(OlapElement parent, String s)
        {
            return getElementChild(parent, s, MatchType.EXACT);
        }

        public OlapElement getElementChild(
            OlapElement parent, String s, MatchType matchType)
        {
            // first look in cube
            OlapElement mdxElement =
                schemaReader.getElementChild(parent, s, matchType);
            if (mdxElement != null) {
                return mdxElement;
            }
            // then look in defined members (removed sf#1084651)

            // then in defined sets
            for (Formula formula : formulas) {
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
            int category)
        {
            return lookupCompound(
                parent, names, failIfNotFound, category, MatchType.EXACT);
        }

        public OlapElement lookupCompound(
                OlapElement parent,
                String[] names,
                boolean failIfNotFound,
                int category,
                MatchType matchType)
        {
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
                    parent, names, failIfNotFound, category, matchType);
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

        public Parameter getParameter(String name) {
            // Look for a parameter defined in the query.
            for (Parameter parameter : parameters) {
                if (parameter.getName().equals(name)) {
                    return parameter;
                }
            }

            // Look for a parameter defined in this connection.
            if (Util.lookup(RolapConnectionProperties.class, name) != null) {
                Object value = connection.getProperty(name);
                // TODO: Don't assume it's a string.
                // TODO: Create expression which will get the value from the
                //  connection at the time the query is executed.
                Literal defaultValue =
                    Literal.createString(String.valueOf(value));
                return new ConnectionParameterImpl(name, defaultValue);
            }

            return super.getParameter(name);
        }

    }

    private static class ConnectionParameterImpl
        extends ParameterImpl
    {
        public ConnectionParameterImpl(String name, Literal defaultValue) {
            super(name, defaultValue, "Connection property", new StringType());
        }

        public Scope getScope() {
            return Scope.Connection;
        }

        public void setValue(Object value) {
            throw MondrianResource.instance().ParameterIsNotModifiable.ex(
                getName(), getScope().name());
        }
    }
}

// End Query.java

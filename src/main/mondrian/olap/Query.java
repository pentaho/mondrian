/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 1998-2002 Kana Software, Inc.
// Copyright (C) 2001-2009 Julian Hyde and others
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
import mondrian.olap.fun.ParameterFunDef;
import mondrian.olap.type.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.*;
import mondrian.util.ArrayStack;

import java.io.*;
import java.util.*;

import mondrian.util.IdentifierParser;
import org.apache.commons.collections.collection.CompositeCollection;

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
     * public-private: This must be public because it is still accessed in
     * rolap.RolapCube
     */
    public Formula[] formulas;

    /**
     * public-private: This must be public because it is still accessed in
     * rolap.RolapConnection
     */
    public QueryAxis[] axes;

    /**
     * public-private: This must be public because it is still accessed in
     * rolap.RolapResult
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
    private long queryTimeout;

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
     * Used for virtual cubes.
     * Comtains a list of base cubes related to a virtual cube
     */
    private List<RolapCube> baseCubes;

    /**
     * If true, enforce validation even when ignoreInvalidMembers is set.
     */
    private boolean strictValidation;

    /**
     * How should the query be returned? Valid values are:
     *    ResultStyle.ITERABLE
     *    ResultStyle.LIST
     *    ResultStyle.MUTABLE_LIST
     * For java4, use LIST
     */
    private ResultStyle resultStyle =
        Util.Retrowoven ? ResultStyle.LIST : ResultStyle.ITERABLE;

    private Map<String, Object> evalCache = new HashMap<String, Object>();

    /**
     * List of aliased expressions defined in this query, and where they are
     * defined. There might be more than one aliased expression with the same
     * name.
     */
    private final List<ScopedNamedSet> scopedNamedSets =
        new ArrayList<ScopedNamedSet>();

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
        boolean strictValidation)
    {
        this(
            connection,
            Util.lookupCube(connection.getSchemaReader(), cube, true),
            formulas,
            axes,
            slicerAxis,
            cellProps,
            new Parameter[0],
            strictValidation);
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
        boolean strictValidation)
    {
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
        this.strictValidation = strictValidation;
        this.alertedNonNativeFunDefs = new HashSet<FunDef>();
        resolve();
    }

    /**
     * Sets the timeout in milliseconds of this Query.
     *
     * <p>Zero means no timeout.
     *
     * @param queryTimeoutMillis Timeout in milliseconds
     */
    public void setQueryTimeoutMillis(long queryTimeoutMillis) {
        this.queryTimeout = queryTimeoutMillis;
    }

    /**
     * Checks whether the property name is present in the query.
     */
    public boolean hasCellProperty(String propertyName) {
        for (QueryPart cellProp : cellProps) {
            if (((CellProperty)cellProp).isNameEquals(propertyName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether any cell property present in the query
     */
    public boolean isCellPropertyEmpty() {
        return cellProps.length == 0;
    }

    /**
     * Adds a new formula specifying a set
     * to an existing query.
     */
    public void addFormula(Id id, Exp exp) {
        addFormula(
            new Formula(false, id, exp, new MemberProperty[0], null, null));
    }

    /**
     * Adds a new formula specifying a member
     * to an existing query.
     *
     * @param id Name of member
     * @param exp Expression for member
     * @param memberProperties Properties of member
     */
    public void addFormula(
        Id id,
        Exp exp,
        MemberProperty[] memberProperties)
    {
        addFormula(new Formula(true, id, exp, memberProperties, null, null));
    }

    /**
     * Adds a new formula specifying a member or a set
     * to an existing query; resolve is called after
     * the formula has been added.
     *
     * @param formula Formula to add to query
     */
    public void addFormula(Formula formula) {
        formulas = Util.append(formulas, formula);
        resolve();
    }

    /**
     * Adds some number of new formulas specifying members
     * or sets to an existing query; resolve is only called
     * once, after all the new members have been added to
     * the query.
     *
     * @param additions Formulas to add to query
     */
    public void addFormulas(Formula... additions) {
        formulas = Util.appendArrays(formulas, additions);
        resolve();
    }

    /**
     * Creates a validator for this query.
     *
     * @return Validator
     */
    public Validator createValidator() {
        return createValidator(
            connection.getSchema().getFunTable(),
            false);
    }

    /**
     * Creates a validator for this query that uses a given function table and
     * function validation policy.
     *
     * @param functionTable Function table
     * @param alwaysResolveFunDef Whether to always resolve function
     *     definitions (see {@link Validator#alwaysResolveFunDef()})
     * @return Validator
     */
    public Validator createValidator(
        FunTable functionTable,
        boolean alwaysResolveFunDef)
    {
        return new QueryValidator(
            functionTable,
            alwaysResolveFunDef,
            Query.this);
    }

    /**
     * @deprecated this method has been deprecated; please use {@link #clone}
     */
    public Query safeClone() {
        return (Query) clone();
    }

    @SuppressWarnings({
        "CloneDoesntCallSuperClone",
        "CloneDoesntDeclareCloneNotSupportedException"
    })
    public Query clone() {
        return new Query(
            connection,
            cube,
            Formula.cloneArray(formulas),
            QueryAxis.cloneArray(axes),
            (slicerAxis == null) ? null : (QueryAxis) slicerAxis.clone(),
            cellProps,
            parameters.toArray(new Parameter[parameters.size()]),
            strictValidation);
    }

    public Connection getConnection() {
        return connection;
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
                    queryTimeout / 1000);
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
     * Gets the query start time
     * @return start time
     */
    public long getQueryStartTime() {
        return startTime;
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
            AxisOrdinal correctOrdinal =
                AxisOrdinal.StandardAxisOrdinal.forLogicalOrdinal(i);
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
        ExpCompiler compiler =
            createCompiler(
                evaluator, validator, Collections.singletonList(resultStyle));
        compile(compiler);
    }

    /**
     * @return true if the relevant property for ignoring invalid members is
     * set to true for this query's environment (a different property is
     * checked depending on whether environment is schema load vs query
     * validation)
     */
    public boolean ignoreInvalidMembers()
    {
        MondrianProperties props = MondrianProperties.instance();
        final boolean load = ((RolapCube) getCube()).isLoadInProgress();
        return
            !strictValidation
            && (load
                ? props.IgnoreInvalidMembers.get()
                : props.IgnoreInvalidMembersDuringQuery.get());
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
        case ITERABLE:
            // For java4, use LIST
            this.resultStyle = (Util.Retrowoven)
                ? ResultStyle.LIST : ResultStyle.ITERABLE;
            break;
        case LIST:
        case MUTABLE_LIST:
            this.resultStyle = resultStyle;
            break;
        default:
            throw ResultStyleException.generateBadType(
                ResultStyle.ITERABLE_LIST_MUTABLELIST,
                resultStyle);
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
                axisCalcs[i] = axes[i].compile(compiler, resultStyle);
            }
        }
        if (slicerAxis != null) {
            slicerCalc = slicerAxis.compile(compiler, resultStyle);
        }
    }

    /**
     * Performs type-checking and validates internal consistency of a query.
     *
     * @param validator Validator
     */
    public void resolve(Validator validator) {
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
        accept(new ParameterFinder());

        // Register all aliased expressions ('expr AS alias') as named sets.
        accept(new AliasedExpressionFinder());

        // Validate formulas.
        if (formulas != null) {
            for (Formula formula : formulas) {
                validator.validate(formula);
            }
        }

        // Validate axes.
        if (axes != null) {
            Set<Integer> axisNames = new HashSet<Integer>();
            for (QueryAxis axis : axes) {
                validator.validate(axis);
                if (!axisNames.add(axis.getAxisOrdinal().logicalOrdinal())) {
                    throw MondrianResource.instance().DuplicateAxis.ex(
                        axis.getAxisName());
                }
            }

            // Make sure that there are no gaps. If there are N axes, then axes
            // 0 .. N-1 should exist.
            int seekOrdinal =
                AxisOrdinal.StandardAxisOrdinal.COLUMNS.logicalOrdinal();
            for (QueryAxis axis : axes) {
                if (!axisNames.contains(seekOrdinal)) {
                    AxisOrdinal axisName =
                        AxisOrdinal.StandardAxisOrdinal.forLogicalOrdinal(
                            seekOrdinal);
                    throw MondrianResource.instance().NonContiguousAxis.ex(
                        seekOrdinal,
                        axisName.name());
                }
                ++seekOrdinal;
            }
        }
        if (slicerAxis != null) {
            slicerAxis.validate(validator);
        }

        // Make sure that no hierarchy is used on more than one axis.
        for (Hierarchy hierarchy : ((RolapCube) getCube()).getHierarchies()) {
            int useCount = 0;
            for (QueryAxis axis : allAxes()) {
                if (axis.getSet().getType().usesHierarchy(hierarchy, true)) {
                    ++useCount;
                }
            }
            if (useCount > 1) {
                throw MondrianResource.instance().HierarchyInIndependentAxes.ex(
                    hierarchy.getUniqueName());
            }
        }
    }

    /**
     * Returns a collection of all axes, including the slicer as the first
     * element, if there is a slicer.
     *
     * @return Collection of all axes including slicer
     */
    private Collection<QueryAxis> allAxes() {
        if (slicerAxis == null) {
            return Arrays.asList(axes);
        } else {
            //noinspection unchecked
            return new CompositeCollection(
                new Collection[] {
                    Collections.singletonList(slicerAxis),
                    Arrays.asList(axes)});
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

    /** Returns the MDX query string. */
    public String toString() {
        resolve();
        return Util.unparse(this);
    }

    public Object[] getChildren() {
        // Chidren are axes, slicer, and formulas (in that order, to be
        // consistent with replaceChild).
        List<QueryPart> list = new ArrayList<QueryPart>();
        list.addAll(Arrays.asList(axes));
        if (slicerAxis != null) {
            list.add(slicerAxis);
        }
        list.addAll(Arrays.asList(formulas));
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
    public void setParameter(String parameterName, Object value) {
        // Need to resolve query before we set parameters, in order to create
        // slots to store them in. (This code will go away when parameters
        // belong to prepared statements.)
        if (parameters.isEmpty()) {
            resolve();
        }

        Parameter param = getSchemaReader(false).getParameter(parameterName);
        if (param == null) {
            throw MondrianResource.instance().UnknownParameter.ex(
                parameterName);
        }
        if (!param.isModifiable()) {
            throw MondrianResource.instance().ParameterIsNotModifiable.ex(
                parameterName, param.getScope().name());
        }
        final Object value2 =
            quickParse(
                parameterName, param.getType(), value, this);
        param.setValue(value2);
    }

    /**
     * Converts a value into something appropriate for a given type.
     *
     * <p>Viz:
     * <ul>
     * <li>For numerics, takes number or string and returns a {@link Number}.
     * <li>For strings, takes string, or calls {@link Object#toString()} on any
     *     other type
     * <li>For members, takes member or string
     * <li>For sets of members, requires a list of members or strings and
     *     converts each element to a member.
     * </ul>
     *
     * @param type Type
     * @param value Value
     * @param query Query
     * @return Value of appropriate type
     * @throws NumberFormatException If value needs to be a number but isn't
     */
    private static Object quickParse(
        String parameterName,
        Type type,
        Object value,
        Query query)
        throws NumberFormatException
    {
        int category = TypeUtil.typeToCategory(type);
        switch (category) {
        case Category.Numeric:
            if (value instanceof Number || value == null) {
                return value;
            }
            if (value instanceof String) {
                String s = (String) value;
                try {
                    return new Integer(s);
                } catch (NumberFormatException e) {
                    return new Double(s);
                }
            }
            throw Util.newInternal(
                "Invalid value '" + value + "' for parameter '" + parameterName
                + "', type " + type);
        case Category.String:
            if (value == null) {
                return null;
            }
            return value.toString();
        case Category.Set:
            if (value instanceof String) {
                value = IdentifierParser.parseIdentifierList((String) value);
            }
            if (!(value instanceof List)) {
                throw Util.newInternal(
                    "Invalid value '" + value + "' for parameter '"
                    + parameterName + "', type " + type);
            }
            List<Member> expList = new ArrayList<Member>();
            final List list = (List) value;
            final SetType setType = (SetType) type;
            final Type elementType = setType.getElementType();
            for (Object o : list) {
                // In keeping with MDX semantics, null members are omitted from
                // lists.
                if (o == null) {
                    continue;
                }
                final Member member =
                    (Member) quickParse(parameterName, elementType, o, query);
                expList.add(member);
            }
            return expList;
        case Category.Member:
            if (value == null) {
                // Setting a member parameter to null is the same as setting to
                // the null member of the hierarchy. May not be equivalent to
                // the default value of the parameter, nor the same as the all
                // member.
                if (type.getHierarchy() != null) {
                    value = type.getHierarchy().getNullMember();
                } else if (type.getDimension() != null) {
                    value = type.getDimension().getHierarchy().getNullMember();
                }
            }
            if (value instanceof String) {
                value = Util.parseIdentifier((String) value);
            }
            if (value instanceof List
                && Util.canCast((List) value, Id.Segment.class))
            {
                final List<Id.Segment> segmentList = Util.cast((List) value);
                final OlapElement olapElement = Util.lookup(query, segmentList);
                if (olapElement instanceof Member) {
                    value = olapElement;
                }
            }
            if (value instanceof Member) {
                if (type.isInstance(value)) {
                    return value;
                }
            }
            throw Util.newInternal(
                "Invalid value '" + value + "' for parameter '"
                + parameterName + "', type " + type);
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

    /**
     * Returns a schema reader.
     *
     * @param accessControlled If true, schema reader returns only elements
     * which are accessible to the connection's current role
     *
     * @return schema reader
     */
    public SchemaReader getSchemaReader(boolean accessControlled) {
        final Role role;
        if (accessControlled) {
            // full access control
            role = getConnection().getRole();
        } else {
            role = null;
        }
        final SchemaReader cubeSchemaReader = cube.getSchemaReader(role);
        return new QuerySchemaReader(cubeSchemaReader, Query.this);
    }

    /**
     * Looks up a member whose unique name is <code>memberUniqueName</code>
     * from cache. If the member is not in cache, returns null.
     */
    public Member lookupMemberFromCache(String memberUniqueName) {
        // first look in defined members
        for (Member member : getDefinedMembers()) {
            if (Util.equalName(member.getUniqueName(), memberUniqueName)
                || Util.equalName(
                    getUniqueNameWithoutAll(member),
                    memberUniqueName))
            {
                return member;
            }
        }
        return null;
    }

    private String getUniqueNameWithoutAll(Member member) {
        // build unique string
        Member parentMember = member.getParentMember();
        if ((parentMember != null) && !parentMember.isAll()) {
            return Util.makeFqName(
                getUniqueNameWithoutAll(parentMember),
                member.getName());
        } else {
            return Util.makeFqName(member.getHierarchy(), member.getName());
        }
    }

    /**
     * Looks up a named set.
     */
    private NamedSet lookupNamedSet(String name) {
        for (Formula formula : formulas) {
            if (!formula.isMember()
                && formula.getElement() != null
                && formula.getName().equals(name))
            {
                return (NamedSet) formula.getElement();
            }
        }
        return null;
    }

    /**
     * Creates a named set defined by an alias.
     */
    public ScopedNamedSet createScopedNamedSet(
        String name,
        QueryPart scope,
        Exp expr)
    {
        final ScopedNamedSet scopedNamedSet =
            new ScopedNamedSet(
                name, scope, expr);
        scopedNamedSets.add(scopedNamedSet);
        return scopedNamedSet;
    }

    /**
     * Looks up a named set defined by an alias.
     *
     * @param nameParts Multi-part identifier for set
     * @param scopeList Parse tree node where name is used (last in list) and
     */
    ScopedNamedSet lookupScopedNamedSet(
        List<Id.Segment> nameParts,
        ArrayStack<QueryPart> scopeList)
    {
        if (nameParts.size() != 1) {
            return null;
        }
        String name = nameParts.get(0).name;
        ScopedNamedSet bestScopedNamedSet = null;
        int bestScopeOrdinal = -1;
        for (ScopedNamedSet scopedNamedSet : scopedNamedSets) {
            if (Util.equalName(scopedNamedSet.name, name)) {
                int scopeOrdinal = scopeList.indexOf(scopedNamedSet.scope);
                if (scopeOrdinal > bestScopeOrdinal) {
                    bestScopedNamedSet = scopedNamedSet;
                    bestScopeOrdinal = scopeOrdinal;
                }
            }
        }
        return bestScopedNamedSet;
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
                Object grandParent = walker.getAncestor(i + 1);
                while ((parent != null) && (grandParent != null)) {
                    if (grandParent instanceof Query) {
                        if (parent instanceof Axis) {
                            throw MondrianResource.instance()
                                .MdxCalculatedFormulaUsedOnAxis.ex(
                                    formulaType,
                                    uniqueName,
                                    ((QueryAxis) parent).getAxisName());

                        } else if (parent instanceof Formula) {
                            String parentFormulaType =
                                ((Formula) parent).isMember()
                                    ? MondrianResource.instance()
                                          .CalculatedMember.str()
                                    : MondrianResource.instance()
                                          .CalculatedSet.str();
                            throw MondrianResource.instance()
                                .MdxCalculatedFormulaUsedInFormula.ex(
                                    formulaType, uniqueName, parentFormulaType,
                                    ((Formula) parent).getUniqueName());

                        } else {
                            throw MondrianResource.instance()
                                .MdxCalculatedFormulaUsedOnSlicer.ex(
                                    formulaType, uniqueName);
                        }
                    }
                    ++i;
                    parent = walker.getAncestor(i);
                    grandParent = walker.getAncestor(i + 1);
                }
                throw MondrianResource.instance()
                    .MdxCalculatedFormulaUsedInQuery.ex(
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
        this.formulas = formulaList.toArray(new Formula[formulaList.size()]);
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
            if (queryElement instanceof MemberExpr
                && ((MemberExpr) queryElement).getMember().equals(mdxElement))
            {
                return false;
            }
            if (queryElement instanceof NamedSetExpr
                && ((NamedSetExpr) queryElement).getNamedSet().equals(
                    mdxElement))
            {
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
            if (formula.isMember()
                && formula.getElement() != null
                && getConnection().getRole().canAccess(formula.getElement()))
            {
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
            throw MondrianResource.instance().MdxAxisShowSubtotalsNotSupported
                .ex(axis);
        }
        axes[axis].setNonEmpty(!showEmpty);
    }

    /**
     * Returns <code>Hierarchy[]</code> used on <code>axis</code>. It calls
     * {@link #collectHierarchies}.
     */
    public Hierarchy[] getMdxHierarchiesOnAxis(AxisOrdinal axis) {
        if (axis.logicalOrdinal() >= axes.length) {
            throw MondrianResource.instance().MdxAxisShowSubtotalsNotSupported
                .ex(axis.logicalOrdinal());
        }
        QueryAxis queryAxis =
            axis.isFilter()
            ? slicerAxis
            : axes[axis.logicalOrdinal()];
        return collectHierarchies(queryAxis.getSet());
    }

    /**
     * Compiles an expression, using a cached compiled expression if available.
     *
     * @param exp Expression
     * @param scalar Whether expression is scalar
     * @param resultStyle Preferred result style; if null, use query's default
     *     result style; ignored if expression is scalar
     * @return compiled expression
     */
    public Calc compileExpression(
        Exp exp,
        boolean scalar,
        ResultStyle resultStyle)
    {
        Evaluator evaluator = RolapEvaluator.create(this);
        final Validator validator = createValidator();
        List<ResultStyle> resultStyleList;
        resultStyleList =
             Collections.singletonList(
                resultStyle != null ? resultStyle : this.resultStyle);
        final ExpCompiler compiler =
            createCompiler(
                evaluator, validator, resultStyleList);
        if (scalar) {
            return compiler.compileScalar(exp, false);
        } else {
            return compiler.compile(exp);
        }
    }

    public ExpCompiler createCompiler() {
        Evaluator evaluator = RolapEvaluator.create(this);
        Validator validator = createValidator();
        return createCompiler(
            evaluator,
            validator,
            Collections.singletonList(resultStyle));
    }

    private ExpCompiler createCompiler(
        final Evaluator evaluator,
        final Validator validator,
        List<ResultStyle> resultStyleList)
    {
        ExpCompiler compiler =
            ExpCompiler.Factory.getExpCompiler(
                evaluator,
                validator,
                resultStyleList);

        final int expDeps =
            MondrianProperties.instance().TestExpDependencies.get();
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
            if (member.isMeasure()) {
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
     * Saves away the base cubes related to the virtual cube
     * referenced in this query
     *
     * @param baseCubes set of base cubes
     */
    public void setBaseCubes(List<RolapCube> baseCubes) {
        this.baseCubes = baseCubes;
    }

    /**
     * return the set of base cubes associated with the virtual cube referenced
     * in this query
     *
     * @return set of base cubes
     */
    public List<RolapCube> getBaseCubes() {
        return baseCubes;
    }

    public Object accept(MdxVisitor visitor) {
        Object o = visitor.visit(this);

        if (visitor.shouldVisitChildren()) {
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
     * Source of metadata within the scope of a query.
     *
     * <p>Note especially that {@link #getCalculatedMember(java.util.List)}
     * returns the calculated members defined in this query.
     */
    private static class QuerySchemaReader extends DelegatingSchemaReader {
        private final Query query;

        public QuerySchemaReader(SchemaReader cubeSchemaReader, Query query) {
            super(cubeSchemaReader);
            this.query = query;
        }

        public SchemaReader withoutAccessControl() {
            return new QuerySchemaReader(
                schemaReader.withoutAccessControl(), query);
        }

        public Member getMemberByUniqueName(
            List<Id.Segment> uniqueNameParts,
            boolean failIfNotFound,
            MatchType matchType)
        {
            final String uniqueName = Util.implode(uniqueNameParts);
            Member member = query.lookupMemberFromCache(uniqueName);
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

        public List<Member> getLevelMembers(
            Level level,
            boolean includeCalculated)
        {
            List<Member> members = super.getLevelMembers(level, false);
            if (includeCalculated) {
                members = Util.addLevelCalculatedMembers(this, level, members);
            }
            return members;
        }

        public Member getCalculatedMember(List<Id.Segment> nameParts) {
            for (final Formula formula : query.formulas) {
                if (!formula.isMember()) {
                    continue;
                }
                Member member = (Member) formula.getElement();
                if (member == null) {
                    continue;
                }
                if (!match(member, nameParts)) {
                    continue;
                }
                if (!query.getConnection().getRole().canAccess(member)) {
                    continue;
                }
                return member;
            }
            return null;
        }

        private static boolean match(
            Member member, List<Id.Segment> nameParts)
        {
            Id.Segment segment = nameParts.get(nameParts.size() - 1);
            while (member.getParentMember() != null) {
                if (!segment.matches(member.getName())) {
                    return false;
                }
                member = member.getParentMember();
                nameParts = nameParts.subList(0, nameParts.size() - 1);
                segment = nameParts.get(nameParts.size() - 1);
            }
            if (segment.matches(member.getName())) {
                return Util.equalName(
                    member.getHierarchy().getUniqueName(),
                    Util.implode(nameParts.subList(0, nameParts.size() - 1)));
            } else if (member.isAll()) {
                return Util.equalName(
                    member.getHierarchy().getUniqueName(),
                    Util.implode(nameParts));
            } else {
                return false;
            }
        }

        public List<Member> getCalculatedMembers(Hierarchy hierarchy) {
            List<Member> result = new ArrayList<Member>();
            // Add calculated members in the cube.
            final List<Member> calculatedMembers =
                super.getCalculatedMembers(hierarchy);
            result.addAll(calculatedMembers);
            // Add calculated members defined in the query.
            for (Member member : query.getDefinedMembers()) {
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
            return query.getDefinedMembers();
        }

        public OlapElement getElementChild(OlapElement parent, Id.Segment s)
        {
            return getElementChild(parent, s, MatchType.EXACT);
        }

        public OlapElement getElementChild(
            OlapElement parent,
            Id.Segment s,
            MatchType matchType)
        {
            // first look in cube
            OlapElement mdxElement =
                schemaReader.getElementChild(parent, s, matchType);
            if (mdxElement != null) {
                return mdxElement;
            }
            // then look in defined members (fixes MONDRIAN-77)

            // then in defined sets
            for (Formula formula : query.formulas) {
                if (formula.isMember()) {
                    continue;       // have already done these
                }
                Id id = formula.getIdentifier();
                if (id.getSegments().size() == 1
                    && id.getSegments().get(0).matches(s.name))
                {
                    return formula.getNamedSet();
                }
            }

            return mdxElement;
        }

        public OlapElement lookupCompound(
            OlapElement parent,
            List<Id.Segment> names,
            boolean failIfNotFound,
            int category,
            MatchType matchType)
        {
            if (matchType == MatchType.EXACT) {
                OlapElement oe = lookupCompound(
                    parent, names, failIfNotFound, category,
                    MatchType.EXACT_SCHEMA);
                if (oe != null) {
                    return oe;
                }
            }
            // First look to ourselves.
            switch (category) {
            case Category.Unknown:
            case Category.Member:
                if (parent == query.cube) {
                    final Member calculatedMember = getCalculatedMember(names);
                    if (calculatedMember != null) {
                        return calculatedMember;
                    }
                }
            }
            switch (category) {
            case Category.Unknown:
            case Category.Set:
                if (parent == query.cube) {
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
                    formulaClone.createElement(query);
                    formulaClone.accept(query.createValidator());
                    olapElement = formulaClone.getMdxMember();
                }
            }
            return olapElement;
        }

        public NamedSet getNamedSet(List<Id.Segment> nameParts) {
            if (nameParts.size() != 1) {
                return null;
            }
            return query.lookupNamedSet(nameParts.get(0).name);
        }

        public Parameter getParameter(String name) {
            // Look for a parameter defined in the query.
            for (Parameter parameter : query.parameters) {
                if (parameter.getName().equals(name)) {
                    return parameter;
                }
            }

            // Look for a parameter defined in this connection.
            if (Util.lookup(RolapConnectionProperties.class, name) != null) {
                Object value = query.connection.getProperty(name);
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

    /**
     * Implementation of {@link mondrian.olap.Validator} that works within a
     * particular query.
     *
     * <p>It's unlikely that we would want a validator that is
     * NOT within a particular query, but by organizing the code this way, with
     * the majority of the code in {@link mondrian.olap.ValidatorImpl}, the
     * dependencies between Validator and Query are explicit.
     */
    private class QueryValidator extends ValidatorImpl {
        private final boolean alwaysResolveFunDef;
        private final SchemaReader schemaReader;

        /**
         * Creates a QueryValidator.
         *
         * @param functionTable Function table
         * @param alwaysResolveFunDef Whether to always resolve function
         *     definitions (see {@link #alwaysResolveFunDef()})
         * @param query Query
         */
        public QueryValidator(
            FunTable functionTable, boolean alwaysResolveFunDef, Query query)
        {
            super(functionTable);
            this.alwaysResolveFunDef = alwaysResolveFunDef;
            this.schemaReader = new ScopedSchemaReader(this, true);
        }

        public SchemaReader getSchemaReader() {
            return schemaReader;
        }

        protected void defineParameter(Parameter param) {
            final String name = param.getName();
            parameters.add(param);
            parametersByName.put(name, param);
        }

        public Query getQuery() {
            return Query.this;
        }

        public boolean alwaysResolveFunDef() {
            return alwaysResolveFunDef;
        }

        public ArrayStack<QueryPart> getScopeStack() {
            return stack;
        }
    }

    /**
     * Schema reader that depends on the current scope during the validation
     * of a query. Depending on the scope, different calculated sets may be
     * visible. The scope is represented by the expression stack inside the
     * validator.
     */
    private static class ScopedSchemaReader extends DelegatingSchemaReader {
        private final QueryValidator queryValidator;
        private final boolean accessControlled;

        /**
         * Creates a ScopedSchemaReader.
         *
         * @param queryValidator Validator that is being used to validate the
         *     query
         * @param accessControlled Access controlled
         */
        private ScopedSchemaReader(
            QueryValidator queryValidator,
            boolean accessControlled)
        {
            super(queryValidator.getQuery().getSchemaReader(accessControlled));
            this.queryValidator = queryValidator;
            this.accessControlled = accessControlled;
        }

        public SchemaReader withoutAccessControl() {
            if (!accessControlled) {
                return this;
            }
            return new ScopedSchemaReader(queryValidator, false);
        }

        public OlapElement lookupCompound(
            OlapElement parent,
            final List<Id.Segment> names,
            boolean failIfNotFound,
            int category,
            MatchType matchType)
        {
            switch (category) {
            case Category.Set:
            case Category.Unknown:
                final ScopedNamedSet namedSet =
                    queryValidator.getQuery().lookupScopedNamedSet(
                        names, queryValidator.getScopeStack());
                if (namedSet != null) {
                    return namedSet;
                }
            }
            return super.lookupCompound(
                parent, names, failIfNotFound, category, matchType);
        }
    }

    public static class ScopedNamedSet implements NamedSet {
        private final String name;
        private final QueryPart scope;
        private Exp expr;

        /**
         * Creates a ScopedNamedSet.
         *
         * @param name Name
         * @param scope Scope of named set (the function call that encloses
         *     the 'expr AS name', often GENERATE or FILTER)
         * @param expr Expression that defines the set
         */
        private ScopedNamedSet(String name, QueryPart scope, Exp expr) {
            this.name = name;
            this.scope = scope;
            this.expr = expr;
        }

        public String getName() {
            return name;
        }

        public String getNameUniqueWithinQuery() {
            return System.identityHashCode(this) + "";
        }

        public boolean isDynamic() {
            return true;
        }

        public Exp getExp() {
            return expr;
        }

        public void setExp(Exp expr) {
            this.expr = expr;
        }

        public void setName(String newName) {
            throw new UnsupportedOperationException();
        }

        public Type getType() {
            return expr.getType();
        }

        public Map<String, Annotation> getAnnotationMap() {
            return Collections.emptyMap();
        }

        public NamedSet validate(Validator validator) {
            Exp newExpr = expr.accept(validator);
            final Type type = newExpr.getType();
            if (type instanceof MemberType
                || type instanceof TupleType)
            {
                newExpr =
                    new UnresolvedFunCall(
                        "{}", Syntax.Braces, new Exp[] {newExpr})
                    .accept(validator);
            }
            this.expr = newExpr;
            return this;
        }

        public String getUniqueName() {
            return name;
        }

        public String getDescription() {
            throw new UnsupportedOperationException();
        }

        public OlapElement lookupChild(
            SchemaReader schemaReader, Id.Segment s, MatchType matchType)
        {
            throw new UnsupportedOperationException();
        }

        public String getQualifiedName() {
            throw new UnsupportedOperationException();
        }

        public String getCaption() {
            throw new UnsupportedOperationException();
        }

        public Hierarchy getHierarchy() {
            throw new UnsupportedOperationException();
        }

        public Dimension getDimension() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Visitor that locates and registers parameters.
     */
    private class ParameterFinder extends MdxVisitorImpl {
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
                    throw MondrianResource.instance()
                        .ParameterDefinedMoreThanOnce.ex(parameterName);
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

    /**
     * Visitor that locates and registers all aliased expressions
     * ('expr AS alias') as named sets. The resulting named sets have scope,
     * therefore they can only be seen and used within that scope.
     */
    private class AliasedExpressionFinder extends MdxVisitorImpl {
        @Override
        public Object visit(QueryAxis queryAxis) {
            registerAlias(queryAxis, queryAxis.getSet());
            return super.visit(queryAxis);
        }

        public Object visit(UnresolvedFunCall call) {
            registerAliasArgs(call);
            return super.visit(call);
        }

        public Object visit(ResolvedFunCall call) {
            registerAliasArgs(call);
            return super.visit(call);
        }

        /**
         * Registers all arguments of a function that are named sets.
         *
         * @param call Function call
         */
        private void registerAliasArgs(FunCall call) {
            for (Exp exp : call.getArgs()) {
                registerAlias((QueryPart) call, exp);
            }
        }

        /**
         * Registers a named set if an expression is of the form "expr AS
         * alias".
         *
         * @param parent Parent node
         * @param exp Expression that may be an "AS"
         */
        private void registerAlias(QueryPart parent, Exp exp) {
            if (exp instanceof FunCall) {
                FunCall call2 = (FunCall) exp;
                if (call2.getSyntax() == Syntax.Infix
                    && call2.getFunName().equals("AS"))
                {
                    // Scope is the function enclosing the 'AS' expression.
                    // For example, in
                    //    Filter(Time.Children AS s, x > y)
                    // the scope of the set 's' is the Filter function.
                    assert call2.getArgCount() == 2;
                    final Id id = (Id) call2.getArg(1);
                    createScopedNamedSet(
                        id.getSegments().get(0).name,
                        (QueryPart) parent,
                        call2.getArg(0));
                }
            }
        }
    }
}

// End Query.java

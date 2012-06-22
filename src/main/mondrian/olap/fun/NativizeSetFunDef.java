/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2009-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.calc.impl.DelegatingTupleList;
import mondrian.mdx.*;
import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.resource.MondrianResource;

import org.apache.log4j.Logger;

import org.olap4j.impl.Olap4jUtil;

import java.util.*;

import static mondrian.olap.fun.NativizeSetFunDef.NativeElementType.*;

/**
 * Definition of the <code>NativizeSet</code> MDX function.
 *
 * @author jrand
 * @since Oct 14, 2009
 */
public class NativizeSetFunDef extends FunDefBase {
    /*
     * Static final fields.
     */
    protected static final Logger LOGGER =
        Logger.getLogger(NativizeSetFunDef.class);

    private static final String SENTINEL_PREFIX = "_Nativized_Sentinel_";
    private static final String MEMBER_NAME_PREFIX = "_Nativized_Member_";
    private static final String SET_NAME_PREFIX = "_Nativized_Set_";
    private static final List<Class<? extends FunDef>> functionWhitelist =
        Arrays.<Class<? extends FunDef>>asList(
            CacheFunDef.class,
            SetFunDef.class,
            CrossJoinFunDef.class,
            NativizeSetFunDef.class);

    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
        "NativizeSet",
        "NativizeSet(<Set>)",
        "Tries to natively evaluate <Set>.",
        new String[] {"fxx"},
        NativizeSetFunDef.class);

    /*
     * Instance final fields.
     */
    private final SubstitutionMap substitutionMap = new SubstitutionMap();
    private final HashSet<Dimension> dimensions =
        new LinkedHashSet<Dimension>();

    private boolean isFirstCompileCall = true;

    /*
     * Instance non-final fields.
     */
    private Exp originalExp;
    private static final String ESTIMATE_MESSAGE =
        "isHighCardinality=%b: estimate=%,d threshold=%,d";
    private static final String PARTIAL_ESTIMATE_MESSAGE =
        "isHighCardinality=%b: partial estimate=%,d threshold=%,d";

    public NativizeSetFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
        LOGGER.debug("---- NativizeSetFunDef constructor");
    }

    public Exp createCall(Validator validator, Exp[] args) {
        LOGGER.debug("NativizeSetFunDef createCall");
        ResolvedFunCall call =
            (ResolvedFunCall) super.createCall(validator, args);
        call.accept(new FindLevelsVisitor(substitutionMap, dimensions));
        return call;
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        LOGGER.debug("NativizeSetFunDef compileCall");
        Exp funArg = call.getArg(0);

        if (MondrianProperties.instance().UseAggregates.get()
            || MondrianProperties.instance().ReadAggregates.get())
        {
            return funArg.accept(compiler);
        }

        final Calc[] calcs = {compiler.compileList(funArg, true)};

        final int arity = calcs[0].getType().getArity();
        assert arity >= 0;
        if (arity == 1 || substitutionMap.isEmpty()) {
            IterCalc calc = (IterCalc) funArg.accept(compiler);
            final boolean highCardinality =
                arity == 1
                && isHighCardinality(funArg, compiler.getEvaluator());
            if (calc == null) {
                // This can happen under JDK1.4: caller wants iterator
                // implementation, but compiler can only provide list.
                // Fall through and use native.
            } else if (calc instanceof ListCalc) {
                return new NonNativeListCalc((ListCalc) calc, highCardinality);
            } else {
                return new NonNativeIterCalc(calc, highCardinality);
            }
        }
        if (isFirstCompileCall) {
            isFirstCompileCall = false;
            originalExp = funArg.clone();
            Query query = compiler.getEvaluator().getQuery();
            call.accept(
                new AddFormulasVisitor(query, substitutionMap, dimensions));
            call.accept(new TransformToFormulasVisitor(query));
            query.resolve();
        }
        return new NativeListCalc(
            call, calcs, compiler, substitutionMap, originalExp);
    }

    private boolean isHighCardinality(Exp funArg, Evaluator evaluator) {
        Level level = findLevel(funArg);
        if (level != null) {
            int cardinality =
                evaluator.getSchemaReader()
                    .getLevelCardinality(level, false, true);
            final int minThreshold = MondrianProperties.instance()
                .NativizeMinThreshold.get();
            final boolean isHighCard = cardinality > minThreshold;
            logHighCardinality(
                ESTIMATE_MESSAGE, minThreshold, cardinality, isHighCard);
            return isHighCard;
        }
        return false;
    }

    private Level findLevel(Exp exp) {
        exp.accept(new FindLevelsVisitor(substitutionMap, dimensions));
        final Collection<Level> levels = substitutionMap.values();
        if (levels.size() == 1) {
            return levels.iterator().next();
        }
        return null;
    }

    private static void logHighCardinality(
        final String estimateMessage,
        long nativizeMinThreshold,
        long estimatedCardinality,
        boolean highCardinality)
    {
        LOGGER.debug(
            String.format(
                estimateMessage,
                highCardinality,
                estimatedCardinality,
                nativizeMinThreshold));
    }

    static class NonNativeCalc implements Calc {
        final Calc parent;
        final boolean nativeEnabled;

        protected NonNativeCalc(Calc parent, final boolean nativeEnabled) {
            assert parent != null;
            this.parent = parent;
            this.nativeEnabled = nativeEnabled;
        }

        public Object evaluate(final Evaluator evaluator) {
            evaluator.setNativeEnabled(nativeEnabled);
            return parent.evaluate(evaluator);
        }

        public boolean dependsOn(final Hierarchy hierarchy) {
            return parent.dependsOn(hierarchy);
        }

        public Type getType() {
            return parent.getType();
        }

        public void accept(final CalcWriter calcWriter) {
            parent.accept(calcWriter);
        }

        public ResultStyle getResultStyle() {
            return parent.getResultStyle();
        }

        /**
         * {@inheritDoc}
         *
         * Default implementation just does 'instanceof TargetClass'. Subtypes
         * that are wrappers should override.
         */
        public boolean isWrapperFor(Class<?> iface) {
            return iface.isInstance(this);
        }

        /**
         * {@inheritDoc}
         *
         * Default implementation just casts to TargetClass.
         * Subtypes that are wrappers should override.
         */
        public <T> T unwrap(Class<T> iface) {
            return iface.cast(this);
        }
    }

    static class NonNativeIterCalc
        extends NonNativeCalc
        implements IterCalc
    {
        protected NonNativeIterCalc(IterCalc parent, boolean highCardinality) {
            super(parent, highCardinality);
        }

        IterCalc parent() {
            return (IterCalc) parent;
        }

        public TupleIterable evaluateIterable(Evaluator evaluator) {
            evaluator.setNativeEnabled(nativeEnabled);
            return parent().evaluateIterable(evaluator);
        }
    }

    static class NonNativeListCalc
        extends NonNativeCalc
        implements ListCalc
    {
        protected NonNativeListCalc(ListCalc parent, boolean highCardinality) {
            super(parent, highCardinality);
        }

        ListCalc parent() {
            return (ListCalc) parent;
        }

        public TupleList evaluateList(Evaluator evaluator) {
            evaluator.setNativeEnabled(nativeEnabled);
            return parent().evaluateList(evaluator);
        }

        public TupleIterable evaluateIterable(Evaluator evaluator) {
            return evaluateList(evaluator);
        }
    }

    public static class NativeListCalc extends AbstractListCalc {
        private final SubstitutionMap substitutionMap;
        private final ListCalc simpleCalc;
        private final ExpCompiler compiler;

        private final Exp originalExp;

        protected NativeListCalc(
            ResolvedFunCall call,
            Calc[] calcs,
            ExpCompiler compiler,
            SubstitutionMap substitutionMap,
            Exp originalExp)
        {
            super(call, calcs);
            LOGGER.debug("---- NativeListCalc constructor");
            this.substitutionMap = substitutionMap;
            this.simpleCalc = (ListCalc) calcs[0];
            this.compiler = compiler;
            this.originalExp = originalExp;
        }

        public TupleList evaluateList(Evaluator evaluator) {
            return computeTuples(evaluator);
        }

        public TupleList computeTuples(Evaluator evaluator) {
            TupleList simplifiedList = evaluateSimplifiedList(evaluator);
            if (simplifiedList.isEmpty()) {
                return simplifiedList;
            }
            if (!isHighCardinality(evaluator, simplifiedList)) {
                return evaluateNonNative(evaluator);
            }
            return evaluateNative(evaluator, simplifiedList);
        }

        private TupleList evaluateSimplifiedList(Evaluator evaluator) {
            final int savepoint = evaluator.savepoint();
            try {
                evaluator.setNonEmpty(false);
                evaluator.setNativeEnabled(false);
                TupleList simplifiedList =
                        simpleCalc.evaluateList(evaluator);
                dumpListToLog("simplified list", simplifiedList);
                return simplifiedList;
            } finally {
                evaluator.restore(savepoint);
            }
        }

        private TupleList evaluateNonNative(Evaluator evaluator) {
            LOGGER.debug(
                "Disabling native evaluation. originalExp="
                    + originalExp);
            ListCalc calc =
                compiler.compileList(getOriginalExp(evaluator.getQuery()));
            final int savepoint = evaluator.savepoint();
            try {
                evaluator.setNonEmpty(true);
                evaluator.setNativeEnabled(false);
                TupleList members = calc.evaluateList(evaluator);
                return members;
            } finally {
                evaluator.restore(savepoint);
            }
        }

        private TupleList evaluateNative(
            Evaluator evaluator, TupleList simplifiedList)
        {
            CrossJoinAnalyzer analyzer =
                new CrossJoinAnalyzer(simplifiedList, substitutionMap);
            String crossJoin = analyzer.getCrossJoinExpression();

            // If the crossjoin expression is empty, then the simplified list
            // already contains the fully evaluated tuple list, so we can
            // return it now without any additional work.
            if (crossJoin.length() == 0) {
                return simplifiedList;
            }

            // Force non-empty to true to create the native list.
            LOGGER.debug(
                "crossjoin reconstituted from simplified list: "
                + String.format(
                    "%n"
                    + crossJoin.replaceAll(",", "%n, ")));
            final int savepoint = evaluator.savepoint();
            try {
                evaluator.setNonEmpty(true);
                evaluator.setNativeEnabled(true);

                TupleList members = analyzer.mergeCalcMembers(
                    evaluateJoinExpression(evaluator, crossJoin));
                return members;
            } finally {
                evaluator.restore(savepoint);
            }
        }

        private Exp getOriginalExp(final Query query) {
            originalExp.accept(
                new TransformFromFormulasVisitor(query, compiler));
            if (originalExp instanceof NamedSetExpr) {
                //named sets get their evaluator cached in RolapResult.
                //We do not want to use the cached evaluator, so pass along the
                //expression instead.
                return ((NamedSetExpr) originalExp).getNamedSet().getExp();
            }
            return originalExp;
        }

        private boolean isHighCardinality(
            Evaluator evaluator, TupleList simplifiedList)
        {
            Util.assertTrue(!simplifiedList.isEmpty());

            SchemaReader schema = evaluator.getSchemaReader();
            List<Member> tuple = simplifiedList.get(0);
            long nativizeMinThreshold =
                MondrianProperties.instance().NativizeMinThreshold.get();
            long estimatedCardinality = simplifiedList.size();

            for (Member member : tuple) {
                String memberName = member.getName();
                if (memberName.startsWith(MEMBER_NAME_PREFIX)) {
                    Level level = member.getLevel();
                    Dimension dimension = level.getDimension();
                    Hierarchy hierarchy = dimension.getHierarchy();

                    String levelName = getLevelNameFromMemberName(memberName);
                    Level hierarchyLevel =
                        Util.lookupHierarchyLevel(hierarchy, levelName);
                    long levelCardinality =
                        getLevelCardinality(schema, hierarchyLevel);
                    estimatedCardinality *= levelCardinality;
                    if (estimatedCardinality >= nativizeMinThreshold) {
                        logHighCardinality(
                            PARTIAL_ESTIMATE_MESSAGE,
                            nativizeMinThreshold,
                            estimatedCardinality,
                            true);
                        return true;
                    }
                }
            }

            boolean isHighCardinality =
                (estimatedCardinality >= nativizeMinThreshold);

            logHighCardinality(
                ESTIMATE_MESSAGE,
                nativizeMinThreshold,
                estimatedCardinality,
                isHighCardinality);
            return isHighCardinality;
        }

        private long getLevelCardinality(SchemaReader schema, Level level) {
            if (cardinalityIsKnown(level)) {
                return level.getApproxRowCount();
            }
            return schema.getLevelCardinality(level, false, true);
        }

        private boolean cardinalityIsKnown(Level level) {
            return level.getApproxRowCount() > 0;
        }

        private TupleList evaluateJoinExpression(
            Evaluator evaluator, String crossJoinExpression)
        {
            Exp unresolved =
                evaluator.getQuery().getConnection()
                    .parseExpression(crossJoinExpression);
            Exp resolved = compiler.getValidator().validate(unresolved, false);
            ListCalc calc = compiler.compileList(resolved);
            return calc.evaluateList(evaluator);
        }
    }

    static class FindLevelsVisitor extends MdxVisitorImpl {
        private final SubstitutionMap substitutionMap;
        private final Set<Dimension> dimensions;

        public FindLevelsVisitor(
            SubstitutionMap substitutionMap, HashSet<Dimension> dimensions)
        {
            this.substitutionMap = substitutionMap;
            this.dimensions = dimensions;
        }

        @Override
        public Object visit(ResolvedFunCall call) {
            if (call.getFunDef() instanceof LevelMembersFunDef) {
                if (call.getArg(0) instanceof LevelExpr) {
                    Level level = ((LevelExpr) call.getArg(0)).getLevel();
                    substitutionMap.put(createMemberId(level), level);
                    dimensions.add(level.getDimension());
                }
            } else if (
                functionWhitelist.contains(call.getFunDef().getClass()))
            {
                for (Exp arg : call.getArgs()) {
                    arg.accept(this);
                }
            }
            turnOffVisitChildren();
            return null;
        }


        @Override
        public Object visit(MemberExpr member) {
            dimensions.add(member.getMember().getDimension());
            return null;
        }
    }

    static class AddFormulasVisitor extends MdxVisitorImpl {
        private final Query query;
        private final Collection<Level> levels;
        private final Set<Dimension> dimensions;

        public AddFormulasVisitor(
            Query query,
            SubstitutionMap substitutionMap,
            Set<Dimension> dimensions)
        {
            LOGGER.debug("---- AddFormulasVisitor constructor");
            this.query = query;
            this.levels = substitutionMap.values();
            this.dimensions = dimensions;
        }

        @Override
        public Object visit(ResolvedFunCall call) {
            if (call.getFunDef() instanceof NativizeSetFunDef) {
                addFormulasToQuery();
            }
            turnOffVisitChildren();
            return null;
        }

        private void addFormulasToQuery() {
            LOGGER.debug("FormulaResolvingVisitor addFormulas");
            List<Formula> formulas = new ArrayList<Formula>();

            for (Level level : levels) {
                Formula memberFormula = createDefaultMemberFormula(level);
                formulas.add(memberFormula);
                formulas.add(createNamedSetFormula(level, memberFormula));
            }

            for (Dimension dim : dimensions) {
                Level level = dim.getHierarchy().getLevels()[0];
                formulas.add(createSentinelFormula(level));
            }

            query.addFormulas(formulas.toArray(new Formula[formulas.size()]));
        }

        private Formula createSentinelFormula(Level level) {
            Id memberId = createSentinelId(level);
            Exp memberExpr = query.getConnection()
                .parseExpression("101010");

            LOGGER.debug(
                "createSentinelFormula memberId="
                + memberId
                + " memberExpr="
                + memberExpr);
            return new Formula(memberId, memberExpr, new MemberProperty[0]);
        }

        private Formula createDefaultMemberFormula(Level level) {
            Id memberId = createMemberId(level);
            Exp memberExpr =
                new UnresolvedFunCall(
                    "DEFAULTMEMBER",
                    Syntax.Property,
                    new Exp[] {hierarchyId(level)});

            LOGGER.debug(
                "createLevelMembersFormulas memberId="
                + memberId
                + " memberExpr="
                + memberExpr);
            return new Formula(memberId, memberExpr, new MemberProperty[0]);
        }

        private Formula createNamedSetFormula(
            Level level, Formula memberFormula)
        {
            Id setId = createSetId(level);
            Exp setExpr = query.getConnection()
                .parseExpression(
                    "{"
                    + memberFormula.getIdentifier().toString()
                    + "}");

            LOGGER.debug(
                "createNamedSetFormula setId="
                + setId
                + " setExpr="
                + setExpr);
            return new Formula(setId, setExpr);
        }
    }

    static class TransformToFormulasVisitor extends MdxVisitorImpl {
        private final Query query;

        public TransformToFormulasVisitor(Query query) {
            LOGGER.debug("---- TransformToFormulasVisitor constructor");
            this.query = query;
        }

        @Override
        public Object visit(ResolvedFunCall call) {
            LOGGER.debug("visit " + call);
            Object result = null;
            if (call.getFunDef() instanceof LevelMembersFunDef) {
                result = replaceLevelMembersReferences(call);
            } else if (
                functionWhitelist.contains(call.getFunDef().getClass()))
            {
                result = visitCallArguments(call);
            }
            turnOffVisitChildren();
            return result;
        }

        private Object replaceLevelMembersReferences(ResolvedFunCall call) {
            LOGGER.debug("replaceLevelMembersReferences " + call);
            Level level = ((LevelExpr) call.getArg(0)).getLevel();
            Id setId = createSetId(level);
            Formula formula = query.findFormula(setId.toString());
            Exp exp = Util.createExpr(formula.getNamedSet());
            return query.createValidator().validate(exp, false);
        }

        private Object visitCallArguments(ResolvedFunCall call) {
            Exp[] exps = call.getArgs();
            LOGGER.debug("visitCallArguments " + call);

            for (int i = 0; i < exps.length; i++) {
                Exp transformedExp = (Exp) exps[i].accept(this);
                if (transformedExp != null) {
                    exps[i] = transformedExp;
                }
            }

            if (exps.length > 1
                && call.getFunDef() instanceof SetFunDef)
            {
                return flattenSetFunDef(call);
            }
            return null;
        }

        private Object flattenSetFunDef(ResolvedFunCall call) {
            List<Exp> newArgs = new ArrayList<Exp>();
            flattenSetMembers(newArgs, call.getArgs());
            addSentinelMembers(newArgs);
            if (newArgs.size() != call.getArgCount()) {
                return new ResolvedFunCall(
                    call.getFunDef(),
                    newArgs.toArray(new Exp[newArgs.size()]),
                    call.getType());
            }
            return null;
        }

        private void flattenSetMembers(List<Exp> result, Exp[] args) {
            for (Exp arg : args) {
                if (arg instanceof ResolvedFunCall
                    && ((ResolvedFunCall)arg).getFunDef() instanceof SetFunDef)
                {
                    flattenSetMembers(result, ((ResolvedFunCall)arg).getArgs());
                } else {
                    result.add(arg);
                }
            }
        }

        private void addSentinelMembers(List<Exp> args) {
            Exp prev = args.get(0);
            for (int i = 1; i < args.size(); i++) {
                Exp curr = args.get(i);
                if (prev.toString().equals(curr.toString())) {
                    OlapElement element = null;
                    if (curr instanceof NamedSetExpr) {
                        element = ((NamedSetExpr) curr).getNamedSet();
                    } else if (curr instanceof MemberExpr) {
                        element = ((MemberExpr) curr).getMember();
                    }
                    if (element != null) {
                        Level level = element.getHierarchy().getLevels()[0];
                        Id memberId = createSentinelId(level);
                        Formula formula =
                            query.findFormula(memberId.toString());
                        args.add(i++, Util.createExpr(formula.getMdxMember()));
                    }
                }
                prev = curr;
            }
        }
    }

    static class TransformFromFormulasVisitor extends MdxVisitorImpl {
        private final Query query;
        private final ExpCompiler compiler;

        public TransformFromFormulasVisitor(Query query, ExpCompiler compiler) {
            LOGGER.debug("---- TransformFromFormulasVisitor constructor");
            this.query = query;
            this.compiler = compiler;
        }

        @Override
        public Object visit(ResolvedFunCall call) {
            LOGGER.debug("visit " + call);
            Object result;
            result = visitCallArguments(call);
            turnOffVisitChildren();
            return result;
        }

        @Override
        public Object visit(NamedSetExpr namedSetExpr) {
            String exprName = namedSetExpr.getNamedSet().getName();
            Exp membersExpr;

            if (exprName.contains(SET_NAME_PREFIX)) {
                String levelMembers = exprName.replaceAll(
                    SET_NAME_PREFIX, "\\[")
                    .replaceAll("_$", "\\]")
                    .replaceAll("_", "\\]\\.\\[")
                    + ".members";
                membersExpr =
                    query.getConnection().parseExpression(levelMembers);
                membersExpr =
                    compiler.getValidator().validate(membersExpr, false);
            } else {
                membersExpr = namedSetExpr.getNamedSet().getExp();
            }
            return membersExpr;
        }


        private Object visitCallArguments(ResolvedFunCall call) {
            Exp[] exps = call.getArgs();
            LOGGER.debug("visitCallArguments " + call);

            for (int i = 0; i < exps.length; i++) {
                Exp transformedExp = (Exp) exps[i].accept(this);
                if (transformedExp != null) {
                    exps[i] = transformedExp;
                }
            }
            return null;
        }
    }

    private static class SubstitutionMap {
        private final Map<String, Level> map = new HashMap<String, Level>();

        public boolean isEmpty() {
            return map.isEmpty();
        }

        public boolean contains(Member member) {
            return map.containsKey(toKey(member));
        }

        public Level get(Member member) {
            return map.get(toKey(member));
        }

        public Level put(Id id, Level level) {
            return map.put(toKey(id), level);
        }

        public Collection<Level> values() {
            return map.values();
        }

        @Override
        public String toString() {
            return map.toString();
        }

        private String toKey(Id id) {
            return id.toString();
        }

        private String toKey(Member member) {
            return member.getUniqueName();
        }
    }

    public static class CrossJoinAnalyzer {

        private final int arity;
        private final Member[] tempTuple;
        private final List<Member> tempTupleAsList;
        private final int[] nativeIndices;
        private final int resultLimit;

        private final List<Collection<String>> nativeMembers;
        private final ReassemblyGuide reassemblyGuide;
        private final TupleList resultList;

        public CrossJoinAnalyzer(
            TupleList simplifiedList, SubstitutionMap substitutionMap)
        {
            long nativizeMaxResults =
                MondrianProperties.instance().NativizeMaxResults.get();
            arity = simplifiedList.getArity();
            tempTuple = new Member[arity];
            tempTupleAsList = Arrays.asList(tempTuple);
            resultLimit = nativizeMaxResults <= 0
                    ? Integer.MAX_VALUE
                    : (int) Math.min(nativizeMaxResults, Integer.MAX_VALUE);

            resultList = TupleCollections.createList(arity);

            reassemblyGuide = classifyMembers(simplifiedList, substitutionMap);
            nativeMembers = findNativeMembers();
            nativeIndices = findNativeIndices();
        }

        public ReassemblyGuide classifyMembers(
            TupleList simplifiedList,
            SubstitutionMap substitutionMap)
        {
            ReassemblyGuide guide = new ReassemblyGuide(0);

            List<ReassemblyCommand> cmdTuple =
                new ArrayList<ReassemblyCommand>(arity);
            for (List<Member> srcTuple : simplifiedList) {
                cmdTuple.clear();
                for (Member mbr : srcTuple) {
                    cmdTuple.add(zz(substitutionMap, mbr));
                }
                guide.addCommandTuple(cmdTuple);
            }
            return guide;
        }

        private ReassemblyCommand zz(
            SubstitutionMap substitutionMap, Member mbr)
        {
            ReassemblyCommand c;
            if (substitutionMap.contains(mbr)) {
                c =
                    new ReassemblyCommand(
                        substitutionMap.get(mbr), LEVEL_MEMBERS);
            } else if (mbr.getName().startsWith(SENTINEL_PREFIX)) {
                c =
                    new ReassemblyCommand(mbr, SENTINEL);
            } else {
                NativeElementType nativeType = !isNativeCompatible(mbr)
                    ? NON_NATIVE
                    : mbr.getMemberType() == Member.MemberType.REGULAR
                    ? ENUMERATED_VALUE
                    : OTHER_NATIVE;
                c = new ReassemblyCommand(mbr, nativeType);
            }
            return c;
        }

        private List<Collection<String>> findNativeMembers() {
            List<Collection<String>> nativeMembers =
                new ArrayList<Collection<String>>(arity);

            for (int i = 0; i < arity; i++) {
                nativeMembers.add(new LinkedHashSet<String>());
            }

            findNativeMembers(reassemblyGuide, nativeMembers);
            return nativeMembers;
        }

        private void findNativeMembers(
            ReassemblyGuide guide,
            List<Collection<String>> nativeMembers)
        {
            List<ReassemblyCommand> commands = guide.getCommands();
            Set<NativeElementType> typesToAdd =
                ReassemblyCommand.getMemberTypes(commands);

            if (typesToAdd.contains(LEVEL_MEMBERS)) {
                typesToAdd.remove(ENUMERATED_VALUE);
            }

            int index = guide.getIndex();
            for (ReassemblyCommand command : commands) {
                NativeElementType type = command.getMemberType();
                if (type.isNativeCompatible() && typesToAdd.contains(type)) {
                    nativeMembers.get(index).add(command.getElementName());
                }

                if (command.hasNextGuide()) {
                    findNativeMembers(command.forNextCol(), nativeMembers);
                }
            }
        }

        private int[] findNativeIndices() {
            int[] indices = new int[arity];
            int nativeColCount = 0;

            for (int i = 0; i < arity; i++) {
                Collection<String> natives = nativeMembers.get(i);
                if (!natives.isEmpty()) {
                    indices[nativeColCount++] = i;
                }
            }

            if (nativeColCount == arity) {
                return indices;
            }

            int[] result = new int[nativeColCount];
            System.arraycopy(indices, 0, result, 0, nativeColCount);
            return result;
        }

        private boolean isNativeCompatible(Member member) {
            return member.isParentChildLeaf()
                || (!member.isMeasure()
                && !member.isCalculated() && !member.isAll());
        }

        private String getCrossJoinExpression() {
            return formatCrossJoin(nativeMembers);
        }

        private String formatCrossJoin(List<Collection<String>> memberLists) {
            StringBuilder buf = new StringBuilder();

            String left = toCsv(memberLists.get(0));
            String right =
                memberLists.size() == 1
                ? ""
                : formatCrossJoin(memberLists.subList(1, memberLists.size()));

            if (left.length() == 0) {
                buf.append(right);
            } else {
                if (right.length() == 0) {
                    buf.append("{").append(left).append("}");
                } else {
                    buf.append("CrossJoin(")
                        .append("{").append(left).append("},")
                        .append(right).append(")");
                }
            }

            return buf.toString();
        }

        private TupleList mergeCalcMembers(TupleList nativeValues) {
            TupleList nativeList =
                adaptList(nativeValues, arity, nativeIndices);

            dumpListToLog("native list", nativeList);
            mergeCalcMembers(reassemblyGuide, new Range(nativeList), null);
            dumpListToLog("result list", resultList);
            return resultList;
        }

        private void mergeCalcMembers(
            ReassemblyGuide guide, Range range, Set<List<Member>> history)
        {
            int col = guide.getIndex();
            if (col == arity - 1) {
                if (history == null) {
                    appendMembers(guide, range);
                } else {
                    appendMembers(guide, range, history);
                }
                return;
            }

            for (ReassemblyCommand command : guide.getCommands()) {
                ReassemblyGuide nextGuide = command.forNextCol();
                tempTuple[col] = null;

                switch (command.getMemberType()) {
                case NON_NATIVE:
                    tempTuple[col] = command.getMember();
                    mergeCalcMembers(
                        nextGuide,
                        range,
                        (history == null
                            ? new HashSet<List<Member>>()
                            : history));
                    break;
                case ENUMERATED_VALUE:
                    Member value = command.getMember();
                    Range valueRange = range.subRangeForValue(value, col);
                    if (!valueRange.isEmpty()) {
                        mergeCalcMembers(nextGuide, valueRange, history);
                    }
                    break;
                case LEVEL_MEMBERS:
                    Level level = command.getLevel();
                    Range levelRange = range.subRangeForValue(level, col);
                    for (Range subRange : levelRange.subRanges(col)) {
                        mergeCalcMembers(nextGuide, subRange, history);
                    }
                    break;
                case OTHER_NATIVE:
                    for (Range subRange : range.subRanges(col)) {
                        mergeCalcMembers(nextGuide, subRange, history);
                    }
                    break;
                default:
                    throw Util.unexpected(command.getMemberType());
                }
            }
        }

        private void appendMembers(ReassemblyGuide guide, Range range) {
            int col = guide.getIndex();

            for (ReassemblyCommand command : guide.getCommands()) {
                switch (command.getMemberType()) {
                case NON_NATIVE:
                    tempTuple[col] = command.getMember();
                    appendTuple(range.getTuple(), tempTupleAsList);
                    break;
                case ENUMERATED_VALUE:
                    Member value = command.getMember();
                    Range valueRange = range.subRangeForValue(value, col);
                    if (!valueRange.isEmpty()) {
                        appendTuple(valueRange.getTuple());
                    }
                    break;
                case LEVEL_MEMBERS:
                case OTHER_NATIVE:
                    for (List<Member> tuple : range.getTuples()) {
                        appendTuple(tuple);
                    }
                    break;
                default:
                    throw Util.unexpected(command.getMemberType());
                }
            }
        }

        private void appendMembers(
            ReassemblyGuide guide, Range range, Set<List<Member>> history)
        {
            int col = guide.getIndex();

            for (ReassemblyCommand command : guide.getCommands()) {
                switch (command.getMemberType()) {
                case NON_NATIVE:
                    tempTuple[col] = command.getMember();
                    if (range.isEmpty()) {
                        appendTuple(tempTupleAsList, history);
                    } else {
                        appendTuple(range.getTuple(), tempTupleAsList, history);
                    }
                    break;
                case ENUMERATED_VALUE:
                    Member value = command.getMember();
                    Range valueRange = range.subRangeForValue(value, col);
                    if (!valueRange.isEmpty()) {
                        appendTuple(
                            valueRange.getTuple(), tempTupleAsList, history);
                    }
                    break;
                case LEVEL_MEMBERS:
                case OTHER_NATIVE:
                    tempTuple[col] = null;
                    for (List<Member> tuple : range.getTuples()) {
                        appendTuple(tuple, tempTupleAsList, history);
                    }
                    break;
                default:
                    throw Util.unexpected(command.getMemberType());
                }
            }
        }

        private void appendTuple(
            List<Member> nonNatives,
            Set<List<Member>> history)
        {
            if (history.add(nonNatives)) {
                appendTuple(nonNatives);
            }
        }

        private void appendTuple(
            List<Member> natives,
            List<Member> nonNatives,
            Set<List<Member>> history)
        {
            List<Member> copy = copyOfTuple(natives, nonNatives);
            if (history.add(copy)) {
                appendTuple(copy);
            }
        }

        private void appendTuple(
            List<Member> natives,
            List<Member> nonNatives)
        {
            appendTuple(copyOfTuple(natives, nonNatives));
        }

        private void appendTuple(List<Member> tuple) {
            resultList.add(tuple);
            checkNativeResultLimit(resultList.size());
        }

        private List<Member> copyOfTuple(
            List<Member> natives,
            List<Member> nonNatives)
        {
            Member[] copy = new Member[arity];
            for (int i = 0; i < arity; i++) {
                copy[i] =
                    (nonNatives.get(i) == null)
                        ? natives.get(i)
                        : nonNatives.get(i);
            }
            return Arrays.asList(copy);
        }

        /**
         * Check the resultSize against the result limit setting. Throws
         * LimitExceededDuringCrossjoin exception if limit exceeded.
         * <p/>
         * It didn't seem appropriate to use the existing Mondrian
         * ResultLimit property, since the meaning and use of that
         * property seems to be a bit ambiguous, otherwise we could
         * simply call Util.checkCJResultLimit.
         *
         * @param resultSize Result limit
         * @throws mondrian.olap.ResourceLimitExceededException
         *
         */
        private void checkNativeResultLimit(int resultSize) {
            // Throw an exeption if the size of the crossjoin exceeds the result
            // limit.
            if (resultLimit < resultSize) {
                throw MondrianResource.instance()
                    .LimitExceededDuringCrossjoin.ex(resultSize, resultLimit);
            }
        }

        public TupleList adaptList(
            final TupleList sourceList,
            final int destSize,
            final int[] destIndices)
        {
            if (sourceList.isEmpty()) {
                return TupleCollections.emptyList(destIndices.length);
            }

            checkNativeResultLimit(sourceList.size());

            TupleList destList =
                new DelegatingTupleList(
                    destSize,
                    new AbstractList<List<Member>>() {
                        @Override
                        public List<Member> get(int index) {
                            final List<Member> sourceTuple =
                                sourceList.get(index);
                            final Member[] members = new Member[destSize];
                            for (int i = 0; i < destIndices.length; i++) {
                                members[destIndices[i]] = sourceTuple.get(i);
                            }
                            return Arrays.asList(members);
                        }

                        @Override
                        public int size() {
                            return sourceList.size();
                        }
                    }
                );

            // The mergeCalcMembers method in this file assumes that the
            // resultList is random access - that calls to get(n) are constant
            // cost, regardless of n. Unfortunately, the TraversalList objects
            // created by HighCardSqlTupleReader are implemented using linked
            // lists, leading to pathologically long run times.
            // This presumes that the ResultStyle is LIST
            if (LOGGER.isDebugEnabled()) {
                String sourceListType =
                    sourceList.getClass().getSimpleName();
                String sourceElementType =
                    String.format("Member[%d]", destSize);
                LOGGER.debug(
                    String.format(
                        "returning native %s<%s> without copying to new list.",
                        sourceListType,
                        sourceElementType));
            }
            return destList;
        }
    }

    // REVIEW: Can we remove this class, and simply use TupleList?
    static class Range {
        private final TupleList list;
        private final int from;
        private final int to;

        public Range(TupleList list)
        {
            this(list, 0, list.size());
        }

        private Range(TupleList list, int from, int to) {
            if (from < 0) {
                throw new IllegalArgumentException("from is must be >= 0");
            }
            if (to > list.size()) {
                throw new IllegalArgumentException(
                    "to must be <= to list size");
            }
            if (from > to) {
                throw new IllegalArgumentException("from must be <= to");
            }

            this.list = list;
            this.from = from;
            this.to = to;
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public int size() {
            return to - from;
        }

        public List<Member> getTuple() {
            if (from >= list.size()) {
                throw new NoSuchElementException();
            }
            return list.get(from);
        }

        public List<List<Member>> getTuples() {
            if (from == 0 && to == list.size()) {
                return list;
            }
            return list.subList(from, to);
        }

        public Member getMember(int cursor, int col) {
            return list.get(cursor).get(col);
        }

        public String toString() {
            return "[" + from + " : " + to + "]";
        }

        private Range subRange(int fromRow, int toRow) {
            return new Range(list, fromRow, toRow);
        }

        public Range subRangeForValue(Member value, int col) {
            int startAt = nextMatching(value, from, col);
            int endAt = nextNonMatching(value, startAt + 1, col);
            return subRange(startAt, endAt);
        }

        public Range subRangeForValue(Level level, int col) {
            int startAt = nextMatching(level, from, col);
            int endAt = nextNonMatching(level, startAt + 1, col);
            return subRange(startAt, endAt);
        }

        public Range subRangeStartingAt(int startAt, int col) {
            Member value = list.get(startAt).get(col);
            int endAt = nextNonMatching(value, startAt + 1, col);
            return subRange(startAt, endAt);
        }

        private int nextMatching(Member value, int startAt, int col) {
            for (int cursor = startAt; cursor < to; cursor++) {
                if (value.equals(list.get(cursor).get(col))) {
                    return cursor;
                }
            }
            return to;
        }

        private int nextMatching(Level level, int startAt, int col) {
            for (int cursor = startAt; cursor < to; cursor++) {
                if (level.equals(list.get(cursor).get(col).getLevel())) {
                    return cursor;
                }
            }
            return to;
        }

        private int nextNonMatching(Member value, int startAt, int col) {
            if (value == null) {
                return nextNonNull(startAt, col);
            }
            for (int cursor = startAt; cursor < to; cursor++) {
                if (!value.equals(list.get(cursor).get(col))) {
                    return cursor;
                }
            }
            return to;
        }

        private int nextNonMatching(Level level, int startAt, int col) {
            if (level == null) {
                return nextNonNull(startAt, col);
            }
            for (int cursor = startAt; cursor < to; cursor++) {
                if (!level.equals(list.get(cursor).get(col).getLevel())) {
                    return cursor;
                }
            }
            return to;
        }

        private int nextNonNull(int startAt, int col) {
            for (int cursor = startAt; cursor < to; cursor++) {
                if (list.get(cursor).get(col) != null) {
                    return cursor;
                }
            }
            return to;
        }

        public Iterable<Range> subRanges(final int col) {
            final Range parent = this;

            return new Iterable<Range>() {
                final int rangeCol = col;

                public Iterator<Range> iterator() {
                    return new RangeIterator(parent, rangeCol);
                }
            };
        }

        public Iterable<Member> getMembers(final int col) {
            return new Iterable<Member>() {
                public Iterator<Member> iterator() {
                    return new Iterator<Member>() {
                        private int cursor = from;

                        public boolean hasNext() {
                            return cursor < to;
                        }

                        public Member next() {
                            if (!hasNext()) {
                                throw new NoSuchElementException();
                            }
                            return getMember(cursor++, col);
                        }

                        public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            };
        }
    }

    public static class RangeIterator
        implements Iterator<Range>
    {
        private final Range parent;
        private final int col;
        private Range precomputed;

        public RangeIterator(Range parent, int col) {
            this.parent = parent;
            this.col = col;
            precomputed = next(parent.from);
        }

        public boolean hasNext() {
            return precomputed != null;
        }

        private Range next(int cursor) {
            return (cursor >= parent.to)
                ? null
                : parent.subRangeStartingAt(cursor, col);
        }

        public Range next() {
            if (precomputed == null) {
                throw new NoSuchElementException();
            }
            Range it = precomputed;
            precomputed = next(precomputed.to);
            return it;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static class ReassemblyGuide {
        private final int index;
        private final List<ReassemblyCommand> commands =
            new ArrayList<ReassemblyCommand>();

        public ReassemblyGuide(int index) {
            this.index = index;
        }

        public int getIndex() {
            return index;
        }

        public List<ReassemblyCommand> getCommands() {
            return Collections.unmodifiableList(commands);
        }

        private void addCommandTuple(List<ReassemblyCommand> commandTuple) {
            ReassemblyCommand curr = currentCommand(commandTuple);

            if (index < commandTuple.size() - 1) {
                curr.forNextCol(index + 1).addCommandTuple(commandTuple);
            }
        }

        private ReassemblyCommand currentCommand(
            List<ReassemblyCommand> commandTuple)
        {
            ReassemblyCommand curr = commandTuple.get(index);
            ReassemblyCommand prev = commands.isEmpty()
                ? null : commands.get(commands.size() - 1);

            if (prev != null && prev.getMemberType() == SENTINEL) {
                commands.set(commands.size() - 1, curr);
            } else if (prev == null
                || !prev.getElement().equals(curr.getElement()))
            {
                commands.add(curr);
            } else {
                curr = prev;
            }
            return curr;
        }

        public String toString() {
            return "" + index + ":" + commands.toString()
                    .replaceAll("=null", "").replaceAll("=", " ") + " ";
        }
    }

    private static class ReassemblyCommand {
        private final OlapElement element;
        private final String elementName;
        private final NativeElementType memberType;
        private ReassemblyGuide nextColGuide;

        public ReassemblyCommand(
            Member member,
            NativeElementType memberType)
        {
            this.element = member;
            this.memberType = memberType;
            this.elementName = member.toString();
        }

        public ReassemblyCommand(
            Level level,
            NativeElementType memberType)
        {
            this.element = level;
            this.memberType = memberType;
            this.elementName = level.toString() + ".members";
        }

        public OlapElement getElement() {
            return element;
        }

        public String getElementName() {
            return elementName;
        }

        public Member getMember() {
            return (Member) element;
        }

        public Level getLevel() {
            return (Level) element;
        }

        public boolean hasNextGuide() {
            return nextColGuide != null;
        }

        public ReassemblyGuide forNextCol() {
            return nextColGuide;
        }

        public ReassemblyGuide forNextCol(int index) {
            if (nextColGuide == null) {
                nextColGuide = new ReassemblyGuide(index);
            }
            return nextColGuide;
        }

        public NativeElementType getMemberType() {
            return memberType;
        }

        public static Set<NativeElementType> getMemberTypes(
            Collection<ReassemblyCommand> commands)
        {
            Set<NativeElementType> types =
                Olap4jUtil.enumSetNoneOf(NativeElementType.class);
            for (ReassemblyCommand command : commands) {
                types.add(command.getMemberType());
            }
            return types;
        }

        @Override
        public String toString() {
            return memberType.toString() + ": " + getElementName();
        }
    }

    enum NativeElementType {
        LEVEL_MEMBERS(true),
        ENUMERATED_VALUE(true),
        OTHER_NATIVE(true),
        NON_NATIVE(false),
        SENTINEL(false);

        private final boolean isNativeCompatible;
        private NativeElementType(boolean isNativeCompatible) {
            this.isNativeCompatible = isNativeCompatible;
        }

        public boolean isNativeCompatible() {
            return isNativeCompatible;
        }
    }

    private static Id createSentinelId(Level level) {
        return hierarchyId(level)
            .append(q(createMangledName(level, SENTINEL_PREFIX)));
    }

    private static Id createMemberId(Level level) {
        return hierarchyId(level)
            .append(q(createMangledName(level, MEMBER_NAME_PREFIX)));
    }

    private static Id createSetId(Level level) {
        return new Id(
            q(createMangledName(level, SET_NAME_PREFIX)));
    }

    private static Id hierarchyId(Level level) {
        Id id = new Id(q(level.getDimension().getName()));
        if (MondrianProperties.instance().SsasCompatibleNaming.get()) {
            id = id.append(q(level.getHierarchy().getName()));
        }
        return id;
    }

    private static Id.Segment q(String s) {
        return new Id.NameSegment(s);
    }

    private static String createMangledName(Level level, String prefix) {
        return prefix
            + level.getUniqueName().replaceAll("[\\[\\]]", "")
            .replaceAll("\\.", "_")
            + "_";
    }

    private static void dumpListToLog(
        String heading, TupleList list)
    {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                String.format(
                    "%s created with %,d rows.", heading, list.size()));
            StringBuilder buf = new StringBuilder(Util.nl);
            for (List<Member> element : list) {
                buf.append(Util.nl);
                buf.append(element);
            }
            LOGGER.debug(buf.toString());
        }
    }

    private static <T> String toCsv(Collection<T> list) {
        StringBuilder buf = new StringBuilder();
        String sep = "";
        for (T element : list) {
            buf.append(sep).append(element);
            sep = ", ";
        }
        return buf.toString();
    }

    private static String getLevelNameFromMemberName(String memberName) {
        // we assume that the last token is the level name
        String tokens[] = memberName.split("_");
        return tokens[tokens.length - 1];
    }
}

// End NativizeSetFunDef.java

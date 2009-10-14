package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractTupleListCalc;
import mondrian.mdx.*;
import mondrian.olap.*;
import static mondrian.olap.fun.NativizeSetFunDef.NativeElementType.*;
import mondrian.olap.type.SetType;
import mondrian.resource.MondrianResource;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Definition of the <code>NativizeSet</code> MDX function.
 *
 * @author jrand
 * @version $Id$
 * @since Oct 14, 2009
 */
public class NativizeSetFunDef extends FunDefBase {
    /*
     * Static final fields.
     */
    protected static final Logger LOGGER =
        Logger.getLogger(NativizeSetFunDef.class);

    private static final long DEFAULT_HIGH_CARDINALITY_THRESHOLD = 200L * 1000L;
    private static final long DEFAULT_NATIVE_RESULT_LIMIT = 100L * 1000L;

    private static final String SENTINAL_PREFIX = "_Nativized_Sentinal_";
    private static final String MEMBER_NAME_PREFIX = "_Nativized_Member_";
    private static final String SET_NAME_PREFIX = "_Nativized_Set_";
    private static final List<Class<? extends FunDef>> functionWhitelist =
        Arrays.asList(
            (Class<? extends FunDef>) CacheFunDef.class,
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
     * Static non-final fields.
     */
    private static long highCardinalityThreshold
        = DEFAULT_HIGH_CARDINALITY_THRESHOLD;
    private static long nativeResultLimit
        = DEFAULT_NATIVE_RESULT_LIMIT;

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

    public NativizeSetFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
        LOGGER.debug("---- NativizeSetFunDef constructor");
    }

    public static void setHighCardinalityThreshold(long value) {
        highCardinalityThreshold = value;
    }

    public static long getHighCardinalityThreshold() {
        return highCardinalityThreshold;
    }

    public static void setNativeResultLimit(long value) {
        nativeResultLimit = value;
    }

    public static long getNativeResultLimit() {
        return nativeResultLimit;
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
        final Calc[] calcs = new Calc[] {compiler.compileList(funArg, true)};

        final int arity = ((SetType) calcs[0].getType()).getArity();
        if (arity < 1) {
            throw new IllegalArgumentException(
                "unexpected value for .getArity() - " + arity);
        } else if (arity == 1) {
            compiler.getEvaluator().setNonEmpty(false);
            return funArg.accept(compiler);
        } else if (substitutionMap.isEmpty()) {
            return funArg.accept(compiler);
        } else {
            if (isFirstCompileCall) {
                isFirstCompileCall = false;
                originalExp = funArg.clone();
                Query query = compiler.getEvaluator().getQuery();
                call.accept(
                    new AddFormulasVisitor(query, substitutionMap, dimensions));
                call.accept(new TransformToFormulasVisitor(query));
                query.resolve();
            }
            return new NativeTupleListCalc(
                call, calcs, compiler, substitutionMap, originalExp);
        }
    }

    public static class NativeTupleListCalc extends AbstractTupleListCalc {
        private final SubstitutionMap substitutionMap;
        private final TupleListCalc simpleCalc;
        private final ExpCompiler compiler;

        private final Exp originalExp;

        protected NativeTupleListCalc(
            ResolvedFunCall call,
            Calc[] calcs,
            ExpCompiler compiler,
            SubstitutionMap substitutionMap,
            Exp originalExp)
        {
            super(call, calcs);
            LOGGER.debug("---- NativeTupleListCalc constructor");
            this.substitutionMap = substitutionMap;
            this.simpleCalc = (TupleListCalc) calcs[0];
            this.compiler = compiler;
            this.originalExp = originalExp;
        }

        public List<Member[]> evaluateTupleList(Evaluator evaluator) {
            return computeTuples(evaluator);
        }

        @SuppressWarnings({"unchecked"})
        public List<Member[]> computeTuples(Evaluator evaluator) {
            // Force non-empty to false to create the simplified list.
            List<Member[]> simplifiedList =
                simpleCalc.evaluateTupleList(evaluator.push(false));
            dumpListToLog("simplified list:", simplifiedList);

            if (simplifiedList.isEmpty()) {
                return simplifiedList;
            }

            if (!isHighCardinality(evaluator, simplifiedList)) {
                LOGGER.info(
                    "Disabling native evaluation. originalExp="
                        + originalExp);
                Query query = evaluator.getQuery();
                query.setVirtualCubeNonNativeCrossJoin();

                Exp parsed =
                    query.getConnection().parseExpression(
                        "{" + originalExp.toString() + "}");
                Exp resolved = compiler.getValidator().validate(parsed, false);
                resolved.accept(
                    new TransformFromFormulasVisitor(query, compiler));
                query.resolve();
                ListCalc calc = compiler.compileList(resolved);
                return calc.evaluateList(evaluator);
            }

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
            return analyzer.mergeCalcMembers(
                evaluateJoinExpression(evaluator.push(true), crossJoin));
        }

        private boolean isHighCardinality(
            Evaluator evaluator, List<Member[]> simplifiedList)
        {
            Util.assertTrue(!simplifiedList.isEmpty());

            SchemaReader schema = evaluator.getSchemaReader();
            Member[] tuple = simplifiedList.get(0);
            long tupleCardinality = 1L;

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
                        schema.getLevelCardinality(hierarchyLevel, false, true);
                    tupleCardinality *= levelCardinality;
                    if (tupleCardinality >= highCardinalityThreshold) {
                        LOGGER.info(
                            "isHighCardinality=true"
                                + " partially computed="
                                + tupleCardinality
                                + " threshold=" + highCardinalityThreshold);
                        return true;
                    }
                }
            }

            int listSize = simplifiedList.size();
            long simplifiedListCardinality = listSize * tupleCardinality;
            boolean isHighCardinality
                = (simplifiedListCardinality >= highCardinalityThreshold);

            LOGGER.info(
                "isHighCardinality="
                    + isHighCardinality
                    + " computed=" + simplifiedListCardinality
                    + " threshold=" + highCardinalityThreshold);
            return isHighCardinality;
        }

        private Iterable<?> evaluateJoinExpression(
            Evaluator evaluator, String crossJoinExpression)
        {
            LOGGER.debug(
                "reconstituted crossJoinExpression:\n"
                    + crossJoinExpression
                    + "\n");

            Exp unresolved = evaluator.getQuery().getConnection()
                .parseExpression(crossJoinExpression);
            Exp resolved = compiler.getValidator().validate(unresolved, false);
            IterCalc calc = compiler.compileIter(resolved);
            return calc.evaluateIterable(evaluator);
        }
    }

    static abstract class OneTimeOnlyVisitor extends MdxVisitorImpl {
        private List<Exp> visited = new ArrayList<Exp>();
        private boolean shouldVisit = true;

        public Object visit(final ResolvedFunCall call) {
            if (hasBeenVisited(call)) {
                return null;
            }
            addVisited(call);
            if (shouldVisit) {
                return visitResolvedFunCall(call);
            }
            return null;
        }

        private boolean hasBeenVisited(final Exp exp) {
            for (Exp visitedCall : visited) {
                //== is correct.  I want them to be the same object
                if (visitedCall == exp) {
                    return true;
                }
            }
            return false;
        }

        public Object visit(final MemberExpr memberExpr) {
            if (hasBeenVisited(memberExpr)) {
                return null;
            }
            addVisited(memberExpr);
            if (shouldVisit) {
                return visitMemberExpr(memberExpr);
            }
            return null;
        }

        private void addVisited(final Exp call) {
            visited.add(call);
        }

        protected void markAllChildrenVisited(final ResolvedFunCall call) {
            setShouldVisit(false);
            for (Exp arg : call.getArgs()) {
                arg.accept(this);
            }
            setShouldVisit(true);
        }

        private void setShouldVisit(final boolean shouldVisit) {
            this.shouldVisit = shouldVisit;
        }

        Object visitMemberExpr(final MemberExpr memberExpr) {
            return null;
        }

        abstract Object visitResolvedFunCall(final ResolvedFunCall call);
    }

    static class FindLevelsVisitor extends OneTimeOnlyVisitor {
        private final SubstitutionMap substitutionMap;
        private final Set<Dimension> dimensions;

        public FindLevelsVisitor(
            SubstitutionMap substitutionMap, HashSet<Dimension> dimensions)
        {
            this.substitutionMap = substitutionMap;
            this.dimensions = dimensions;
        }

        @Override
        public Object visitResolvedFunCall(ResolvedFunCall call) {
            if (call.getFunDef() instanceof LevelMembersFunDef) {
                Level level = ((LevelExpr) call.getArg(0)).getLevel();
                substitutionMap.put(createMemberId(level), level);
                dimensions.add(level.getDimension());
            } else if (
                functionWhitelist.contains(call.getFunDef().getClass()))
            {
                for (Exp arg : call.getArgs()) {
                    arg.accept(this);
                }
            } else {
                markAllChildrenVisited(call);
            }
            return null;
        }


        @Override
        public Object visitMemberExpr(MemberExpr member) {
            dimensions.add(member.getMember().getDimension());
            return null;
        }
    }

    static class AddFormulasVisitor extends OneTimeOnlyVisitor {
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
        public Object visitResolvedFunCall(ResolvedFunCall call) {
            if (call.getFunDef() instanceof NativizeSetFunDef) {
                addFormulasToQuery();
            }
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
                formulas.add(createSentinalFormula(level));
            }

            query.addFormulas(formulas.toArray(new Formula[formulas.size()]));
        }

        private Formula createSentinalFormula(Level level) {
            Id memberId = createSentinalId(level);
            Exp memberExpr = query.getConnection()
                .parseExpression("101010");

            LOGGER.debug(
                "createSentinalFormula memberId="
                + memberId
                + " memberExpr="
                + memberExpr);
            return new Formula(memberId, memberExpr, new MemberProperty[0]);
        }

        private Formula createDefaultMemberFormula(Level level) {
            Id memberId = createMemberId(level);
            String defaultMember =
                "[" + level.getDimension().getName() + "].DEFAULTMEMBER";
            Exp memberExpr = query.getConnection()
                .parseExpression(defaultMember);

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

    static class TransformToFormulasVisitor extends OneTimeOnlyVisitor {
        private final Query query;

        public TransformToFormulasVisitor(Query query) {
            LOGGER.debug("---- TransformToFormulasVisitor constructor");
            this.query = query;
        }

        @Override
        public Object visitResolvedFunCall(ResolvedFunCall call) {
            LOGGER.debug("visit " + call);
            Object result = null;
            if (call.getFunDef() instanceof LevelMembersFunDef) {
                result = replaceLevelMembersReferences(call);
            } else if (
                functionWhitelist.contains(call.getFunDef().getClass()))
            {
                result = visitCallArguments(call);
            } else {
                markAllChildrenVisited(call);
            }
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
                && call.getFunDef() instanceof SetFunDef) {
                return flattenSetFunDef(call);
            }
            return null;
        }

        private Object flattenSetFunDef(ResolvedFunCall call) {
            List<Exp> newArgs = new ArrayList<Exp>();
            flattenSetMembers(newArgs, call.getArgs());
            addSentinalMembers(newArgs);
            if (newArgs.size() != call.getArgCount()) {
                return new ResolvedFunCall(
                    call.getFunDef()
                    , newArgs.toArray(new Exp[newArgs.size()])
                    , call.getType());
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

        private void addSentinalMembers(List<Exp> args) {
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
                        Id memberId = createSentinalId(level);
                        Formula formula =
                            query.findFormula(memberId.toString());
                        args.add(i++, Util.createExpr(formula.getMdxMember()));
                    }
                }
                prev = curr;
            }
        }
    }

    static class TransformFromFormulasVisitor extends OneTimeOnlyVisitor {
        private final Query query;
        private final ExpCompiler compiler;

        public TransformFromFormulasVisitor(Query query, ExpCompiler compiler) {
            LOGGER.debug("---- TransformFromFormulasVisitor constructor");
            this.query = query;
            this.compiler = compiler;
        }

        @Override
        public Object visitResolvedFunCall(ResolvedFunCall call) {
            LOGGER.debug("visit " + call);
            Object result;
            result = visitCallArguments(call);

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

        private final int tupleSize;
        private final Member[] tempTuple;
        private final int[] nativeIndices;
        private final int resultLimit;

        private final List<Collection<String>> nativeMembers;
        private final ReassemblyGuide reassemblyGuide;
        private final List<Member[]> resultList;
        private final Set<List<Member>> potentialDups;

        public CrossJoinAnalyzer(
            List<Member[]> simplifiedList, SubstitutionMap substitutionMap)
        {
            tupleSize = simplifiedList.get(0).length;
            tempTuple = new Member[tupleSize];
            resultLimit = nativeResultLimit <= 0
                    ? Integer.MAX_VALUE
                    : (int) Math.min(nativeResultLimit, Integer.MAX_VALUE);

            new ArrayList<Collection<String>>();
            resultList = new ArrayList<Member[]>();
            potentialDups = new HashSet<List<Member>>();

            reassemblyGuide = classifyMembers(simplifiedList, substitutionMap);
            nativeMembers = findNativeMembers();
            nativeIndices = findNativeIndices();
        }

        public ReassemblyGuide classifyMembers(
            List<Member[]> simplifiedList,
            SubstitutionMap substitutionMap)
        {
            ReassemblyGuide guide = new ReassemblyGuide(0);

            for (Member[] srcTuple : simplifiedList) {
                ReassemblyCommand[] cmdTuple = new ReassemblyCommand[tupleSize];

                for (int i = 0; i < tupleSize; i++) {
                    Member mbr = srcTuple[i];

                    if (substitutionMap.contains(mbr)) {
                        cmdTuple[i] = new ReassemblyCommand(
                            substitutionMap.get(mbr), LEVEL_MEMBERS);
                    } else if (mbr.getName().startsWith(SENTINAL_PREFIX)) {
                        cmdTuple[i] = new ReassemblyCommand(mbr, SENTINAL);
                    } else {
                        NativeElementType nativeType = !isNativeCompatible(mbr)
                            ? NON_NATIVE
                            : mbr.getMemberType() == Member.MemberType.REGULAR
                            ? ENUMERATED_VALUE
                            : OTHER_NATIVE;
                        cmdTuple[i] = new ReassemblyCommand(mbr, nativeType);
                    }
                }
                guide.addCommandTuple(cmdTuple);
            }
            return guide;
        }

        private List<Collection<String>> findNativeMembers() {
            List<Collection<String>> nativeMembers
                = new ArrayList<Collection<String>>(tupleSize);

            for (int i = 0; i < tupleSize; i++) {
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
            EnumSet<NativeElementType> typesToAdd =
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
            int[] indices = new int[tupleSize];
            int nativeColCount = 0;

            for (int i = 0; i < tupleSize; i++) {
                Collection<String> natives = nativeMembers.get(i);
                if (!natives.isEmpty()) {
                    indices[nativeColCount++] = i;
                }
            }

            if (nativeColCount == tupleSize) {
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

        private List<Member[]> mergeCalcMembers(Object nativeValues) {
            List<Member[]> nativeList =
                (nativeValues instanceof List)
                ? adaptList((List<?>) nativeValues, tupleSize, nativeIndices)
                : copyList(
                    (Iterable<?>) nativeValues, tupleSize, nativeIndices);

            dumpListToLog("native list:", nativeList);
            mergeCalcMembers(reassemblyGuide, new Range(nativeList), true);
            dumpListToLog("result list:", resultList);
            return resultList;
        }

        private void mergeCalcMembers(
            ReassemblyGuide guide, Range range, boolean isNew)
        {
            int col = guide.getIndex();
            if (col == tupleSize - 1) {
                appendMembers(guide, range, isNew);
                return;
            }

            for (ReassemblyCommand command : guide.getCommands()) {
                ReassemblyGuide nextGuide = command.forNextCol();

                switch (command.getMemberType()) {
                case NON_NATIVE:
                    tempTuple[col] = command.getMember();
                    mergeCalcMembers(nextGuide, range, false);
                    break;
                case ENUMERATED_VALUE:
                    Member value = command.getMember();
                    tempTuple[col] = value;
                    Range valueRange = range.subRangeForValue(value, col);
                    mergeCalcMembers(nextGuide, valueRange, isNew);
                    break;
                case LEVEL_MEMBERS:
                    Level level = command.getLevel();
                    Range levelRange = range.subRangeForValue(level, col);
                    for (Range subRange : levelRange.subRanges(col)) {
                        tempTuple[col] = subRange.getFirstMember(col);
                        mergeCalcMembers(nextGuide, subRange, isNew);
                    }
                    break;
                case OTHER_NATIVE:
                    for (Range subRange : range.subRanges(col)) {
                        tempTuple[col] = subRange.getFirstMember(col);
                        mergeCalcMembers(nextGuide, subRange, isNew);
                    }
                    break;
                default:
                    throw Util.unexpected(command.getMemberType());
                }
            }
        }

        private void appendMembers(
            ReassemblyGuide guide, Range range, boolean isNew)
        {
            int col = guide.getIndex();

            for (ReassemblyCommand command : guide.getCommands()) {
                switch (command.getMemberType()) {
                case NON_NATIVE:
                case ENUMERATED_VALUE:
                    tempTuple[col] = command.getMember();
                    appendTupleToResultIfNew(isNew);
                    break;
                case LEVEL_MEMBERS:
                case OTHER_NATIVE:
                    for (Member member : range.getMembers(col)) {
                        tempTuple[col] = member;
                        appendTupleToResultIfNew(isNew);
                    }
                    break;
                default:
                    throw Util.unexpected(command.getMemberType());
                }
            }
        }

        private void appendTupleToResultIfNew(boolean isNew) {
            Member[] newTuple = copyOfTuple(tempTuple);
            if (isNew || potentialDups.add(Arrays.asList(newTuple))) {
                resultList.add(newTuple);
                checkNativeResultLimit(resultList.size());
            }
        }

        private Member[] copyOfTuple(Member[] sourceTuple) {
            // In Java 6, one could use
            // 'copy = Arrays.copyOf(sourceTuple, tupleSize)'
            Member[] copy = new Member[sourceTuple.length];
            System.arraycopy(sourceTuple, 0, copy, 0, sourceTuple.length);
            return copy;
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

        public List<Member[]> adaptList(
            final List<?> source, final int destSize, final int[] destIndices)
        {
            checkNativeResultLimit(source.size());

            if (source.isEmpty()) {
                return Collections.emptyList();
            }

            final Object element = source.get(0);
            if ((element instanceof Member[])
                && ((Member[]) element).length == destSize)
            {
                LOGGER.info("Using native list as is.");
                return Util.cast(source);
            }

            LOGGER.info("Using native list with widening adapter.");
            return new AbstractList<Member[]>() {
                final RowAdapter rowAdapter =
                    RowAdapter.createAdapter(element, destSize, destIndices);
                final List<?> delegate = source;

                public Member[] get(int index) {
                    return rowAdapter.copyOf(delegate.get(index));
                }

                public int size() {
                    return delegate.size();
                }
            };
        }

        public List<Member[]> copyList(
            Iterable<?> source, int destSize, int destIndices[])
        {
            Iterator<?> iterator = source.iterator();
            List<Member[]> dest = new ArrayList<Member[]>();

            LOGGER.info("Copying native list.");
            if (iterator.hasNext()) {
                Object element = iterator.next();
                RowAdapter adapter =
                    RowAdapter.createAdapter(element, destSize, destIndices);

                dest.add(adapter.copyOf(element));
                int size = 1;
                while (iterator.hasNext()) {
                    checkNativeResultLimit(size++);
                    dest.add(adapter.copyOf(iterator.next()));
                }
            }

            return dest;
        }

        private static abstract class RowAdapter {
            abstract Member[] copyOf(Object o);

            public static RowAdapter createAdapter(
                Object source, int destSize, int[] destIndices)
            {
                return (source instanceof Member)
                    ? adaptMember(destSize, destIndices[0])
                    : adaptTuple(destSize, destIndices);
            }

            private static RowAdapter adaptMember(
                final int destSize, final int destIndex)
            {
                return new RowAdapter() {
                    public Member[] copyOf(Object source) {
                        Member[] copy = new Member[destSize];
                        copy[destIndex] = (Member) source;
                        return copy;
                    }
                };
            }

            private static RowAdapter adaptTuple(
                final int destSize, final int[] destIndices)
            {
                return new RowAdapter() {
                    public Member[] copyOf(Object o) {
                        Member[] source = (Member[]) o;
                        Member[] copy = new Member[destSize];
                        for (int i = 0; i < destIndices.length; i++) {
                            copy[destIndices[i]] = source[i];
                        }
                        return copy;
                    }
                };
            }
        }
    }

    static class Range {
        private final List<Member[]> list;
        private final int from;
        private final int to;

        public Range(List<Member[]> list)
        {
            this(list, 0, list.size());
        }

        private Range(List<Member[]> list, int from, int to) {
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

        public Member getMember(int cursor, int col) {
            return list.get(cursor)[col];
        }

        public Member getFirstMember(int col) {
            return list.get(from)[col];
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
            Member value = list.get(startAt)[col];
            int endAt = nextNonMatching(value, startAt + 1, col);
            return subRange(startAt, endAt);
        }

        private int nextMatching(Member value, int startAt, int col) {
            for (int cursor = startAt; cursor < to; cursor++) {
                if (value.equals(list.get(cursor)[col])) {
                    return cursor;
                }
            }
            return to;
        }

        private int nextMatching(Level level, int startAt, int col) {
            for (int cursor = startAt; cursor < to; cursor++) {
                if (level.equals(list.get(cursor)[col].getLevel())) {
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
                if (!value.equals(list.get(cursor)[col])) {
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
                if (!level.equals(list.get(cursor)[col].getLevel())) {
                    return cursor;
                }
            }
            return to;
        }

        private int nextNonNull(int startAt, int col) {
            for (int cursor = startAt; cursor < to; cursor++) {
                if (list.get(cursor)[col] != null) {
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
        private final List<ReassemblyCommand> commands
            = new ArrayList<ReassemblyCommand>();

        public ReassemblyGuide(int index) {
            this.index = index;
        }

        public int getIndex() {
            return index;
        }

        public List<ReassemblyCommand> getCommands() {
            return Collections.unmodifiableList(commands);
        }

        private void addCommandTuple(ReassemblyCommand[] commandTuple) {
            ReassemblyCommand curr = currentCommand(commandTuple);

            if (index < commandTuple.length - 1) {
                curr.forNextCol(index + 1).addCommandTuple(commandTuple);
            }
        }

        private ReassemblyCommand currentCommand(
            ReassemblyCommand[] commandTuple)
        {
            ReassemblyCommand curr = commandTuple[index];
            ReassemblyCommand prev = commands.isEmpty()
                ? null : commands.get(commands.size() - 1);

            if (prev != null && prev.getMemberType() == SENTINAL) {
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

        public static EnumSet<NativeElementType>
        getMemberTypes(Collection<ReassemblyCommand> commands) {
            EnumSet<NativeElementType> types =
                EnumSet.noneOf(NativeElementType.class);
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
        SENTINAL(false);

        private final boolean isNativeCompatible;
        private NativeElementType(boolean isNativeCompatible) {
            this.isNativeCompatible = isNativeCompatible;
        }

        public boolean isNativeCompatible() {
            return isNativeCompatible;
        }
    }

    private static Id createSentinalId(Level level) {
        return createId(level.getDimension().getName(),
            createMangledName(level, SENTINAL_PREFIX));
    }

    private static Id createMemberId(Level level) {
        return createId(level.getDimension().getName(),
            createMangledName(level, MEMBER_NAME_PREFIX));
    }

    private static Id createSetId(Level level) {
        return createId(createMangledName(level, SET_NAME_PREFIX));
    }

    private static Id createId(String... names) {
        ArrayList<Id.Segment> segments = new ArrayList<Id.Segment>();
        for (String name : names) {
            segments.add(new Id.Segment(name, Id.Quoting.QUOTED));
        }
        return new Id(segments);
    }

    private static String createMangledName(Level level, String prefix) {
        return prefix
            + level.getUniqueName().replaceAll("[\\[\\]]", "")
            .replaceAll("\\.", "_")
            + "_";
    }

    private static void dumpListToLog(
        String heading, List<Member[]> nativeList)
    {
        if (LOGGER.isDebugEnabled()) {
            StringBuilder buf = new StringBuilder("\n" + heading + "\n");
            for (Member[] element : nativeList) {
                buf.append(Util.commaList("\n", Arrays.asList(element)));
            }
            LOGGER.debug(buf.toString());
        }
    }

    private static <T> String toCsv(Iterable<T> list) {
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
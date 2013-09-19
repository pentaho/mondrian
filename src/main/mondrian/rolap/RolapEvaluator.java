/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;

import mondrian.calc.Calc;
import mondrian.calc.ParameterSlot;
import mondrian.olap.*;
import mondrian.olap.fun.FunUtil;
import mondrian.server.Statement;
import mondrian.spi.Dialect;
import mondrian.util.Format;

import org.apache.log4j.Logger;

import java.util.*;

/**
 * <code>RolapEvaluator</code> evaluates expressions in a dimensional
 * environment.
 *
 * <p>The context contains a member (which may be the default member)
 * for every dimension in the current cube. Certain operations, such as
 * evaluating a calculated member or a tuple, change the current context.
 *
 * <p>There are two ways of preserving context.
 *
 * <p>First, the {@link #push}
 * method creates a verbatim copy of the evaluator. Use that copy for
 * computations, and any changes of state will be made only to the copy.
 *
 * <p>Second, the {@link #savepoint()} method tells the evaluator to create a
 * checkpoint of its state, and returns an {@code int} value that can later be
 * passed to {@link #restore(int)}.
 *
 * <p>The {@code savepoint} method is recommended for most purposes, because the
 * initial checkpoint is extremely cheap. Each call that modifies state (such as
 * {@link mondrian.olap.Evaluator#setContext(mondrian.olap.Member)}) creates, at
 * a modest cost, an entry on an internal command stack.
 *
 * <p>One occasion that you would use {@code push} is when creating an
 * iterator, and the iterator needs its own evaluator context, even if the
 * code that created the iterator later reverts the context. In this case,
 * the iterator's constructor should call {@code push}.
 *
 * <h3>Developers note</h3>
 *
 * <p>Many of the methods in this class are performance-critical. Where
 * possible they are declared 'final' so that the JVM can optimize calls to
 * these methods. If future functionality requires it, the 'final' modifier
 * can be removed and these methods can be overridden.
 *
 * @author jhyde
 * @since 10 August, 2001
 */
public class RolapEvaluator implements Evaluator {
    private static final Logger LOGGER = Logger.getLogger(RolapEvaluator.class);

    /**
     * Dummy value to represent null results in the expression cache.
     */
    private static final Object nullResult = new Object();

    private final RolapMember[] currentMembers;
    private final RolapEvaluator parent;
    protected CellReader cellReader;
    private final int ancestorCommandCount;

    private Member expandingMember;
    private boolean firstExpanding;
    private boolean nonEmpty;
    protected final RolapEvaluatorRoot root;
    private int iterationLength;
    private boolean evalAxes;

    private final RolapCalculation[] calculations;
    private int calculationCount;

    /**
     * List of lists of tuples or members, rarely used, but overrides the
     * ordinary dimensional context if set when a cell value comes to be
     * evaluated.
     */
    protected final List<List<List<Member>>> aggregationLists;

    private final List<Member> slicerMembers;
    private boolean nativeEnabled;
    private Member[] nonAllMembers;
    private int commandCount;
    private Object[] commands;

    /**
     * Set of expressions actively being expanded. Prevents infinite cycle of
     * expansions.
     *
     * @return Mutable set of expressions being expanded
     */
    public Set<Exp> getActiveNativeExpansions() {
        return root.activeNativeExpansions;
    }

    /**
     * States of the finite state machine for determining the max solve order
     * for the "scoped" behavior.
     */
    private enum ScopedMaxSolveOrderFinderState {
        START,
        AGG_SCOPE,
        CUBE_SCOPE,
        QUERY_SCOPE
    }

    /**
     * Creates a non-root evaluator.
     *
     * @param root Root context for stack of evaluators (contains information
     *   which does not change during the evaluation)
     * @param parent Parent evaluator, not null
     * @param aggregationList List of tuples to add to aggregation context,
     *     or null
     */
    protected RolapEvaluator(
        RolapEvaluatorRoot root,
        RolapEvaluator parent,
        List<List<Member>> aggregationList)
    {
        this.iterationLength = 1;
        this.root = root;
        assert parent != null;
        this.parent = parent;

        ancestorCommandCount =
            parent.ancestorCommandCount + parent.commandCount;
        nonEmpty = parent.nonEmpty;
        nativeEnabled = parent.nativeEnabled;
        evalAxes = parent.evalAxes;
        cellReader = parent.cellReader;
        currentMembers = parent.currentMembers.clone();
        calculations = parent.calculations.clone();
        calculationCount = parent.calculationCount;
        slicerMembers = new ArrayList<Member>(parent.slicerMembers);

        commands = new Object[10];
        commands[0] = Command.SAVEPOINT; // sentinel
        commandCount = 1;

        // Build aggregationLists, combining parent's aggregationLists (if not
        // null) and the new aggregation list (if any).
        List<List<List<Member>>> aggregationLists = null;
        if (parent.aggregationLists != null) {
            aggregationLists =
                new ArrayList<List<List<Member>>>(parent.aggregationLists);
        }
        if (aggregationList != null) {
            if (aggregationLists == null) {
                aggregationLists = new ArrayList<List<List<Member>>>();
            }
            aggregationLists.add(aggregationList);
            List<Member> tuple = aggregationList.get(0);
            for (Member member : tuple) {
                setContext(member.getHierarchy().getAllMember());
            }
        }
        this.aggregationLists = aggregationLists;

        expandingMember = parent.expandingMember;
    }

    /**
     * Creates a root evaluator.
     *
     * @param root Shared context between this evaluator and its children
     */
    public RolapEvaluator(RolapEvaluatorRoot root) {
        this.iterationLength = 1;
        this.root = root;
        this.parent = null;
        ancestorCommandCount = 0;
        nonEmpty = false;
        nativeEnabled =
            MondrianProperties.instance().EnableNativeNonEmpty.get()
            || MondrianProperties.instance().EnableNativeCrossJoin.get();
        evalAxes = false;
        cellReader = null;
        currentMembers = root.defaultMembers.clone();
        calculations = new RolapCalculation[currentMembers.length];
        calculationCount = 0;
        slicerMembers = new ArrayList<Member>();
        aggregationLists = null;

        commands = new Object[10];
        commands[0] = Command.SAVEPOINT; // sentinel
        commandCount = 1;

        for (RolapMember member : currentMembers) {
            if (member.isEvaluated()) {
                addCalculation(member, true);
            }
        }

        // we expect client to set CellReader
    }

    /**
     * Creates an evaluator.
     */
    public static Evaluator create(Statement statement) {
        final RolapEvaluatorRoot root = new RolapEvaluatorRoot(statement);
        return new RolapEvaluator(root);
    }

    public RolapCube getMeasureCube() {
        final RolapMember measure = currentMembers[0];
        if (measure instanceof RolapStoredMeasure) {
            return ((RolapStoredMeasure) measure).getCube();
        }
        return null;
    }

    public boolean mightReturnNullForUnrelatedDimension() {
        if (!MondrianProperties.instance()
            .IgnoreMeasureForNonJoiningDimension.get())
        {
            return false;
        }
        RolapCube virtualCube = getCube();
        return virtualCube.isVirtual();
    }

    public boolean needToReturnNullForUnrelatedDimension(Member[] members) {
        assert mightReturnNullForUnrelatedDimension()
            : "Should not even call this method if nulls are impossible";
        RolapCube baseCube = getMeasureCube();
        if (baseCube == null) {
            return false;
        }
        RolapCube virtualCube = getCube();
        if (virtualCube.shouldIgnoreUnrelatedDimensions(baseCube.getName())) {
            return false;
        }
        Set<Dimension> nonJoiningDimensions =
            baseCube.nonJoiningDimensions(members);
        return !nonJoiningDimensions.isEmpty();
    }

    public boolean nativeEnabled() {
        return nativeEnabled;
    }

    public boolean currentIsEmpty() {
        // If a cell evaluates to null, it is always deemed empty.
        Object o = evaluateCurrent();
        if (o == Util.nullValue || o == null) {
            return true;
        }
        final RolapCube measureCube = getMeasureCube();
        if (measureCube == null) {
            return false;
        }
        // For other cell values (e.g. zero), the cell is deemed empty if the
        // number of fact table rows is zero.
        final int savepoint = savepoint();
        try {
            setContext(measureCube.getFactCountMeasure());
            o = evaluateCurrent();
        } finally {
            restore(savepoint);
        }
        return o == null
           || (o instanceof Number && ((Number) o).intValue() == 0);
    }

    public Member getPreviousContext(Hierarchy hierarchy) {
        for (RolapEvaluator e = this; e != null; e = e.parent) {
            for (int i = commandCount - 1; i > 0;) {
                Command command = (Command) commands[i];
                if (command == Command.SET_CONTEXT) {
                    return (Member) commands[i - 2];
                }
                i -= command.width;
            }
        }
        return null;
    }

    public final QueryTiming getTiming() {
        return root.execution.getQueryTiming();
    }

    public final int savepoint() {
        final int commandCount1 = commandCount;
        if (commands[commandCount - 1] == Command.SAVEPOINT) {
            // Already at a save point; no need to create another.
            return commandCount1;
        }

        // enough room for CHECKSUM command, if asserts happen to be enabled
        ensureCommandCapacity(commandCount + 3);
        commands[commandCount++] = Command.SAVEPOINT;
        //noinspection AssertWithSideEffects
        assert !Util.DEBUG || addChecksumStateCommand();
        return commandCount1;
    }

    public final void setNativeEnabled(boolean nativeEnabled) {
        if (nativeEnabled != this.nativeEnabled) {
            ensureCommandCapacity(commandCount + 2);
            commands[commandCount++] = this.nativeEnabled;
            commands[commandCount++] = Command.SET_NATIVE_ENABLED;
            this.nativeEnabled = nativeEnabled;
        }
    }

    protected final Logger getLogger() {
        return LOGGER;
    }

    public final Member[] getMembers() {
        return currentMembers;
    }

    public final Member[] getNonAllMembers() {
        if (nonAllMembers == null) {
            nonAllMembers = new RolapMember[root.nonAllPositionCount];
            for (int i = 0; i < root.nonAllPositionCount; i++) {
                int nonAllPosition = root.nonAllPositions[i];
                nonAllMembers[i] = currentMembers[nonAllPosition];
            }
        }
        return nonAllMembers;
    }

    public final List<List<List<Member>>> getAggregationLists() {
        return aggregationLists;
    }

    final void setCellReader(CellReader cellReader) {
        if (cellReader != this.cellReader) {
            ensureCommandCapacity(commandCount + 2);
            commands[commandCount++] = this.cellReader;
            commands[commandCount++] = Command.SET_CELL_READER;
            this.cellReader = cellReader;
        }
    }

    public final RolapCube getCube() {
        return root.cube;
    }

    public final Query getQuery() {
        return root.query;
    }

    public final int getDepth() {
        return 0;
    }

    public final RolapEvaluator getParent() {
        return parent;
    }

    public final SchemaReader getSchemaReader() {
        return root.schemaReader;
    }

    public Date getQueryStartTime() {
        return root.getQueryStartTime();
    }

    public Dialect getDialect() {
        return root.currentDialect;
    }

    public final RolapEvaluator push(Member[] members) {
        final RolapEvaluator evaluator = _push(null);
        evaluator.setContext(members);
        return evaluator;
    }

    public final RolapEvaluator push(Member member) {
        final RolapEvaluator evaluator = _push(null);
        evaluator.setContext(member);
        return evaluator;
    }

    public final Evaluator push(boolean nonEmpty) {
        final RolapEvaluator evaluator = _push(null);
        evaluator.setNonEmpty(nonEmpty);
        return evaluator;
    }

    public final Evaluator push(boolean nonEmpty, boolean nativeEnabled) {
        final RolapEvaluator evaluator = _push(null);
        evaluator.setNonEmpty(nonEmpty);
        evaluator.setNativeEnabled(nativeEnabled);
        return evaluator;
    }

    public final RolapEvaluator push() {
        return _push(null);
    }

    private void ensureCommandCapacity(int minCapacity) {
        if (minCapacity > commands.length) {
            int newCapacity = commands.length * 2;
            if (newCapacity < minCapacity) {
                newCapacity = minCapacity;
            }
            commands = Util.copyOf(commands, newCapacity);
        }
    }

    /**
     * Adds a command to the stack that ensures that the state after restoring
     * is the same as the current state.
     *
     * <p>Returns true so that can conveniently be called from 'assert'.
     *
     * @return true
     */
    private boolean addChecksumStateCommand() {
        // assume that caller has checked that command array is large enough
        commands[commandCount++] = checksumState();
        commands[commandCount++] = Command.CHECKSUM;
        return true;
    }

    /**
     * Creates a clone of the current validator.
     *
     * @param aggregationList List of tuples to add to aggregation context,
     *     or null
     */
    protected RolapEvaluator _push(List<List<Member>> aggregationList) {
        root.execution.checkCancelOrTimeout();
        return new RolapEvaluator(root, this, aggregationList);
    }

    public final void restore(int savepoint) {
        while (commandCount > savepoint) {
            ((Command) commands[--commandCount]).execute(this);
        }
    }

    public final Evaluator pushAggregation(List<List<Member>> list) {
        return _push(list);
    }

    /**
     * Returns true if the other object is a {@link RolapEvaluator} with
     * identical context.
     */
    public final boolean equals(Object obj) {
        if (!(obj instanceof RolapEvaluator)) {
            return false;
        }
        RolapEvaluator that = (RolapEvaluator) obj;
        return Arrays.equals(this.currentMembers, that.currentMembers);
    }

    public final int hashCode() {
        return Util.hashArray(0, this.currentMembers);
    }

    /**
     * Adds a slicer member to the evaluator context, and remember it as part
     * of the slicer. The slicer members are passed onto derived evaluators
     * so that functions using those evaluators can choose to ignore the
     * slicer members. One such function is CrossJoin emptiness check.
     *
     * @param member a member in the slicer
     */
    public final void setSlicerContext(Member member) {
        setContext(member);
        slicerMembers.add(member);
    }

    /**
     * Return the list of slicer members in the current evaluator context.
     * @return slicerMembers
     */
    public final List<Member> getSlicerMembers() {
        return slicerMembers;
    }

    public final Member setContext(Member member) {
        // Note: the body of this function is identical to calling
        // 'setContext(member, true)'. We inline the logic for performance.

        final RolapMemberBase m = (RolapMemberBase) member;
        final int ordinal = m.getHierarchy().getOrdinalInCube();
        final RolapMember previous = currentMembers[ordinal];

        // If the context is unchanged, save ourselves some effort. It would be
        // a mistake to use equals here; we might treat the visual total member
        // 'Gender.All' the same as the true 'Gender.All' because they have the
        // same unique name, and that would be wrong.
        if (m == previous) {
            return previous;
        }
        // We call 'exists' before 'removeCalcMember' for efficiency.
        // 'exists' has a smaller stack to search before 'removeCalcMember'
        // adds an 'ADD_CALCULATION' command.
        if (!exists(ordinal)) {
            ensureCommandCapacity(commandCount + 3);
            commands[commandCount++] = previous;
            commands[commandCount++] = ordinal;
            commands[commandCount++] = Command.SET_CONTEXT;
        }
        if (previous.isEvaluated()) {
            removeCalculation(previous, false);
        }
        currentMembers[ordinal] = m;
        if (previous.isAll() && !m.isAll() && isNewPosition(ordinal)) {
            root.nonAllPositions[root.nonAllPositionCount] = ordinal;
            root.nonAllPositionCount++;
        }
        if (m.isEvaluated()) {
            addCalculation(m, false);
        }
        nonAllMembers = null;
        return previous;
    }

    public final void setContext(Member member, boolean safe) {
        final RolapMemberBase m = (RolapMemberBase) member;
        final int ordinal = m.getHierarchy().getOrdinalInCube();
        final RolapMember previous = currentMembers[ordinal];

        // If the context is unchanged, save ourselves some effort. It would be
        // a mistake to use equals here; we might treat the visual total member
        // 'Gender.All' the same as the true 'Gender.All' because they have the
        // same unique name, and that would be wrong.
        if (m == previous) {
            return;
        }
        if (safe) {
            // We call 'exists' before 'removeCalcMember' for efficiency.
            // 'exists' has a smaller stack to search before 'removeCalcMember'
            // adds an 'ADD_CALCULATION' command.
            if (!exists(ordinal)) {
                ensureCommandCapacity(commandCount + 3);
                commands[commandCount++] = previous;
                commands[commandCount++] = ordinal;
                commands[commandCount++] = Command.SET_CONTEXT;
            }
        }
        if (previous.isEvaluated()) {
            removeCalculation(previous, false);
        }
        currentMembers[ordinal] = m;
        if (previous.isAll() && !m.isAll() && isNewPosition(ordinal)) {
            root.nonAllPositions[root.nonAllPositionCount] = ordinal;
            root.nonAllPositionCount++;
        }
        if (m.isEvaluated()) {
            addCalculation(m, false);
        }
        nonAllMembers = null;
    }

    /**
     * Returns whether a member of the hierarchy with a given ordinal has been
     * preserved on the stack since the last savepoint.
     *
     * @param ordinal Hierarchy ordinal
     * @return Whether there is a member with the given hierarchy ordinal on
     *   the stack
     */
    private boolean exists(int ordinal) {
        for (int i = commandCount - 1;;) {
            final Command command = (Command) commands[i];
            switch (command) {
            case SAVEPOINT:
                return false;
            case SET_CONTEXT:
                final Integer memberOrdinal = (Integer) commands[i - 1];
                if (ordinal == memberOrdinal) {
                    return true;
                }
                break;
            }
            i -= command.width;
        }
    }

    private boolean isNewPosition(int ordinal) {
        for (int nonAllPosition : root.nonAllPositions) {
            if (ordinal == nonAllPosition) {
                return false;
            }
        }
        return true;
    }

    public final void setContext(List<Member> memberList) {
        for (int i = 0, n = memberList.size(); i < n; i++) {
            Member member = memberList.get(i);
            assert member != null : "null member in " + memberList;
            setContext(member);
        }
    }

    public final void setContext(List<Member> memberList, boolean safe) {
        for (int i = 0, n = memberList.size(); i < n; i++) {
            Member member = memberList.get(i);
            assert member != null : "null member in " + memberList;
            setContext(member, safe);
        }
    }

    public final void setContext(Member[] members) {
        for (int i = 0, length = members.length; i < length; i++) {
            Member member = members[i];
            assert member != null
                : "null member in " + Arrays.toString(members);
            setContext(member);
        }
    }

    public final void setContext(Member[] members, boolean safe) {
        for (int i = 0, length = members.length; i < length; i++) {
            Member member = members[i];
            assert member != null : Arrays.asList(members);
            setContext(member, safe);
        }
    }

    public final RolapMember getContext(Hierarchy hierarchy) {
        return currentMembers[((RolapHierarchy) hierarchy).getOrdinalInCube()];
    }

    /**
     * More specific version of {@link #getContext(mondrian.olap.Hierarchy)},
     * for internal code.
     *
     * @param hierarchy Hierarchy
     * @return current member
     */
    public final RolapMember getContext(RolapHierarchy hierarchy) {
        return currentMembers[hierarchy.getOrdinalInCube()];
    }

    public final Object evaluateCurrent() {
        // Get the member in the current context which is (a) calculated, and
        // (b) has the highest solve order. If there are no calculated members,
        // go ahead and compute the cell.
        RolapCalculation maxSolveMember;
        switch (calculationCount) {
        case 0:
            final Object o = cellReader.get(this);
            if (o == Util.nullValue) {
                return null;
            }
            return o;

        case 1:
            maxSolveMember = calculations[0];
            break;

        default:
            switch (root.solveOrderMode) {
            case ABSOLUTE:
                maxSolveMember = getAbsoluteMaxSolveOrder();
                break;
            case SCOPED:
                maxSolveMember = getScopedMaxSolveOrder();
                break;
            default:
                throw Util.unexpected(root.solveOrderMode);
            }
        }
        final int savepoint = savepoint();
        maxSolveMember.setContextIn(this);
        final Calc calc = maxSolveMember.getCompiledExpression(root);
        final Object o;
        try {
            o = calc.evaluate(this);
        } finally {
            restore(savepoint);
        }
        if (o == Util.nullValue) {
            return null;
        }
        return o;
    }

    void setExpanding(Member member) {
        assert member != null;
        ensureCommandCapacity(commandCount + 3);
        commands[commandCount++] = this.expandingMember;
        commands[commandCount++] = this.firstExpanding;
        commands[commandCount++] = Command.SET_EXPANDING;
        expandingMember = member;
        firstExpanding = true; // REVIEW: is firstExpanding used?

        final int totalCommandCount = commandCount + ancestorCommandCount;
        if (totalCommandCount > root.recursionCheckCommandCount) {
            checkRecursion(this, commandCount - 4);

            // Set the threshold where we will next check for infinite
            // recursion.
            root.recursionCheckCommandCount =
                totalCommandCount + (root.defaultMembers.length << 4);
        }
    }

    /**
     * Returns the calculated member being currently expanded.
     *
     * <p>This can be useful if many calculated members are generated with
     * essentially the same expression. The compiled expression can call this
     * method to find which instance of the member is current, and therefore the
     * calculated members can share the same {@link Calc} object.
     *
     * @return Calculated member currently being expanded
     */
    Member getExpanding() {
        return expandingMember;
    }

    /**
     * Makes sure that there is no evaluator with identical context on the
     * stack.
     *
     * @param eval Evaluator
     * @throws mondrian.olap.fun.MondrianEvaluationException if there is a loop
     */
    private static void checkRecursion(RolapEvaluator eval, int c) {
        RolapMember[] members = eval.currentMembers.clone();
        Member expanding = eval.expandingMember;

        // Find an ancestor evaluator that has identical context to this one:
        // same member context, and expanding the same calculation.
        while (true) {
            if (c < 0) {
                eval = eval.parent;
                if (eval == null) {
                    return;
                }
                c = eval.commandCount - 1;
            } else {
                Command command = (Command) eval.commands[c];
                switch (command) {
                case SET_CONTEXT:
                    int memberOrdinal = (Integer) eval.commands[c - 1];
                    RolapMember member = (RolapMember) eval.commands[c - 2];
                    members[memberOrdinal] = member;
                    break;
                case SET_EXPANDING:
                    expanding = (RolapMember) eval.commands[c - 2];
                    if (Arrays.equals(members, eval.currentMembers)
                        && expanding == eval.expandingMember)
                    {
                        throw FunUtil.newEvalException(
                            null,
                            "Infinite loop while evaluating calculated member '"
                            + eval.expandingMember + "'; context stack is "
                            + eval.getContextString());
                    }
                }
                c -= command.width;
            }
        }
    }

    private String getContextString() {
        RolapMember[] members = currentMembers.clone();
        final boolean skipDefaultMembers = true;
        final StringBuilder buf = new StringBuilder("{");
        int frameCount = 0;
        boolean changedSinceLastSavepoint = false;
        for (RolapEvaluator eval = this; eval != null; eval = eval.parent) {
            if (eval.expandingMember == null) {
                continue;
            }
            for (int c = eval.commandCount - 1; c > 0;) {
                Command command = (Command) eval.commands[c];
                switch (command) {
                case SAVEPOINT:
                    if (changedSinceLastSavepoint) {
                        if (frameCount++ > 0) {
                            buf.append(", ");
                        }
                        buf.append("(");
                        int memberCount = 0;
                        for (Member m : members) {
                            if (skipDefaultMembers
                                && m == m.getHierarchy().getDefaultMember())
                            {
                                continue;
                            }
                            if (memberCount++ > 0) {
                                buf.append(", ");
                            }
                            buf.append(m.getUniqueName());
                        }
                        buf.append(")");
                    }
                    changedSinceLastSavepoint = false;
                    break;
                case SET_CONTEXT:
                    changedSinceLastSavepoint = true;
                    int memberOrdinal = (Integer) eval.commands[c - 1];
                    RolapMember member = (RolapMember) eval.commands[c - 2];
                    members[memberOrdinal] = member;
                    break;
                }
                c -= command.width;
            }
        }
        buf.append("}");
        return buf.toString();
    }

    public final Object getProperty(String name, Object defaultValue) {
        Object o = defaultValue;
        int maxSolve = Integer.MIN_VALUE;
        int i = -1;
        for (Member member : getNonAllMembers()) {
            i++;
            // more than one usage
            if (member == null) {
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug(
                        "RolapEvaluator.getProperty: member == null "
                        + " , count=" + i);
                }
                continue;
            }

            // Don't call member.getPropertyValue unless this member's
            // solve order is greater than one we've already seen.
            // The getSolveOrder call is cheap call compared to the
            // getPropertyValue call, and when we're evaluating millions
            // of members, this has proven to make a significant performance
            // difference.
            final int solve = member.getSolveOrder();
            if (solve > maxSolve) {
                final Object p = member.getPropertyValue(name);
                if (p != null) {
                    o = p;
                    maxSolve = solve;
                }
            }
        }
        return o;
    }

    /**
     * Returns the format string for this cell. This is computed by evaluating
     * the format expression in the current context, and therefore different
     * cells may have different format strings.
     *
     * @post return != null
     */
    public final String getFormatString() {
        final Exp formatExp =
            (Exp) getProperty(Property.FORMAT_EXP_PARSED.name, null);
        if (formatExp == null) {
            return "Standard";
        }
        final Calc formatCalc = root.getCompiled(formatExp, true, null);
        final Object o = formatCalc.evaluate(this);
        if (o == null) {
            return "Standard";
        }
        return o.toString();
    }

    private Format getFormat() {
        final String formatString = getFormatString();
        return getFormat(formatString);
    }

    private Format getFormat(String formatString) {
        return Format.get(formatString, root.connection.getLocale());
    }

    public final Locale getConnectionLocale() {
        return root.connection.getLocale();
    }

    public final String format(Object o) {
        if (o == Util.nullValue) {
            o = null;
        }
        if (o instanceof Throwable) {
            return "#ERR: " + o.toString();
        }
        Format format = getFormat();
        return format.format(o);
    }

    public final String format(Object o, String formatString) {
        if (o == Util.nullValue) {
            o = null;
        }
        if (o instanceof Throwable) {
            return "#ERR: " + o.toString();
        }
        Format format = getFormat(formatString);
        return format.format(o);
    }

    /**
     * Creates a key which uniquely identifes an expression and its
     * context. The context includes members of dimensions which the
     * expression is dependent upon.
     */
    private Object getExpResultCacheKey(ExpCacheDescriptor descriptor) {
        // in NON EMPTY mode the result depends on everything, e.g.
        // "NON EMPTY [Customer].[Name].members" may return different results
        // for 1997-01 and 1997-02
        final List<Object> key;
        if (nonEmpty) {
            key = new ArrayList<Object>(currentMembers.length + 1);
            key.add(descriptor.getExp());
            //noinspection ManualArrayToCollectionCopy
            for (RolapMember currentMember : currentMembers) {
                key.add(currentMember);
            }
        } else {
            final int[] hierarchyOrdinals =
                descriptor.getDependentHierarchyOrdinals();
            key = new ArrayList<Object>(hierarchyOrdinals.length + 1);
            key.add(descriptor.getExp());
            for (final int hierarchyOrdinal : hierarchyOrdinals) {
                final Member member = currentMembers[hierarchyOrdinal];
                assert member != null;
                key.add(member);
            }
        }
        return key;
    }

    public final Object getCachedResult(ExpCacheDescriptor cacheDescriptor) {
        // Look up a cached result, and if not present, compute one and add to
        // cache. Use a dummy value to represent nulls.
        final Object key = getExpResultCacheKey(cacheDescriptor);
        Object result = root.getCacheResult(key);
        if (result == null) {
            boolean aggCacheDirty = cellReader.isDirty();
            int aggregateCacheMissCountBefore = cellReader.getMissCount();
            result = cacheDescriptor.evaluate(this);
            int aggregateCacheMissCountAfter = cellReader.getMissCount();

            boolean isValidResult;

            if (!aggCacheDirty
                && (aggregateCacheMissCountBefore
                    == aggregateCacheMissCountAfter))
            {
                // Cache the evaluation result as valid result if the
                // evaluation did not use any missing aggregates. Missing
                // aggregates could be used when aggregate cache is not fully
                // loaded, or if new missing aggregates are seen.
                isValidResult = true;
            } else {
                // Cache the evaluation result as invalid result if the
                // evaluation uses missing aggregates.
                isValidResult = false;
            }
            root.putCacheResult(
                key,
                result == null ? nullResult : result,
                isValidResult);
        } else if (result == nullResult) {
            result = null;
        }

        return result;
    }

    public final void clearExpResultCache(boolean clearValidResult) {
        root.clearResultCache(clearValidResult);
    }

    public final boolean isNonEmpty() {
        return nonEmpty;
    }

    public final void setNonEmpty(boolean nonEmpty) {
        if (nonEmpty != this.nonEmpty) {
            ensureCommandCapacity(commandCount + 2);
            commands[commandCount++] = this.nonEmpty;
            commands[commandCount++] = Command.SET_NON_EMPTY;
            this.nonEmpty = nonEmpty;
        }
    }

    public final RuntimeException newEvalException(Object context, String s) {
        return FunUtil.newEvalException((FunDef) context, s);
    }

    public final NamedSetEvaluator getNamedSetEvaluator(
        NamedSet namedSet,
        boolean create)
    {
        return root.evaluateNamedSet(namedSet, create);
    }

    public final SetEvaluator getSetEvaluator(
        Exp exp,
        boolean create)
    {
        return root.evaluateSet(exp, create);
    }

    public final int getMissCount() {
        return cellReader.getMissCount();
    }

    public final Object getParameterValue(ParameterSlot slot) {
        return root.getParameterValue(slot);
    }

    final void addCalculation(
        RolapCalculation calculation,
        boolean reversible)
    {
        assert calculation != null;
        calculations[calculationCount++] = calculation;

        if (reversible && !(calculation instanceof RolapMember)) {
            // Add command to remove this calculation.
            ensureCommandCapacity(commandCount + 2);
            commands[commandCount++] = calculation;
            commands[commandCount++] = Command.REMOVE_CALCULATION;
        }
    }

    /**
     * Returns the member with the highest solve order according to AS2000
     * rules. This was the behavior prior to solve order mode being
     * configurable.
     *
     * <p>The SOLVE_ORDER value is absolute regardless of where it is defined;
     * e.g. a query defined calculated member with a SOLVE_ORDER of 1 always
     * takes precedence over a cube defined value of 2.
     *
     * <p>No special consideration is given to the aggregate function.
     */
    private RolapCalculation getAbsoluteMaxSolveOrder() {
        // Find member with the highest solve order.
        RolapCalculation maxSolveMember = calculations[0];
        for (int i = 1; i < calculationCount; i++) {
            RolapCalculation member = calculations[i];
            if (expandsBefore(member, maxSolveMember)) {
                maxSolveMember = member;
            }
        }
        return maxSolveMember;
    }

    /**
     * Returns the member with the highest solve order according to AS2005
     * scoping rules.
     *
     * <p>By default, cube calculated members are resolved before any session
     * scope calculated members, and session scope members are resolved before
     * any query defined calculation.  The SOLVE_ORDER value only applies within
     * the scope in which it was defined.
     *
     * <p>The aggregate function is always applied to base members; i.e. as if
     * SOLVE_ORDER was defined to be the lowest value in a given evaluation in a
     * SSAS2000 sense.
     */
    private RolapCalculation getScopedMaxSolveOrder() {
        // Finite state machine that determines the member with the highest
        // solve order.
        RolapCalculation maxSolveMember = null;
        ScopedMaxSolveOrderFinderState state =
            ScopedMaxSolveOrderFinderState.START;
        for (int i = 0; i < calculationCount; i++) {
            RolapCalculation calculation = calculations[i];
            switch (state) {
            case START:
                maxSolveMember = calculation;
                if (maxSolveMember.containsAggregateFunction()) {
                    state = ScopedMaxSolveOrderFinderState.AGG_SCOPE;
                } else if (maxSolveMember.isCalculatedInQuery()) {
                    state = ScopedMaxSolveOrderFinderState.QUERY_SCOPE;
                } else {
                    state = ScopedMaxSolveOrderFinderState.CUBE_SCOPE;
                }
                break;

            case AGG_SCOPE:
                if (calculation.containsAggregateFunction()) {
                    if (expandsBefore(calculation, maxSolveMember)) {
                        maxSolveMember = calculation;
                    }
                } else if (calculation.isCalculatedInQuery()) {
                    maxSolveMember = calculation;
                    state = ScopedMaxSolveOrderFinderState.QUERY_SCOPE;
                } else {
                    maxSolveMember = calculation;
                    state = ScopedMaxSolveOrderFinderState.CUBE_SCOPE;
                }
                break;

            case CUBE_SCOPE:
                if (calculation.containsAggregateFunction()) {
                    continue;
                }

                if (calculation.isCalculatedInQuery()) {
                    maxSolveMember = calculation;
                    state = ScopedMaxSolveOrderFinderState.QUERY_SCOPE;
                } else if (expandsBefore(calculation, maxSolveMember)) {
                    maxSolveMember = calculation;
                }
                break;

            case QUERY_SCOPE:
                if (calculation.containsAggregateFunction()) {
                    continue;
                }

                if (calculation.isCalculatedInQuery()) {
                    if (expandsBefore(calculation, maxSolveMember)) {
                        maxSolveMember = calculation;
                    }
                }
                break;
            }
        }

        return maxSolveMember;
    }

    /**
     * Returns whether a given calculation expands before another.
     * A calculation expands before another if its solve order is higher,
     * or if its solve order is the same and its dimension ordinal is lower.
     *
     * @param calc1 First calculated member or tuple
     * @param calc2 Second calculated member or tuple
     * @return Whether calc1 expands before calc2
     */
    private boolean expandsBefore(
        RolapCalculation calc1,
        RolapCalculation calc2)
    {
        final int solveOrder1 = calc1.getSolveOrder();
        final int solveOrder2 = calc2.getSolveOrder();
        if (solveOrder1 > solveOrder2) {
            return true;
        } else {
            return solveOrder1 == solveOrder2
                && calc1.getHierarchyOrdinal()
                    < calc2.getHierarchyOrdinal();
        }
    }

    final void removeCalculation(
        RolapCalculation calculation,
        boolean reversible)
    {
        for (int i = 0; i < calculationCount; i++) {
            if (calculations[i] == calculation) {
                // overwrite this member with the end member
                --calculationCount;
                calculations[i] = calculations[calculationCount];
                assert calculations[i] != null;
                calculations[calculationCount] = null; // to allow gc

                if (reversible && !(calculation instanceof RolapMember)) {
                    // Add a command to re-add the calculation.
                    ensureCommandCapacity(commandCount + 2);
                    commands[commandCount++] = calculation;
                    commands[commandCount++] = Command.ADD_CALCULATION;
                }
                return;
            }
        }
        throw new AssertionError(
            "calculation " + calculation + " not on stack");
    }

    public final int getIterationLength() {
        return iterationLength;
    }

    public final void setIterationLength(int iterationLength) {
        ensureCommandCapacity(commandCount + 2);
        commands[commandCount++] = this.iterationLength;
        commands[commandCount++] = Command.SET_ITERATION_LENGTH;
        this.iterationLength = iterationLength;
    }

    public final boolean isEvalAxes() {
        return evalAxes;
    }

    public final void setEvalAxes(boolean evalAxes) {
        if (evalAxes != this.evalAxes) {
            ensureCommandCapacity(commandCount + 2);
            commands[commandCount++] = this.evalAxes;
            commands[commandCount++] = Command.SET_EVAL_AXES;
            this.evalAxes = evalAxes;
        }
    }

    private int checksumState() {
        int h = 0;
        h = h * 31 + Arrays.asList(currentMembers).hashCode();
        h = h * 31 + new HashSet<RolapCalculation>(
            Arrays.asList(calculations)
                .subList(0, calculationCount)).hashCode();
        h = h * 31 + slicerMembers.hashCode();
        h = h * 31 + (expandingMember == null ? 0 : expandingMember.hashCode());
        h = h * 31
            + (aggregationLists == null ? 0 : aggregationLists.hashCode());
        h = h * 31
            + (nonEmpty ? 0x1 : 0x2)
            + (nativeEnabled ? 0x4 : 0x8)
            + (firstExpanding ? 0x10 : 0x20)
            + (evalAxes ? 0x40 : 0x80);
        if (false) {
            // Enable this code block to debug checksum mismatches.
            System.err.println(
                "h=" + h + ": " + Arrays.asList(
                    Arrays.asList(currentMembers),
                    new HashSet<RolapCalculation>(
                        Arrays.asList(calculations).subList(
                            0, calculationCount)),
                    expandingMember,
                    aggregationLists,
                    nonEmpty,
                    nativeEnabled,
                    firstExpanding,
                    evalAxes));
        }
        return h;
    }

    /**
     * Checks if unrelated dimensions to the measure in the current context
     * should be ignored.
     * @return boolean
     */
    public boolean shouldIgnoreUnrelatedDimensions() {
        return getCube().shouldIgnoreUnrelatedDimensions(
            getMeasureCube().getName());
    }

    private enum Command {
        SET_CONTEXT(2) {
            @Override
            void execute(RolapEvaluator evaluator) {
                final int memberOrdinal =
                    (Integer) evaluator.commands[--evaluator.commandCount];
                final RolapMember member =
                    (RolapMember) evaluator.commands[--evaluator.commandCount];
                evaluator.setContext(member, false);
            }
        },
        SET_NATIVE_ENABLED(1) {
            @Override
            void execute(RolapEvaluator evaluator) {
                evaluator.nativeEnabled =
                    (Boolean) evaluator.commands[--evaluator.commandCount];
            }
        },
        SET_NON_EMPTY(1) {
            @Override
            void execute(RolapEvaluator evaluator) {
                evaluator.nonEmpty =
                    (Boolean) evaluator.commands[--evaluator.commandCount];
            }
        },
        SET_EVAL_AXES(1) {
            @Override
            void execute(RolapEvaluator evaluator) {
                evaluator.evalAxes =
                    (Boolean) evaluator.commands[--evaluator.commandCount];
            }
        },
        SET_EXPANDING(2) {
            @Override
            void execute(RolapEvaluator evaluator) {
                evaluator.firstExpanding =
                    (Boolean) evaluator.commands[--evaluator.commandCount];
                evaluator.expandingMember =
                    (Member) evaluator.commands[--evaluator.commandCount];
            }
        },
        SET_ITERATION_LENGTH(1) {
            @Override
            void execute(RolapEvaluator evaluator) {
                evaluator.iterationLength =
                    (Integer) evaluator.commands[--evaluator.commandCount];
            }
        },
        SET_CELL_READER(1) {
            @Override
            void execute(RolapEvaluator evaluator) {
                evaluator.cellReader =
                    (CellReader) evaluator.commands[--evaluator.commandCount];
            }
        },
        CHECKSUM(1) {
            @Override
            void execute(RolapEvaluator evaluator) {
                final int value =
                    (Integer) evaluator.commands[--evaluator.commandCount];
                final int currentState = evaluator.checksumState();
                assert value == currentState
                    : "Current checksum " + currentState
                      + " != previous checksum " + value;
            }
        },
        ADD_CALCULATION(1) {
            @Override
            void execute(RolapEvaluator evaluator) {
                final RolapCalculation calculation =
                    (RolapCalculation)
                        evaluator.commands[--evaluator.commandCount];
                evaluator.calculations[evaluator.calculationCount++] =
                    calculation;
            }
        },
        REMOVE_CALCULATION(1) {
            @Override
            void execute(RolapEvaluator evaluator) {
                final RolapCalculation calculation =
                    (RolapCalculation)
                        evaluator.commands[--evaluator.commandCount];
                evaluator.removeCalculation(calculation, false);
            }
        },
        SAVEPOINT(0) {
            @Override
            void execute(RolapEvaluator evaluator) {
                // nothing to do; command is just a marker
            }
        };

        public final int width;

        Command(int argCount) {
            this.width = argCount + 1;
        }

        abstract void execute(RolapEvaluator evaluator);
    }
}

// End RolapEvaluator.java

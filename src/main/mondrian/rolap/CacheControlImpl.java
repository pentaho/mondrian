/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2015 Pentaho Corporation..  All rights reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.agg.SegmentCacheManager;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.spi.SegmentColumn;
import mondrian.util.ArraySortedSet;

import org.eigenbase.util.property.BooleanProperty;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import javax.sql.DataSource;

/**
 * Implementation of {@link CacheControl} API.
 *
 * @author jhyde
 * @since Sep 27, 2006
 */
public class CacheControlImpl implements CacheControl {
    private final RolapConnection connection;

    /**
     * Object to lock before making changes to the member cache.
     *
     * <p>The "member cache" is a figure of speech: each RolapHierarchy has its
     * own MemberCache object. But to provide transparently serialized access
     * to the "member cache" via the interface CacheControl, provide a common
     * lock here.
     *
     * <p>NOTE: static member is a little too wide a scope for this lock,
     * because in theory a JVM can contain multiple independent instances of
     * mondrian.
     */
    private static final Object MEMBER_CACHE_LOCK = new Object();

    /**
     * Creates a CacheControlImpl.
     *
     * @param connection Connection
     */
    public CacheControlImpl(RolapConnection connection) {
        super();
        this.connection = connection;
    }

    // cell cache control
    public CellRegion createMemberRegion(Member member, boolean descendants) {
        if (member == null) {
            throw new NullPointerException();
        }
        final ArrayList<Member> list = new ArrayList<Member>();
        list.add(member);
        return new MemberCellRegion(list, descendants);
    }

    public CellRegion createMemberRegion(
        boolean lowerInclusive,
        Member lowerMember,
        boolean upperInclusive,
        Member upperMember,
        boolean descendants)
    {
        if (lowerMember == null) {
            lowerInclusive = false;
        }
        if (upperMember == null) {
            upperInclusive = false;
        }
        return new MemberRangeCellRegion(
            (RolapMember) lowerMember, lowerInclusive,
            (RolapMember) upperMember, upperInclusive,
            descendants);
    }

    public CellRegion createCrossjoinRegion(CellRegion... regions) {
        assert regions != null;
        assert regions.length >= 2;
        final HashSet<Dimension> set = new HashSet<Dimension>();
        final List<CellRegionImpl> list = new ArrayList<CellRegionImpl>();
        for (CellRegion region : regions) {
            int prevSize = set.size();
            List<Dimension> dimensionality = region.getDimensionality();
            set.addAll(dimensionality);
            if (set.size() < prevSize + dimensionality.size()) {
                throw MondrianResource.instance()
                    .CacheFlushCrossjoinDimensionsInCommon.ex(
                        getDimensionalityList(regions));
            }

            flattenCrossjoin((CellRegionImpl) region, list);
        }
        return new CrossjoinCellRegion(list);
    }

    // Returns e.g. "'[[Product]]', '[[Time], [Product]]'"
    private String getDimensionalityList(CellRegion[] regions) {
        StringBuilder buf = new StringBuilder();
        int k = 0;
        for (CellRegion region : regions) {
            if (k++ > 0) {
                buf.append(", ");
            }
            buf.append("'");
            buf.append(region.getDimensionality().toString());
            buf.append("'");
        }
        return buf.toString();
    }

    public CellRegion createUnionRegion(CellRegion... regions)
    {
        if (regions == null) {
            throw new NullPointerException();
        }
        if (regions.length < 2) {
            throw new IllegalArgumentException();
        }
        final List<CellRegionImpl> list = new ArrayList<CellRegionImpl>();
        for (CellRegion region : regions) {
            if (!region.getDimensionality().equals(
                    regions[0].getDimensionality()))
            {
                throw MondrianResource.instance()
                    .CacheFlushUnionDimensionalityMismatch.ex(
                        regions[0].getDimensionality().toString(),
                        region.getDimensionality().toString());
            }
            list.add((CellRegionImpl) region);
        }
        return new UnionCellRegion(list);
    }

    public CellRegion createMeasuresRegion(Cube cube) {
        Dimension measuresDimension = null;
        for (Dimension dim : cube.getDimensions()) {
            if (dim.isMeasures()) {
                measuresDimension = dim;
                break;
            }
        }
        if (measuresDimension == null) {
            throw new MondrianException(
                "No measures dimension found for cube "
                + cube.getName());
        }
        final List<Member> measures =
            cube.getSchemaReader(null).withLocus().getLevelMembers(
                measuresDimension.getHierarchy().getLevels()[0],
                false);
        if (measures.size() == 0) {
            return new EmptyCellRegion();
        }
        return new MemberCellRegion(measures, false);
    }

    public void flush(final CellRegion region) {
        Locus.execute(
            connection,
            "Flush",
            new Locus.Action<Void>() {
                public Void execute() {
                    flushInternal(region);
                    return null;
                }
            });
    }

    private void flushInternal(CellRegion region) {
        if (region instanceof EmptyCellRegion) {
            return;
        }
        final List<Dimension> dimensionality = region.getDimensionality();
        boolean found = false;
        for (Dimension dimension : dimensionality) {
            if (dimension.isMeasures()) {
                found = true;
                break;
            }
        }
        if (!found) {
            throw MondrianResource.instance().CacheFlushRegionMustContainMembers
                .ex();
        }
        final UnionCellRegion union = normalize((CellRegionImpl) region);
        for (CellRegionImpl cellRegion : union.regions) {
            // Figure out the bits.
            flushNonUnion(cellRegion);
        }
    }

    /**
     * Flushes a list of cell regions.
     *
     * @param cellRegionList List of cell regions
     */
    protected void flushRegionList(List<CellRegion> cellRegionList) {
        final CellRegion cellRegion;
        switch (cellRegionList.size()) {
        case 0:
            return;
        case 1:
            cellRegion = cellRegionList.get(0);
            break;
        default:
            final CellRegion[] cellRegions =
                cellRegionList.toArray(new CellRegion[cellRegionList.size()]);
            cellRegion = createUnionRegion(cellRegions);
            break;
        }
        if (!containsMeasures(cellRegion)) {
            for (RolapCube cube : connection.getSchema().getCubeList()) {
                flush(
                    createCrossjoinRegion(
                        createMeasuresRegion(cube),
                        cellRegion));
            }
        } else {
            flush(cellRegion);
        }
    }

    private boolean containsMeasures(CellRegion cellRegion) {
        final List<Dimension> dimensionList = cellRegion.getDimensionality();
        for (Dimension dimension : dimensionList) {
            if (dimension.isMeasures()) {
                return true;
            }
        }
        return false;
    }

    public void trace(String message) {
        // ignore message
    }

    public boolean isTraceEnabled() {
        return false;
    }

    public void flushSchemaCache() {
        RolapSchemaPool.instance().clear();
        // In some cases, the request might originate from a reference
        // to the schema which isn't in the pool anymore. We must also call
        // the cleanup procedure on the current connection.
        if (connection != null
            && connection.getSchema() != null)
        {
            connection.getSchema().finalCleanUp();
        }
    }

    // todo: document
    public void flushSchema(
        String catalogUrl,
        String connectionKey,
        String jdbcUser,
        String dataSourceStr)
    {
        RolapSchemaPool.instance().remove(
            catalogUrl,
            connectionKey,
            jdbcUser,
            dataSourceStr);
    }

    // todo: document
    public void flushSchema(
        String catalogUrl,
        DataSource dataSource)
    {
        RolapSchemaPool.instance().remove(
            catalogUrl,
            dataSource);
    }

    /**
     * Flushes the given RolapSchema instance from the pool
     *
     * @param schema RolapSchema
     */
    public void flushSchema(Schema schema) {
        if (RolapSchema.class.isInstance(schema)) {
            RolapSchemaPool.instance().remove((RolapSchema)schema);
        } else {
            throw new UnsupportedOperationException(
                schema.getClass().getName() + " cannot be flushed");
        }
    }

    protected void flushNonUnion(CellRegion region) {
        throw new UnsupportedOperationException();
    }

    /**
     * Normalizes a CellRegion into a union of crossjoins of member regions.
     *
     * @param region Region
     * @return normalized region
     */
    UnionCellRegion normalize(CellRegionImpl region) {
        // Search for Union within a Crossjoin.
        //   Crossjoin(a1, a2, Union(r1, r2, r3), a4)
        // becomes
        //   Union(
        //     Crossjoin(a1, a2, r1, a4),
        //     Crossjoin(a1, a2, r2, a4),
        //     Crossjoin(a1, a2, r3, a4))

        // First, decompose into a flat list of non-union regions.
        List<CellRegionImpl> nonUnionList = new LinkedList<CellRegionImpl>();
        flattenUnion(region, nonUnionList);

        for (int i = 0; i < nonUnionList.size(); i++) {
            while (true) {
                CellRegionImpl nonUnionRegion = nonUnionList.get(i);
                UnionCellRegion firstUnion = findFirstUnion(nonUnionRegion);
                if (firstUnion == null) {
                    break;
                }
                List<CellRegionImpl> list = new ArrayList<CellRegionImpl>();
                for (CellRegionImpl unionComponent : firstUnion.regions) {
                    // For each unionComponent in (r1, r2, r3),
                    // create Crossjoin(a1, a2, r1, a4).
                    CellRegionImpl cj =
                        copyReplacing(
                            nonUnionRegion,
                            firstUnion,
                            unionComponent);
                    list.add(cj);
                }
                // Replace one element which contained a union with several
                // which contain one fewer union. (Double-linked list helps
                // here.)
                nonUnionList.remove(i);
                nonUnionList.addAll(i, list);
            }
        }
        return new UnionCellRegion(nonUnionList);
    }

    private CellRegionImpl copyReplacing(
        CellRegionImpl region,
        CellRegionImpl seek,
        CellRegionImpl replacement)
    {
        if (region == seek) {
            return replacement;
        }
        if (region instanceof UnionCellRegion) {
            final UnionCellRegion union = (UnionCellRegion) region;
            List<CellRegionImpl> list = new ArrayList<CellRegionImpl>();
            for (CellRegionImpl child : union.regions) {
                list.add(copyReplacing(child, seek, replacement));
            }
            return new UnionCellRegion(list);
        }
        if (region instanceof CrossjoinCellRegion) {
            final CrossjoinCellRegion crossjoin = (CrossjoinCellRegion) region;
            List<CellRegionImpl> list = new ArrayList<CellRegionImpl>();
            for (CellRegionImpl child : crossjoin.components) {
                list.add(copyReplacing(child, seek, replacement));
            }
            return new CrossjoinCellRegion(list);
        }
        // This region is atomic, and since regions are immutable we don't need
        // to clone.
        return region;
    }

    /**
     * Flatten a region into a list of regions none of which are unions.
     *
     * @param region Cell region
     * @param list Target list
     */
    private void flattenUnion(
        CellRegionImpl region,
        List<CellRegionImpl> list)
    {
        if (region instanceof UnionCellRegion) {
            UnionCellRegion union = (UnionCellRegion) region;
            for (CellRegionImpl region1 : union.regions) {
                flattenUnion(region1, list);
            }
        } else {
            list.add(region);
        }
    }

    /**
     * Flattens a region into a list of regions none of which are unions.
     *
     * @param region Cell region
     * @param list Target list
     */
    private void flattenCrossjoin(
        CellRegionImpl region,
        List<CellRegionImpl> list)
    {
        if (region instanceof CrossjoinCellRegion) {
            CrossjoinCellRegion crossjoin = (CrossjoinCellRegion) region;
            for (CellRegionImpl component : crossjoin.components) {
                flattenCrossjoin(component, list);
            }
        } else {
            list.add(region);
        }
    }

    private UnionCellRegion findFirstUnion(CellRegion region) {
        final CellRegionVisitor visitor =
            new CellRegionVisitorImpl() {
                public void visit(UnionCellRegion region) {
                    throw new FoundOne(region);
                }
            };
        try {
            ((CellRegionImpl) region).accept(visitor);
            return null;
        } catch (FoundOne foundOne) {
            return foundOne.region;
        }
    }

    /**
     * Returns a list of members of the Measures dimension which are mentioned
     * somewhere in a region specification.
     *
     * @param region Cell region
     * @return List of members mentioned in cell region specification
     */
    public static List<Member> findMeasures(CellRegion region) {
        final List<Member> list = new ArrayList<Member>();
        final CellRegionVisitor visitor =
            new CellRegionVisitorImpl() {
                public void visit(MemberCellRegion region) {
                    if (region.dimension.isMeasures()) {
                        list.addAll(region.memberList);
                    }
                }

                public void visit(MemberRangeCellRegion region) {
                    if (region.level.getDimension().isMeasures()) {
                        // FIXME: don't allow range on measures dimension
                        assert false : "ranges on measures dimension";
                    }
                }
            };
        ((CellRegionImpl) region).accept(visitor);
        return list;
    }

    public static SegmentColumn[] findAxisValues(CellRegion region) {
        final List<SegmentColumn> list =
            new ArrayList<SegmentColumn>();
        final CellRegionVisitor visitor =
            new CellRegionVisitorImpl() {
                public void visit(MemberCellRegion region) {
                    if (region.dimension.isMeasures()) {
                        return;
                    }
                    final Map<String, Set<Comparable>> levels =
                        new HashMap<String, Set<Comparable>>();
                    for (Member member : region.memberList) {
                        while (true) {
                            if (member == null || member.isAll()) {
                                break;
                            }
                            final String ccName =
                                ((RolapLevel) member.getLevel()).getKeyExp()
                                    .getGenericExpression();
                            if (!levels.containsKey(ccName)) {
                                levels.put(
                                    ccName, new HashSet<Comparable>());
                            }
                                levels.get(ccName).add(
                                    (Comparable)((RolapMember)member).getKey());
                            member = member.getParentMember();
                        }
                    }
                    for (Entry<String, Set<Comparable>> entry
                        : levels.entrySet())
                    {
                        // Now sort and convert to an ArraySortedSet.
                        final Comparable[] keys =
                            entry.getValue().toArray(
                                new Comparable[entry.getValue().size()]);
                        if (keys.length == 1 && keys[0].equals(true)) {
                            list.add(
                                new SegmentColumn(
                                    entry.getKey(),
                                    -1,
                                    null));
                        } else {
                            Arrays.sort(
                                keys,
                                Util.SqlNullSafeComparator.instance);
                            //noinspection unchecked
                            list.add(
                                new SegmentColumn(
                                    entry.getKey(),
                                    -1,
                                    new ArraySortedSet(keys)));
                        }
                    }
                }

                public void visit(MemberRangeCellRegion region) {
                    // We translate all ranges into wildcards.
                    // FIXME Optimize this by resolving the list of members
                    // into an actual list of values for ConstrainedColumn
                    list.add(
                        new SegmentColumn(
                            region.level.getKeyExp().getGenericExpression(),
                            -1,
                            null));
                }
            };
        ((CellRegionImpl) region).accept(visitor);
        return list.toArray(new SegmentColumn[list.size()]);
    }

    public static List<RolapStar> getStarList(CellRegion region) {
        // Figure out which measure (therefore star) it belongs to.
        List<RolapStar> starList = new ArrayList<RolapStar>();
        final List<Member> measuresList = findMeasures(region);
        for (Member measure : measuresList) {
            if (measure instanceof RolapStoredMeasure) {
                RolapStoredMeasure storedMeasure = (RolapStoredMeasure) measure;
                final RolapStar.Measure starMeasure =
                    (RolapStar.Measure) storedMeasure.getStarMeasure();
                if (!starList.contains(starMeasure.getStar())) {
                    starList.add(starMeasure.getStar());
                }
            }
        }
        return starList;
    }

    public void printCacheState(
        final PrintWriter pw,
        final CellRegion region)
    {
        final List<RolapStar> starList = getStarList(region);
        for (RolapStar star : starList) {
            star.print(pw, "", false);
        }
        final SegmentCacheManager manager =
            MondrianServer.forConnection(connection)
                .getAggregationManager().cacheMgr;
        Locus.execute(
            connection,
            "CacheControlImpl.printCacheState",
            new Locus.Action<Void>() {
                public Void execute() {
                    manager.printCacheState(region, pw, Locus.peek());
                    return null;
                }
            });
    }

    public MemberSet createMemberSet(Member member, boolean descendants)
    {
        return new SimpleMemberSet(
            Collections.singletonList((RolapMember) member),
            descendants);
    }

    public MemberSet createMemberSet(
        boolean lowerInclusive,
        Member lowerMember,
        boolean upperInclusive,
        Member upperMember,
        boolean descendants)
    {
        if (upperMember != null && lowerMember != null) {
            assert upperMember.getLevel().equals(lowerMember.getLevel());
        }
        if (lowerMember == null) {
            lowerInclusive = false;
        }
        if (upperMember == null) {
            upperInclusive = false;
        }
        return new RangeMemberSet(
            stripMember((RolapMember) lowerMember), lowerInclusive,
            stripMember((RolapMember) upperMember), upperInclusive,
            descendants);
    }

    public MemberSet createUnionSet(MemberSet... args)
    {
        //noinspection unchecked
        return new UnionMemberSet((List) Arrays.asList(args));
    }

    public MemberSet filter(Level level, MemberSet baseSet) {
        if (level instanceof RolapCubeLevel) {
            // be forgiving
            level = ((RolapCubeLevel) level).getRolapLevel();
        }
        return ((MemberSetPlus) baseSet).filter((RolapLevel) level);
    }

    public void flush(MemberSet memberSet) {
        // REVIEW How is flush(s) different to executing createDeleteCommand(s)?
        synchronized (MEMBER_CACHE_LOCK) {
            // firstly clear all cache associated with native sets
            connection.getSchema().getNativeRegistry().flushAllNativeSetCache();
            final List<CellRegion> cellRegionList = new ArrayList<CellRegion>();
            ((MemberSetPlus) memberSet).accept(
                new MemberSetVisitorImpl() {
                    public void visit(RolapMember member) {
                        flushMember(member, cellRegionList);
                    }
                }
           );
            // STUB: flush the set: another visitor

            // finally, flush cells now invalid
            flushRegionList(cellRegionList);
        }
    }

    public void printCacheState(PrintWriter pw, MemberSet set)
    {
        synchronized (MEMBER_CACHE_LOCK) {
            pw.println("need to implement printCacheState"); // TODO:
        }
    }

    public MemberEditCommand createCompoundCommand(
        List<MemberEditCommand> commandList)
    {
        //noinspection unchecked
        return new CompoundCommand((List) commandList);
    }

    public MemberEditCommand createCompoundCommand(
        MemberEditCommand... commands)
    {
        //noinspection unchecked
        return new CompoundCommand((List) Arrays.asList(commands));
    }

    public MemberEditCommand createDeleteCommand(Member member) {
        if (member == null) {
            throw new IllegalArgumentException("cannot delete null member");
        }
        if (((RolapLevel) member.getLevel()).isParentChild()) {
            throw new IllegalArgumentException(
                "delete member not supported for parent-child hierarchy");
        }
        return createDeleteCommand(createMemberSet(member, false));
    }

    public MemberEditCommand createDeleteCommand(MemberSet s) {
        return new DeleteMemberCommand((MemberSetPlus) s);
    }

    public MemberEditCommand createAddCommand(
        Member member) throws IllegalArgumentException
    {
        if (member == null) {
            throw new IllegalArgumentException("cannot add null member");
        }
        if (((RolapLevel) member.getLevel()).isParentChild()) {
            throw new IllegalArgumentException(
                "add member not supported for parent-child hierarchy");
        }
        return new AddMemberCommand((RolapMember) member);
    }

    public MemberEditCommand createMoveCommand(Member member, Member loc)
        throws IllegalArgumentException
    {
        if (member == null) {
            throw new IllegalArgumentException("cannot move null member");
        }
        if (((RolapLevel) member.getLevel()).isParentChild()) {
            throw new IllegalArgumentException(
                "move member not supported for parent-child hierarchy");
        }
        if (loc == null) {
            throw new IllegalArgumentException(
                "cannot move member to null location");
        }
        // TODO: check that MEMBER and LOC (its new parent) have appropriate
        // Levels
        return new MoveMemberCommand((RolapMember) member, (RolapMember) loc);
    }

    public MemberEditCommand createSetPropertyCommand(
        Member member,
        String name,
        Object value)
        throws IllegalArgumentException
    {
        if (member == null) {
            throw new IllegalArgumentException(
                "cannot set properties on null member");
        }
        if (((RolapLevel) member.getLevel()).isParentChild()) {
            throw new IllegalArgumentException(
                "set properties not supported for parent-child hierarchy");
        }
        // TODO: validate that prop NAME exists for Level of MEMBER
        return new ChangeMemberPropsCommand(
            new SimpleMemberSet(
                Collections.singletonList((RolapMember) member),
                false),
            Collections.singletonMap(name, value));
    }

    public MemberEditCommand createSetPropertyCommand(
        MemberSet members,
        Map<String, Object> propertyValues)
        throws IllegalArgumentException
    {
        // TODO: check that members all at same Level, and validate that props
        // exist
        validateSameLevel((MemberSetPlus) members);
        return new ChangeMemberPropsCommand(
            (MemberSetPlus) members,
            propertyValues);
    }

    /**
     * Validates that all members of a member set are the same level.
     *
     * @param memberSet Member set
     * @throws IllegalArgumentException if members are from more than one level
     */
    private void validateSameLevel(MemberSetPlus memberSet)
        throws IllegalArgumentException
    {
        memberSet.accept(
            new MemberSetVisitor() {
                final Set<RolapLevel> levelSet = new HashSet<RolapLevel>();

                private void visitMember(
                    RolapMember member,
                    boolean descendants)
                {
                    final String message =
                        "all members in set must belong to same level";
                    if (levelSet.add(member.getLevel())
                        && levelSet.size() > 1)
                    {
                        throw new IllegalArgumentException(message);
                    }
                    if (descendants
                        && member.getLevel().getChildLevel() != null)
                    {
                        throw new IllegalArgumentException(message);
                    }
                }

                public void visit(SimpleMemberSet simpleMemberSet) {
                    for (RolapMember member : simpleMemberSet.members) {
                        visitMember(member, simpleMemberSet.descendants);
                    }
                }

                public void visit(UnionMemberSet unionMemberSet) {
                    for (MemberSetPlus item : unionMemberSet.items) {
                        item.accept(this);
                    }
                }

                public void visit(RangeMemberSet rangeMemberSet) {
                    visitMember(
                        rangeMemberSet.lowerMember,
                        rangeMemberSet.descendants);
                    visitMember(
                        rangeMemberSet.upperMember,
                        rangeMemberSet.descendants);
                }
            }
       );
    }

    public void execute(MemberEditCommand cmd) {
        final BooleanProperty prop =
            MondrianProperties.instance().EnableRolapCubeMemberCache;
        if (prop.get()) {
            throw new IllegalArgumentException(
                "Member cache control operations are not allowed unless "
                + "property " + prop.getPath() + " is false");
        }
        synchronized (MEMBER_CACHE_LOCK) {
            // Make sure that a Locus is in the Execution stack,
            // since some operations might require DB access.
            Execution execution;
            try {
                execution =
                    Locus.peek().execution;
            } catch (EmptyStackException e) {
                if (connection == null) {
                    throw new IllegalArgumentException("Connection required");
                }
                execution = new Execution(connection.getInternalStatement(), 0);
            }
            final Locus locus = new Locus(
                execution,
                "CacheControlImpl.execute",
                "when modifying the member cache.");
            Locus.push(locus);
            try {
                // Execute the command
                final List<CellRegion> cellRegionList =
                    new ArrayList<CellRegion>();
                ((MemberEditCommandPlus) cmd).execute(cellRegionList);

                // Flush the cells touched by the regions
                for (CellRegion memberRegion : cellRegionList) {
                    // Iterate over the cubes, create a cross region with
                    // its measures, and flush the data cells.
                    // It is possible that some regions don't intersect
                    // with a cube. We will intercept the exceptions and
                    // skip to the next cube if necessary.
                    final List<Dimension> dimensions =
                        memberRegion.getDimensionality();
                    if (dimensions.size() > 0) {
                        for (Cube cube
                            : dimensions.get(0) .getSchema().getCubes())
                        {
                            try {
                                final List<CellRegionImpl> crossList =
                                    new ArrayList<CellRegionImpl>();
                                crossList.add(
                                    (CellRegionImpl)
                                        createMeasuresRegion(cube));
                                crossList.add((CellRegionImpl) memberRegion);
                                final CellRegion crossRegion =
                                    new CrossjoinCellRegion(crossList);
                                flush(crossRegion);
                            } catch (UndeclaredThrowableException e) {
                                if (e.getCause()
                                    instanceof InvocationTargetException)
                                {
                                    final InvocationTargetException ite =
                                        (InvocationTargetException)e.getCause();
                                    if (ite.getTargetException()
                                        instanceof MondrianException)
                                    {
                                        final MondrianException me =
                                            (MondrianException)
                                                ite.getTargetException();
                                        if (me.getMessage()
                                            .matches(
                                                "^Mondrian Error:Member "
                                                + "'\\[.*\\]' not found$"))
                                        {
                                            continue;
                                        }
                                    }
                                }
                                throw new MondrianException(e);
                            } catch (MondrianException e) {
                                if (e.getMessage()
                                    .matches(
                                        "^Mondrian Error:Member "
                                        + "'\\[.*\\]' not found$"))
                                {
                                    continue;
                                }
                                throw e;
                            }
                        }
                    }
                }
                // Apply it all.
                ((MemberEditCommandPlus) cmd).commit();
            } finally {
                Locus.pop(locus);
            }
        }
    }

    private static MemberCache getMemberCache(RolapMember member) {
        final MemberReader memberReader =
            member.getHierarchy().getMemberReader();
        SmartMemberReader smartMemberReader =
            (SmartMemberReader) memberReader;
        return smartMemberReader.getMemberCache();
    }

    // cell cache control implementation

    /**
     * Cell region formed by a list of members.
     *
     * @see MemberRangeCellRegion
     */
    static class MemberCellRegion implements CellRegionImpl {
        private final List<Member> memberList;
        private final Dimension dimension;

        MemberCellRegion(List<Member> memberList, boolean descendants) {
            assert memberList.size() > 0;
            this.memberList = memberList;
            this.dimension = (memberList.get(0)).getDimension();
            Util.discard(descendants);
        }

        public List<Dimension> getDimensionality() {
            return Collections.singletonList(dimension);
        }

        public String toString() {
            return Util.commaList("Member", memberList);
        }

        public void accept(CellRegionVisitor visitor) {
            visitor.visit(this);
        }

        public List<Member> getMemberList() {
            return memberList;
        }
    }

    /**
     * An empty cell region.
     */
    static class EmptyCellRegion implements CellRegionImpl {
        public void accept(CellRegionVisitor visitor) {
            visitor.visit(this);
        }
        public List<Dimension> getDimensionality() {
            return Collections.emptyList();
        }
    }

    /**
     * Cell region formed a range of members between a lower and upper bound.
     */
    static class MemberRangeCellRegion implements CellRegionImpl {
        private final RolapMember lowerMember;
        private final boolean lowerInclusive;
        private final RolapMember upperMember;
        private final boolean upperInclusive;
        private final boolean descendants;
        private final RolapLevel level;

        MemberRangeCellRegion(
            RolapMember lowerMember,
            boolean lowerInclusive,
            RolapMember upperMember,
            boolean upperInclusive,
            boolean descendants)
        {
            assert lowerMember != null || upperMember != null;
            assert lowerMember == null
                || upperMember == null
                || lowerMember.getLevel() == upperMember.getLevel();
            assert !(lowerMember == null && lowerInclusive);
            assert !(upperMember == null && upperInclusive);
            this.lowerMember = lowerMember;
            this.lowerInclusive = lowerInclusive;
            this.upperMember = upperMember;
            this.upperInclusive = upperInclusive;
            this.descendants = descendants;
            this.level =
                lowerMember == null
                ? upperMember.getLevel()
                : lowerMember.getLevel();
        }

        public List<Dimension> getDimensionality() {
            return Collections.singletonList(level.getDimension());
        }

        public RolapLevel getLevel() {
            return level;
        }

        public String toString() {
            final StringBuilder sb = new StringBuilder("Range(");
            if (lowerMember == null) {
                sb.append("null");
            } else {
                sb.append(lowerMember);
                if (lowerInclusive) {
                    sb.append(" inclusive");
                } else {
                    sb.append(" exclusive");
                }
            }
            sb.append(" to ");
            if (upperMember == null) {
                sb.append("null");
            } else {
                sb.append(upperMember);
                if (upperInclusive) {
                    sb.append(" inclusive");
                } else {
                    sb.append(" exclusive");
                }
            }
            sb.append(")");
            return sb.toString();
        }

        public void accept(CellRegionVisitor visitor) {
            visitor.visit(this);
        }

        public boolean getLowerInclusive() {
            return lowerInclusive;
        }

        public RolapMember getLowerBound() {
            return lowerMember;
        }

        public boolean getUpperInclusive() {
            return upperInclusive;
        }

        public RolapMember getUpperBound() {
            return upperMember;
        }
    }

    /**
     * Cell region formed by a cartesian product of two or more CellRegions.
     */
    static class CrossjoinCellRegion implements CellRegionImpl {
        final List<Dimension> dimensions;
        private List<CellRegionImpl> components =
            new ArrayList<CellRegionImpl>();

        CrossjoinCellRegion(List<CellRegionImpl> regions) {
            final List<Dimension> dimensionality = new ArrayList<Dimension>();
            compute(regions, components, dimensionality);
            dimensions = Collections.unmodifiableList(dimensionality);
        }

        private static void compute(
            List<CellRegionImpl> regions,
            List<CellRegionImpl> components,
            List<Dimension> dimensionality)
        {
            final Set<Dimension> dimensionSet = new HashSet<Dimension>();
            for (CellRegionImpl region : regions) {
                addComponents(region, components);

                final List<Dimension> regionDimensionality =
                    region.getDimensionality();
                dimensionality.addAll(regionDimensionality);
                dimensionSet.addAll(regionDimensionality);
                assert dimensionSet.size() == dimensionality.size()
                    : "dimensions in common";
            }
        }

        public void accept(CellRegionVisitor visitor) {
            visitor.visit(this);
            for (CellRegion component : components) {
                CellRegionImpl cellRegion = (CellRegionImpl) component;
                cellRegion.accept(visitor);
            }
        }

        private static void addComponents(
            CellRegionImpl region,
            List<CellRegionImpl> list)
        {
            if (region instanceof CrossjoinCellRegion) {
                CrossjoinCellRegion crossjoinRegion =
                    (CrossjoinCellRegion) region;
                for (CellRegionImpl component : crossjoinRegion.components) {
                    list.add(component);
                }
            } else {
                list.add(region);
            }
        }

        public List<Dimension> getDimensionality() {
            return dimensions;
        }

        public String toString() {
            return Util.commaList("Crossjoin", components);
        }

        public List<CellRegion> getComponents() {
            return Util.cast(components);
        }
    }

    private static class UnionCellRegion implements CellRegionImpl {
        private final List<CellRegionImpl> regions;

        UnionCellRegion(List<CellRegionImpl> regions) {
            this.regions = regions;
            assert regions.size() >= 1;

            // All regions must have same dimensionality.
            for (int i = 1; i < regions.size(); i++) {
                final CellRegion region0 = regions.get(0);
                final CellRegion region = regions.get(i);
                assert region0.getDimensionality().equals(
                    region.getDimensionality());
            }
        }

        public List<Dimension> getDimensionality() {
            return regions.get(0).getDimensionality();
        }

        public String toString() {
            return Util.commaList("Union", regions);
        }

        public void accept(CellRegionVisitor visitor) {
            visitor.visit(this);
            for (CellRegionImpl cellRegion : regions) {
                cellRegion.accept(visitor);
            }
        }
    }

    interface CellRegionImpl extends CellRegion {
        void accept(CellRegionVisitor visitor);
    }

    /**
     * Visitor that visits various sub-types of
     * {@link mondrian.olap.CacheControl.CellRegion}.
     */
    interface CellRegionVisitor {
        void visit(MemberCellRegion region);
        void visit(MemberRangeCellRegion region);
        void visit(UnionCellRegion region);
        void visit(CrossjoinCellRegion region);
        void visit(EmptyCellRegion region);
    }

    private static class FoundOne extends RuntimeException {
        private final transient UnionCellRegion region;

        public FoundOne(UnionCellRegion region) {
            this.region = region;
        }
    }

    /**
     * Default implementation of {@link CellRegionVisitor}.
     */
    private static class CellRegionVisitorImpl implements CellRegionVisitor {
        public void visit(MemberCellRegion region) {
            // nothing
        }

        public void visit(MemberRangeCellRegion region) {
            // nothing
        }

        public void visit(UnionCellRegion region) {
            // nothing
        }

        public void visit(CrossjoinCellRegion region) {
            // nothing
        }

        public void visit(EmptyCellRegion region) {
            // nothing
        }
    }


    // ~ member cache control implementation ----------------------------------

    /**
     * Implementation-specific extensions to the
     * {@link mondrian.olap.CacheControl.MemberEditCommand} interface.
     */
    interface MemberEditCommandPlus extends MemberEditCommand {
        /**
         * Executes this command, and gathers a list of cell regions affected
         * in the {@code cellRegionList} parameter. The caller will flush the
         * cell regions later.
         *
         * @param cellRegionList Populated with a list of cell regions which
         * are invalidated by this action
         */
        void execute(final List<CellRegion> cellRegionList);

        void commit();
    }

    /**
     * Implementation-specific extensions to the
     * {@link mondrian.olap.CacheControl.MemberSet} interface.
     */
    interface MemberSetPlus extends MemberSet {
        /**
         * Accepts a visitor.
         *
         * @param visitor Visitor
         */
        void accept(MemberSetVisitor visitor);

        /**
         * Filters this member set, returning a member set containing all
         * members at a given Level. When applicable, returns this member set
         * unchanged.
         *
         * @param level Level
         * @return Member set with members not at the given level removed
         */
        MemberSetPlus filter(RolapLevel level);
    }

    /**
     * Visits the subclasses of {@link MemberSetPlus}.
     */
    interface MemberSetVisitor {
        void visit(SimpleMemberSet s);
        void visit(UnionMemberSet s);
        void visit(RangeMemberSet s);
    }

    /**
     * Default implementation of {@link MemberSetVisitor}.
     *
     * <p>The default implementation may not be efficient. For example, if
     * flushing a range of members from the cache, you may not wish to fetch
     * all of the members into the cache in order to flush them.
     */
    public static abstract class MemberSetVisitorImpl
        implements MemberSetVisitor
    {
        public void visit(UnionMemberSet s) {
            for (MemberSetPlus item : s.items) {
                item.accept(this);
            }
        }

        public void visit(RangeMemberSet s) {
            final MemberReader memberReader =
                s.level.getHierarchy().getMemberReader();
            visitRange(
                memberReader, s.level, s.lowerMember, s.upperMember,
                s.descendants);
        }

        protected void visitRange(
            MemberReader memberReader,
            RolapLevel level,
            RolapMember lowerMember,
            RolapMember upperMember,
            boolean recurse)
        {
            final List<RolapMember> list = new ArrayList<RolapMember>();
            memberReader.getMemberRange(level, lowerMember, upperMember, list);
            for (RolapMember member : list) {
                visit(member);
            }
            if (recurse) {
                list.clear();
                memberReader.getMemberChildren(lowerMember, list);
                if (list.isEmpty()) {
                    return;
                }
                RolapMember lowerChild = list.get(0);
                list.clear();
                memberReader.getMemberChildren(upperMember, list);
                if (list.isEmpty()) {
                    return;
                }
                RolapMember upperChild = list.get(list.size() - 1);
                visitRange(
                    memberReader, level, lowerChild, upperChild, recurse);
            }
        }

        public void visit(SimpleMemberSet s) {
            for (RolapMember member : s.members) {
                visit(member);
            }
        }

        /**
         * Visits a single member.
         *
         * @param member Member
         */
        public abstract void visit(RolapMember member);
    }

    /**
     * Member set containing no members.
     */
    static class EmptyMemberSet implements MemberSetPlus {
        public static final EmptyMemberSet INSTANCE = new EmptyMemberSet();

        private EmptyMemberSet() {
            // prevent instantiation except for singleton
        }

        public void accept(MemberSetVisitor visitor) {
            // nothing
        }

        public MemberSetPlus filter(RolapLevel level) {
            return this;
        }

        public String toString() {
            return "Empty";
        }
    }

    /**
     * Member set defined by a list of members from one hierarchy.
     */
    static class SimpleMemberSet implements MemberSetPlus {
        public final List<RolapMember> members;
        // the set includes the descendants of all members
        public final boolean descendants;
        public final RolapHierarchy hierarchy;

        SimpleMemberSet(List<RolapMember> members, boolean descendants) {
            this.members = new ArrayList<RolapMember>(members);
            stripMemberList(this.members);
            this.descendants = descendants;
            this.hierarchy =
                members.isEmpty()
                    ? null
                    : members.get(0).getHierarchy();
        }

        public String toString() {
            return Util.commaList("Member", members);
        }

        public void accept(MemberSetVisitor visitor) {
            // Don't descend the subtrees here: may not want to load them into
            // cache.
            visitor.visit(this);
        }

        public MemberSetPlus filter(RolapLevel level) {
            List<RolapMember> filteredMembers = new ArrayList<RolapMember>();
            for (RolapMember member : members) {
                if (member.getLevel().equals(level)) {
                    filteredMembers.add(member);
                }
            }
            if (filteredMembers.isEmpty()) {
                return EmptyMemberSet.INSTANCE;
            } else if (filteredMembers.equals(members)) {
                return this;
            } else {
                return new SimpleMemberSet(filteredMembers, false);
            }
        }
    }

    /**
     * Member set defined by the union of other member sets.
     */
    static class UnionMemberSet implements MemberSetPlus {
        private final List<MemberSetPlus> items;

        UnionMemberSet(List<MemberSetPlus> items) {
            this.items = items;
        }

        public String toString() {
            final StringBuilder sb = new StringBuilder("Union(");
            for (int i = 0; i < items.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                MemberSetPlus item = items.get(i);
                sb.append(item.toString());
            }
            sb.append(")");
            return sb.toString();
        }

        public void accept(MemberSetVisitor visitor) {
            visitor.visit(this);
        }

        public MemberSetPlus filter(RolapLevel level) {
            final List<MemberSetPlus> filteredItems =
                new ArrayList<MemberSetPlus>();
            for (MemberSetPlus item : items) {
                final MemberSetPlus filteredItem = item.filter(level);
                if (filteredItem == EmptyMemberSet.INSTANCE) {
                    // skip it
                } else {
                    assert !(filteredItem instanceof EmptyMemberSet);
                    filteredItems.add(filteredItem);
                }
            }
            if (filteredItems.isEmpty()) {
                return EmptyMemberSet.INSTANCE;
            } else if (filteredItems.equals(items)) {
                return this;
            } else {
                return new UnionMemberSet(filteredItems);
            }
        }
    }

    /**
     * Member set defined by a range of members between a lower and upper
     * bound.
     */
    static class RangeMemberSet implements MemberSetPlus {
        private final RolapMember lowerMember;
        private final boolean lowerInclusive;
        private final RolapMember upperMember;
        private final boolean upperInclusive;
        private final boolean descendants;
        private final RolapLevel level;

        RangeMemberSet(
            RolapMember lowerMember,
            boolean lowerInclusive,
            RolapMember upperMember,
            boolean upperInclusive,
            boolean descendants)
        {
            assert lowerMember != null || upperMember != null;
            assert lowerMember == null
                || upperMember == null
                || lowerMember.getLevel() == upperMember.getLevel();
            assert !(lowerMember == null && lowerInclusive);
            assert !(upperMember == null && upperInclusive);
            assert !(lowerMember instanceof RolapCubeMember);
            assert !(upperMember instanceof RolapCubeMember);
            this.lowerMember = lowerMember;
            this.lowerInclusive = lowerInclusive;
            this.upperMember = upperMember;
            this.upperInclusive = upperInclusive;
            this.descendants = descendants;
            this.level =
                lowerMember == null
                ? upperMember.getLevel()
                : lowerMember.getLevel();
        }

        public String toString() {
            final StringBuilder sb = new StringBuilder("Range(");
            if (lowerMember == null) {
                sb.append("null");
            } else {
                sb.append(lowerMember);
                if (lowerInclusive) {
                    sb.append(" inclusive");
                } else {
                    sb.append(" exclusive");
                }
            }
            sb.append(" to ");
            if (upperMember == null) {
                sb.append("null");
            } else {
                sb.append(upperMember);
                if (upperInclusive) {
                    sb.append(" inclusive");
                } else {
                    sb.append(" exclusive");
                }
            }
            sb.append(")");
            return sb.toString();
        }

        public void accept(MemberSetVisitor visitor) {
            // Don't traverse the range here: may not want to load it into cache
            visitor.visit(this);
        }

        public MemberSetPlus filter(RolapLevel level) {
            if (level == this.level) {
                return this;
            } else {
                return filter2(level, this.level, lowerMember, upperMember);
            }
        }

        public MemberSetPlus filter2(
            RolapLevel seekLevel,
            RolapLevel level,
            RolapMember lower,
            RolapMember upper)
        {
            if (level == seekLevel) {
                return new RangeMemberSet(
                    lower, lowerInclusive, upper, upperInclusive, false);
            } else if (descendants
                && level.getHierarchy() == seekLevel.getHierarchy()
                && level.getDepth() < seekLevel.getDepth())
            {
                final MemberReader memberReader =
                    level.getHierarchy().getMemberReader();
                final List<RolapMember> list = new ArrayList<RolapMember>();
                memberReader.getMemberChildren(lower, list);
                if (list.isEmpty()) {
                    return EmptyMemberSet.INSTANCE;
                }
                RolapMember lowerChild = list.get(0);
                list.clear();
                memberReader.getMemberChildren(upper, list);
                if (list.isEmpty()) {
                    return EmptyMemberSet.INSTANCE;
                }
                RolapMember upperChild = list.get(list.size() - 1);
                return filter2(
                    seekLevel, (RolapLevel) level.getChildLevel(),
                    lowerChild, upperChild);
            } else {
                return EmptyMemberSet.INSTANCE;
            }
        }
    }

    /**
     * Command consisting of a set of commands executed in sequence.
     */
    private static class CompoundCommand implements MemberEditCommandPlus {
        private final List<MemberEditCommandPlus> commandList;

        CompoundCommand(List<MemberEditCommandPlus> commandList) {
            this.commandList = commandList;
        }

        public String toString() {
            return Util.commaList("Compound", commandList);
        }

        public void execute(final List<CellRegion> cellRegionList) {
            for (MemberEditCommandPlus command : commandList) {
                command.execute(cellRegionList);
            }
        }

        public void commit() {
            for (MemberEditCommandPlus command : commandList) {
                command.commit();
            }
        }
    }

    /**
     * Command that deletes a member and its descendants from the cache.
     */
    private class DeleteMemberCommand
        extends MemberSetVisitorImpl
        implements MemberEditCommandPlus
    {
        private final MemberSetPlus set;
        private List<CellRegion> cellRegionList;
        private Callable<Boolean> callable;

        DeleteMemberCommand(MemberSetPlus set) {
            this.set = set;
        }

        public String toString() {
            return "DeleteMemberCommand(" + set + ")";
        }

        public void execute(final List<CellRegion> cellRegionList) {
            // NOTE: use of cellRegionList makes this class non-reentrant
            this.cellRegionList = cellRegionList;
            set.accept(this);
            this.cellRegionList = null;
        }

        public void visit(RolapMember member) {
            this.callable =
                deleteMember(member, member.getParentMember(), cellRegionList);
        }

        public void commit() {
            try {
                callable.call();
            } catch (Exception e) {
                throw new MondrianException(e);
            }
        }
    }

    /**
     * Command that adds a new member to the cache.
     */
    private class AddMemberCommand implements MemberEditCommandPlus {
        private final RolapMember member;
        private Callable<Boolean> callable;

        public AddMemberCommand(RolapMember member) {
            assert member != null;
            this.member = stripMember(member);
        }

        public String toString() {
            return "AddMemberCommand(" + member + ")";
        }

        public void execute(List<CellRegion> cellRegionList) {
            this.callable =
                addMember(member, member.getParentMember(), cellRegionList);
        }

        public void commit() {
            try {
                callable.call();
            } catch (Exception e) {
                throw new MondrianException(e);
            }
        }
    }

    /**
     * Command that moves a member to a new parent.
     */
    private class MoveMemberCommand implements MemberEditCommandPlus {
        private final RolapMember member;
        private final RolapMember newParent;
        private Callable<Boolean> callable1;
        private Callable<Boolean> callable2;

        MoveMemberCommand(RolapMember member, RolapMember newParent) {
            this.member = member;
            this.newParent = newParent;
        }

        public String toString() {
            return "MoveMemberCommand(" + member + ", " + newParent + ")";
        }

        public void execute(final List<CellRegion> cellRegionList) {
            this.callable1 =
                deleteMember(member, member.getParentMember(), cellRegionList);
            this.callable2 =
                addMember(member, newParent, cellRegionList);
        }

        public void commit() {
            try {
                ((RolapMemberBase) member).setParentMember(newParent);
                callable1.call();
                ((RolapMemberBase) member).setUniqueName(member.getKey());
                callable2.call();
            } catch (Exception e) {
                throw new MondrianException(e);
            }
        }
    }

    /**
     * Command that changes one or more properties of a member.
     */
    private class ChangeMemberPropsCommand
        extends MemberSetVisitorImpl
        implements MemberEditCommandPlus
    {
        final MemberSetPlus memberSet;
        final Map<String, Object> propertyValues;
        final List<RolapMember> members =
            new ArrayList<RolapMember>();

        ChangeMemberPropsCommand(
            MemberSetPlus memberSet,
            Map<String, Object> propertyValues)
        {
            this.memberSet = memberSet;
            this.propertyValues = propertyValues;
        }

        public String toString() {
            return "CreateMemberPropsCommand(" + memberSet
                + ", " + propertyValues + ")";
        }

        public void execute(List<CellRegion> cellRegionList) {
            // ignore cellRegionList - no changes to cell cache
            memberSet.accept(this);
        }

        public void visit(RolapMember member) {
            members.add(member);
        }

        public void commit() {
            for (RolapMember member : members) {
                // Change member's properties.
                member = stripMember(member);
                final MemberCache memberCache = getMemberCache(member);
                final Object cacheKey =
                    memberCache.makeKey(
                        member.getParentMember(),
                        member.getKey());
                final RolapMember cacheMember = memberCache.getMember(cacheKey);
                if (cacheMember == null) {
                    return;
                }
                for (Map.Entry<String, Object> entry
                    : propertyValues.entrySet())
                {
                    cacheMember.setProperty(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    private static RolapMember stripMember(RolapMember member) {
        if (member instanceof RolapCubeMember) {
            member = ((RolapCubeMember) member).member;
        }
        return member;
    }

    private static void stripMemberList(List<RolapMember> members) {
        for (int i = 0; i < members.size(); i++) {
            RolapMember member = members.get(i);
            if (member instanceof RolapCubeMember) {
                members.set(i, ((RolapCubeMember) member).member);
            }
        }
    }

    private Callable<Boolean> deleteMember(
        final RolapMember member,
        final RolapMember previousParent,
        List<CellRegion> cellRegionList)
    {
        // Cells for member and its ancestors are now invalid.
        // It's sufficient to flush the member.
        cellRegionList.add(createMemberRegion(member, true));

        return new Callable<Boolean>() {
            public Boolean call() throws Exception {
                final MemberCache memberCache = getMemberCache(member);
                final MemberChildrenConstraint memberConstraint =
                    new ChildByNameConstraint(
                        new Id.NameSegment(member.getName()));

                // Remove the member from its parent's lists. First try the
                // unconstrained cache.
                final List<RolapMember> childrenList =
                    memberCache.getChildrenFromCache(
                        previousParent,
                        DefaultMemberChildrenConstraint.instance());
                if (childrenList != null) {
                    // A list existed before. Let's splice it.
                    childrenList.remove(member);
                    memberCache.putChildren(
                        previousParent,
                        DefaultMemberChildrenConstraint.instance(),
                        childrenList);
                }

                // Now make sure there is no constrained cache entry
                // for this member's parent.
                memberCache.putChildren(
                    previousParent,
                    memberConstraint,
                    null);

                // Let's update the level members cache.
                final List<RolapMember> levelMembers =
                    memberCache
                        .getLevelMembersFromCache(
                            member.getLevel(),
                            DefaultTupleConstraint.instance());
                if (levelMembers != null) {
                    levelMembers.remove(member);
                    memberCache.putChildren(
                        member.getLevel(),
                        DefaultTupleConstraint.instance(),
                        childrenList);
                }

                // Remove the member itself. The MemberCacheHelper takes care of
                // removing the member's children as well.
                final Object key =
                    memberCache.makeKey(previousParent, member.getKey());
                memberCache.removeMember(key);

                return true;
            }
        };
    }

    /**
     * Adds a member to cache.
     *
     * @param member Member
     * @param parent Member's parent (generally equals member.getParentMember)
     * @param cellRegionList List of cell regions to be flushed
     *
     * @return Callable that yields true when the member has been added to the
     * cache
     */
    private Callable<Boolean> addMember(
        final RolapMember member,
        final RolapMember parent,
        List<CellRegion> cellRegionList)
    {
        // Cells for all of member's ancestors are now invalid. It's sufficient
        // to flush its parent.
        cellRegionList.add(createMemberRegion(parent, false));

        return new Callable<Boolean>() {
            public Boolean call() throws Exception {
                final MemberCache memberCache = getMemberCache(member);
                final MemberChildrenConstraint memberConstraint =
                    new ChildByNameConstraint(
                        new Id.NameSegment(member.getName()));

                // Check if there is already a list in cache
                // constrained by a wildcard.
                List<RolapMember> childrenList =
                    memberCache.getChildrenFromCache(
                        parent,
                        DefaultMemberChildrenConstraint.instance());
                if (childrenList == null) {
                    // There was no cached list. We can ignore.
                } else {
                    // A list existed before. We can save a SQL query.
                    // Might be immutable. Let's append to it.
                    if (childrenList.isEmpty()) {
                        childrenList = new ArrayList<RolapMember>();
                    }
                    childrenList.add(member);
                    memberCache.putChildren(
                        parent,
                        memberConstraint,
                        childrenList);
                }

                final List<RolapMember> levelMembers =
                    memberCache
                        .getLevelMembersFromCache(
                            member.getLevel(),
                            DefaultTupleConstraint.instance());
                if (levelMembers != null) {
                    // There was already a cached list.
                    // Let's append to it.
                    levelMembers.add(member);
                    memberCache.putChildren(
                        member.getLevel(),
                        DefaultTupleConstraint.instance(),
                        levelMembers);
                }

                // Now add the member itself into cache
                final Object memberKey =
                    memberCache.makeKey(
                        member.getParentMember(),
                        member.getKey());
                memberCache.putMember(memberKey, member);

                return true;
            }
        };
    }

    /**
     * Removes a member from cache.
     *
     * @param member Member
     * @param cellRegionList Populated with cell region to be flushed
     */
    private void flushMember(
        RolapMember member,
        List<CellRegion> cellRegionList)
    {
        final MemberCache memberCache = getMemberCache(member);
        final Object key =
            memberCache.makeKey(member.getParentMember(), member.getKey());
        memberCache.removeMember(key);
        cellRegionList.add(createMemberRegion(member, false));
    }
}

// End CacheControlImpl.java

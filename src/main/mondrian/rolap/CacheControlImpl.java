/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.resource.MondrianResource;
import mondrian.olap.CacheControl;

import javax.sql.DataSource;
import java.util.*;
import java.io.PrintWriter;

/**
 * Implementation of {@link CacheControl} API.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 27, 2006
 */
public class CacheControlImpl implements CacheControl {
    public CellRegion createMemberRegion(Member member, boolean descendants) {
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
                throw MondrianResource.instance().
                    CacheFlushCrossjoinDimensionsInCommon.ex(
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
                regions[0].getDimensionality())) {
                throw MondrianResource.instance().
                    CacheFlushUnionDimensionalityMismatch.ex(
                    regions[0].getDimensionality().toString(),
                    region.getDimensionality().toString());
            }
            list.add((CellRegionImpl) region);
        }
        return new UnionCellRegion(list);
    }

    public CellRegion createMeasuresRegion(Cube cube) {
        final Dimension measuresDimension = cube.getDimensions()[0];
        final Member[] measures =
            cube.getSchemaReader(null).getLevelMembers(
                measuresDimension.getHierarchy().getLevels()[0],
                false);
        return new MemberCellRegion(Arrays.asList(measures), false);
    }

    public void flush(CellRegion region) {
        final List<Dimension> dimensionality = region.getDimensionality();
        boolean found = false;
        for (Dimension dimension : dimensionality) {
            if (dimension.isMeasures()) {
                found = true;
                break;
            }
        }
        if (!found) {
            throw MondrianResource.instance().
                CacheFlushRegionMustContainMembers.ex();
        }
        final UnionCellRegion union = normalize((CellRegionImpl) region);
        for (CellRegionImpl cellRegion : union.regions) {
            // Figure out the bits.
            flushNonUnion(cellRegion);
        }
    }

    public void trace(String message) {
        // ignore message
    }

    public void flushSchemaCache() {
        throw new UnsupportedOperationException();
    }

    // todo: document
    public void flushSchema(
        String catalogUrl,
        String connectionKey,
        String jdbcUser,
        String dataSourceStr)
    {
        RolapSchema.Pool.instance().remove(
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
        RolapSchema.Pool.instance().remove(
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
            RolapSchema.Pool.instance().remove((RolapSchema)schema);
        } else {
            throw new UnsupportedOperationException(schema.getClass().getName()+
                    " cannot be flushed");
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
    static List<Member> findMeasures(CellRegion region) {
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

    static List<RolapStar> getStarList(CellRegion region) {
        // Figure out which measure (therefore star) it belongs to.
        List<RolapStar> starList = new ArrayList<RolapStar>();
        final List<Member> measuresList = findMeasures(region);
        for (Member member : measuresList) {
            RolapStoredMeasure measure = (RolapStoredMeasure) member;
            final RolapStar.Measure starMeasure =
                (RolapStar.Measure) measure.getStarMeasure();
            if (!starList.contains(starMeasure.getStar())) {
                starList.add(starMeasure.getStar());
            }
        }
        return starList;
    }

    public void printCacheState(
        PrintWriter pw,
        CellRegion region)
    {
        List<RolapStar> starList = getStarList(region);
        for (RolapStar star : starList) {
            star.print(pw, "", false);
        }
    }

    /**
     * Cell region formed by a list of members.
     *
     * @see MemberRangeCellRegion
     */
    static class MemberCellRegion implements CellRegionImpl {
        private final List<Member> memberList;
        private final Dimension dimension;
        private final boolean descendants;

        MemberCellRegion(List<Member> memberList, boolean descendants) {
            assert memberList.size() > 0;
            this.memberList = memberList;
            this.dimension = (memberList.get(0)).getDimension();
            this.descendants = descendants;
        }

        public List<Dimension> getDimensionality() {
            return Collections.singletonList(dimension);
        }

        public String toString() {
            final StringBuilder sb = new StringBuilder("Member(");
            for (int i = 0; i < memberList.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                Member member = memberList.get(i);
                sb.append(member.toString());
            }
            sb.append(")");
            return sb.toString();
        }

        public void accept(CellRegionVisitor visitor) {
            visitor.visit(this);
        }

        public List<Member> getMemberList() {
            return memberList;
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
            this.level = lowerMember == null ?
                upperMember.getLevel() :
                lowerMember.getLevel();
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
                assert dimensionSet.size() == dimensionality.size() :
                    "dimensions in common";
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
            final StringBuilder sb = new StringBuilder("Crosssjoin(");
            for (int i = 0; i < components.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                CellRegion component = components.get(i);
                sb.append(component.toString());
            }
            sb.append(")");
            return sb.toString();
        }

        public List<CellRegion> getComponents() {
            return (List<CellRegion>) (List) components;
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
            final StringBuilder sb = new StringBuilder("Union(");
            for (int i = 0; i < regions.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                CellRegion component = regions.get(i);
                sb.append(component.toString());
            }
            sb.append(")");
            return sb.toString();
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
     * Visitor which visits various sub-types of {@link CellRegion}.
     */
    interface CellRegionVisitor {
        void visit(MemberCellRegion region);
        void visit(MemberRangeCellRegion region);
        void visit(UnionCellRegion region);
        void visit(CrossjoinCellRegion region);
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
    }
}

// End CacheControlImpl.java

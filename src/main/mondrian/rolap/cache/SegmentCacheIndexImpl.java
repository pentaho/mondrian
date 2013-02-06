/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.cache;

import mondrian.rolap.BitKey;
import mondrian.rolap.RolapUtil;
import mondrian.rolap.agg.*;
import mondrian.spi.*;
import mondrian.util.*;

import java.io.PrintWriter;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Future;

/**
 * Data structure that identifies which segments contain cells.
 *
 * <p>Not thread safe.</p>
 *
 * @author Julian Hyde
 */
public class SegmentCacheIndexImpl implements SegmentCacheIndex {

    private final Map<List, List<SegmentHeader>> bitkeyMap =
        new HashMap<List, List<SegmentHeader>>();

    /**
     * The fact map allows us to spot quickly which
     * segments have facts relating to a given header.
     */
    private final Map<List, FactInfo> factMap =
        new HashMap<List, FactInfo>();

    /**
     * The fuzzy fact map allows us to spot quickly which
     * segments have facts relating to a given header, but doesn't
     * consider the compound predicates in the key. This allows
     * flush operations to be consistent.
     */
    // TODO Get rid of the fuzzy map once we have a way to parse
    // compound predicates into rich objects that can be serialized
    // as part of the SegmentHeader.
    private final Map<List, FuzzyFactInfo> fuzzyFactMap =
        new HashMap<List, FuzzyFactInfo>();

    private final Map<SegmentHeader, HeaderInfo> headerMap =
        new HashMap<SegmentHeader, HeaderInfo>();

    private final Thread thread;

    /**
     * Creates a SegmentCacheIndexImpl.
     *
     * @param thread Thread that must be used to execute commands.
     */
    public SegmentCacheIndexImpl(Thread thread) {
        this.thread = thread;
        assert thread != null;
    }

    public static List makeConverterKey(SegmentHeader header) {
        return Arrays.asList(
            header.schemaName,
            header.schemaChecksum,
            header.cubeName,
            header.rolapStarFactTableName,
            header.measureName,
            header.compoundPredicates);
    }

    public static List makeConverterKey(CellRequest request, AggregationKey key)
    {
        return Arrays.asList(
            request.getMeasure().getStar().getSchema().getName(),
            request.getMeasure().getStar().getSchema().getChecksum(),
            request.getMeasure().getCubeName(),
            request.getMeasure().getStar().getFactTable().getAlias(),
            request.getMeasure().getName(),
            AggregationKey.getCompoundPredicateStringList(
                key.getStar(),
                key.getCompoundPredicateList()));
    }

    public List<SegmentHeader> locate(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String measureName,
        String rolapStarFactTableName,
        BitKey constrainedColsBitKey,
        Map<String, Comparable> coordinates,
        List<String> compoundPredicates)
    {
        checkThread();

        List<SegmentHeader> list = Collections.emptyList();
        final List starKey =
            makeBitkeyKey(
                schemaName,
                schemaChecksum,
                cubeName,
                rolapStarFactTableName,
                constrainedColsBitKey,
                measureName,
                compoundPredicates);
        final List<SegmentHeader> headerList = bitkeyMap.get(starKey);
        if (headerList == null) {
            return Collections.emptyList();
        }
        for (SegmentHeader header : headerList) {
            if (matches(header, coordinates, compoundPredicates)) {
                // Be lazy. Don't allocate a list unless there is at least one
                // entry.
                if (list.isEmpty()) {
                    list = new ArrayList<SegmentHeader>();
                }
                list.add(header);
            }
        }
        return list;
    }

    public boolean add(
        SegmentHeader header,
        boolean loading,
        SegmentBuilder.SegmentConverter converter)
    {
        checkThread();

        HeaderInfo headerInfo = headerMap.get(header);
        if (headerInfo != null) {
            if (loading && headerInfo.slot == null) {
                headerInfo.slot = new SlotFuture<SegmentBody>();
                // Returns true. same as creating.
                return true;
            }
            return false;
        }
        headerInfo = new HeaderInfo();
        if (loading) {
            headerInfo.slot = new SlotFuture<SegmentBody>();
        }
        headerMap.put(header, headerInfo);

        final List bitkeyKey = makeBitkeyKey(header);
        List<SegmentHeader> headerList = bitkeyMap.get(bitkeyKey);
        if (headerList == null) {
            headerList = new ArrayList<SegmentHeader>();
            bitkeyMap.put(bitkeyKey, headerList);
        }
        headerList.add(header);

        final List factKey = makeFactKey(header);
        FactInfo factInfo = factMap.get(factKey);
        if (factInfo == null) {
            factInfo = new FactInfo();
            factMap.put(factKey, factInfo);
        }
        factInfo.headerList.add(header);
        factInfo.bitkeyPoset.add(header.getConstrainedColumnsBitKey());
        if (converter != null) {
            factInfo.converter = converter;
        }

        final List fuzzyFactKey = makeFuzzyFactKey(header);
        FuzzyFactInfo fuzzyFactInfo = fuzzyFactMap.get(fuzzyFactKey);
        if (fuzzyFactInfo == null) {
            fuzzyFactInfo = new FuzzyFactInfo();
            fuzzyFactMap.put(fuzzyFactKey, fuzzyFactInfo);
        }
        fuzzyFactInfo.headerList.add(header);
        return true;
    }

    public void loadSucceeded(SegmentHeader header, SegmentBody body) {
        checkThread();

        final HeaderInfo headerInfo = headerMap.get(header);
        assert headerInfo != null
            : "segment header " + header.getUniqueID() + " is missing";
        assert headerInfo.slot != null
            : "segment header " + header.getUniqueID() + " is not loading";
        if (!headerInfo.slot.isDone()) {
            headerInfo.slot.put(body);
        }
        if (headerInfo.removeAfterLoad) {
            remove(header);
        }
    }

    public void loadFailed(SegmentHeader header, Throwable throwable) {
        checkThread();

        final HeaderInfo headerInfo = headerMap.get(header);
        assert headerInfo != null
            : "segment header " + header.getUniqueID() + " is missing";
        assert headerInfo.slot != null
            : "segment header " + header.getUniqueID() + " is not loading";
        headerInfo.slot.fail(throwable);
        remove(header);
    }

    public void remove(SegmentHeader header) {
        checkThread();

        final HeaderInfo headerInfo = headerMap.get(header);
        if (headerInfo == null) {
            return;
        }
        if (headerInfo.slot != null && !headerInfo.slot.isDone()) {
            // Cannot remove while load is pending; flag for removal after load
            headerInfo.removeAfterLoad = true;
            return;
        }

        headerMap.remove(header);

        final List factKey = makeFactKey(header);
        final FactInfo factInfo = factMap.get(factKey);
        if (factInfo != null) {
            factInfo.headerList.remove(header);
            if (factInfo.headerList.size() == 0) {
                factMap.remove(factKey);
            }
        }

        final List fuzzyFactKey = makeFuzzyFactKey(header);
        final FuzzyFactInfo fuzzyFactInfo = fuzzyFactMap.get(fuzzyFactKey);
        if (fuzzyFactInfo != null) {
            fuzzyFactInfo.headerList.remove(header);
            if (fuzzyFactInfo.headerList.size() == 0) {
                fuzzyFactMap.remove(fuzzyFactKey);
            }
        }

        final List bitkeyKey = makeBitkeyKey(header);
        final List<SegmentHeader> headerList = bitkeyMap.get(bitkeyKey);
        headerList.remove(header);
        if (headerList.size() == 0) {
            bitkeyMap.remove(bitkeyKey);
            factInfo.bitkeyPoset.remove(header.getConstrainedColumnsBitKey());
        }
    }

    private void checkThread() {
        assert thread == Thread.currentThread()
            : "expected " + thread + ", but was " + Thread.currentThread();
    }

    public static boolean matches(
        SegmentHeader header,
        Map<String, Comparable> coords,
        List<String> compoundPredicates)
    {
        if (!header.compoundPredicates.equals(compoundPredicates)) {
            return false;
        }
        for (Map.Entry<String, Comparable> entry : coords.entrySet()) {
            // Check if the segment explicitly excludes this coordinate.
            final SegmentColumn excludedColumn =
                header.getExcludedRegion(entry.getKey());
            if (excludedColumn != null) {
                final SortedSet<Comparable> values =
                    excludedColumn.getValues();
                if (values == null || values.contains(entry.getValue())) {
                    return false;
                }
            }
            // Check if the dimensionality of the segment intersects
            // with the coordinate.
            final SegmentColumn constrainedColumn =
                header.getConstrainedColumn(entry.getKey());
            if (constrainedColumn == null) {
                // One of the required column/value pairs is not a constraining
                // column for the header. This will not happen if the header
                // has been acquired from bitkeyMap, but may happen if a list
                // of mixed-dimensionality headers is being scanned.
                return false;
            }
            final SortedSet<Comparable> values =
                constrainedColumn.getValues();
            if (values != null
                && !values.contains(entry.getValue()))
            {
                return false;
            }
        }
        return true;
    }

    public List<SegmentHeader> intersectRegion(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String measureName,
        String rolapStarFactTableName,
        SegmentColumn[] region)
    {
        checkThread();

        final List factKey = makeFuzzyFactKey(
            schemaName,
            schemaChecksum,
            cubeName,
            rolapStarFactTableName,
            measureName);
        final FuzzyFactInfo factInfo = fuzzyFactMap.get(factKey);
        List<SegmentHeader> list = Collections.emptyList();
        if (factInfo == null) {
            return list;
        }
        for (SegmentHeader header : factInfo.headerList) {
            if (intersects(header, region)) {
                // Be lazy. Don't allocate a list unless there is at least one
                // entry.
                if (list.isEmpty()) {
                    list = new ArrayList<SegmentHeader>();
                }
                list.add(header);
            }
        }
        return list;
    }

    private boolean intersects(
        SegmentHeader header,
        SegmentColumn[] region)
    {
        // most selective condition first
        if (region.length == 0) {
            return true;
        }
        for (SegmentColumn regionColumn : region) {
            final SegmentColumn headerColumn =
                header.getConstrainedColumn(regionColumn.getColumnExpression());
            if (headerColumn == null) {
                // If the segment header doesn't contain a column specified
                // by the region, then it always implicitly intersects.
                // This allows flush operations to be valid.
                return true;
            }
            final SortedSet<Comparable> regionValues =
                regionColumn.getValues();
            final SortedSet<Comparable> headerValues =
                headerColumn.getValues();
            if (headerValues == null || regionValues == null) {
                // This is a wildcard, so it always intersects.
                return true;
            }
            for (Comparable myValue : regionValues) {
                if (headerValues.contains(myValue)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void printCacheState(PrintWriter pw) {
        checkThread();
        final List<List<SegmentHeader>> values =
            new ArrayList<List<SegmentHeader>>(
                bitkeyMap.values());
        Collections.sort(
            values,
            new Comparator<List<SegmentHeader>>() {
                public int compare(
                    List<SegmentHeader> o1,
                    List<SegmentHeader> o2)
                {
                    if (o1.size() == 0) {
                        return -1;
                    }
                    if (o2.size() == 0) {
                        return 1;
                    }
                    return o1.get(0).getUniqueID()
                        .compareTo(o2.get(0).getUniqueID());
                }
            });
        for (List<SegmentHeader> key : values) {
            final List<SegmentHeader> headerList =
                new ArrayList<SegmentHeader>(key);
            Collections.sort(
                headerList,
                new Comparator<SegmentHeader>() {
                    public int compare(SegmentHeader o1, SegmentHeader o2) {
                        return o1.getUniqueID().compareTo(o2.getUniqueID());
                    }
                });
            for (SegmentHeader header : headerList) {
                pw.println(header.getDescription());
            }
        }
    }

    public Future<SegmentBody> getFuture(SegmentHeader header) {
        checkThread();

        return headerMap.get(header).slot;
    }

    public SegmentBuilder.SegmentConverter getConverter(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String rolapStarFactTableName,
        String measureName,
        List<String> compoundPredicates)
    {
        checkThread();

        final List factKey = makeFactKey(
            schemaName,
            schemaChecksum,
            cubeName,
            rolapStarFactTableName,
            measureName,
            compoundPredicates);
        final FactInfo factInfo = factMap.get(factKey);
        if (factInfo == null) {
            return null;
        }
        return factInfo.converter;
    }

    public void setConverter(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String rolapStarFactTableName,
        String measureName,
        List<String> compoundPredicates,
        SegmentBuilder.SegmentConverter converter)
    {
        checkThread();

        final List factKey = makeFactKey(
            schemaName,
            schemaChecksum,
            cubeName,
            rolapStarFactTableName,
            measureName,
            compoundPredicates);
        final FactInfo factInfo = factMap.get(factKey);
        assert factInfo != null : "should have called 'add' first";
        if (factInfo == null) {
            return;
        }
        factInfo.converter = converter;
    }

    private List makeBitkeyKey(SegmentHeader header) {
        return makeBitkeyKey(
            header.schemaName,
            header.schemaChecksum,
            header.cubeName,
            header.rolapStarFactTableName,
            header.constrainedColsBitKey,
            header.measureName,
            header.compoundPredicates);
    }

    private List makeBitkeyKey(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String rolapStarFactTableName,
        BitKey constrainedColsBitKey,
        String measureName,
        List<String> compoundPredicates)
    {
        return Arrays.asList(
            schemaName,
            schemaChecksum,
            cubeName,
            rolapStarFactTableName,
            constrainedColsBitKey,
            measureName,
            compoundPredicates);
    }

    private List makeFactKey(SegmentHeader header) {
        return makeFactKey(
            header.schemaName,
            header.schemaChecksum,
            header.cubeName,
            header.rolapStarFactTableName,
            header.measureName,
            header.compoundPredicates);
    }

    private List makeFactKey(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String rolapStarFactTableName,
        String measureName,
        List<String> compoundPredicates)
    {
        return Arrays.asList(
            schemaName,
            schemaChecksum,
            cubeName,
            rolapStarFactTableName,
            measureName,
            compoundPredicates);
    }

    private List makeFuzzyFactKey(SegmentHeader header) {
        return makeFuzzyFactKey(
            header.schemaName,
            header.schemaChecksum,
            header.cubeName,
            header.rolapStarFactTableName,
            header.measureName);
    }

    private List makeFuzzyFactKey(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String rolapStarFactTableName,
        String measureName)
    {
        return Arrays.asList(
            schemaName,
            schemaChecksum,
            cubeName,
            rolapStarFactTableName,
            measureName);
    }

    public List<List<SegmentHeader>> findRollupCandidates(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String measureName,
        String rolapStarFactTableName,
        BitKey constrainedColsBitKey,
        Map<String, Comparable> coordinates,
        List<String> compoundPredicates)
    {
        final List factKey = makeFactKey(
            schemaName,
            schemaChecksum,
            cubeName,
            rolapStarFactTableName,
            measureName,
            compoundPredicates);
        final FactInfo factInfo = factMap.get(factKey);
        if (factInfo == null) {
            return Collections.emptyList();
        }

        // Iterate over all dimensionalities that are a superset of the desired
        // columns and for which a segment is known to exist.
        //
        // It helps that getAncestors returns dimensionalities with fewer bits
        // set first. These will contain fewer cells, and therefore be less
        // effort to roll up.

        final List<List<SegmentHeader>> list =
            new ArrayList<List<SegmentHeader>>();
        final List<BitKey> ancestors =
            factInfo.bitkeyPoset.getAncestors(constrainedColsBitKey);
        for (BitKey bitKey : ancestors) {
            final List bitkeyKey = makeBitkeyKey(
                schemaName,
                schemaChecksum,
                cubeName,
                rolapStarFactTableName,
                bitKey,
                measureName,
                compoundPredicates);
            final List<SegmentHeader> headers = bitkeyMap.get(bitkeyKey);
            assert headers != null : "bitkeyPoset / bitkeyMap inconsistency";

            // For columns that are still present after roll up, make sure that
            // the required value is in the range covered by the segment.
            // Of the columns that are being aggregated away, are all of
            // them wildcarded? If so, this segment is a match. If not, we
            // will need to combine with other segments later.
            findRollupCandidatesAmong(coordinates, list, headers);
        }
        return list;
    }

    /**
     * Finds rollup candidates among a list of headers with the same
     * dimensionality.
     *
     * <p>For each column that is being aggregated away, we need to ensure that
     * we have all values of that column. If the column is wildcarded, it's
     * easy. For example, if we wish to roll up to create Segment1:</p>
     *
     * <pre>Segment1(Year=1997, MaritalStatus=*)</pre>
     *
     * <p>then clearly Segment2:</p>
     *
     * <pre>Segment2(Year=1997, MaritalStatus=*, Gender=*, Nation=*)</pre>
     *
     * <p>has all gender and Nation values. If the values are specified as a
     * list:</p>
     *
     * <pre>Segment3(Year=1997, MaritalStatus=*, Gender={M, F}, Nation=*)</pre>
     *
     * <p>then we need to check the metadata. We see that Gender has two
     * distinct values in the database, and we have two values, therefore we
     * have all of them.</p>
     *
     * <p>What if we have multiple non-wildcard columns? Consider:</p>
     *
     * <pre>
     *     Segment4(Year=1997, MaritalStatus=*, Gender={M},
                    Nation={Mexico, USA})
     *     Segment5(Year=1997, MaritalStatus=*, Gender={F},
                    Nation={USA})
     *     Segment6(Year=1997, MaritalStatus=*, Gender={F, M},
                    Nation={Canada, Mexico, Honduras, Belize})
     * </pre>
     *
     * <p>The problem is similar to finding whether a collection of rectangular
     * regions covers a rectangle (or, generalizing to n dimensions, an
     * n-cube). Or better, find a minimal collection of regions.</p>
     *
     * <p>Our algorithm solves it by iterating over all combinations of values.
     * Those combinations are exponential in theory, but tractible in practice,
     * using the following trick. The algorithm reduces the number of
     * combinations by looking for values that are always treated the same. In
     * the above, Canada, Honduras and Belize are always treated the same, so to
     * prove covering, it is sufficient to prove that all combinations involving
     * Canada are covered.</p>
     *
     * @param coordinates Coordinates
     * @param list List to write candidates to
     * @param headers Headers of candidate segments
     */
    private void findRollupCandidatesAmong(
        Map<String, Comparable> coordinates,
        List<List<SegmentHeader>> list,
        List<SegmentHeader> headers)
    {
        final List<Pair<SegmentHeader, List<SegmentColumn>>> matchingHeaders =
            new ArrayList<Pair<SegmentHeader, List<SegmentColumn>>>();
        headerLoop:
        for (SegmentHeader header : headers) {
            // Skip headers that have exclusions.
            //
            // TODO: This is a bit harsh.
            if (!header.getExcludedRegions().isEmpty()) {
                continue;
            }

            List<SegmentColumn> nonWildcards =
                new ArrayList<SegmentColumn>();
            for (SegmentColumn column : header.getConstrainedColumns()) {
                final SegmentColumn constrainedColumn =
                    header.getConstrainedColumn(column.columnExpression);

                // REVIEW: How are null key values represented in coordinates?
                // Assuming that they are represented by null ref.
                if (coordinates.containsKey(column.columnExpression)) {
                    // Matching column. Will not be aggregated away. Needs
                    // to be in range.
                    Comparable value =
                        coordinates.get(column.columnExpression);
                    if (value == null) {
                        value = RolapUtil.sqlNullValue;
                    }
                    if (constrainedColumn.values != null
                        && !constrainedColumn.values.contains(value))
                    {
                        continue headerLoop;
                    }
                } else {
                    // Non-matching column. Will be aggregated away. Needs
                    // to be wildcarded (or some more complicated conditions
                    // to be dealt with later).
                    if (constrainedColumn.values != null) {
                        nonWildcards.add(constrainedColumn);
                    }
                }
            }

            if (nonWildcards.isEmpty()) {
                list.add(Collections.singletonList(header));
            } else {
                matchingHeaders.add(Pair.of(header, nonWildcards));
            }
        }

        // Find combinations of segments that can roll up. Need at least two.
        if (matchingHeaders.size() < 2) {
            return;
        }

        // Collect the list of non-wildcarded columns.
        final List<SegmentColumn> columnList = new ArrayList<SegmentColumn>();
        final List<String> columnNameList = new ArrayList<String>();
        for (Pair<SegmentHeader, List<SegmentColumn>> pair : matchingHeaders) {
            for (SegmentColumn column : pair.right) {
                if (!columnNameList.contains(column.columnExpression)) {
                    final int valueCount = column.getValueCount();
                    if (valueCount <= 0) {
                        // Impossible to safely roll up. If we don't know the
                        // number of values, we don't know that we have all of
                        // them.
                        return;
                    }
                    columnList.add(column);
                    columnNameList.add(column.columnExpression);
                }
            }
        }

        // Gather known values of each column. For each value, remember which
        // segments refer to it.
        final List<List<Comparable>> valueLists =
            new ArrayList<List<Comparable>>();
        for (SegmentColumn column : columnList) {
            // For each value, which equivalence class it belongs to.
            final SortedMap<Comparable, BitSet> valueMap =
                new TreeMap<Comparable, BitSet>(RolapUtil.ROLAP_COMPARATOR);

            int h = -1;
            for (SegmentHeader header : Pair.leftIter(matchingHeaders)) {
                ++h;
                final SegmentColumn column1 =
                    header.getConstrainedColumn(
                        column.columnExpression);
                if (column1.getValues() == null) {
                    // Wildcard. Mark all values as present.
                    for (Entry<Comparable, BitSet> entry : valueMap.entrySet())
                    {
                        for (int pos = 0;
                            pos < entry.getValue().cardinality();
                            pos++)
                        {
                            entry.getValue().set(pos);
                        }
                    }
                } else {
                    for (Comparable value : column1.getValues()) {
                        BitSet bitSet = valueMap.get(value);
                        if (bitSet == null) {
                            bitSet = new BitSet();
                            valueMap.put(value, bitSet);
                        }
                        bitSet.set(h);
                    }
                }
            }

            // Is the number of values discovered equal to the known cardinality
            // of the column? If not, we can't cover the space.
            if (valueMap.size() < column.valueCount) {
                return;
            }

            // Build equivalence sets of values. These group together values
            // that are used identically in segments.
            //
            // For instance, given segments Sx over column c,
            //
            // S1: c = {1, 2, 3, 4}
            // S2: c = {3, 4, 5}
            // S3: c = {3, 6, 7, 8}
            //
            // the equivalence classes are:
            //
            // E1 = {1, 2} used in {S1}
            // E2 = {3} used in {S1, S2, S3}
            // E3 = {4} used in {S1, S2}
            // E4 = {6, 7, 8} used in {S3}
            //
            // The equivalence classes reduce the size of the search space. (In
            // this case, from 8 values to 4 classes.) We can use any value in a
            // class to stand for all values.
            final Map<BitSet, Comparable> eqclassPrimaryValues =
                new HashMap<BitSet, Comparable>();
            for (Map.Entry<Comparable, BitSet> entry : valueMap.entrySet()) {
                final BitSet bitSet = entry.getValue();
                if (!eqclassPrimaryValues.containsKey(bitSet)) {
                    final Comparable value = entry.getKey();
                    eqclassPrimaryValues.put(bitSet, value);
                }
            }
            valueLists.add(
                new ArrayList<Comparable>(
                    eqclassPrimaryValues.values()));
        }

        // Iterate over every combination of values, and make sure that some
        // segment can satisfy each.
        //
        // TODO: A greedy algorithm would probably be better. Rather than adding
        // the first segment that contains a particular value combination, add
        // the segment that contains the most value combinations that we are are
        // not currently covering.
        final CartesianProductList<Comparable> tuples =
            new CartesianProductList<Comparable>(valueLists);
        final List<SegmentHeader> usedSegments = new ArrayList<SegmentHeader>();
        final List<SegmentHeader> unusedSegments =
            new ArrayList<SegmentHeader>(Pair.left(matchingHeaders));
        tupleLoop:
        for (List<Comparable> tuple : tuples) {
            // If the value combination is handled by one of the used segments,
            // great!
            for (SegmentHeader segment : usedSegments) {
                if (contains(segment, tuple, columnNameList)) {
                    continue tupleLoop;
                }
            }
            // Does one of the unused segments contain it? Use the first one we
            // find.
            for (SegmentHeader segment : unusedSegments) {
                if (contains(segment, tuple, columnNameList)) {
                    unusedSegments.remove(segment);
                    usedSegments.add(segment);
                    continue tupleLoop;
                }
            }
            // There was a value combination not contained in any of the
            // segments. Fail.
            return;
        }
        list.add(usedSegments);
    }

    private boolean contains(
        SegmentHeader segment,
        List<Comparable> values,
        List<String> columns)
    {
        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i);
            final SegmentColumn column =
                segment.getConstrainedColumn(columnName);
            final SortedSet<Comparable> valueSet = column.getValues();
            if (valueSet != null && !valueSet.contains(values.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static class FactInfo {
        private static final PartiallyOrderedSet.Ordering<BitKey> ORDERING =
            new PartiallyOrderedSet.Ordering<BitKey>() {
                public boolean lessThan(BitKey e1, BitKey e2) {
                    return e2.isSuperSetOf(e1);
                }
            };

        private final List<SegmentHeader> headerList =
            new ArrayList<SegmentHeader>();

        private final PartiallyOrderedSet<BitKey> bitkeyPoset =
            new PartiallyOrderedSet<BitKey>(ORDERING);

        private SegmentBuilder.SegmentConverter converter;

        FactInfo() {
        }
    }

    private static class FuzzyFactInfo {
        private final List<SegmentHeader> headerList =
            new ArrayList<SegmentHeader>();

        FuzzyFactInfo() {
        }
    }

    private static class HeaderInfo {
        private SlotFuture<SegmentBody> slot;
        private boolean removeAfterLoad;
    }
}

// End SegmentCacheIndexImpl.java

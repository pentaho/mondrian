/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.olap.Aggregator;
import mondrian.olap.Util;
import mondrian.rolap.*;
import mondrian.rolap.agg.Segment.ExcludedRegion;
import mondrian.spi.*;
import mondrian.spi.Dialect.Datatype;
import mondrian.util.ArraySortedSet;
import mondrian.util.Pair;

import org.apache.log4j.Logger;

import org.olap4j.impl.UnmodifiableArrayList;

import java.math.BigInteger;
import java.util.*;
import java.util.Map.Entry;

/**
 * Helper class that contains methods to convert between
 * {@link Segment} and {@link SegmentHeader}, and also
 * {@link SegmentWithData} and {@link SegmentBody}.
 *
 * @author LBoudreau
 */
public class SegmentBuilder {
    private static final Logger LOGGER =
        Logger.getLogger(SegmentBuilder.class);
    /**
     * Converts a segment plus a {@link SegmentBody} into a
     * {@link mondrian.rolap.agg.SegmentWithData}.
     *
     * @param segment Segment
     * @param sb Segment body
     * @return SegmentWithData
     */
    public static SegmentWithData addData(Segment segment, SegmentBody sb) {
        // Load the axis keys for this segment
        SegmentAxis[] axes =
            new SegmentAxis[segment.predicates.length];
        for (int i = 0; i < segment.predicates.length; i++) {
            StarColumnPredicate predicate =
                segment.predicates[i];
            axes[i] =
                new SegmentAxis(
                    predicate,
                    sb.getAxisValueSets()[i],
                    sb.getNullAxisFlags()[i]);
        }
        final SegmentDataset dataSet = createDataset(sb, axes);
        return new SegmentWithData(segment, dataSet, axes);
    }

    /**
     * Creates a SegmentDataset that contains the cached
     * data and is initialized to be used with the supplied segment.
     *
     * @param body Segment with which the returned dataset will be associated
     * @param axes Segment axes, containing actual column values
     * @return A SegmentDataset object that contains cached data.
     */
    private static SegmentDataset createDataset(
        SegmentBody body,
        SegmentAxis[] axes)
    {
        final SegmentDataset dataSet;
        if (body instanceof DenseDoubleSegmentBody) {
            dataSet =
                new DenseDoubleSegmentDataset(
                    axes,
                    (double[]) body.getValueArray(),
                    body.getNullValueIndicators());
        } else if (body instanceof DenseIntSegmentBody) {
            dataSet =
                new DenseIntSegmentDataset(
                    axes,
                    (int[]) body.getValueArray(),
                    body.getNullValueIndicators());
        } else if (body instanceof DenseObjectSegmentBody) {
            dataSet =
                new DenseObjectSegmentDataset(
                    axes, (Object[]) body.getValueArray());
        } else if (body instanceof SparseSegmentBody) {
            dataSet = new SparseSegmentDataset(body.getValueMap());
        } else {
            throw Util.newInternal(
                "Unknown segment body type: " + body.getClass() + ": " + body);
        }
        return dataSet;
    }

    /**
     * Creates a segment from a SegmentHeader. The star,
     * constrainedColsBitKey, constrainedColumns and measure arguments are a
     * helping hand, because we know what we were looking for.
     *
     * @param header The header to convert.
     * @param star Star
     * @param constrainedColumnsBitKey Constrained columns
     * @param constrainedColumns Constrained columns
     * @param measure Measure
     * @return Segment
     */
    public static Segment toSegment(
        SegmentHeader header,
        RolapStar star,
        BitKey constrainedColumnsBitKey,
        RolapStar.Column[] constrainedColumns,
        RolapStar.Measure measure,
        List<StarPredicate> compoundPredicates)
    {
        final List<StarColumnPredicate> predicateList =
            new ArrayList<StarColumnPredicate>();
        for (int i = 0; i < constrainedColumns.length; i++) {
            RolapStar.Column constrainedColumn = constrainedColumns[i];
            final SortedSet<Comparable> values =
                header.getConstrainedColumns().get(i).values;
            StarColumnPredicate predicate;
            if (values == null) {
                predicate =
                    new LiteralColumnPredicate(
                        new PredicateColumn(
                            RolapSchema.BadRouter.INSTANCE,
                            constrainedColumn.getExpression()),
                        true);
            } else if (values.size() == 1) {
                predicate =
                    new ValueColumnPredicate(
                        new PredicateColumn(
                            RolapSchema.BadRouter.INSTANCE,
                            constrainedColumn.getExpression()),
                        values.first());
            } else {
                final List<StarColumnPredicate> valuePredicateList =
                    new ArrayList<StarColumnPredicate>();
                for (Comparable value : values) {
                    valuePredicateList.add(
                        new ValueColumnPredicate(
                            new PredicateColumn(
                                RolapSchema.BadRouter.INSTANCE,
                                constrainedColumn.getExpression()),
                            value));
                }
                predicate =
                    new ListColumnPredicate(
                        new PredicateColumn(
                            RolapSchema.BadRouter.INSTANCE,
                            constrainedColumn.getExpression()),
                        valuePredicateList);
            }
            predicateList.add(predicate);
        }

        return new Segment(
            star,
            constrainedColumnsBitKey,
            constrainedColumns,
            measure,
            predicateList.toArray(
                new StarColumnPredicate[predicateList.size()]),
            new ExcludedRegionList(header),
            compoundPredicates);
    }

    private static List<Comparable> getColumnValsAtCellKey(
        SegmentBody body, CellKey cellKey)
    {
        List<Comparable> columnValues = new ArrayList<Comparable>();
        for (int i = 0; i < body.getAxisValueSets().length; i++) {
            List<Comparable> columnVals = new ArrayList<Comparable>();
            columnVals.addAll(body.getAxisValueSets()[i]);
            int valCoord = body.getNullAxisFlags()[i]
                ? cellKey.getAxis(i) - 1
                : cellKey.getAxis(i);
            if (valCoord >= 0) {
                columnValues.add(columnVals.get(valCoord));
            } else {
                columnValues.add(null);
            }
        }
        return columnValues;
    }

    /**
     * Given a collection of segments, all of the same dimensionality, rolls up
     * to create a segment with reduced dimensionality.
     *
     * @param map Source segment headers and bodies
     * @param keepColumns A list of column names to keep as part of
     * the rolled up segment.
     * @param targetBitkey The column bit key to match with the
     * resulting segment.
     * @param rollupAggregator The aggregator to use to rollup.
     * @return Segment header and body of requested dimensionality
     * @param datatype The data type to use.
     */
    public static Pair<SegmentHeader, SegmentBody> rollup(
        Map<SegmentHeader, SegmentBody> map,
        Set<String> keepColumns,
        BitKey targetBitkey,
        Aggregator rollupAggregator,
        Datatype datatype)
    {
        class AxisInfo {
            SegmentColumn column;
            SortedSet<Comparable> requestedValues;
            SortedSet<Comparable> valueSet;
            Comparable[] values;
            boolean hasNull;
            int src;
            boolean lostPredicate;
        }
        assert allHeadersHaveSameDimensionality(map.keySet());

        // store the map values in a list to assure the first header
        // loaded here is consistent w/ the first segment processed below.
        List<Map.Entry<SegmentHeader, SegmentBody>>  segments =
            UnmodifiableArrayList.of(map.entrySet());
        final SegmentHeader firstHeader = segments.get(0).getKey();
        final AxisInfo[] axes =
            new AxisInfo[keepColumns.size()];
        int z = 0, j = 0;
        for (SegmentColumn column : firstHeader.getConstrainedColumns()) {
            if (keepColumns.contains(column.columnExpression)) {
                final AxisInfo axisInfo = axes[z++] = new AxisInfo();
                axisInfo.src = j;
                axisInfo.column = column;
                axisInfo.requestedValues = column.values;
            }
            j++;
        }

        // Compute the sets of values in each axis of the target segment. These
        // are the intersection of the input axes.
        for (Map.Entry<SegmentHeader, SegmentBody> entry : segments) {
            final SegmentHeader header = entry.getKey();
            for (AxisInfo axis : axes) {
                final SortedSet<Comparable> values =
                    entry.getValue().getAxisValueSets()[axis.src];
                final SegmentColumn headerColumn =
                    header.getConstrainedColumn(axis.column.columnExpression);
                final boolean hasNull =
                    entry.getValue().getNullAxisFlags()[axis.src];
                final SortedSet<Comparable> requestedValues =
                    headerColumn.getValues();
                if (axis.valueSet == null) {
                    axis.valueSet = new TreeSet<Comparable>(values);
                    axis.hasNull = hasNull;
                    axis.requestedValues = requestedValues;
                } else {
                    final SortedSet<Comparable> filteredValues;
                    final boolean filteredHasNull;
                    if (axis.requestedValues == null
                        && requestedValues == null)
                    {
                        // there are 2+ segments that are unconstrained for
                        // this axis.  While unconstrained, individually
                        // they may not have all values present.
                        // Make sure we don't lose any values.
                        filteredValues = axis.valueSet;
                        filteredValues.addAll(new TreeSet<Comparable>(values));
                        filteredHasNull = hasNull || axis.hasNull;
                    } else if (axis.requestedValues == null) {
                        filteredValues = values;
                        filteredHasNull = hasNull;
                        axis.column = headerColumn;
                    } else if (requestedValues == null) {
                        // this axis is wildcarded
                        filteredValues = axis.requestedValues;
                        filteredHasNull = axis.hasNull;
                    } else {
                        filteredValues = Util.intersect(
                            requestedValues,
                            axis.requestedValues);

                        // SegmentColumn predicates cannot ask for the null
                        // value (at present).
                        filteredHasNull = false;
                    }
                    axis.valueSet = filteredValues;
                    axis.hasNull = axis.hasNull || filteredHasNull;
                    if (!Util.equals(axis.requestedValues, requestedValues)) {
                        if (axis.requestedValues == null) {
                            // Downgrade from wildcard to a specific list.
                            axis.requestedValues = requestedValues;
                        } else if (requestedValues != null) {
                            // Segment requests have incompatible predicates.
                            // Best we can say is "we must have asked for the
                            // values that came back".
                            axis.lostPredicate = true;
                        }
                    }
                }
            }
        }

        for (AxisInfo axis : axes) {
            axis.values =
                axis.valueSet.toArray(new Comparable[axis.valueSet.size()]);
        }

        // Populate cells.
        //
        // (This is a rough implementation, very inefficient. It makes all
        // segment types pretend to be sparse, for purposes of reading. It
        // maps all axis ordinals to a value, then back to an axis ordinal,
        // even if this translation were not necessary, say if the source and
        // target axes had the same set of values. And it always creates a
        // sparse segment.
        //
        // We should do really efficient rollup if the source is an array: we
        // should box values (e.g double to Double and back), and we should read
        // a stripe of values from the and add them up into a single cell.
        final Map<CellKey, List<Object>> cellValues =
            new HashMap<CellKey, List<Object>>();
        List<List<Comparable>> addedIntersections =
            new ArrayList<List<Comparable>>();

        for (Map.Entry<SegmentHeader, SegmentBody> entry : map.entrySet()) {
            final int[] pos = new int[axes.length];
            final Comparable[][] valueArrays =
                new Comparable[firstHeader.getConstrainedColumns().size()][];
            final SegmentBody body = entry.getValue();

            // Copy source value sets into arrays. For axes that are being
            // projected away, store null.
            z = 0;
            for (SortedSet<Comparable> set : body.getAxisValueSets()) {
                valueArrays[z] = keepColumns.contains(
                    firstHeader.getConstrainedColumns().get(z).columnExpression)
                    ? set.toArray(new Comparable[set.size()])
                    : null;
                ++z;
            }
            Map<CellKey, Object> v = body.getValueMap();
            entryLoop:
            for (Map.Entry<CellKey, Object> vEntry : v.entrySet()) {
                z = 0;
                for (int i = 0; i < vEntry.getKey().size(); i++) {
                    final Comparable[] valueArray = valueArrays[i];
                    if (valueArray == null) {
                        continue;
                    }
                    final int ordinal = vEntry.getKey().getOrdinals()[i];
                    final int targetOrdinal;
                    if (axes[z].hasNull && ordinal == valueArray.length) {
                        targetOrdinal = axes[z].valueSet.size();
                    } else {
                        final Comparable value = valueArray[ordinal];
                        if (value == null) {
                            targetOrdinal = axes[z].valueSet.size();
                        } else {
                            targetOrdinal =
                                Util.binarySearch(
                                    axes[z].values,
                                    0, axes[z].values.length,
                                    value);
                        }
                    }
                    if (targetOrdinal >= 0) {
                        pos[z++] = targetOrdinal;
                    } else {
                        // This happens when one of the rollup candidate doesn't
                        // contain the requested cell.
                        continue entryLoop;
                    }
                }
                final CellKey ck = CellKey.Generator.newCellKey(pos);
                if (!cellValues.containsKey(ck)) {
                    cellValues.put(ck, new ArrayList<Object>());
                }
                List<Comparable> colValues =  getColumnValsAtCellKey(
                    body, vEntry.getKey());
                if (!addedIntersections.contains(colValues)) {
                    // only add the cell value if we haven't already.
                    // there is a potential double add if segments overlap
                    cellValues.get(ck).add(vEntry.getValue());
                    addedIntersections.add(colValues);
                }
            }
        }

        // Build the axis list.
        final List<Pair<SortedSet<Comparable>, Boolean>> axisList =
            new ArrayList<Pair<SortedSet<Comparable>, Boolean>>();

        BigInteger bigValueCount = BigInteger.ONE;
        for (AxisInfo axis : axes) {
            axisList.add(Pair.of(axis.valueSet, axis.hasNull));
            int size = axis.values.length;
            bigValueCount = bigValueCount.multiply(
                BigInteger.valueOf(axis.hasNull ? size + 1 : size));
        }

        // The logic used here for the sparse check follows
        // SegmentLoader.setAxisDataAndDecideSparseUse.
        // The two methods use different data structures (AxisInfo/SegmentAxis)
        // so combining logic is probably more trouble than it's worth.
        final boolean sparse =
            bigValueCount.compareTo(
                BigInteger.valueOf(Integer.MAX_VALUE)) > 0
                || SegmentLoader.useSparse(
                    bigValueCount.doubleValue(),
                    cellValues.size());

        final int[] axisMultipliers =
            computeAxisMultipliers(axisList);

        final SegmentBody body;
        // Peak at the values and determine the best way to store them
        // (whether to use a dense native dataset or a sparse one.
        if (cellValues.size() == 0) {
            // Just store the data into an empty dense object dataset.
            body =
                new DenseObjectSegmentBody(
                    new Object[0],
                    axisList);
        } else if (sparse) {
            // The rule says we must use a sparse dataset.
            // First, aggregate the values of each key.
            final Map<CellKey, Object> data =
                new HashMap<CellKey, Object>();
            for (Entry<CellKey, List<Object>> entry
                : cellValues.entrySet())
            {
                data.put(
                    CellKey.Generator.newCellKey(entry.getKey().getOrdinals()),
                    rollupAggregator.aggregate(
                        entry.getValue(),
                        datatype));
            }
            body =
                new SparseSegmentBody(
                    data,
                    axisList);
        } else {
            final BitSet nullValues;
            final int valueCount = bigValueCount.intValue();
            switch (datatype) {
            case Integer:
                final int[] ints = new int[valueCount];
                nullValues = Util.bitSetBetween(0, valueCount);
                for (Entry<CellKey, List<Object>> entry
                    : cellValues.entrySet())
                {
                    final int offset =
                        CellKey.Generator.getOffset(
                            entry.getKey().getOrdinals(), axisMultipliers);
                    final Object value =
                        rollupAggregator.aggregate(
                            entry.getValue(),
                            datatype);
                    if (value != null) {
                        ints[offset] = (Integer) value;
                        nullValues.clear(offset);
                    }
                }
                body =
                    new DenseIntSegmentBody(
                        nullValues,
                        ints,
                        axisList);
                break;
            case Numeric:
                final double[] doubles = new double[valueCount];
                nullValues = Util.bitSetBetween(0, valueCount);
                for (Entry<CellKey, List<Object>> entry
                    : cellValues.entrySet())
                {
                    final int offset =
                        CellKey.Generator.getOffset(
                            entry.getKey().getOrdinals(), axisMultipliers);
                    final Object value =
                        rollupAggregator.aggregate(
                            entry.getValue(),
                            datatype);
                    if (value != null) {
                        doubles[offset] = (Double) value;
                        nullValues.clear(offset);
                    }
                }
                body =
                    new DenseDoubleSegmentBody(
                        nullValues,
                        doubles,
                        axisList);
                break;
            default:
                final Object[] objects = new Object[valueCount];
                for (Entry<CellKey, List<Object>> entry
                    : cellValues.entrySet())
                {
                    final int offset =
                        CellKey.Generator.getOffset(
                            entry.getKey().getOrdinals(), axisMultipliers);
                    objects[offset] =
                        rollupAggregator.aggregate(
                            entry.getValue(),
                            datatype);
                }
                body =
                    new DenseObjectSegmentBody(
                        objects,
                        axisList);
            }
        }

        // Create header.
        final List<SegmentColumn> constrainedColumns =
            new ArrayList<SegmentColumn>();
        for (int i = 0; i < axes.length; i++) {
            AxisInfo axisInfo = axes[i];
            constrainedColumns.add(
                new SegmentColumn(
                    axisInfo.column.getColumnExpression(),
                    axisInfo.column.getValueCount(),
                    axisInfo.lostPredicate
                        ? axisList.get(i).left
                        : axisInfo.column.values));
        }
        final SegmentHeader header =
            new SegmentHeader(
                firstHeader.schemaName,
                firstHeader.schemaChecksum,
                firstHeader.cubeName,
                firstHeader.measureName,
                constrainedColumns,
                firstHeader.compoundPredicates,
                firstHeader.rolapStarFactTableName,
                targetBitkey,
                Collections.<SegmentColumn>emptyList());
        if (LOGGER.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            builder.append("Rolling up segments with parameters: \n");
            builder.append("keepColumns=" + keepColumns + "\n");
            builder.append("aggregator=" + rollupAggregator + "\n");
            builder.append("datatype=" + datatype + "\n");
            for (Map.Entry<SegmentHeader, SegmentBody > segment : segments) {
                builder.append(segment.getKey() + "\n");
                builder.append(segment.getValue().toString());
            }
            builder.append("AxisInfos constructed:");
            for (AxisInfo axis : axes) {
                SortedSet<Comparable> colVals = axis.column.getValues();
                builder.append(
                    String.format(
                        "column.columnExpression=%s\n"
                        + "column.valueCount=%s\n"
                        + "column.values=%s\n"
                        + "requestedValues=%s\n"
                        + "valueSet=%s\n"
                        + "values=%s\n"
                        + "hasNull=%b\n"
                        + "src=%d\n"
                        + "lostPredicate=%b\n",
                        axis.column.columnExpression,
                        axis.column.getValueCount(),
                        Arrays.toString(
                            colVals == null ? null
                                : colVals.toArray()),
                        axis.requestedValues,
                        axis.valueSet,
                        Arrays.asList(axis.values),
                        axis.hasNull,
                        axis.src,
                        axis.lostPredicate));
            }
            builder.append("Resulted in Segment:  \n");
            builder.append(header);
            builder.append(body.toString());
            LOGGER.debug(builder.toString());
        }
        return Pair.of(header, body);
    }

    private static boolean allHeadersHaveSameDimensionality(
        Set<SegmentHeader> headers)
    {
        final Iterator<SegmentHeader> headerIter = headers.iterator();
        final SegmentHeader firstHeader = headerIter.next();
        BitKey bitKey = firstHeader.getConstrainedColumnsBitKey();
        while (headerIter.hasNext()) {
            final SegmentHeader nextHeader = headerIter.next();
            if (!bitKey.equals(nextHeader.getConstrainedColumnsBitKey())) {
                return false;
            }
        }
        return true;
    }

    private static int[] computeAxisMultipliers(
        List<Pair<SortedSet<Comparable>, Boolean>> axes)
    {
        final int[] axisMultipliers = new int[axes.size()];
        int multiplier = 1;
        for (int i = axes.size() - 1; i >= 0; --i) {
            axisMultipliers[i] = multiplier;
            // if the nullAxisFlag is set we need to offset by 1.
            int nullAxisAdjustment = axes.get(i).right ? 1 : 0;
            multiplier *= (axes.get(i).left.size() + nullAxisAdjustment);
        }
        return axisMultipliers;
    }

    private static class ExcludedRegionList
        extends AbstractList<Segment.ExcludedRegion>
        implements Segment.ExcludedRegion
    {
        private final int cellCount;
        private final SegmentHeader header;
        public ExcludedRegionList(SegmentHeader header) {
            this.header = header;
            int cellCount = 1;
            for (SegmentColumn cc : header.getExcludedRegions()) {
                // TODO find a way to approximate the cardinality
                // of wildcard columns.
                if (cc.values != null) {
                    cellCount *= cc.values.size();
                }
            }
            this.cellCount = cellCount;
        }

        public void describe(StringBuilder buf) {
            // TODO
        }

        public int getArity() {
            return header.getConstrainedColumns().size();
        }

        public int getCellCount() {
            return cellCount;
        }

        public boolean wouldContain(Object[] keys) {
            assert keys.length == header.getConstrainedColumns().size();
            for (int i = 0; i < keys.length; i++) {
                final SegmentColumn excl =
                    header.getExcludedRegion(
                        header.getConstrainedColumns().get(i).columnExpression);
                if (excl == null) {
                    continue;
                }
                if (excl.values.contains(keys[i])) {
                    return true;
                }
            }
            return false;
        }

        public ExcludedRegion get(int index) {
            return this;
        }

        public int size() {
            return 1;
        }
    }

    /**
     * Tells if the passed segment is a subset of this segment
     * and could be used for a rollup in cache operation.
     * @param segment A segment which might be a subset of the
     * current segment.
     * @return True or false.
     */
    public static boolean isSubset(
        SegmentHeader header,
        Segment segment)
    {
        if (!segment.getStar().getSchema().getName()
            .equals(header.schemaName))
        {
            return false;
        }
        if (!segment.getStar().getFactTable().getAlias()
                .equals(header.rolapStarFactTableName))
        {
            return false;
        }
        if (!segment.measure.getName().equals(header.measureName)) {
            return false;
        }
        if (!segment.measure.getCubeName().equals(header.cubeName)) {
            return false;
        }
        if (segment.getConstrainedColumnsBitKey()
                .equals(header.constrainedColsBitKey))
        {
            return true;
        }
        return false;
    }

    public static List<SegmentColumn> toConstrainedColumns(
        RolapSchema.PhysStatistic statistic,
        StarColumnPredicate[] predicates,
        RolapStar.Column[] baseColumns)
    {
        List<SegmentColumn> ccs =
            new ArrayList<SegmentColumn>();
        for (int i = 0; i < predicates.length; i++) {
            StarColumnPredicate predicate = predicates[i];
            final List<Comparable> values =
                new ArrayList<Comparable>();
            predicate.values(Util.cast(values));
            final Comparable[] valuesArray =
                values.toArray(new Comparable[values.size()]);
            final ArraySortedSet<Comparable> valueList;
            if (valuesArray.length == 1 && valuesArray[0].equals(true)) {
                valueList = null;
            } else {
                Arrays.sort(
                    valuesArray,
                    Util.SqlNullSafeComparator.instance);
                valueList = new ArraySortedSet<Comparable>(valuesArray);
            }
            ccs.add(
                new SegmentColumn(
                    baseColumns[i].getExpression().toSql(),
                    statistic.getColumnCardinality(
                        predicate.getColumn().physColumn.relation,
                        predicate.getColumn().physColumn,
                        -1),
                    valueList));
        }
        return ccs;
    }

    /**
     * Creates a SegmentHeader object describing the supplied
     * Segment object.
     *
     * @param statistic Source of statistics about columns
     * @param segment A segment object for which we want to generate
     * a SegmentHeader.
     * @return A SegmentHeader describing the supplied Segment object.
     */
    public static SegmentHeader toHeader(
        RolapSchema.PhysStatistic statistic,
        Segment segment)
    {
        final List<SegmentColumn> cc =
            SegmentBuilder.toConstrainedColumns(
                statistic, segment.predicates, segment.columns);
        final List<String> cp = new ArrayList<String>();

        StringBuilder buf = new StringBuilder();
        for (StarPredicate compoundPredicate : segment.compoundPredicateList) {
            buf.setLength(0);
            compoundPredicate.toSql(segment.star.getSqlQueryDialect(), buf);
            cp.add(buf.toString());
        }
        final RolapSchema schema = segment.star.getSchema();
        return new SegmentHeader(
            schema.getName(),
            schema.getChecksum(),
            segment.measure.getCubeName(),
            segment.measure.getName(),
            cc,
            cp,
            segment.star.getFactTable().getAlias(),
            segment.constrainedColumnsBitKey,
            Collections.<SegmentColumn>emptyList());
    }

    private static RolapStar.Column[] getConstrainedColumns(
        RolapStar star,
        BitKey bitKey)
    {
        final List<RolapStar.Column> list =
            new ArrayList<RolapStar.Column>();
        for (int bit : bitKey) {
            list.add(star.getColumn(bit));
        }
        return list.toArray(new RolapStar.Column[list.size()]);
    }

    /**
     * Functor to convert a segment header and body into a
     * {@link mondrian.rolap.agg.SegmentWithData}.
     */
    public static interface SegmentConverter {
        SegmentWithData convert(
            SegmentHeader header,
            SegmentBody body);
    }

    /**
     * Implementation of {@link SegmentConverter} that uses an
     * {@link mondrian.rolap.agg.AggregationKey}
     * and {@link mondrian.rolap.agg.CellRequest} as context to
     * convert a {@link mondrian.spi.SegmentHeader}.
     *
     * <p>This is nasty. A converter might be used for several headers, not
     * necessarily with the context as the cell request and aggregation key.
     * Converters only exist for fact tables and compound predicate combinations
     * for which we have already done a load request.</p>
     *
     * <p>It would be much better if there was a way to convert compound
     * predicates from strings to predicates. Then we could obsolete the
     * messy context inside converters, and maybe obsolete converters
     * altogether.</p>
     */
    public static class SegmentConverterImpl implements SegmentConverter {
        private final AggregationKey key;
        private final CellRequest request;

        public SegmentConverterImpl(AggregationKey key, CellRequest request) {
            this.key = key;
            this.request = request;
        }

        public SegmentWithData convert(
            SegmentHeader header,
            SegmentBody body)
        {
            final Segment segment =
                toSegment(
                    header,
                    key.getStar(),
                    header.getConstrainedColumnsBitKey(),
                    getConstrainedColumns(
                        key.getStar(),
                        header.getConstrainedColumnsBitKey()),
                    request.getMeasure(),
                    key.getCompoundPredicateList());
            return addData(segment, body);
        }
    }

    /**
     * Implementation of {@link SegmentConverter} that uses a star measure
     * and a list of {@link StarPredicate}.
     */
    public static class StarSegmentConverter implements SegmentConverter {
        private final RolapStar.Measure measure;
        private final List<StarPredicate> compoundPredicateList;

        public StarSegmentConverter(
            RolapStar.Measure measure,
            List<StarPredicate> compoundPredicateList)
        {
            this.measure = measure;
            this.compoundPredicateList = compoundPredicateList;
        }

        public SegmentWithData convert(
            SegmentHeader header,
            SegmentBody body)
        {
            final Segment segment =
                toSegment(
                    header,
                    measure.getStar(),
                    header.getConstrainedColumnsBitKey(),
                    getConstrainedColumns(
                        measure.getStar(),
                        header.getConstrainedColumnsBitKey()),
                    measure,
                    compoundPredicateList);
            return addData(segment, body);
        }
    }
}

// End SegmentBuilder.java

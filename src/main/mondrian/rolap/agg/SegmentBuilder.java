/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.olap.Aggregator;
import mondrian.olap.Util;
import mondrian.rolap.*;
import mondrian.rolap.agg.Segment.ExcludedRegion;
import mondrian.rolap.sql.SqlQuery;
import mondrian.spi.*;
import mondrian.util.ArraySortedSet;
import mondrian.util.Pair;

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
                    body.getIndicators());
        } else if (body instanceof DenseIntSegmentBody) {
            dataSet =
                new DenseIntSegmentDataset(
                    axes, (int[]) body.getValueArray(), body.getIndicators());
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
                    new LiteralStarPredicate(
                        constrainedColumn,
                        true);
            } else if (values.size() == 1) {
                predicate =
                    new ValueColumnPredicate(
                        constrainedColumn,
                        values.first());
            } else {
                final List<StarColumnPredicate> valuePredicateList =
                    new ArrayList<StarColumnPredicate>();
                for (Object value : values) {
                    valuePredicateList.add(
                        new ValueColumnPredicate(
                            constrainedColumn,
                            value));
                }
                predicate =
                    new ListColumnPredicate(
                        constrainedColumn,
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
     */
    public static Pair<SegmentHeader, SegmentBody> rollup(
        Map<SegmentHeader, SegmentBody> map,
        Set<String> keepColumns,
        BitKey targetBitkey,
        Aggregator rollupAggregator)
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
        final SegmentHeader firstHeader = map.keySet().iterator().next();
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
        for (Map.Entry<SegmentHeader, SegmentBody> entry : map.entrySet()) {
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
                    if (axis.requestedValues == null) {
                        filteredValues = values;
                        filteredHasNull = hasNull;
                    } else {
                        filteredValues = Util.intersect(
                            values,
                            axis.requestedValues);

                        // SegmentColumn predicates cannot ask for the null
                        // value (at present).
                        filteredHasNull = false;
                    }
                    axis.valueSet.addAll(filteredValues);
                    axis.hasNull = axis.hasNull || filteredHasNull;
                    if (!Util.equals(axis.requestedValues, requestedValues)) {
                        if (axis.requestedValues == null) {
                            // Downgrade from wildcard to a specific list.
                            axis.requestedValues = requestedValues;
                        } else {
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
                    pos[z++] = targetOrdinal;
                }
                final CellKey ck = CellKey.Generator.newCellKey(pos);
                if (!cellValues.containsKey(ck)) {
                    cellValues.put(ck, new ArrayList<Object>());
                }
                cellValues.get(ck).add(vEntry.getValue());
            }
        }

        // Build the axis list.
        final List<Pair<SortedSet<Comparable>, Boolean>> axisList =
            new ArrayList<Pair<SortedSet<Comparable>, Boolean>>();
        final BitSet nullIndicators = new BitSet(axes.length);
        int nbValues = 1;
        for (int i = 0; i < axes.length; i++) {
            axisList.add(
                new Pair<SortedSet<Comparable>, Boolean>(
                    axes[i].valueSet, axes[i].hasNull));
            nullIndicators.set(i, axes[i].hasNull);
            nbValues *= axes[i].hasNull
                ? axes[i].values.length + 1
                : axes[i].values.length;
         }

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
        } else if (SegmentLoader.useSparse(
                cellValues.size(),
                cellValues.size() - nullIndicators.cardinality()))
        {
            // The rule says we must use a sparse dataset.
            // First, aggregate the values of each key.
            final Map<CellKey, Object> data =
                new HashMap<CellKey, Object>();
            for (Entry<CellKey, List<Object>> entry
                : cellValues.entrySet())
            {
                data.put(
                    CellKey.Generator.newCellKey(entry.getKey().getOrdinals()),
                    rollupAggregator.aggregate(entry.getValue()));
            }
            body =
                new SparseSegmentBody(
                    data,
                    axisList);
        } else {
            // Peek at the value class. We will use a native dataset
            // if possible.
            final Object peek =
                cellValues.entrySet().iterator().next().getValue().get(0);
            if (peek instanceof Double) {
                final double[] data = new double[nbValues];
                for (Entry<CellKey, List<Object>> entry
                    : cellValues.entrySet())
                {
                    final int offset =
                        CellKey.Generator.getOffset(
                            entry.getKey().getOrdinals(), axisMultipliers);
                    data[offset] =
                        (Double)rollupAggregator.aggregate(entry.getValue());
                }
                body =
                    new DenseDoubleSegmentBody(
                        nullIndicators,
                        data,
                        axisList);
            } else if (peek instanceof Integer) {
                final int[] data = new int[nbValues];
                for (Entry<CellKey, List<Object>> entry
                    : cellValues.entrySet())
                {
                    final int offset =
                        CellKey.Generator.getOffset(
                            entry.getKey().getOrdinals(), axisMultipliers);
                    data[offset] =
                        (Integer)rollupAggregator.aggregate(entry.getValue());
                }
                body =
                    new DenseIntSegmentBody(
                        nullIndicators,
                        data,
                        axisList);
            } else {
                final Object[] data = new Object[nbValues];
                for (Entry<CellKey, List<Object>> entry
                    : cellValues.entrySet())
                {
                    final int offset =
                        CellKey.Generator.getOffset(
                            entry.getKey().getOrdinals(), axisMultipliers);
                    data[offset] =
                        (Object)rollupAggregator.aggregate(entry.getValue());
                }
                body =
                    new DenseObjectSegmentBody(
                        data,
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

        return Pair.of(header, body);
    }

    private static int[] computeAxisMultipliers(
        List<Pair<SortedSet<Comparable>, Boolean>> axes)
    {
        final int[] axisMultipliers = new int[axes.size()];
        int multiplier = 1;
        for (int i = axes.size() - 1; i >= 0; --i) {
            axisMultipliers[i] = multiplier;
            multiplier *= axes.get(i).left.size();
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
        StarColumnPredicate[] predicates)
    {
        return toConstrainedColumns(
            Arrays.asList(predicates));
    }

    public static List<SegmentColumn> toConstrainedColumns(
        Collection<StarColumnPredicate> predicates)
    {
        List<SegmentColumn> ccs =
            new ArrayList<SegmentColumn>();
        for (StarColumnPredicate predicate : predicates) {
            final List<Comparable> values =
                new ArrayList<Comparable>();
            predicate.values(Util.cast(values));
            final Comparable[] valuesArray =
                values.toArray(new Comparable[values.size()]);
            if (valuesArray.length == 1 && valuesArray[0].equals(true)) {
                ccs.add(
                    new SegmentColumn(
                        predicate.getConstrainedColumn()
                            .getExpression().getGenericExpression(),
                        predicate.getConstrainedColumn().getCardinality(),
                        null));
            } else {
                Arrays.sort(
                    valuesArray,
                    Util.SqlNullSafeComparator.instance);
                ccs.add(
                    new SegmentColumn(
                        predicate.getConstrainedColumn()
                            .getExpression().getGenericExpression(),
                        predicate.getConstrainedColumn().getCardinality(),
                        new ArraySortedSet(valuesArray)));
            }
        }
        return ccs;
    }

    /**
     * Creates a SegmentHeader object describing the supplied
     * Segment object.
     *
     * @param segment A segment object for which we want to generate
     * a SegmentHeader.
     * @return A SegmentHeader describing the supplied Segment object.
     */
    public static SegmentHeader toHeader(Segment segment) {
        final List<SegmentColumn> cc =
            SegmentBuilder.toConstrainedColumns(segment.predicates);
        final List<String> cp = new ArrayList<String>();

        StringBuilder buf = new StringBuilder();

        for (StarPredicate compoundPredicate : segment.compoundPredicateList) {
            buf.setLength(0);
            SqlQuery query =
                new SqlQuery(
                    segment.star.getSqlQueryDialect());
            compoundPredicate.toSql(query, buf);
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

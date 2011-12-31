/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/
package mondrian.rolap.agg;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mondrian.olap.Util;
import mondrian.rolap.BitKey;
import mondrian.rolap.RolapCacheRegion;
import mondrian.rolap.StarColumnPredicate;
import mondrian.rolap.StarPredicate;
import mondrian.rolap.sql.SqlQuery;
import mondrian.spi.Dialect;

/**
 * SegmentHeaders are the key objects used to retrieve the segments
 * from the segment cache.
 *
 * <p>The segment header objects are immutable and fully serializable.
 *
 * <p>The headers have each an ID which is a SHA-256 checksum of the
 * following properties, concatenated. See
 * {@link SegmentHeader#getUniqueID()}
 * <ul>
 * <li>Schema Name</li>
 * <li>Cube Name</li>
 * <li>Measure Name</li>
 * <li>For each column:</li>
 *   <ul>
 *   <li>Column table name</li>
 *   <li>Column physical name</li>
 *   <li>For each predicate value:</li>
 *     <ul>
 *     <li>The equivalent of
 *     <code>String.valueof([value object])</code></li>
 *     </ul>
 *   </ul>
 * </ul>
 *
 * @author LBoudreau
 * @version $Id$
 */
public class SegmentHeader implements Serializable {
    private static final long serialVersionUID = 8696439182886512850L;
    private final int arity;
    private final ConstrainedColumn[] constrainedColumns;
    private final String[] compoundPredicates;
    private final String measureName;
    private final String cubeName;
    private final String schemaName;
    private final String rolapStarFactTableName;
    private final BitKey constrainedColsBitKey;
    private final int hashCode;
    private byte[] uniqueID = null;
    private String description = null;
    private final String schemaChecksum;

    /**
     * Base constructor for segment headers.
     * @param schemaName The name of the schema which this
     * header belongs to.
     * @param cubeName The name of the cube this segment belongs to.
     * @param measureName The name of the measure which defines
     * this header.
     * @param constrainedColumns An array of constrained columns
     * objects which define the predicated of this segment header.
     */
    public SegmentHeader(
        String schemaName,
        String schemaChecksum,
        String cubeName,
        String measureName,
        ConstrainedColumn[] constrainedColumns,
        String rolapStarFactTableName,
        BitKey constrainedColsBitKey)
    {
        this(
            schemaName, schemaChecksum, cubeName, measureName,
            constrainedColumns, new String[0],
            rolapStarFactTableName, constrainedColsBitKey);
    }
    /**
     * Base constructor for segment headers.
     * @param schemaName The name of the schema which this
     * header belongs to.
     * @param cubeName The name of the cube this segment belongs to.
     * @param measureName The name of the measure which defines
     * this header.
     * @param constrainedColumns An array of constrained columns
     * objects which define the predicated of this segment header.
     */
    public SegmentHeader(
        String schemaName,
        String schemaChecksum,
        String cubeName,
        String measureName,
        ConstrainedColumn[] constrainedColumns,
        String[] compoundPredicates,
        String rolapStarFactTableName,
        BitKey constrainedColsBitKey)
    {
        this.constrainedColumns = constrainedColumns;
        this.schemaName = schemaName;
        this.schemaChecksum = schemaChecksum;
        this.cubeName = cubeName;
        this.measureName = measureName;
        this.compoundPredicates = compoundPredicates;
        this.rolapStarFactTableName = rolapStarFactTableName;
        this.constrainedColsBitKey = constrainedColsBitKey;
        this.arity = constrainedColumns.length;
        // Hash code might be used extensively. Better compute
        // it up front. Make sure the columns are ordered in a
        // deterministic order (alpha...)
        int hash = 42;
        hash = Util.hash(hash, schemaName);
        hash = Util.hash(hash, schemaChecksum);
        hash = Util.hash(hash, cubeName);
        hash = Util.hash(hash, measureName);
        for (ConstrainedColumn col : this.constrainedColumns) {
            hash = Util.hash(hash, col.columnExpression);
            for (Object val : col.values) {
                hash = Util.hash(hash, val);
            }
        }
        for (String col : this.compoundPredicates) {
            hash = Util.hash(hash, col);
        }
        this.hashCode = hash;
    }

    public int hashCode() {
        return hashCode;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof SegmentHeader)) {
            return false;
        }
        return ((SegmentHeader)obj).getUniqueID().equals(this.getUniqueID());
    }

    public static ConstrainedColumn[] forCacheRegion(
        RolapCacheRegion region)
    {
        final ConstrainedColumn[] cc =
            new ConstrainedColumn[region.getColumnPredicates().size()];
        int i = 0;
        for (StarColumnPredicate predicate : region.getColumnPredicates()) {
            // First get the values
            final List<Object> values = new ArrayList<Object>();
            predicate.values(values);
            // Now build the CC object
            cc[i] =
                new SegmentHeader.ConstrainedColumn(
                    predicate.getColumn().toSql(),
                    values.toArray());
            i++;
        }
        return cc;
    }

    /**
     * Creates a SegmentHeader object describing the supplied
     * Segment object.
     * @param segment A segment object for which we want to generate
     * a SegmentHeader.
     * @return A SegmentHeader describing the supplied Segment object.
     */
    public static SegmentHeader forSegment(
        Segment segment,
        List<StarPredicate> compoundPredicates)
    {
        final ConstrainedColumn[] cc =
            new ConstrainedColumn[segment.axes.length];
        final String cp[] = new String[compoundPredicates.size()];
        for (int i = 0; i < segment.axes.length; i++) {
            Aggregation.Axis axis = segment.axes[i];
            // First get the values
            final List<Object> values = new ArrayList<Object>();
            axis.getPredicate().values(values);
            // Now build the CC object
            cc[i] =
                new SegmentHeader.ConstrainedColumn(
                    axis.getPredicate().getColumn().toSql(),
                    values.toArray());
        }
        final StringBuilder buf = new StringBuilder();
        final Dialect dialect =
                segment.aggregation.getStar().getSqlQueryDialect();
        for (int i = 0; i < compoundPredicates.size(); i++) {
            buf.setLength(0);
            compoundPredicates.get(i).toSql(dialect, buf);
            cp[i] = buf.toString();
        }
        return
            new SegmentHeader(
                segment.measure.getStar().getSchema().getName(),
                segment.measure.getStar().getSchema().getChecksum(),
                segment.measure.getCubeName(),
                segment.measure.getName(),
                cc,
                cp,
                segment.aggregation.getStar().getFactTable().getAlias(),
                segment.aggregation.getConstrainedColumnsBitKey());
    }

    /**
     * Creates a clone of this header by replacing some of the
     * constrained columns in the process.
     * @param overrideValues A list of constrained columns to either
     * replace or add to the original header.
     * @return A clone of the header with the columns replaced.
     */
    public SegmentHeader clone(ConstrainedColumn[] overrideValues) {
        Map<String, ConstrainedColumn> colsToAdd =
            new HashMap<String, ConstrainedColumn>();
        for (ConstrainedColumn cc : this.constrainedColumns) {
            colsToAdd.put(cc.columnExpression, cc);
        }
        for (ConstrainedColumn override : overrideValues) {
            colsToAdd.put(override.columnExpression, override);
        }
        return
            new SegmentHeader(
                schemaName,
                schemaChecksum,
                cubeName,
                measureName,
                colsToAdd.values()
                    .toArray(new ConstrainedColumn[colsToAdd.size()]),
                rolapStarFactTableName,
                constrainedColsBitKey);
    }

    /**
     * Constrained columns are part of the SegmentHeader and SegmentCache.
     * They uniquely identify a constrained column within a segment.
     * Each segment can have many constrained columns. Each column can
     * be constrained by multiple values at once (similar to a SQL in()
     * predicate).
     *
     * <p>They are immutable and serializable.
     */
    public static class ConstrainedColumn implements Serializable {
        private static final long serialVersionUID = -5227838916517784720L;
        final String columnExpression;
        final Object[] values;
        private int hashCode = Integer.MIN_VALUE;

        /**
         * Constructor for ConstrainedColumn.
         * @param columnExpression Name of the source table into which the
         * constrained column is, as defined in the Mondrian schema.
         * @param valueList List of values to constrain the
         * column to. Use objects like Integer, Boolean, String
         * or Double.
         */
        public ConstrainedColumn(
            String columnExpression,
            Object[] valueList)
        {
            this.columnExpression = columnExpression;
            this.values = new Object[valueList.length];
            System.arraycopy(
                valueList,
                0,
                this.values,
                0,
                valueList.length);
        }
        /**
         * Returns the column expression of this constrained column.
         * @return A column expression.
         */
        public String getColumnExpression() {
            return columnExpression;
        }
        /**
         * Returns an array of predicate values for this column.
         * @return An array of object values.
         */
        public Object[] getValues() {
            return values;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ConstrainedColumn)) {
                return false;
            }
            ConstrainedColumn that = (ConstrainedColumn) obj;
            if (!this.columnExpression.equals(that.columnExpression)) {
                return false;
            }
            if (this.values.length != that.values.length) {
                return false;
            }
            for (int i = 0; i < this.values.length; i++) {
                if (!this.values[i].equals(that.values[i])) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            if (this.hashCode  == Integer.MIN_VALUE) {
                int hash = super.hashCode();
                hash = Util.hash(hash, this.columnExpression);
                for (Object val : this.values) {
                    hash = Util.hash(hash, val);
                }
                this.hashCode = hash;
            }
            return hashCode;
        }
    }

    public String toString() {
        return this.getDescription();
    }

    /**
     * Returns the arity of this SegmentHeader.
     * @return The arity as an integer number.
     */
    public int getArity() {
        return arity;
    }

    /**
     * Returns an array of constrained columns which define this segment
     * header. The caller should consider this list immutable.
     * @return An array of ConstrainedColumns
     */
    public ConstrainedColumn[] getConstrainedColumns() {
        ConstrainedColumn[] copy =
            new ConstrainedColumn[this.constrainedColumns.length];
        System.arraycopy(
            constrainedColumns,
            0,
            copy,
            0,
            constrainedColumns.length);
        return copy;
    }

    /**
     * Returns the constrained column object, if any, corresponding
     * to a column name and a table name.
     * @param columnExpression The column name we want.
     * @return A Constrained column, or null.
     */
    public ConstrainedColumn getConstrainedColumn(
        String columnExpression)
    {
        for (ConstrainedColumn c : constrainedColumns) {
            if (c.columnExpression.equals(columnExpression)) {
                return c;
            }
        }
        return null;
    }

    public BitKey getConstrainedColumnsBitKey() {
        return this.constrainedColsBitKey.copy();
    }

    /**
     * Tells if the passed segment is a subset of this segment
     * and could be used for a rollup in cache operation.
     * @param segment A segment which might be a subset of the
     * current segment.
     * @return True or false.
     */
    public boolean isSubset(Segment segment) {
        if (!segment.aggregation.getStar().getSchema().getName()
                .equals(schemaName))
        {
            return false;
        }
        if (!segment.aggregation.getStar().getFactTable().getAlias()
                .equals(rolapStarFactTableName))
        {
            return false;
        }
        if (!segment.measure.getName().equals(measureName)) {
            return false;
        }
        if (!segment.measure.getCubeName().equals(cubeName)) {
            return false;
        }
        if (segment.aggregation.getConstrainedColumnsBitKey()
                .equals(constrainedColsBitKey))
        {
            return true;
        }
        return false;
    }

    /**
     * Returns a unique identifier for this header. The identifier
     * can be used for storage and will be the same across segments
     * which have the same schema name, cube name, measure name,
     * and for each constrained column, the same column name, table name,
     * and predicate values.
     * @return A unique identification string.
     */
    public String getUniqueID() {
        if (this.uniqueID == null) {
            StringBuilder hashSB = new StringBuilder();
            hashSB.append(this.schemaName);
            hashSB.append(this.schemaChecksum);
            hashSB.append(this.cubeName);
            hashSB.append(this.measureName);
            for (ConstrainedColumn c : constrainedColumns) {
                hashSB.append(c.columnExpression);
                for (Object value : c.values) {
                    hashSB.append(String.valueOf(value));
                }
            }
            for (String c : compoundPredicates) {
                hashSB.append(c);
            }
            this.uniqueID = Util.checksumSha256(hashSB.toString());
        }
        final StringBuffer hexString = new StringBuffer();
        for (int i = 0; i < this.uniqueID.length; i++) {
          hexString.append(
              Integer.toHexString(0xFF & this.uniqueID[i]));
        }
        return hexString.toString();
    }

    /**
     * Returns a human readable description of this
     * segment header.
     * @return A string describing the header.
     */
    public String getDescription() {
        if (this.description == null) {
            StringBuilder descriptionSB = new StringBuilder();
            descriptionSB.append("*Segment Header\n");
            descriptionSB.append("Schema:[");
            descriptionSB.append(this.schemaName);
            descriptionSB.append("]\nChecksum:[");
            descriptionSB.append(this.schemaChecksum);
            descriptionSB.append("]\nCube:[");
            descriptionSB.append(this.cubeName);
            descriptionSB.append("]\nMeasure:[");
            descriptionSB.append(this.measureName);
            descriptionSB.append("]\n");
            descriptionSB.append("Axes:[");
            for (ConstrainedColumn c : constrainedColumns) {
                descriptionSB.append("\n\t{");
                descriptionSB.append(c.columnExpression);
                descriptionSB.append("=(");
                for (Object value : c.values) {
                    descriptionSB.append("'");
                    descriptionSB.append(value);
                    descriptionSB.append("',");
                }
                descriptionSB.deleteCharAt(descriptionSB.length() - 1);
                descriptionSB.append(")}");
            }
            descriptionSB.append("]\n");
            descriptionSB.append("Compound Predicates:[");
            for (String c : compoundPredicates) {
                descriptionSB.append("\n\t{");
                descriptionSB.append(c);
            }
            descriptionSB
                .append("]\n")
                .append("ID:[")
                .append(getUniqueID())
                .append("]\n");
            this.description = descriptionSB.toString();
        }
        return description;
    }
}

// End SegmentHeader.java

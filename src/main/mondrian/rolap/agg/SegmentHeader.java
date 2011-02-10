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
import java.util.List;

import mondrian.olap.Util;

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
    private static final long serialVersionUID = -6478016915339461330L;
    private final int arity;
    private final ConstrainedColumn[] constrainedColumns;
    private final String measureName;
    private final String cubeName;
    private final String schemaName;
    private final int hashCode;
    private byte[] uniqueID = null;
    private String description = null;

    /**
     * Base constructor for segment headers.
     * @see #forSegment(Segment)
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
        String cubeName,
        String measureName,
        ConstrainedColumn[] constrainedColumns)
    {
        this.constrainedColumns = constrainedColumns;
        this.schemaName = schemaName;
        this.cubeName = cubeName;
        this.measureName = measureName;
        this.arity = constrainedColumns.length;
        // Hash code might be used extensively. Better compute
        // it up front.
        int hash = 42;
        hash = Util.hash(hash, schemaName);
        hash = Util.hash(hash, cubeName);
        hash = Util.hash(hash, measureName);
        for (ConstrainedColumn col : constrainedColumns) {
            hash = Util.hash(hash, col.tableName);
            hash = Util.hash(hash, col.columnName);
            for (Object val : col.values) {
                hash = Util.hash(hash, val);
            }
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
        return ((SegmentHeader)obj).getUniqueID() == this.getUniqueID();
    }

    /**
     * Creates a SegmentHeader object describing the supplied
     * Segment object.
     * @param segment A segment object for which we want to generate
     * a SegmentHeader.
     * @return A SegmentHeader describing the supplied Segment object.
     */
    public static SegmentHeader forSegment(Segment segment) {
        final ConstrainedColumn[] cc =
            new ConstrainedColumn[segment.axes.length];
        for (int i = 0; i < segment.axes.length; i++) {
            Aggregation.Axis axis = segment.axes[i];
            final List<Object> values = new ArrayList<Object>();
            axis.getPredicate().values(values);
            cc[i] =
                new SegmentHeader.ConstrainedColumn(
                    axis.getPredicate().getConstrainedColumn()
                        .getTable().getTableName(),
                    axis.getPredicate().getConstrainedColumn()
                        .getName(),
                    values.toArray());
        }
        return
            new SegmentHeader(
                segment.measure.getStar().getSchema().getName(),
                segment.measure.getCubeName(),
                segment.measure.getName(),
                cc);
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
        private static final long serialVersionUID = -2639097927260237524L;
        final String tableName;
        final String columnName;
        final Object[] values;
        /**
         * Constructor for ConstrainedColumn.
         * @param tableName Name of the source table into which the
         * constrained column is, as defined in the Mondrian schema.
         * @param columnName Name of the column to constrain, as defined
         * in the Mondrian schema.
         * @param valueList List of values to constrain the
         * column to. Use objects like Integer, Boolean, String
         * or Double.
         */
        public ConstrainedColumn(
                String tableName,
                String columnName,
                Object[] valueList)
        {
            this.tableName = tableName;
            this.columnName = columnName;
            this.values = new Object[valueList.length];
            System.arraycopy(
                valueList,
                0,
                this.values,
                0,
                valueList.length);
        }
        /**
         * Returns the table name of this constrained column.
         * @return A table name.
         */
        public String getTableName() {
            return tableName;
        }
        /**
         * Returns the column name of this constrained column.
         * It is not necessarely unique since columns of the same
         * name can be found across multiple tables.
         * @return A column name.
         */
        public String getColumnName() {
            return columnName;
        }
        /**
         * Returns an array of predicate values for this column.
         * @return An array of object values.
         */
        public Object[] getValues() {
            return values;
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
            hashSB.append(this.cubeName);
            hashSB.append(this.measureName);
            for (ConstrainedColumn c : constrainedColumns) {
                hashSB.append(c.tableName);
                hashSB.append(c.columnName);
                for (Object value : c.values) {
                    hashSB.append(String.valueOf(value));
                }
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
            descriptionSB.append("}\nCube:[");
            descriptionSB.append(this.cubeName);
            descriptionSB.append("]\nMeasure:[");
            descriptionSB.append(this.measureName);
            descriptionSB.append("]\n");
            descriptionSB.append("Predicates:[");
            for (ConstrainedColumn c : constrainedColumns) {
                descriptionSB.append("\n\t{");
                descriptionSB.append(c.tableName);
                descriptionSB.append(".");
                descriptionSB.append(c.columnName);
                descriptionSB.append("=(");
                for (Object value : c.values) {
                    descriptionSB.append("'");
                    descriptionSB.append(value);
                    descriptionSB.append("',");
                }
                descriptionSB.deleteCharAt(descriptionSB.length() - 1);
                descriptionSB.append(")}");
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
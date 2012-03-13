/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.spi;

import mondrian.olap.Util;
import mondrian.rolap.BitKey;
import mondrian.util.ByteString;

import java.io.Serializable;
import java.util.*;

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
 */
public class SegmentHeader implements Serializable {
    private static final long serialVersionUID = 8696439182886512850L;
    private final int arity;
    private final List<SegmentColumn> constrainedColumns;
    private final List<SegmentColumn> excludedRegions;
    public final List<String> compoundPredicates;
    public final String measureName;
    public final String cubeName;
    public final String schemaName;
    public final String rolapStarFactTableName;
    public final BitKey constrainedColsBitKey;
    private final int hashCode;
    private ByteString uniqueID;
    private String description;
    public final ByteString schemaChecksum;

    /**
     * Creates a segment header.
     *
     * @param schemaName The name of the schema which this
     * header belongs to.
     * @param schemaChecksum Schema checksum
     * @param cubeName The name of the cube this segment belongs to.
     * @param measureName The name of the measure which defines
     * this header.
     * @param constrainedColumns An array of constrained columns
     * objects which define the predicated of this segment header.
     * @param compoundPredicates Compound predicates (Must not be null, but
     * typically empty.)
     * @param rolapStarFactTableName Star fact table name
     * @param constrainedColsBitKey Constrained columns bit key
     * @param excludedRegions Excluded regions. (Must not be null, but typically
     */
    public SegmentHeader(
        String schemaName,
        ByteString schemaChecksum,
        String cubeName,
        String measureName,
        List<SegmentColumn> constrainedColumns,
        List<String> compoundPredicates,
        String rolapStarFactTableName,
        BitKey constrainedColsBitKey,
        List<SegmentColumn> excludedRegions)
    {
        this.constrainedColumns = constrainedColumns;
        this.excludedRegions = excludedRegions;
        this.schemaName = schemaName;
        this.schemaChecksum = schemaChecksum;
        assert schemaChecksum != null;
        this.cubeName = cubeName;
        this.measureName = measureName;
        this.compoundPredicates = compoundPredicates;
        this.rolapStarFactTableName = rolapStarFactTableName;
        this.constrainedColsBitKey = constrainedColsBitKey;
        this.arity = constrainedColumns.size();
        // Hash code might be used extensively. Better compute
        // it up front.
        this.hashCode = computeHashCode();
    }

    private int computeHashCode() {
        int hash = 42;
        hash = Util.hash(hash, schemaName);
        hash = Util.hash(hash, schemaChecksum);
        hash = Util.hash(hash, cubeName);
        hash = Util.hash(hash, measureName);
        for (SegmentColumn col : this.constrainedColumns) {
            hash = Util.hash(hash, col.columnExpression);
            if (col.values != null) {
                hash = Util.hashArray(hash, col.values.toArray());
            }
        }
        for (SegmentColumn col : this.excludedRegions) {
            hash = Util.hash(hash, col.columnExpression);
            if (col.values != null) {
                hash = Util.hashArray(hash, col.values.toArray());
            }
        }
        hash = Util.hash(hash, compoundPredicates);
        return hash;
    }

    public int hashCode() {
        return hashCode;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof SegmentHeader)) {
            return false;
        }
        final SegmentHeader that = (SegmentHeader) obj;
        return getUniqueID().equals(that.getUniqueID())
            && excludedRegions.equals(that.excludedRegions);
    }

    /**
     * Creates a clone of this header by replacing some of the
     * constrained columns in the process.
     * @param overrideValues A list of constrained columns to either
     * replace or add to the original header.
     * @return A clone of the header with the columns replaced.
     */
    public SegmentHeader clone(SegmentColumn[] overrideValues) {
        Map<String, SegmentColumn> colsToAdd =
            new HashMap<String, SegmentColumn>();
        for (SegmentColumn cc : this.constrainedColumns) {
            colsToAdd.put(cc.columnExpression, cc);
        }
        for (SegmentColumn override : overrideValues) {
            colsToAdd.put(override.columnExpression, override);
        }
        return
            new SegmentHeader(
                schemaName,
                schemaChecksum,
                cubeName,
                measureName,
                new ArrayList<SegmentColumn>(colsToAdd.values()),
                Collections.<String>emptyList(),
                rolapStarFactTableName,
                constrainedColsBitKey,
                Collections.<SegmentColumn>emptyList());
    }

    /**
     * Checks if this header can be constrained by a given region.
     *
     * <p>It will return false if the region covers one of the axis in
     * its entirety.
     *
     * <p>It will return false if the region sits outside of the bounds
     * of this header. This means that when performing a flush operation,
     * the header must be scrapped altogether.
     */
    public boolean canConstrain(SegmentColumn[] region) {
        boolean atLeastOnePresent = false;
        for (SegmentColumn ccToFlush : region) {
            SegmentColumn ccActual =
                getConstrainedColumn(ccToFlush.columnExpression);
            if (ccActual != null) {
                final SegmentColumn ccActualExcl =
                    getExcludedRegion(ccToFlush.columnExpression);
                if (ccToFlush.values == null
                    || (ccActualExcl != null
                        && ccActualExcl.values != null
                        && ccActualExcl.merge(ccToFlush).values == null))
                {
                    // This means that the whole axis is excluded.
                    // Better destroy that segment.
                    return false;
                }
                if (ccActual.values != null
                    && ccActual.values.equals(ccToFlush.values))
                {
                    // This means that the whole axis is excluded.
                    // Better destroy that segment.
                    return false;
                }
                // We know there is at least one column on which
                // we can constrain.
                atLeastOnePresent = true;
            }
        }
        return atLeastOnePresent;
    }

    /**
     * Applies a set of exclusions to this segment header and returns
     * a new segment header representing the original one to which a
     * region has been excluded.
     *
     * @param region Region
     * @return Header with constraint applied
     */
    public SegmentHeader constrain(SegmentColumn[] region) {
        final Map<String, SegmentColumn> newRegions =
            new HashMap<String, SegmentColumn>();
        for (SegmentColumn excludedRegion : excludedRegions) {
            newRegions.put(
                excludedRegion.columnExpression,
                excludedRegion);
        }
        for (SegmentColumn col : region) {
            if (getConstrainedColumn(col.columnExpression) == null) {
                continue;
            }
            if (newRegions.containsKey(col.columnExpression)) {
                newRegions.put(
                    col.columnExpression,
                    newRegions.get(col.columnExpression)
                        .merge(col));
            } else {
                newRegions.put(
                    col.columnExpression,
                    col);
            }
        }
        assert newRegions.size() > 0;
        return
            new SegmentHeader(
                schemaName,
                schemaChecksum,
                cubeName,
                measureName,
                constrainedColumns,
                compoundPredicates,
                rolapStarFactTableName,
                constrainedColsBitKey,
                new ArrayList<SegmentColumn>(newRegions.values()));
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

    public List<SegmentColumn> getExcludedRegions() {
        return excludedRegions;
    }

    /**
     * Returns a list of constrained columns which define this segment
     * header. The caller should consider this list immutable.
     *
     * @return List of ConstrainedColumns
     */
    public List<SegmentColumn> getConstrainedColumns() {
        return constrainedColumns;
    }

    /**
     * Returns the constrained column object, if any, corresponding
     * to a column name and a table name.
     * @param columnExpression The column name we want.
     * @return A Constrained column, or null.
     */
    public SegmentColumn getConstrainedColumn(
        String columnExpression)
    {
        for (SegmentColumn c : constrainedColumns) {
            if (c.columnExpression.equals(columnExpression)) {
                return c;
            }
        }
        return null;
    }

    public SegmentColumn getExcludedRegion(
        String columnExpression)
    {
        for (SegmentColumn c : excludedRegions) {
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
     * Returns a unique identifier for this header. The identifier
     * can be used for storage and will be the same across segments
     * which have the same schema name, cube name, measure name,
     * and for each constrained column, the same column name, table name,
     * and predicate values.
     * @return A unique identification string.
     */
    public ByteString getUniqueID() {
        if (this.uniqueID == null) {
            StringBuilder hashSB = new StringBuilder();
            hashSB.append(this.schemaName);
            hashSB.append(this.schemaChecksum);
            hashSB.append(this.cubeName);
            hashSB.append(this.measureName);
            for (SegmentColumn c : constrainedColumns) {
                hashSB.append(c.columnExpression);
                if (c.values != null) {
                    for (Object value : c.values) {
                        hashSB.append(String.valueOf(value));
                    }
                }
            }
            for (SegmentColumn c : excludedRegions) {
                hashSB.append(c.columnExpression);
                if (c.values != null) {
                    for (Object value : c.values) {
                        hashSB.append(String.valueOf(value));
                    }
                }
            }
            for (String c : compoundPredicates) {
                hashSB.append(c);
            }
            this.uniqueID =
                new ByteString(Util.digestSha256(hashSB.toString()));
        }
        return uniqueID;
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
            for (SegmentColumn c : constrainedColumns) {
                descriptionSB.append("\n    {");
                descriptionSB.append(c.columnExpression);
                descriptionSB.append("=(");
                if (c.values == null) {
                    descriptionSB.append("* ");
                } else {
                    for (Object value : c.values) {
                        descriptionSB.append("'");
                        descriptionSB.append(value);
                        descriptionSB.append("',");
                    }
                }
                descriptionSB.deleteCharAt(descriptionSB.length() - 1);
                descriptionSB.append(")}");
            }
            descriptionSB.append("]\n");
            descriptionSB.append("Excluded Regions:[");
            for (SegmentColumn c : excludedRegions) {
                descriptionSB.append("\n    {");
                descriptionSB.append(c.columnExpression);
                descriptionSB.append("=(");
                if (c.values == null) {
                    descriptionSB.append("* ");
                } else {
                    for (Object value : c.values) {
                        descriptionSB.append("'");
                        descriptionSB.append(value);
                        descriptionSB.append("',");
                    }
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

/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2004 Galt Johnson
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

/**
 * Enumerates the types of levels.
 *
 * @author Galt Johnson
 * @since 5 April, 2004
 * @version $Id$
 */
public class DimensionType extends EnumeratedValues.BasicValue {
    /**
     * The constructor is private so that no spurious instances of DimensionType can
     * be created.
     * @param name The name of the new enumerated value.
     * @param ordinal The ordinal value of the enumerated value.
     */
    private DimensionType(String name, int ordinal) {
        super(name, ordinal, null);
    }

    public static final int StandardDimensionORDINAL = 0;

    /**
     * Indicates that the dimension is not related to time.
     */
    public static final DimensionType StandardDimension =
            new DimensionType("StandardDimension", StandardDimensionORDINAL);

    public static final int TimeDimensionORDINAL = 1;

    /**
     * Indicates that a dimension is a time deimsnsion.
     */
    public static final DimensionType TimeDimension =
            new DimensionType("TimeDimension", TimeDimensionORDINAL);

    /**
     * Contains all of the valid values for {@link DimensionType}.
     */
    public static final EnumeratedValues enumeration =
            new EnumeratedValues(
                    new DimensionType[] {
                        StandardDimension, TimeDimension,
                    }
            );
}

// End LevelType.java
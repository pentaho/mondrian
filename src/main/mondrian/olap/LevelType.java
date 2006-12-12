/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;



/**
 * Enumerates the types of levels.
 *
 * @author jhyde
 * @since 5 April, 2004
 * @version $Id$
 */
public enum LevelType {

    /** Indicates that the level is not related to time. */
    Regular,

    /**
     * Indicates that a level refers to years.
     * It must be used in a dimension whose type is
     * {@link DimensionType#TimeDimension}.
     */
    TimeYears,

    /**
     * Indicates that a level refers to quarters.
     * It must be used in a dimension whose type is
     * {@link DimensionType#TimeDimension}.
     */
    TimeQuarters,

    /**
     * Indicates that a level refers to months.
     * It must be used in a dimension whose type is
     * {@link DimensionType#TimeDimension}.
     */
    TimeMonths,

    /**
     * Indicates that a level refers to weeks.
     * It must be used in a dimension whose type is
     * {@link DimensionType#TimeDimension}.
     */
    TimeWeeks,

    /**
     * Indicates that a level refers to days.
     * It must be used in a dimension whose type is
     * {@link DimensionType#TimeDimension}.
     */
    TimeDays,

    /**
     * Indicates that a level holds the null member.
     */
    Null;

    public boolean isTime() {
        return ordinal() >= TimeYears.ordinal() &&
            ordinal() <= TimeDays.ordinal();
    }
}

// End LevelType.java

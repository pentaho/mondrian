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
public class LevelType extends EnumeratedValues.BasicValue {

    private LevelType(String name, int ordinal) {
        super(name, ordinal, null);
    }

    public static final int RegularORDINAL = 0;
    /** Indicates that the level is not related to time. */
    public static final LevelType Regular =
            new LevelType("Regular", RegularORDINAL);

    public static final int TimeYearsORDINAL = 1;

    /**
     * Indicates that a level refers to years.
     * It must be used in a dimension whose type is
     * {@link DimensionType#TimeDimension}.
     */
    public static final LevelType TimeYears =
            new LevelType("TimeYears", TimeYearsORDINAL);

    public static final int TimeQuartersORDINAL = 2;

    /**
     * Indicates that a level refers to quarters.
     * It must be used in a dimension whose type is
     * {@link DimensionType#TimeDimension}.
     */
    public static final LevelType TimeQuarters =
            new LevelType("TimeQuarters", TimeQuartersORDINAL);

    public static final int TimeMonthsORDINAL = 3;

    /**
     * Indicates that a level refers to months.
     * It must be used in a dimension whose type is
     * {@link DimensionType#TimeDimension}.
     */
    public static final LevelType TimeMonths =
            new LevelType("TimeMonths", TimeMonthsORDINAL);

    public static final int TimeWeeksORDINAL = 4;

    /**
     * Indicates that a level refers to weeks.
     * It must be used in a dimension whose type is
     * {@link DimensionType#TimeDimension}.
     */
    public static final LevelType TimeWeeks =
            new LevelType("TimeWeeks", TimeWeeksORDINAL);

    public static final int TimeDaysORDINAL = 5;

    /**
     * Indicates that a level refers to days.
     * It must be used in a dimension whose type is
     * {@link DimensionType#TimeDimension}.
     */
    public static final LevelType TimeDays =
            new LevelType("TimeDays", TimeDaysORDINAL);

    /**
     * Contains all of the valid values for {@link LevelType}.
     */
    public static final EnumeratedValues enumeration =
            new EnumeratedValues(
                    new LevelType[] {
                        Regular, TimeYears, TimeQuarters, TimeMonths,
                        TimeWeeks, TimeDays,
                    }
            );
    public static LevelType lookup(String s) {
        return (LevelType) enumeration.getValue(s, true);
    }
    public boolean isTime() {
        return ordinal >= TimeYearsORDINAL && ordinal <= TimeDaysORDINAL;
    }
}

// End LevelType.java

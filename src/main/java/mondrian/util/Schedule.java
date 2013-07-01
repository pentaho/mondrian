/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import java.sql.Time;
import java.util.*;

/**
 * A <code>Schedule</code> generates a series of time events.
 *
 * <p> Create a schedule using one of the factory methods:<ul>
 * <li>{@link #createOnce},</li>
 * <li>{@link #createDaily},</li>
 * <li>{@link #createWeekly},</li>
 * <li>{@link #createMonthlyByDay},</li>
 * <li>{@link #createMonthlyByWeek}.</li></ul>
 *
 * <p> Then use the {@link #nextOccurrence} method to find the next occurrence
 * after a particular point in time.
 *
 * <p> The <code>begin</code> and <code>end</code> parameters represent the
 * points in time between which the schedule is active. Both are optional.
 * However, if a schedule type supports a <code>period</code> parameter, and
 * you supply a value greater than 1, <code>begin</code> is used to determine
 * the start of the cycle. If <code>begin</code> is not specified, the cycle
 * starts at the epoch (January 1st, 1970).
 *
 * <p> The {@link Date} parameters in this API -- <code>begin</code> and
 * <code>end</code>, the <code>time</code> parameter to {@link #createOnce},
 * and the <code>earliestDate</code> parameter and value returned from {@link
 * #nextOccurrence} -- always represent a point in time (GMT), not a local
 * time.  If a schedule is to start at 12 noon Tokyo time, April 1st, 2002, it
 * is the application's reponsibility to convert this into a UTC {@link Date}
 * value.
 *
 * @author jhyde
 * @since May 7, 2002
 */
public class Schedule {

    // members

    private DateSchedule dateSchedule;
    private TimeSchedule timeSchedule;
    private TimeZone tz;
    private Date begin;
    private Date end;

    // constants

    /**
     * Indicates that a schedule should fire on the last day of the month.
     * @see #createMonthlyByDay
     */
    public static final int LAST_DAY_OF_MONTH = 0;
    /**
     * Indicates that a schedule should fire on the last week of the month.
     * @see #createMonthlyByWeek
     */
    public static final int LAST_WEEK_OF_MONTH = 0;

    static final TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");

    static final int allDaysOfWeekBitmap =
        (1 << Calendar.MONDAY)
        | (1 << Calendar.TUESDAY)
        | (1 << Calendar.WEDNESDAY)
        | (1 << Calendar.THURSDAY)
        | (1 << Calendar.FRIDAY)
        | (1 << Calendar.SATURDAY)
        | (1 << Calendar.SUNDAY);
    static final int allDaysOfMonthBitmap =
        0xefffFffe // bits 1..31
        | (1 << LAST_DAY_OF_MONTH);
    static final int allWeeksOfMonthBitmap =
        0x0000003e // bits 1..5
        | (1 << LAST_WEEK_OF_MONTH);

    // constructor(s) and factory methods

    /**
     * Please use the factory methods {@link #createDaily} etc. to create a
     * Schedule.
     */
    private Schedule(
        DateSchedule dateSchedule,
        TimeSchedule timeSchedule,
        TimeZone tz,
        Date begin,
        Date end)
    {
        this.dateSchedule = dateSchedule;
        this.timeSchedule = timeSchedule;
        this.tz = tz;
        this.begin = begin;
        this.end = end;
    }

    /**
     * Creates a calendar which fires only once.
     *
     * @param date date and time to fire, must be UTC
     * @param tz timezone
     *
     * @pre tz != null
     * @pre date != null
     * @post return != null
     */
    public static Schedule createOnce(Date date, TimeZone tz) {
        Calendar calendar = ScheduleUtil.createCalendar(date);
        Time timeOfDay = ScheduleUtil.createTime(
            calendar.get(Calendar.HOUR_OF_DAY),
            calendar.get(Calendar.MINUTE),
            calendar.get(Calendar.SECOND));
        calendar.add(Calendar.SECOND, 1);
        Date datePlusDelta = calendar.getTime();
        return createDaily(date, datePlusDelta, tz, timeOfDay, 1);
    }

    /**
     * Creates a calendar which fires every day.
     *
     * @param begin open lower bound, may be null, must be UTC
     * @param end closed upper bound, may be null, must be UTC
     * @param tz timezone
     * @param timeOfDay time at which to fire
     * @param period causes the schedule to fire every <code>period</code>
     *     days. If <code>period</code> is greater than 1, the cycle starts
     *     at the begin point of the schedule, or at the epoch (1 January,
     *     1970) if <code>begin</code> is not specified.
     *
     * @pre tz != null
     * @pre period > 0
     * @post return != null
     */
    public static Schedule createDaily(
        Date begin,
        Date end,
        TimeZone tz,
        Time timeOfDay,
        int period)
    {
        DateSchedule dateSchedule =
            new DailyDateSchedule(
                begin == null ? null : ScheduleUtil.createCalendar(begin),
                period);
        return new Schedule(
            dateSchedule,
            new OnceTimeSchedule(ScheduleUtil.createTimeCalendar(timeOfDay)),
            tz,
            begin,
            end);
    }

    /**
     * Creates a calendar which fires on particular days each week.
     *
     * @param tz timezone
     * @param daysOfWeekBitmap a bitmap of day values, for example
     *     <code>(1 << {@link Calendar#TUESDAY}) |
     *           (1 << {@link Calendar#THURSDAY})</code> to fire on Tuesdays
     *     and Thursdays
     * @param timeOfDay time at which to fire
     * @param begin open lower bound, may be null
     * @param end closed upper bound, may be null
     * @param period causes the schedule to be active every <code>period</code>
     *     weeks. If <code>period</code> is greater than 1, the cycle starts
     *     at the begin point of the schedule, or at the epoch (1 January,
     *     1970) if <code>begin</code> is not specified.
     *
     * @pre tz != null
     * @pre period > 0
     * @post return != null
     */
    public static Schedule createWeekly(
        Date begin,
        Date end,
        TimeZone tz,
        Time timeOfDay,
        int period,
        int daysOfWeekBitmap)
    {
        DateSchedule dateSchedule =
            new WeeklyDateSchedule(
                begin == null ? null : ScheduleUtil.createCalendar(begin),
                period,
                daysOfWeekBitmap);
        return new Schedule(
            dateSchedule,
            new OnceTimeSchedule(ScheduleUtil.createTimeCalendar(timeOfDay)),
            tz,
            begin,
            end);
    }

    /**
     * Creates a calendar which fires on particular days of each month.
     * For example,<blockquote>
     *
     * <pre>createMonthlyByDay(
     *     null, null, TimeZone.getTimeZone("PST"), 1,
     *     (1 << 12) | (1 << 14) | (1 << {@link #LAST_DAY_OF_MONTH}))</pre>
     *
     * </blockquote> creates a schedule which fires on the 12th, 14th and last
     * day of the month.
     *
     * @param begin open lower bound, may be null
     * @param end closed upper bound, may be null
     * @param tz timezone
     * @param daysOfMonthBitmap a bitmap of day values, may include
     *     {@link #LAST_DAY_OF_MONTH}
     * @param timeOfDay time at which to fire
     * @param period causes the schedule to be active every <code>period</code>
     *     months. If <code>period</code> is greater than 1, the cycle starts
     *     at the begin point of the schedule, or at the epoch (1 January,
     *     1970) if <code>begin</code> is not specified.
     *
     * @pre tz != null
     * @pre period > 0
     * @post return != null
     */
    public static Schedule createMonthlyByDay(
        Date begin,
        Date end,
        TimeZone tz,
        Time timeOfDay,
        int period,
        int daysOfMonthBitmap)
    {
        DateSchedule dateSchedule =
            new MonthlyByDayDateSchedule(
                begin == null ? null : ScheduleUtil.createCalendar(begin),
                period, daysOfMonthBitmap);
        return new Schedule(
            dateSchedule,
            new OnceTimeSchedule(ScheduleUtil.createTimeCalendar(timeOfDay)),
            tz,
            begin,
            end);
    }

    /**
     * Creates a calendar which fires on particular days of particular weeks of
     * a month. For example,<blockquote>
     *
     * <pre>createMonthlyByWeek(
     *     null, null, TimeZone.getTimeZone("PST"),
     *     (1 << Calendar.TUESDAY) | (1 << Calendar.THURSDAY),
     *     (1 << 2) | (1 << {@link #LAST_WEEK_OF_MONTH})</pre>
     *
     * </blockquote> creates a schedule which fires on the 2nd and last Tuesday
     * and Thursday of the month.
     *
     * @param begin open lower bound, may be null
     * @param end closed upper bound, may be null
     * @param tz timezone
     * @param daysOfWeekBitmap a bitmap of day values, for example
     *     <code>(1 << Calendar.TUESDAY) | (1 << Calendar.THURSDAY)</code>
     * @param weeksOfMonthBitmap a bitmap of week values (may include
     *     {@link #LAST_WEEK_OF_MONTH}
     * @param timeOfDay time at which to fire
     * @param period causes the schedule be active every <code>period</code>
     *     months. If <code>period</code> is greater than 1, the cycle starts
     *     at the begin point of the schedule, or at the epoch (1 January,
     *     1970) if <code>begin</code> is not specified.
     *
     * @pre tz != null
     * @pre period > 0
     * @post return != null
     */
    public static Schedule createMonthlyByWeek(
        Date begin,
        Date end,
        TimeZone tz,
        Time timeOfDay,
        int period,
        int daysOfWeekBitmap,
        int weeksOfMonthBitmap)
    {
        DateSchedule dateSchedule =
            new MonthlyByWeekDateSchedule(
                begin == null ? null : ScheduleUtil.createCalendar(begin),
                period,
                daysOfWeekBitmap,
                weeksOfMonthBitmap);
        return new Schedule(
            dateSchedule,
            new OnceTimeSchedule(ScheduleUtil.createTimeCalendar(timeOfDay)),
            tz,
            begin,
            end);
    }

    /**
     * Returns the next occurrence of this schedule after a given date. If
     * <code>after</code> is null, returns the first occurrence. If there are
     * no further occurrences, returns null.
     *
     * @param after if not null, returns the first occurrence after this
     *    point in time; if null, returns the first occurrence ever.
     * @param strict If <code>after</code> is an occurrence,
     *     <code>strict</code> determines whether this method returns it, or
     *     the next occurrence. If <code>strict</code> is true, the value
     *     returned is strictly greater than <code>after</code>.
     */
    public Date nextOccurrence(Date after, boolean strict) {
        if (after == null
            || begin != null && begin.after(after))
        {
            after = begin;
            strict = false;
        }
        if (after == null) {
            after = new Date(0);
        }
        Date next = nextOccurrence0(after, strict);
        // if there is an upper bound, and this is not STRICTLY before it,
        // there's no next occurrence
        if (next != null
            && end != null
            && !next.before(end))
        {
            next = null;
        }
        return next;
    }

    private Date nextOccurrence0(Date after, boolean strict) {
        Calendar next = ScheduleUtil.createCalendar(after);
        if (tz == null || tz.getID().equals("GMT")) {
            return nextOccurrence1(next, strict);
        } else {
            int offset;
            if (next == null) {
                offset = tz.getRawOffset();
            } else {
                offset = ScheduleUtil.timezoneOffset(tz, next);
            }
            // Add the offset to the calendar, so that the calendar looks like
            // the local time (even though it is still in GMT). Suppose an
            // event runs at 12:00 JST each day. At 02:00 GMT they ask for the
            // next event. We convert this to local time, 11:00 JST, by adding
            // the 9 hour offset. We will convert the result back to GMT by
            // subtracting the offset.
            next.add(Calendar.MILLISECOND, offset);
            Date result = nextOccurrence1(next, strict);
            if (result == null) {
                return null;
            }
            Calendar resultCalendar = ScheduleUtil.createCalendar(result);
            int offset2 = ScheduleUtil.timezoneOffset(tz, resultCalendar);
            // Shift the result back again.
            resultCalendar.add(Calendar.MILLISECOND, -offset2);
            return resultCalendar.getTime();
        }
    }

    private Date nextOccurrence1(Calendar earliest, boolean strict) {
        Calendar earliestDay = ScheduleUtil.floor(earliest);
        Calendar earliestTime = ScheduleUtil.getTime(earliest);
        // first, try a later time on the same day
        Calendar nextDay = dateSchedule.nextOccurrence(earliestDay, false);
        Calendar nextTime = timeSchedule.nextOccurrence(earliestTime, strict);
        if (nextTime == null) {
            // next, try the first time on a later day
            nextDay = dateSchedule.nextOccurrence(earliestDay, true);
            nextTime =
                timeSchedule.nextOccurrence(ScheduleUtil.midnightTime, false);
        }
        if (nextDay == null || nextTime == null) {
            return null;
        }
        nextDay.set(Calendar.HOUR_OF_DAY, nextTime.get(Calendar.HOUR_OF_DAY));
        nextDay.set(Calendar.MINUTE, nextTime.get(Calendar.MINUTE));
        nextDay.set(Calendar.SECOND, nextTime.get(Calendar.SECOND));
        nextDay.set(Calendar.MILLISECOND, nextTime.get(Calendar.MILLISECOND));
        return nextDay.getTime();
    }
}

/**
 * A <code>TimeSchedule</code> generates a series of times within a day.
 */
interface TimeSchedule {
    /**
     * Returns the next occurrence at or after <code>after</code>. If
     * <code>after</code> is null, returns the first occurrence. If there are
     * no further occurrences, returns null.
     *
     * @param strict if true, return time must be after <code>after</code>, not
     *     equal to it
     */
    Calendar nextOccurrence(Calendar earliest, boolean strict);
}

/**
 * A <code>OnceTimeSchedule</code> fires at one and only one time.
 */
class OnceTimeSchedule implements TimeSchedule {
    Calendar time;
    OnceTimeSchedule(Calendar time) {
        ScheduleUtil.assertTrue(time != null);
        ScheduleUtil.assertTrue(ScheduleUtil.isTime(time));
        this.time = time;
    }

    public Calendar nextOccurrence(Calendar after, boolean strict) {
        if (after == null) {
            return time;
        }
        if (time.after(after)) {
            return time;
        }
        if (!strict && time.equals(after)) {
            return time;
        }
        return null;
    }
}

/**
 * A <code>DateSchedule</code> returns a series of dates.
 */
interface DateSchedule {
    /**
     * Returns the next date when this schedule fires.
     *
     * @pre earliest != null
     */
    Calendar nextOccurrence(Calendar earliest, boolean strict);
};

/**
 * A <code>DailyDateSchedule</code> fires every day.
 */
class DailyDateSchedule implements DateSchedule {
    int period;
    int beginOrdinal;
    DailyDateSchedule(Calendar begin, int period) {
        this.period = period;
        ScheduleUtil.assertTrue(period > 0, "period must be positive");
        this.beginOrdinal = ScheduleUtil.julianDay(
            begin == null ? ScheduleUtil.epochDay : begin);
    }

    public Calendar nextOccurrence(Calendar day, boolean strict) {
        day = (Calendar) day.clone();
        if (strict) {
            day.add(Calendar.DATE, 1);
        }
        while (true) {
            int ordinal = ScheduleUtil.julianDay(day);
            if ((ordinal - beginOrdinal) % period == 0) {
                return day;
            }
            day.add(Calendar.DATE, 1);
        }
    }
}

/**
 * A <code>WeeklyDateSchedule</code> fires every week. A bitmap indicates
 * which days of the week it fires.
 */
class WeeklyDateSchedule implements DateSchedule {
    int period;
    int beginOrdinal;
    int daysOfWeekBitmap;

    WeeklyDateSchedule(Calendar begin, int period, int daysOfWeekBitmap) {
        this.period = period;
        ScheduleUtil.assertTrue(period > 0, "period must be positive");
        this.beginOrdinal = ScheduleUtil.julianDay(
            begin == null ? ScheduleUtil.epochDay : begin) / 7;
        this.daysOfWeekBitmap = daysOfWeekBitmap;
        ScheduleUtil.assertTrue(
            (daysOfWeekBitmap & Schedule.allDaysOfWeekBitmap) != 0,
            "weekly schedule must have at least one day set");
        ScheduleUtil.assertTrue(
            (daysOfWeekBitmap & Schedule.allDaysOfWeekBitmap)
            == daysOfWeekBitmap,
            "weekly schedule has bad bits set: " + daysOfWeekBitmap);
    }

    public Calendar nextOccurrence(Calendar earliest, boolean strict) {
        earliest = (Calendar) earliest.clone();
        if (strict) {
            earliest.add(Calendar.DATE, 1);
        }
        int i = 7 + period; // should be enough
        while (i-- > 0) {
            int dayOfWeek = earliest.get(Calendar.DAY_OF_WEEK);
            if ((daysOfWeekBitmap & (1 << dayOfWeek)) != 0) {
                int ordinal = ScheduleUtil.julianDay(earliest) / 7;
                if ((ordinal - beginOrdinal) % period == 0) {
                    return earliest;
                }
            }
            earliest.add(Calendar.DATE, 1);
        }
        throw ScheduleUtil.newInternal(
            "weekly date schedule is looping -- maybe the bitmap is empty: "
            + daysOfWeekBitmap);
    }
}

/**
 * A <code>MonthlyByDayDateSchedule</code> fires on a particular set of days
 * every month.
 */
class MonthlyByDayDateSchedule implements DateSchedule {
    int period;
    int beginMonth;
    int daysOfMonthBitmap;

    MonthlyByDayDateSchedule(
        Calendar begin,
        int period,
        int daysOfMonthBitmap)
    {
        this.period = period;
        ScheduleUtil.assertTrue(period > 0, "period must be positive");
        this.beginMonth = begin == null ? 0 : monthOrdinal(begin);
        this.daysOfMonthBitmap = daysOfMonthBitmap;
        ScheduleUtil.assertTrue(
            (daysOfMonthBitmap & Schedule.allDaysOfMonthBitmap) != 0,
            "monthly day schedule must have at least one day set");
        ScheduleUtil.assertTrue(
            (daysOfMonthBitmap & Schedule.allDaysOfMonthBitmap)
            == daysOfMonthBitmap,
            "monthly schedule has bad bits set: " + daysOfMonthBitmap);
    }

    public Calendar nextOccurrence(Calendar earliest, boolean strict) {
        earliest = (Calendar) earliest.clone();
        if (strict) {
            earliest.add(Calendar.DATE, 1);
        }
        int i = 31 + period; // should be enough
        while (i-- > 0) {
            int month = monthOrdinal(earliest);
            if ((month - beginMonth) % period != 0) {
                // not this month! move to first of next month
                earliest.set(Calendar.DAY_OF_MONTH, 1);
                earliest.add(Calendar.MONTH, 1);
                continue;
            }
            int dayOfMonth = earliest.get(Calendar.DAY_OF_MONTH);
            if ((daysOfMonthBitmap & (1 << dayOfMonth)) != 0) {
                return earliest;
            }
            earliest.add(Calendar.DATE, 1);
            if ((daysOfMonthBitmap & (1 << Schedule.LAST_DAY_OF_MONTH)) != 0
                && earliest.get(Calendar.DAY_OF_MONTH) == 1)
            {
                // They want us to fire on the last day of the month, and
                // now we're at the first day of the month, so we must have
                // been at the last. Backtrack and return it.
                earliest.add(Calendar.DATE, -1);
                return earliest;
            }
        }
        throw ScheduleUtil.newInternal(
            "monthly-by-day date schedule is looping -- maybe "
            + "the bitmap is empty: " + daysOfMonthBitmap);
    }

    private static int monthOrdinal(Calendar earliest) {
        return earliest.get(Calendar.YEAR) * 12
            + earliest.get(Calendar.MONTH);
    }
}

/**
 * A <code>MonthlyByWeekDateSchedule</code> fires on particular days of
 * particular weeks of a month.
 */
class MonthlyByWeekDateSchedule implements DateSchedule {
    int period;
    int beginMonth;
    int daysOfWeekBitmap;
    int weeksOfMonthBitmap;

    MonthlyByWeekDateSchedule(
        Calendar begin,
        int period,
        int daysOfWeekBitmap,
        int weeksOfMonthBitmap)
    {
        this.period = period;
        ScheduleUtil.assertTrue(period > 0, "period must be positive");
        this.beginMonth = begin == null ? 0 : monthOrdinal(begin);
        this.daysOfWeekBitmap = daysOfWeekBitmap;
        ScheduleUtil.assertTrue(
            (daysOfWeekBitmap & Schedule.allDaysOfWeekBitmap) != 0,
            "weekly schedule must have at least one day set");
        ScheduleUtil.assertTrue(
            (daysOfWeekBitmap & Schedule.allDaysOfWeekBitmap)
            == daysOfWeekBitmap,
            "weekly schedule has bad bits set: " + daysOfWeekBitmap);
        this.weeksOfMonthBitmap = weeksOfMonthBitmap;
        ScheduleUtil.assertTrue(
            (weeksOfMonthBitmap & Schedule.allWeeksOfMonthBitmap) != 0,
            "weeks of month schedule must have at least one week set");
        ScheduleUtil.assertTrue(
            (weeksOfMonthBitmap & Schedule.allWeeksOfMonthBitmap)
            == weeksOfMonthBitmap,
            "week of month schedule has bad bits set: "
            + weeksOfMonthBitmap);
    }

    public Calendar nextOccurrence(Calendar earliest, boolean strict) {
        earliest = (Calendar) earliest.clone();
        if (strict) {
            earliest.add(Calendar.DATE, 1);
        }
         // should be enough... worst case is '5th Monday of every 3rd month'
        int i = 365 + period;
        while (i-- > 0) {
            int month = monthOrdinal(earliest);
            if ((month - beginMonth) % period != 0) {
                // not this month! move to first of next month
                earliest.set(Calendar.DAY_OF_MONTH, 1);
                earliest.add(Calendar.MONTH, 1);
                continue;
            }
            // is it one of the days we're interested in?
            int dayOfWeek = earliest.get(Calendar.DAY_OF_WEEK);
            if ((daysOfWeekBitmap & (1 << dayOfWeek)) != 0) {
                // is it the Yth occurrence of day X?
                int dayOfMonth = earliest.get(Calendar.DAY_OF_MONTH);
                int weekOfMonth = (dayOfMonth + 6) / 7; // 1-based
                if ((weeksOfMonthBitmap & (1 << weekOfMonth)) != 0) {
                    return earliest;
                }
                // is it the last occurrence of day X?
                if ((weeksOfMonthBitmap & (1 << Schedule.LAST_WEEK_OF_MONTH))
                    != 0)
                {
                    // we're in the last week of the month iff a week later is
                    // in the first week of the next month
                    earliest.add(Calendar.WEEK_OF_MONTH, 1);
                    boolean isLast = earliest.get(Calendar.DAY_OF_MONTH) <= 7;
                    earliest.add(Calendar.WEEK_OF_MONTH, -1);
                    if (isLast) {
                        return earliest;
                    }
                }
            }
            earliest.add(Calendar.DATE, 1);
        }
        throw ScheduleUtil.newInternal(
            "monthy-by-week date schedule is cyclic");
    }

    private static int monthOrdinal(Calendar earliest) {
        return earliest.get(Calendar.YEAR) * 12
            + earliest.get(Calendar.MONTH);
    }
}

/**
 * Utility functions for {@link Schedule} and supporting classes.
 */
class ScheduleUtil {
    static final Calendar epochDay = ScheduleUtil.createCalendar(new Date(0));
    static final Calendar midnightTime =
        ScheduleUtil.createTimeCalendar(0, 0, 0);

    public static void assertTrue(boolean b) {
        if (!b) {
            throw new Error("assertion failed");
        }
    }

    public static void assertTrue(boolean b, String s) {
        if (!b) {
            throw new Error("assertion failed: " + s);
        }
    }

    public static Error newInternal() {
        return new Error("internal error");
    }

    public static Error newInternal(Throwable e, String s) {
        return new Error("internal error '" + e + "': " + s);
    }

    public static Error newInternal(String s) {
        return new Error("internal error: " + s);
    }

    public static boolean lessThan(Time t1, Time t2, boolean strict) {
        if (strict) {
            return t1.getTime() < t2.getTime();
        } else {
            return t1.getTime() <= t2.getTime();
        }
    }

    public static boolean lessThan(Date d1, Date d2, boolean strict) {
        if (strict) {
            return d1.getTime() < d2.getTime();
        } else {
            return d1.getTime() <= d2.getTime();
        }
    }

    public static boolean is0000(Calendar calendar) {
        return calendar.get(Calendar.HOUR_OF_DAY) == 0
            && calendar.get(Calendar.MINUTE) == 0
            && calendar.get(Calendar.SECOND) == 0
            && calendar.get(Calendar.MILLISECOND) == 0;
    }

    public static boolean isTime(Calendar calendar) {
        return calendar.get(Calendar.YEAR)
            == ScheduleUtil.epochDay.get(Calendar.YEAR)
            && calendar.get(Calendar.DAY_OF_YEAR)
            == ScheduleUtil.epochDay.get(Calendar.DAY_OF_YEAR);
    }

    /**
     * Returns a calendar rounded down to the previous midnight.
     */
    public static Calendar floor(Calendar calendar) {
        if (calendar == null) {
            return null;
        }
        calendar = (Calendar) calendar.clone();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar;
    }

    /**
     * Returns a calendar rounded up to the next midnight, unless it is already
     * midnight.
     */
    public static Calendar ceiling(Calendar calendar) {
        if (calendar == null) {
            return null;
        }
        if (is0000(calendar)) {
            return calendar;
        }
        calendar = (Calendar) calendar.clone();
        calendar.add(Calendar.DATE, 1);
        return calendar;
    }

    /**
     * Extracts the time part of a date. Given a null date, returns null.
     */
    public static Calendar getTime(Calendar calendar) {
        if (calendar == null) {
            return null;
        }
        return createTimeCalendar(
            calendar.get(Calendar.HOUR_OF_DAY),
            calendar.get(Calendar.MINUTE),
            calendar.get(Calendar.SECOND));
    }

    /**
     * Creates a calendar in UTC, and initializes it to <code>date</code>.
     *
     * @pre date != null
     * @post return != null
     */
    public static Calendar createCalendar(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(Schedule.utcTimeZone);
        calendar.setTime(date);
        return calendar;
    }

    /**
     * Creates a calendar in UTC, and initializes it to a given year, month,
     * day, hour, minute, second. <b>NOTE: month is 1-based</b>
     */
    public static Calendar createCalendar(
        int year,
        int month,
        int day,
        int hour,
        int minute,
        int second)
    {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(Schedule.utcTimeZone);
        calendar.toString(); // calls complete()
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.MONTH, month - 1); // CONVERT TO 0-BASED!!
        calendar.set(Calendar.DAY_OF_MONTH, day);
        calendar.set(Calendar.HOUR_OF_DAY, hour);
        calendar.set(Calendar.MINUTE, minute);
        calendar.set(Calendar.SECOND, second);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar;
    }

    /**
     * Creates a calendar from a time. Milliseconds are ignored.
     *
     * @pre time != null
     * @post return != null
     */
    public static Calendar createTimeCalendar(Time time) {
        Calendar calendar = (Calendar) ScheduleUtil.epochDay.clone();
        calendar.setTimeZone(Schedule.utcTimeZone);
        calendar.setTime(time);
        return createTimeCalendar(
            calendar.get(Calendar.HOUR_OF_DAY),
            calendar.get(Calendar.MINUTE),
            calendar.get(Calendar.SECOND));
    }

    /**
     * Creates a calendar and sets it to a given hours, minutes, seconds.
     */
    public static Calendar createTimeCalendar(
        int hours,
        int minutes,
        int seconds)
    {
        Calendar calendar = (Calendar) ScheduleUtil.epochDay.clone();
        calendar.set(Calendar.HOUR_OF_DAY, hours);
        calendar.set(Calendar.MINUTE, minutes);
        calendar.set(Calendar.SECOND, seconds);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar;
    }

    /**
     * Creates a calendar and sets it to a given year, month, date.
     */
    public static Calendar createDateCalendar(
        int year, int month, int dayOfMonth)
    {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(Schedule.utcTimeZone);
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.MONTH, month);
        calendar.set(Calendar.DAY_OF_MONTH, dayOfMonth);
        return calendar;
    }
    /**
     * Creates a {@link java.sql.Time}
     */
    public static Time createTime(int hour, int minutes, int second) {
        return new Time(
            createTimeCalendar(hour, minutes, second).getTime().getTime());
    }
    /**
     * Returns the julian day number of a given date. (Is there a better way
     * to do this?)
     */
    public static int julianDay(Calendar calendar) {
        int year = calendar.get(Calendar.YEAR),
            day = calendar.get(Calendar.DAY_OF_YEAR),
            leapDays = (year / 4) - (year / 100) + (year / 400);
        return year * 365 + leapDays + day;
    }
    /**
     * Returns the offset from UTC in milliseconds in this timezone on a given
     * date.
     */
    public static int timezoneOffset(TimeZone tz, Calendar calendar) {
        return tz.getOffset(
            calendar.get(Calendar.ERA),
            calendar.get(Calendar.YEAR),
            calendar.get(Calendar.MONTH),
            calendar.get(Calendar.DAY_OF_MONTH),
            calendar.get(Calendar.DAY_OF_WEEK),
            (1000
             * (60
                * (60 * calendar.get(Calendar.HOUR_OF_DAY)
                   + calendar.get(Calendar.MINUTE))
                + calendar.get(Calendar.SECOND))
             + calendar.get(Calendar.MILLISECOND)));
    }
}

// End Schedule.java

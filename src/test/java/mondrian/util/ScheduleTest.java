/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.util;

import junit.framework.TestCase;

import java.sql.Time;
import java.util.*;

public class ScheduleTest extends TestCase {
    public static final Time time0827 = ScheduleUtil.createTime(8, 27, 00);
    public static final Time time1600 = ScheduleUtil.createTime(16, 00, 0);
    public static final Time time0000 = ScheduleUtil.createTime(00, 00, 0);
    public static final Time time0233 = ScheduleUtil.createTime(02, 33, 0);
    public static final TimeZone gmtTz = TimeZone.getTimeZone("GMT");
    public static final TimeZone pstTz =
        TimeZone.getTimeZone("America/Los_Angeles"); // GMT-8
    public static final TimeZone jstTz = TimeZone.getTimeZone("Asia/Tokyo");
    public static final TimeZone sgtTz =
        TimeZone.getTimeZone("Asia/Singapore"); // GMT+8
    public static final int weekdays =
        (1 << Calendar.MONDAY) |
        (1 << Calendar.TUESDAY) |
        (1 << Calendar.WEDNESDAY) |
        (1 << Calendar.THURSDAY) |
        (1 << Calendar.FRIDAY);
    private static final String[] daysOfWeek = {
        null, "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"
    };

    public ScheduleTest(String name) {
        super(name);
    }

    // helper methods

    static void assertEquals(Calendar c1, Calendar c2) {
        if (c1 == null || c2 == null) {
            assertEquals((Object) c1, (Object) c2);
        } else {
            // do the checks on 'smaller' objects -- otherwise the
            // failure message is too long to see in the debugger
            assertEquals(c1.getTimeZone(), c2.getTimeZone());
            assertEquals(c1.getTime(), c2.getTime());
        }
    }

    static void assertEquals(Date expected, Calendar actual) {
        if (expected == null || actual == null) {
            assertEquals((Object) expected, (Object) actual);
        } else {
            assertEquals(expected, actual.getTime());
        }
    }

    static void assertEquals(
        int year, int month, int day, String dow,
        int hour, int minute, Date actual)
    {
        assertEquals(toDate(year, month, day, dow, hour, minute), actual);
    }

    static void assertEquals(Calendar expected, Date actual) {
        if (expected == null || actual == null) {
            assertEquals((Object) expected, (Object) actual);
        } else {
            assertEquals(expected.getTime(), actual);
        }
    }

    static void assertScheduleCount(
        Schedule schedule,
        Date d,
        Date last,
        int expectedCount)
    {
        int count = 0;
        while (true) {
            Date next = schedule.nextOccurrence(d, true);
            if (next == null) {
                break;
            }
            count++;
            d = next;
            if (count > 100) {
                break; // we're looping
            }
        }
        assertEquals(last, d); // last occurrence
        assertEquals("schedule count", expectedCount, count);
    }

    static Date toDate(
        int year,
        int month,
        int day,
        String dow,
        int hour,
        int minute)
    {
        return toDate(year, month, day, dow, hour, minute, gmtTz);
    }

    static Date toDate(
        int year,
        int month,
        int day,
        String dow,
        int hour,
        int minute,
        TimeZone tz)
    {
        Calendar calendar =
            ScheduleUtil.createCalendar(year, month, day, hour, minute, 0);
        calendar.setTimeZone(tz);
        assertEquals(daysOfWeek[calendar.get(Calendar.DAY_OF_WEEK)], dow);
        return calendar.getTime();
    }
    // --------------------------------------------------------------------
    // test cases

    public void testOnceTimeSchedule() {
        Calendar calendar0827 = ScheduleUtil.createCalendar(time0827);
        OnceTimeSchedule onceTimeSchedule = new OnceTimeSchedule(calendar0827);
        Calendar t = onceTimeSchedule.nextOccurrence(null, true);
        assertEquals(calendar0827, t);
        Calendar calendar1600 = ScheduleUtil.createCalendar(time1600);
        t = onceTimeSchedule.nextOccurrence(calendar1600, true);
        assertEquals((Calendar) null, t);
        t = onceTimeSchedule.nextOccurrence(calendar0827, true);
        assertEquals((Calendar) null, t);
        t = onceTimeSchedule.nextOccurrence(calendar0827, false);
        assertEquals(calendar0827, t);
        Calendar calendar0000 = ScheduleUtil.createCalendar(time0000);
        t = onceTimeSchedule.nextOccurrence(calendar0000, false);
        assertEquals(calendar0827, t);
    }

    public void testOnce() {
        Schedule schedule =
            Schedule.createOnce(toDate(2002, 04, 23, "Tue", 8, 27), gmtTz);
        Date d;
        d = schedule.nextOccurrence(null, false);
        assertEquals(2002, 04, 23, "Tue", 8, 27, d);
        d = schedule.nextOccurrence(toDate(2002, 04, 23, "Tue", 8, 27), false);
        assertEquals(2002, 04, 23, "Tue", 8, 27, d);
        d = schedule.nextOccurrence(toDate(2002, 04, 23, "Tue", 8, 27), true);
        assertEquals(null, d);
        d = schedule.nextOccurrence(toDate(2002, 06, 03, "Mon", 16, 00), false);
        assertEquals(null, d);
        d = schedule.nextOccurrence(toDate(2002, 04, 20, "Sat", 23, 00), true);
        assertEquals(2002, 04, 23, "Tue", 8, 27, d);
        d = schedule.nextOccurrence(toDate(2002, 04, 20, "Sat", 23, 00), false);
        assertEquals(2002, 04, 23, "Tue", 8, 27, d);
    }

    public void testDaily() {
        int period = 1;
        Schedule schedule = Schedule.createDaily(
            toDate(2002, 04, 20, "Sat", 8, 27),
            toDate(2002, 06, 03, "Mon", 8, 27),
            gmtTz, time0827, period);
        Date d;
        d = schedule.nextOccurrence(null, false);
        assertEquals(2002, 4, 20, "Sat", 8, 27, d);
        d = schedule.nextOccurrence(toDate(2002, 04, 20, "Sat", 8, 27), false);
        assertEquals(2002, 4, 20, "Sat", 8, 27, d);
        d = schedule.nextOccurrence(toDate(2002, 04, 20, "Sat", 23, 00), false);
        assertEquals(2002, 04, 21, "Sun", 8, 27, d);
        d = schedule.nextOccurrence(toDate(2002, 06, 03, "Mon", 8, 27), false);
        assertEquals(null, d); // upper-bound is closed
        d = schedule.nextOccurrence(toDate(2002, 06, 03, "Mon", 16, 00), false);
        assertEquals(null, d);
        d = schedule.nextOccurrence(toDate(2002, 06, 04, "Tue", 8, 27), false);
        assertEquals(null, d);
    }

    public void testDailyNoUpperLimit() {
        int period = 1;
        Schedule schedule = Schedule.createDaily(
            toDate(2002, 4, 20, "Sat", 8, 27), null, gmtTz, time0827,
            period);
        Date d = schedule.nextOccurrence(null, false);
        assertEquals(2002, 4, 20, "Sat", 8, 27, d);
        d = schedule.nextOccurrence(toDate(2002, 06, 03, "Mon", 16, 00), false);
        assertEquals(2002, 06, 04, "Tue", 8, 27, d);
    }

    public void testDailyPeriodic() {
        int period = 10;
        Schedule schedule = Schedule.createDaily(
            toDate(2002, 4, 20, "Sat", 8, 27),
            toDate(2002, 06, 03, "Mon", 8, 27),
            gmtTz, time0827, period);
        Date d = schedule.nextOccurrence(null, false);
        assertEquals(2002, 4, 20, "Sat", 8, 27, d);
        d = schedule.nextOccurrence(toDate(2002, 4, 20, "Sat", 8, 27), true);
        assertEquals(2002, 04, 30, "Tue", 8, 27, d);
    }

    public void testWeeklyEmptyBitmapFails() {
        boolean failed = false;
        try {
            Schedule.createWeekly(null, null, gmtTz, time0827, 1, 0);
        } catch (Throwable e) {
            failed = true;
        }
        assertTrue(failed);
    }

    public void testWeeklyBadBitmapFails() {
        boolean failed = false;
        try {
            int period = 1;
            Schedule.createWeekly(
                null, null, gmtTz, time0827, period, (1 << 8));
        } catch (Throwable e) {
            failed = true;
        }
        assertTrue(failed);
    }

    public void testWeekly() {
        int thuesday =
            (1 << Calendar.TUESDAY) |
            (1 << Calendar.THURSDAY);
        int period = 1;
        Schedule schedule = Schedule.createWeekly(
            toDate(2002, 4, 20, "Sat", 8, 27),
            toDate(2002, 06, 05, "Wed", 12, 00),
            gmtTz, time0827, period, thuesday);
        Date d;
        d = schedule.nextOccurrence(null, false);
        assertEquals(2002, 04, 23, "Tue", 8, 27, d);
        d = schedule.nextOccurrence(toDate(2002, 04, 23, "Tue", 8, 27), false);
        assertEquals(2002, 04, 23, "Tue", 8, 27, d);
        assertScheduleCount(
            schedule, d, toDate(2002, 06, 04, "Tue", 8, 27), 12);
    }

    public void testMonthlyByDay() {
        int period = 1;
        int daysOfMonth =
            (1 << 12) | (1 << 21) | (1 << Schedule.LAST_DAY_OF_MONTH);
        Schedule schedule =
            Schedule.createMonthlyByDay(
                toDate(2002, 4, 20, "Sat", 8, 27),
                toDate(2002, 07, 10, "Wed", 12, 00),
                gmtTz, time0827, period, daysOfMonth);
        Date d;
        d = schedule.nextOccurrence(null, false);
        assertEquals(2002, 04, 21, "Sun", 8, 27, d);
        d = schedule.nextOccurrence(d, true);
        assertEquals(2002, 04, 30, "Tue", 8, 27, d);
        d = schedule.nextOccurrence(d, false);
        assertEquals(2002, 04, 30, "Tue", 8, 27, d);
        d = schedule.nextOccurrence(d, true);
        assertEquals(2002, 05, 12, "Sun", 8, 27, d);
        d = schedule.nextOccurrence(d, false);
        assertEquals(2002, 05, 12, "Sun", 8, 27, d);
        assertScheduleCount(schedule, d, toDate(2002, 6, 30, "Sun", 8, 27), 5);
    }

    public void testMonthlyByDayPeriodic() {
        int daysOfMonth =
            (1 << 12) | (1 << 21) | (1 << Schedule.LAST_DAY_OF_MONTH);
        int period = 2;
        Schedule schedule =
            Schedule.createMonthlyByDay(
                toDate(2002, 04, 30, "Tue", 8, 27),
                toDate(2002, 7, 10, "Wed", 12, 00),
                gmtTz, time0827, period, daysOfMonth);
        Date d;
        // strict=true means strictly greater than null (-infinity), not
        // strictly greater than the start time (apr30), so apr30 is
        // correct
        d = schedule.nextOccurrence(null, true);
        assertEquals(2002, 04, 30, "Tue", 8, 27, d);
        d = schedule.nextOccurrence(d, false);
        assertEquals(2002, 04, 30, "Tue", 8, 27, d);
        d = schedule.nextOccurrence(d, true);
        assertEquals(2002, 06, 12, "Wed", 8, 27, d);
        d = schedule.nextOccurrence(d, false);
        assertEquals(2002, 06, 12, "Wed", 8, 27, d);
        assertScheduleCount(schedule, d, toDate(2002, 6, 30, "Sun", 8, 27), 2);
    }

    public void testMonthlyByWeek() {
        int period = 3;
        int daysOfWeek = (1 << Calendar.THURSDAY) | (1 << Calendar.SUNDAY);
        int weeksOfMonth = (1 << 2) | (1 << Schedule.LAST_WEEK_OF_MONTH);
        Schedule schedule =
            Schedule.createMonthlyByWeek(
                toDate(2002, 4, 20, "Sat", 8, 27),
                toDate(2004, 4, 19, "Mon", 12, 00),
                gmtTz, time0827, period, daysOfWeek, weeksOfMonth);
        Date d;
        d = schedule.nextOccurrence(null, false);
        assertEquals(2002, 04, 25, "Thu", 8, 27, d);
        d = schedule.nextOccurrence(toDate(2002, 04, 23, "Tue", 8, 27), false);
        assertEquals(2002, 04, 25, "Thu", 8, 27, d);
        d = schedule.nextOccurrence(d, true);
        assertEquals(2002, 04, 28, "Sun", 8, 27, d);
        d = schedule.nextOccurrence(d, true);
        assertEquals(2002, 7, 11, "Thu", 8, 27, d);
        assertScheduleCount(schedule, d, toDate(2004, 4, 11, "Sun", 8, 27), 29);
    }

    public void testTimeZone
            () {
        int period = 1;
        int daysOfWeek = (1 << Calendar.THURSDAY);
        int weeksOfMonth = (1 << Schedule.LAST_WEEK_OF_MONTH);
        Schedule schedule = Schedule.createMonthlyByWeek(
            toDate(2002, 3, 07, "Thu", 14, 00),
            toDate(2004, 4, 19, "Mon", 12, 00),
            jstTz, time0827, period, daysOfWeek, weeksOfMonth);
        Date d;
        d = schedule.nextOccurrence(null, true);
        // 1st occurrence is
        // Thu 28 Mar 08:27 JST, which is
        // Wed 27 Mar 23:27 GMT (9 hours difference) and
        // Wed 27 Mar 15:27 PST (a further 8 hours)
        assertEquals(2002, 03, 27, "Wed", 23, 27, d);
        d = schedule.nextOccurrence(d, true);
        // 2nd occurrence is
        // Thu 25 Apr 08:27 JST, which is
        // Wed 24 Apr 23:27 GMT (Japan does not have daylight savings)
        assertEquals(2002, 04, 24, "Wed", 23, 27, d);
        d = schedule.nextOccurrence(d, true);
        d = schedule.nextOccurrence(d, true);
        d = schedule.nextOccurrence(d, true);
        // 5th occurrence is
        // Thu 25 Jul 08:27 JST, which is
        // Wed 24 Jul 23:27 GMT
        assertEquals(2002, 07, 24, "Wed", 23, 27, d);
        d = schedule.nextOccurrence(d, true);
        d = schedule.nextOccurrence(d, true);
        d = schedule.nextOccurrence(d, true);
        // 8th occurrence is
        // Thu 31 Oct 08:27 JST, which is
        // Wed 30 Oct 23:27 GMT
        assertEquals(2002, 10, 30, "Wed", 23, 27, d);
        d = schedule.nextOccurrence(d, true);
        d = schedule.nextOccurrence(d, true);
        d = schedule.nextOccurrence(d, true);
        d = schedule.nextOccurrence(d, true);
        d = schedule.nextOccurrence(d, true);
        d = schedule.nextOccurrence(d, true);
        d = schedule.nextOccurrence(d, true);
        d = schedule.nextOccurrence(d, true);
        d = schedule.nextOccurrence(d, true);
        d = schedule.nextOccurrence(d, true);
        d = schedule.nextOccurrence(d, true);
        d = schedule.nextOccurrence(d, true);
        d = schedule.nextOccurrence(d, true);
        d = schedule.nextOccurrence(d, true);
        // 22nd occurrence is
        // Thu 25 Dec 08:27 JST, 2003, which is
        // Wed 24 Dec 23:27 GMT. Note that
        // this is NOT the last Wednesday in the month.
        assertEquals(2003, 12, 24, "Wed", 23, 27, d);
    }

    public void testTimeZoneChange
            () {
        int period = 1;
        TimeZone tz = pstTz;
        Schedule schedule = Schedule.createDaily(
            toDate(2002, 04, 03, "Wed", 8, 27, tz), null,
            tz, time0233, period);
        Date d;
        d = schedule.nextOccurrence(null, false);
        // 1st occurrence is
        // Thu 04 Apr 02:33 PST which is
        // Thu 04 Apr 10:33 GMT (no daylight savings yet)
        assertEquals(toDate(2002, 04, 04, "Thu", 02, 33, tz), d);
        d = schedule.nextOccurrence(d, true);
        d = schedule.nextOccurrence(d, true);
        // 3rd occurrence is
        // Sat 06 Apr 02:33 PST which is
        // Sat 06 Apr 10:33 GMT (still no daylight savings)
        assertEquals(2002, 04, 06, "Sat", 10, 33, d);
        d = schedule.nextOccurrence(d, true);
        // 4th occurrence occurs during the switch to daylight savings,
        // Sun 07 Apr 01:33 PST which is equivalent to
        // Sun 07 Apr 02:33 PDT which is
        // Sun 07 Apr 09:33 GMT
        assertEquals(2002, 04, 07, "Sun", 9, 33, d);
        d = schedule.nextOccurrence(d, true);
        // 5th occurrence is
        // Mon 08 Apr 02:33 PDT which is
        // Mon 08 Apr 09:33 GMT (daylight savings has started)
        assertEquals(2002, 04, 8, "Mon", 9, 33, d);
        for (int i = 5; i < 206; i++) {
            d = schedule.nextOccurrence(d, true);
        }
        // 206th occurrence is
        // Sat 26 Oct 02:33 PDT which is
        // Sat 26 Oct 09:33 GMT
        assertEquals(2002, 10, 26, "Sat", 9, 33, d);
        d = schedule.nextOccurrence(d, true);
        // 207th occurrence occurs during the 'fall back',
        // don't care what time we fire as long as we only fire once
        // Sun 27 Oct 01:33 PDT which is equivalent to
        // Sun 27 Oct 02:33 PST which is
        // Sat 27 Oct 10:33 GMT
        assertEquals(toDate(2002, 10, 27, "Sun", 02, 33, tz), d);
        d = schedule.nextOccurrence(d, true);
        // 208th occurrence is
        // Mon 28 Oct 02:33 PST which is
        // Mon 28 Oct 10:33 GMT
        assertEquals(2002, 10, 28, "Mon", 10, 33, d);
        d = schedule.nextOccurrence(d, true);
        // 209th occurrence is
        // Tue 29 Oct 02:33 PST which is
        // Tue 29 Oct 10:33 GMT
        assertEquals(2002, 10, 29, "Tue", 10, 33, d);
    }
}

// End ScheduleTest.java

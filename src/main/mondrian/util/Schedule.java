/*
// $Id$
// Saffron preprocessor and data engine
// Copyright (C) 2002 Julian Hyde <julian.hyde@mail.com>
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Library General Public
// License as published by the Free Software Foundation; either
// version 2 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Library General Public License for more details.
//
// You should have received a copy of the GNU Library General Public
// License along with this library; if not, write to the
// Free Software Foundation, Inc., 59 Temple Place - Suite 330,
// Boston, MA  02111-1307, USA.
//
// See the COPYING file located in the top-level-directory of
// the archive of this library for complete text of license.
//
// jhyde, May 10, 2002
*/

package mondrian.util;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.sql.Time;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

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
 * @version $Id$
 **/
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
			(1 << Calendar.MONDAY) |
			(1 << Calendar.TUESDAY) |
			(1 << Calendar.WEDNESDAY) |
			(1 << Calendar.THURSDAY) |
			(1 << Calendar.FRIDAY) |
			(1 << Calendar.SATURDAY) |
			(1 << Calendar.SUNDAY);
	static final int allDaysOfMonthBitmap = 0xefffFffe | // bits 1..31
			(1 << LAST_DAY_OF_MONTH);
	static final int allWeeksOfMonthBitmap = 0x0000003e | // bits 1..5
			(1 << LAST_WEEK_OF_MONTH);

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
			Date end) {
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
			Date begin, Date end, TimeZone tz, Time timeOfDay, int period) {
		DateSchedule dateSchedule = new DailyDateSchedule(
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
	 * @param daysOfMonthBitmap a bitmap of day values, for example
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
			Date begin, Date end, TimeZone tz,
			Time timeOfDay, int period, int daysOfWeekBitmap) {
		DateSchedule dateSchedule = new WeeklyDateSchedule(
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
			Date begin, Date end, TimeZone tz, Time timeOfDay, int period,
			int daysOfMonthBitmap) {
		DateSchedule dateSchedule = new MonthlyByDayDateSchedule(
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
	 * @param daysOfMonthBitmap a bitmap of day values, for example
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
			Date begin, Date end, TimeZone tz,
			Time timeOfDay, int period, int daysOfWeekBitmap,
			int weeksOfMonthBitmap) {
		DateSchedule dateSchedule = new MonthlyByWeekDateSchedule(
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
		if (after == null ||
				begin != null && begin.after(after)) {
			after = begin;
			strict = false;
		}
		if (after == null) {
			after = new Date(0);
		}
		Date next = nextOccurrence0(after, strict);
		// if there is an upper bound, and this is not STRICTLY before it,
		// there's no next occurrence
		if (next != null &&
				end != null &&
				!next.before(end)) {
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
			nextTime = timeSchedule.nextOccurrence(ScheduleUtil.midnightTime, false);
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


	/**
	 * Returns a JUnit test to test the Schedule class. This method is
	 * automatically recognized by JUnit test harnesses.
	 */
	public static Test suite() {
		TestSuite suite = new TestSuite();
		suite.addTestSuite(ScheduleTestCase.class);
		return suite;
	}

	/**
	 * JUnit regression suite for {@link Schedule}.
	 */
	public static class ScheduleTestCase extends TestCase {
		public static final Time time0827 = ScheduleUtil.createTime( 8,27,00);
		public static final Time time1600 = ScheduleUtil.createTime(16,00,0);
		public static final Time time0000 = ScheduleUtil.createTime(00,00,0);
		public static final Time time0233 = ScheduleUtil.createTime(02,33,0);
		public static final TimeZone gmtTz = TimeZone.getTimeZone("GMT");
		public static final TimeZone pstTz = TimeZone.getTimeZone("America/Los_Angeles"); // GMT-8
		public static final TimeZone jstTz = TimeZone.getTimeZone("Asia/Tokyo");
		public static final TimeZone sgtTz = TimeZone.getTimeZone("Asia/Singapore"); // GMT+8
		public static final int weekdays =
				(1 << Calendar.MONDAY) |
				(1 << Calendar.TUESDAY) |
				(1 << Calendar.WEDNESDAY) |
				(1 << Calendar.THURSDAY) |
				(1 << Calendar.FRIDAY);
		public static final String[] daysOfWeek = new String[] {
			null,"Sun","Mon","Tue","Wed","Thu","Fri","Sat"};

		public ScheduleTestCase(String name) {
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
				int year, int month, int day, String dow, int hour, int minute,
				Date actual) {
			assertEquals(toDate(year,month,day,dow,hour,minute), actual);
		}
		static void assertEquals(Calendar expected, Date actual) {
			if (expected == null || actual == null) {
				assertEquals((Object) expected, (Object) actual);
			} else {
				assertEquals(expected.getTime(), actual);
			}
		}
		static void assertScheduleCount(
				Schedule schedule, Date d, Date last, int expectedCount) {
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
				int year, int month, int day, String dow,
				int hour, int minute) {
			return toDate(year, month, day, dow, hour, minute, gmtTz);
		}
		static Date toDate(
				int year, int month, int day, String dow,
				int hour, int minute, TimeZone tz) {
			Calendar calendar = ScheduleUtil.createCalendar(
					year,month,day,hour,minute,0);
			calendar.setTimeZone(tz);
			assertEquals(
					daysOfWeek[calendar.get(Calendar.DAY_OF_WEEK)], dow);
			return calendar.getTime();
		}
		// --------------------------------------------------------------------
		// test cases

		public void testOnceTimeSchedule() {
			Calendar calendar0827 = ScheduleUtil.createCalendar(time0827);
			OnceTimeSchedule onceTimeSchedule = new OnceTimeSchedule(
					calendar0827);
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
			int period = 1;
			Schedule schedule = Schedule.createOnce(
					toDate(2002,04,23,"Tue", 8,27), gmtTz);
			Date d;
			d = schedule.nextOccurrence(null,false);
			assertEquals(2002,04,23,"Tue", 8,27, d);
			d = schedule.nextOccurrence(toDate(2002,04,23,"Tue", 8,27), false);
			assertEquals(2002,04,23,"Tue", 8,27, d);
			d = schedule.nextOccurrence(toDate(2002,04,23,"Tue", 8,27), true);
			assertEquals(null, d);
			d = schedule.nextOccurrence(toDate(2002,06,03,"Mon",16,00),false);
			assertEquals(null, d);
			d = schedule.nextOccurrence(toDate(2002,04,20,"Sat",23,00), true);
			assertEquals(2002,04,23,"Tue", 8,27, d);
			d = schedule.nextOccurrence(toDate(2002,04,20,"Sat",23,00), false);
			assertEquals(2002,04,23,"Tue", 8,27, d);
		}
		public void testDaily() {
			int period = 1;
			Schedule schedule = Schedule.createDaily(
					toDate(2002,04,20,"Sat", 8,27), toDate(2002,06,03,"Mon", 8,27),
					gmtTz, time0827, period);
			Date d;
			d = schedule.nextOccurrence(null, false);
			assertEquals(2002,4,20,"Sat",8,27,d);
			d = schedule.nextOccurrence(toDate(2002,04,20,"Sat", 8,27),false);
			assertEquals(2002,4,20,"Sat",8,27, d);
			d = schedule.nextOccurrence(toDate(2002,04,20,"Sat",23,00), false);
			assertEquals(2002,04,21,"Sun", 8,27, d);
			d = schedule.nextOccurrence(toDate(2002,06,03,"Mon", 8,27), false);
			assertEquals(null, d); // upper-bound is closed
			d = schedule.nextOccurrence(toDate(2002,06,03,"Mon",16,00), false);
			assertEquals(null, d);
			d = schedule.nextOccurrence(toDate(2002,06,04,"Tue", 8,27), false);
			assertEquals(null, d);
		}
		public void testDailyNoUpperLimit() {
			int period = 1;
			Schedule schedule = Schedule.createDaily(
					toDate(2002,4,20,"Sat",8,27), null, gmtTz, time0827,
					period);
			Date d = schedule.nextOccurrence(null,false);
			assertEquals(2002,4,20,"Sat",8,27, d);
			d = schedule.nextOccurrence(toDate(2002,06,03,"Mon",16,00), false);
			assertEquals(2002,06,04,"Tue", 8,27, d);
		}
		public void testDailyPeriodic() {
			int period = 10;
			Schedule schedule = Schedule.createDaily(
					toDate(2002,4,20,"Sat",8,27), toDate(2002,06,03,"Mon", 8,27),
					gmtTz, time0827, period);
			Date d = schedule.nextOccurrence(null,false);
			assertEquals(2002,4,20,"Sat",8,27, d);
			d = schedule.nextOccurrence(toDate(2002,4,20,"Sat",8,27),true);
			assertEquals(2002,04,30,"Tue", 8,27, d);
		}
		public void testWeeklyEmptyBitmapFails() {
			boolean failed = false;
			try {
				int period = 1;
				Schedule schedule = Schedule.createWeekly(
						null, null, gmtTz, time0827, 1, 0);
			} catch (Throwable e) {
				failed = true;
			}
			assertTrue(failed);
		}
		public void testWeeklyBadBitmapFails() {
			boolean failed = false;
			try {
				int period = 1;
				Schedule schedule = Schedule.createWeekly(
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
					toDate(2002,4,20,"Sat",8,27), toDate(2002,06,05,"Wed",12,00),
					gmtTz, time0827, period, thuesday);
			Date d;
			d = schedule.nextOccurrence(null, false);
			assertEquals(2002,04,23,"Tue", 8,27, d);
			d = schedule.nextOccurrence(toDate(2002,04,23,"Tue", 8,27), false);
			assertEquals(2002,04,23,"Tue", 8,27, d);
			assertScheduleCount(schedule, d, toDate(2002,06,04,"Tue", 8,27), 12);
		}
		public void testMonthlyByDay() {
			int period = 1;
			int daysOfMonth = (1 << 12) | (1 << 21) | (1 << LAST_DAY_OF_MONTH);
			Schedule schedule = Schedule.createMonthlyByDay(
					toDate(2002,4,20,"Sat",8,27), toDate(2002,07,10,"Wed",12,00),
					gmtTz, time0827, period, daysOfMonth);
			Date d;
			d = schedule.nextOccurrence(null, false);
			assertEquals(2002,04,21,"Sun", 8,27, d);
			d = schedule.nextOccurrence(d, true);
			assertEquals(2002,04,30,"Tue", 8,27, d);
			d = schedule.nextOccurrence(d, false);
			assertEquals(2002,04,30,"Tue", 8,27, d);
			d = schedule.nextOccurrence(d, true);
			assertEquals(2002,05,12,"Sun", 8,27, d);
			d = schedule.nextOccurrence(d, false);
			assertEquals(2002,05,12,"Sun", 8,27, d);
			assertScheduleCount(schedule, d, toDate(2002,6,30,"Sun",8,27), 5);
		}
		public void testMonthlyByDayPeriodic() {
			int daysOfMonth = (1 << 12) | (1 << 21) | (1 << LAST_DAY_OF_MONTH);
			int period = 2;
			Schedule schedule = Schedule.createMonthlyByDay(
					toDate(2002,04,30,"Tue", 8,27),
					toDate(2002,7,10,"Wed",12,00),
					gmtTz, time0827, period, daysOfMonth);
			Date d;
			// strict=true means strictly greater than null (-infinity), not
			// strictly greater than the start time (apr30), so apr30 is
			// correct
			d = schedule.nextOccurrence(null, true);
			assertEquals(2002,04,30,"Tue", 8,27, d);
			d = schedule.nextOccurrence(d, false);
			assertEquals(2002,04,30,"Tue", 8,27, d);
			d = schedule.nextOccurrence(d, true);
			assertEquals(2002,06,12,"Wed", 8,27, d);
			d = schedule.nextOccurrence(d, false);
			assertEquals(2002,06,12,"Wed", 8,27, d);
			assertScheduleCount(schedule, d, toDate(2002,6,30,"Sun",8,27), 2);
		}
		public void testMonthlyByWeek() {
			int period = 3;
			int daysOfWeek = (1 << Calendar.THURSDAY) | (1 << Calendar.SUNDAY);
			int weeksOfMonth = (1 << 2) | (1 << Schedule.LAST_WEEK_OF_MONTH);
			Schedule schedule = Schedule.createMonthlyByWeek(
					toDate(2002,4,20,"Sat",8,27),
					toDate(2004,4,19,"Mon",12,00),
					gmtTz, time0827, period, daysOfWeek, weeksOfMonth);
			Date d;
			d = schedule.nextOccurrence(null, false);
			assertEquals(2002,04,25,"Thu",8,27, d);
			d = schedule.nextOccurrence(toDate(2002,04,23,"Tue", 8,27), false);
			assertEquals(2002,04,25,"Thu",8,27, d);
			d = schedule.nextOccurrence(d, true);
			assertEquals(2002,04,28,"Sun", 8,27, d);
			d = schedule.nextOccurrence(d, true);
			assertEquals(2002,7,11,"Thu", 8,27, d);
			assertScheduleCount(schedule, d, toDate(2004,4,11,"Sun",8,27), 29);
		}
		public void testTimeZone() {
			int period = 1;
			int daysOfWeek = (1 << Calendar.THURSDAY);
			int weeksOfMonth = (1 << Schedule.LAST_WEEK_OF_MONTH);
			Schedule schedule = Schedule.createMonthlyByWeek(
					toDate(2002,3,07,"Thu",14,00),
					toDate(2004,4,19,"Mon",12,00),
					jstTz, time0827, period, daysOfWeek, weeksOfMonth);
			Date d;
			d = schedule.nextOccurrence(null, true);
			// 1st occurrence is
			// Thu 28 Mar 08:27 JST, which is
			// Wed 27 Mar 23:27 GMT (9 hours difference) and
			// Wed 27 Mar 15:27 PST (a further 8 hours)
			assertEquals(2002,03,27,"Wed",23,27,d);
			d = schedule.nextOccurrence(d, true);
			// 2nd occurrence is
			// Thu 25 Apr 08:27 JST, which is
			// Wed 24 Apr 23:27 GMT (Japan does not have daylight savings)
			assertEquals(2002,04,24,"Wed",23,27,d);
			d = schedule.nextOccurrence(d, true);
			d = schedule.nextOccurrence(d, true);
			d = schedule.nextOccurrence(d, true);
			// 5th occurrence is
			// Thu 25 Jul 08:27 JST, which is
			// Wed 24 Jul 23:27 GMT
			assertEquals(2002,07,24,"Wed",23,27,d);
			d = schedule.nextOccurrence(d, true);
			d = schedule.nextOccurrence(d, true);
			d = schedule.nextOccurrence(d, true);
			// 8th occurrence is
			// Thu 31 Oct 08:27 JST, which is
			// Wed 30 Oct 23:27 GMT
			assertEquals(2002,10,30,"Wed",23,27,d);
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
			assertEquals(2003,12,24,"Wed",23,27,d);
		}
		public void testTimeZoneChange() {
			int period = 1;
			TimeZone tz = pstTz;
			Schedule schedule = Schedule.createDaily(
					toDate(2002,04,03,"Wed", 8,27,tz), null,
					tz, time0233, period);
			Date d;
			d = schedule.nextOccurrence(null, false);
			// 1st occurrence is
			// Thu 04 Apr 02:33 PST which is
			// Thu 04 Apr 10:33 GMT (no daylight savings yet)
			assertEquals(toDate(2002,04,04,"Thu",02,33,tz),d);
			d = schedule.nextOccurrence(d, true);
			d = schedule.nextOccurrence(d, true);
			// 3rd occurrence is
			// Sat 06 Apr 02:33 PST which is
			// Sat 06 Apr 10:33 GMT (still no daylight savings)
			assertEquals(2002,04,06,"Sat",10,33,d);
			d = schedule.nextOccurrence(d, true);
			// 4th occurrence occurs during the switch to daylight savings,
			// Sun 07 Apr 01:33 PST which is equivalent to
			// Sun 07 Apr 02:33 PDT which is
			// Sun 07 Apr 09:33 GMT
			assertEquals(2002,04,07,"Sun", 9,33,d);
			d = schedule.nextOccurrence(d, true);
			// 5th occurrence is
			// Mon 08 Apr 02:33 PDT which is
			// Mon 08 Apr 09:33 GMT (daylight savings has started)
			assertEquals(2002,04, 8,"Mon", 9,33,d);
			for (int i = 5; i < 206; i++) {
				d = schedule.nextOccurrence(d, true);
			}
			// 206th occurrence is
			// Sat 26 Oct 02:33 PDT which is
			// Sat 26 Oct 09:33 GMT
			assertEquals(2002,10,26,"Sat", 9,33,d);
			d = schedule.nextOccurrence(d, true);
			// 207th occurrence occurs during the 'fall back',
			// don't care what time we fire as long as we only fire once
			// Sun 27 Oct 01:33 PDT which is equivalent to
			// Sun 27 Oct 02:33 PST which is
			// Sat 27 Oct 10:33 GMT
			assertEquals(toDate(2002,10,27,"Sun",02,33,tz),d);
			d = schedule.nextOccurrence(d, true);
			// 208th occurrence is
			// Mon 28 Oct 02:33 PST which is
			// Mon 28 Oct 10:33 GMT
			assertEquals(2002,10,28,"Mon",10,33,d);
			d = schedule.nextOccurrence(d, true);
			// 209th occurrence is
			// Tue 29 Oct 02:33 PST which is
			// Tue 29 Oct 10:33 GMT
			assertEquals(2002,10,29,"Tue",10,33,d);
		}
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
				(daysOfWeekBitmap & Schedule.allDaysOfWeekBitmap) == daysOfWeekBitmap,
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
				"weekly date schedule is looping -- maybe the " +
				"bitmap is empty: " + daysOfWeekBitmap);
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
			Calendar begin, int period, int daysOfMonthBitmap) {
		this.period = period;
		ScheduleUtil.assertTrue(period > 0, "period must be positive");
		this.beginMonth = begin == null ? 0 : monthOrdinal(begin);
		this.daysOfMonthBitmap = daysOfMonthBitmap;
		ScheduleUtil.assertTrue(
				(daysOfMonthBitmap & Schedule.allDaysOfMonthBitmap) != 0,
				"monthly day schedule must have at least one day set");
		ScheduleUtil.assertTrue(
				(daysOfMonthBitmap & Schedule.allDaysOfMonthBitmap) ==
				daysOfMonthBitmap,
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
			if ((daysOfMonthBitmap & (1 << Schedule.LAST_DAY_OF_MONTH)) != 0 &&
					earliest.get(Calendar.DAY_OF_MONTH) == 1) {
				// They want us to fire on the last day of the month, and
				// now we're at the first day of the month, so we must have
				// been at the last. Backtrack and return it.
				earliest.add(Calendar.DATE, -1);
				return earliest;
			}
		}
		throw ScheduleUtil.newInternal(
				"monthly-by-day date schedule is looping -- maybe " +
				"the bitmap is empty: " + daysOfMonthBitmap);
	}

	private static int monthOrdinal(Calendar earliest) {
		return earliest.get(Calendar.YEAR) * 12 +
			earliest.get(Calendar.MONTH);
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
			Calendar begin, int period, int daysOfWeekBitmap,
			int weeksOfMonthBitmap) {
		this.period = period;
		ScheduleUtil.assertTrue(period > 0, "period must be positive");
		this.beginMonth = begin == null ? 0 : monthOrdinal(begin);
		this.daysOfWeekBitmap = daysOfWeekBitmap;
		ScheduleUtil.assertTrue(
				(daysOfWeekBitmap & Schedule.allDaysOfWeekBitmap) != 0,
				"weekly schedule must have at least one day set");
		ScheduleUtil.assertTrue(
				(daysOfWeekBitmap & Schedule.allDaysOfWeekBitmap) ==
				daysOfWeekBitmap,
				"weekly schedule has bad bits set: " + daysOfWeekBitmap);
		this.weeksOfMonthBitmap = weeksOfMonthBitmap;
		ScheduleUtil.assertTrue(
				(weeksOfMonthBitmap & Schedule.allWeeksOfMonthBitmap) != 0,
				"weeks of month schedule must have at least one week set");
		ScheduleUtil.assertTrue(
				(weeksOfMonthBitmap & Schedule.allWeeksOfMonthBitmap) ==
				weeksOfMonthBitmap,
				"week of month schedule has bad bits set: " +
				weeksOfMonthBitmap);
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
						!= 0) {
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
		return earliest.get(Calendar.YEAR) * 12 +
			earliest.get(Calendar.MONTH);
	}
}

/**
 * Utility functions for {@link Schedule} and supporting classes.
 */
class ScheduleUtil {
	static final Calendar epochDay = ScheduleUtil.createCalendar(new Date(0));
	static final Calendar midnightTime = ScheduleUtil.createTimeCalendar(0,0,0);

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
		return calendar.get(Calendar.HOUR_OF_DAY) == 0 &&
			calendar.get(Calendar.MINUTE) == 0 &&
			calendar.get(Calendar.SECOND) == 0 &&
			calendar.get(Calendar.MILLISECOND) == 0;
	}
	public static boolean isTime(Calendar calendar) {
		return calendar.get(Calendar.YEAR) ==
				ScheduleUtil.epochDay.get(Calendar.YEAR) &&
			calendar.get(Calendar.DAY_OF_YEAR) ==
				ScheduleUtil.epochDay.get(Calendar.DAY_OF_YEAR);
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
			int year, int month, int day, int hour, int minute, int second) {
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
			int hours, int minutes, int seconds) {
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
			int year, int month, int dayOfMonth) {
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
                (1000 *
					(60 *
						(60 * calendar.get(Calendar.HOUR_OF_DAY) +
							calendar.get(Calendar.MINUTE)) +
					calendar.get(Calendar.SECOND)) +
				calendar.get(Calendar.MILLISECOND)));
	}
}

// End Schedule.java

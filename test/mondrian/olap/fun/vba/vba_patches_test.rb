# This software is subject to the terms of the Eclipse Public License v1.0
# Agreement, available at the following URL:
# http://www.eclipse.org/legal/epl-v10.html.
# You must accept the terms of that agreement to use this software.
#
# Copyright (C) 2026 eazyBI
# All Rights Reserved.

# frozen_string_literal: true

require_relative "../../../../test_helper"

# Tests for local patches to mondrian.olap.fun.vba.Vba.
#
# Most VBA date functions had their signatures widened from (Date) to
# (Object) so that calculated members — which Mondrian's validator
# always types as Numeric regardless of runtime return type — can be
# passed as date arguments. Runtime coercion is done by a shared
# castToDate helper.
#
# Tests use a behavior-preservation strategy: for each patched function,
# run the same expression with (a) a stock Date literal and (b) a
# calc-member that evaluates to the same date, and assert the two paths
# return the same value. This isolates the patch's invariant ("Object
# path works the same as Date path") from any pre-existing Mondrian
# quirks in the underlying math (e.g. DateDiff sign inversion on
# non-"d" intervals is a stock bug, unrelated to this patch).
describe "Vba patches" do
  DATE_LITERAL = 'DateSerial(2025, 9, 30)'
  NULL_EXPRESSION = 'CASE WHEN 1=2 THEN Now() END'
  NON_DATE_EXPRESSION = '123'
  INVALID_TYPE_MESSAGE = /must be of type Date/

  before(:all) do
    create_olap_connection
  end

  after(:all) do
    @olap&.close
  end

  def execute_result(mdx)
    @olap.execute(mdx).values.first
  end

  def execute_on_date_measure(date_formula, result_formula)
    execute_result <<~MDX
      WITH
        MEMBER [Measures].[Date] AS #{date_formula}
        MEMBER [Measures].[Result] AS #{result_formula}
      SELECT {[Measures].[Result]} ON COLUMNS FROM [Sales]
    MDX
  end

  # Run `expression_template` (which contains `<Date>` placeholders) twice:
  # once with the placeholders replaced by a Date literal, once with a
  # calc-member that evaluates to the same date. Assert the two results
  # are equal. This asserts the Object-widening patch is
  # behavior-preserving without committing to any specific expected value.
  def assert_behavior_preserved(expression_template, date_literal = DATE_LITERAL)
    date_form = expression_template.gsub('<Date>', date_literal)
    calc_form = expression_template.gsub('<Date>', '[Measures].[Date]')

    baseline = execute_result(<<~MDX)
      WITH MEMBER [Measures].[Result] AS #{date_form}
      SELECT {[Measures].[Result]} ON COLUMNS FROM [Sales]
    MDX

    via_calc = execute_on_date_measure(date_literal, calc_form)

    assert_equal normalize(baseline), normalize(via_calc),
      "Object path must match Date path for: #{expression_template}"
  end

  # Java::JavaUtil::Date instances do not `==`-compare sensibly; compare
  # by milliseconds.
  def normalize(value)
    value.respond_to?(:getTime) ? value.getTime : value
  end

  def assert_null_date_returns_null(expression_template)
    calc_form = expression_template.gsub('<Date>', '[Measures].[Date]')
    result = execute_on_date_measure(NULL_EXPRESSION, calc_form)
    assert_nil result, "Null date argument should produce null: #{expression_template}"
  end

  def assert_non_date_raises(expression_template)
    calc_form = expression_template.gsub('<Date>', '[Measures].[Date]')
    result = execute_on_date_measure(NON_DATE_EXPRESSION, calc_form)
    assert_match INVALID_TYPE_MESSAGE, result.message,
      "Non-Date argument should raise: #{expression_template}"
  end

  describe "castToDate (direct Java-level tests)" do
    # castToDate is package-private so JavaFunDef.scan (which registers all
    # public static methods on Vba as MDX functions) doesn't expose this
    # internal helper.
    let(:cast_to_date_method) do
      method = Java::MondrianOlapFunVba::Vba.java_class
        .getDeclaredMethod("castToDate", java.lang.Object.java_class)
      method.setAccessible(true)
      method
    end

    def cast_to_date(arg)
      cast_to_date_method.invoke(nil, arg)
    rescue Java::JavaLangReflect::InvocationTargetException => e
      raise e.cause
    end

    it "passes through a java.util.Date argument" do
      date = Java::JavaUtil::Date.new(1234567890000)
      assert_equal date.getTime, cast_to_date(date).getTime
    end

    it "returns null for a null argument" do
      assert_nil cast_to_date(nil)
    end

    it "accepts a java.sql.Timestamp (subclass of Date)" do
      timestamp = Java::JavaSql::Timestamp.new(1234567890000)
      result = cast_to_date(timestamp)
      refute_nil result
      assert_equal timestamp.getTime, result.getTime
    end

    it "raises InvalidArgumentException for a String argument" do
      error = assert_raises(Java::MondrianOlap::InvalidArgumentException) do
        cast_to_date("not a date")
      end
      assert_match INVALID_TYPE_MESSAGE, error.message
    end

    it "raises InvalidArgumentException for an Integer argument" do
      error = assert_raises(Java::MondrianOlap::InvalidArgumentException) do
        cast_to_date(42)
      end
      assert_match INVALID_TYPE_MESSAGE, error.message
    end
  end

  describe "DateAdd with Object date" do
    # Variety of intervals and `number` shapes (integer, fractional,
    # negative) — the `floor != number` branch in dateAdd has distinct
    # logic from the integer path, so fractional cases are important.
    [
      %q{DateAdd("d", 1, <Date>)},
      %q{DateAdd("d", -1, <Date>)},
      %q{DateAdd("d", 0.5, <Date>)},       # fractional: exercises floor != number branch
      %q{DateAdd("yyyy", 1, <Date>)},
      %q{DateAdd("yyyy", -5, <Date>)},
      %q{DateAdd("q", 2, <Date>)},
      %q{DateAdd("m", 1, <Date>)},
      %q{DateAdd("m", -13, <Date>)},       # crosses year boundary
      %q{DateAdd("ww", 3, <Date>)},
      %q{DateAdd("h", 12, <Date>)},
      %q{DateAdd("h", 36, <Date>)},        # crosses day boundary
      %q{DateAdd("n", 5, <Date>)},
      %q{DateAdd("s", 30, <Date>)},
    ].each do |template|
      it "calc-member path matches Date-literal path: #{template}" do
        assert_behavior_preserved template
      end
    end

    it "handles a leap-year Feb 29 cross-boundary add" do
      # Feb 29 2024 + 1 year = Feb 28 2025 (not Feb 29). This is a
      # VBA semantic case where the underlying Calendar math matters.
      assert_behavior_preserved %q{DateAdd("yyyy", 1, <Date>)},
        'DateSerial(2024, 2, 29)'
    end

    it "returns null when the date argument is null" do
      assert_null_date_returns_null %q{DateAdd("d", 1, <Date>)}
    end

    it "raises when the date argument is a non-Date, non-null value" do
      assert_non_date_raises %q{DateAdd("d", 1, <Date>)}
    end
  end

  describe "DateDiff with Object dates" do
    # 2-arg overload: DateDiff(interval, date1, date2).
    # Exercise multiple intervals. Note: only "d" has correct sign in
    # stock Mondrian; other intervals return date1-date2 instead of
    # date2-date1. We don't assert specific values — behavior-
    # preservation is the invariant.
    [
      %q{DateDiff("d", <Date>, <Date>)},
      %q{DateDiff("d", DateSerial(2025,1,1), <Date>)},    # asymmetric
      %q{DateDiff("d", <Date>, DateSerial(2025,12,31))},  # asymmetric
      %q{DateDiff("s", <Date>, <Date>)},
      %q{DateDiff("m", <Date>, DateAdd("m", 6, <Date>))},
      %q{DateDiff("yyyy", <Date>, DateAdd("yyyy", 3, <Date>))},
      %q{DateDiff("ww", <Date>, DateAdd("d", 30, <Date>))},
      %q{DateDiff("n", <Date>, DateAdd("h", 2, <Date>))},
    ].each do |template|
      it "2-arg: calc-member path matches Date-literal path: #{template}" do
        assert_behavior_preserved template
      end
    end

    # 3-arg overload: DateDiff(interval, date1, date2, firstDayOfWeek).
    [
      %q{DateDiff("d", <Date>, DateAdd("d", 30, <Date>), 1)},  # Sunday first
      %q{DateDiff("d", <Date>, DateAdd("d", 30, <Date>), 2)},  # Monday first
      %q{DateDiff("ww", <Date>, DateAdd("d", 30, <Date>), 2)},
    ].each do |template|
      it "3-arg: calc-member path matches Date-literal path: #{template}" do
        assert_behavior_preserved template
      end
    end

    # 4-arg overload: DateDiff(interval, date1, date2, firstDayOfWeek, firstWeekOfYear).
    [
      %q{DateDiff("d", <Date>, DateAdd("d", 30, <Date>), 1, 1)},
      %q{DateDiff("ww", <Date>, DateAdd("d", 30, <Date>), 2, 2)},
    ].each do |template|
      it "4-arg: calc-member path matches Date-literal path: #{template}" do
        assert_behavior_preserved template
      end
    end

    it "returns null when either date expression is null (2-arg)" do
      assert_null_date_returns_null %q{DateDiff("s", <Date>, <Date>)}
    end

    it "raises when the date expression is non-Date (2-arg)" do
      assert_non_date_raises %q{DateDiff("s", <Date>, <Date>)}
    end

    it "raises when the date expression is non-Date (3-arg)" do
      assert_non_date_raises %q{DateDiff("d", <Date>, <Date>, 2)}
    end

    it "raises when the date expression is non-Date (4-arg)" do
      assert_non_date_raises %q{DateDiff("d", <Date>, <Date>, 1, 1)}
    end
  end

  describe "DatePart with Object date" do
    # 2-arg overload: DatePart(interval, date). Each interval has its
    # own extraction path in the Calendar code — cover them all.
    [
      %q{DatePart("yyyy", <Date>)},
      %q{DatePart("q", <Date>)},
      %q{DatePart("m", <Date>)},
      %q{DatePart("y", <Date>)},    # day of year
      %q{DatePart("d", <Date>)},    # day of month
      %q{DatePart("w", <Date>)},    # weekday
      %q{DatePart("ww", <Date>)},   # week of year
      %q{DatePart("h", <Date>)},
      %q{DatePart("n", <Date>)},
      %q{DatePart("s", <Date>)},
    ].each do |template|
      it "2-arg: calc-member path matches Date-literal path: #{template}" do
        assert_behavior_preserved template
      end
    end

    # 3-arg overload: firstDayOfWeek only affects "w" and "ww".
    [
      %q{DatePart("w", <Date>, 1)},     # Sunday first
      %q{DatePart("w", <Date>, 2)},     # Monday first
      %q{DatePart("ww", <Date>, 1)},
      %q{DatePart("ww", <Date>, 2)},
      %q{DatePart("yyyy", <Date>, 2)},  # firstDayOfWeek ignored for non-week intervals
    ].each do |template|
      it "3-arg: calc-member path matches Date-literal path: #{template}" do
        assert_behavior_preserved template
      end
    end

    # 4-arg overload: firstWeekOfYear only affects "w" and "ww".
    [
      %q{DatePart("ww", <Date>, 1, 1)},  # vbFirstJan1
      %q{DatePart("ww", <Date>, 1, 2)},  # vbFirstFourDays
      %q{DatePart("ww", <Date>, 2, 2)},
      %q{DatePart("d", <Date>, 1, 1)},   # both args ignored for non-week intervals
    ].each do |template|
      it "4-arg: calc-member path matches Date-literal path: #{template}" do
        assert_behavior_preserved template
      end
    end

    it "returns null when the date expression is null (2-arg)" do
      assert_null_date_returns_null %q{DatePart("d", <Date>)}
    end

    it "raises when the date expression is non-Date (2-arg)" do
      assert_non_date_raises %q{DatePart("d", <Date>)}
    end

    it "raises when the date expression is non-Date (3-arg)" do
      assert_non_date_raises %q{DatePart("w", <Date>, 2)}
    end

    it "raises when the date expression is non-Date (4-arg)" do
      assert_non_date_raises %q{DatePart("ww", <Date>, 2, 2)}
    end
  end

  # Simple unary getters and value-normalizers: each reads a specific
  # Calendar field (or normalizes to date-only / time-only). All follow
  # the same Date→Object widening pattern.
  describe "Simple date getters with Object date" do
    [
      %q{Year(<Date>)},
      %q{Month(<Date>)},
      %q{Day(<Date>)},
      %q{Hour(<Date>)},
      %q{Minute(<Date>)},
      %q{Second(<Date>)},
      %q{Weekday(<Date>)},
      %q{Weekday(<Date>, 1)},       # 2-arg overload, Sunday first
      %q{Weekday(<Date>, 2)},       # 2-arg overload, Monday first
      %q{DateValue(<Date>)},        # returns Date
      %q{TimeValue(<Date>)},        # returns Date
    ].each do |template|
      it "calc-member path matches Date-literal path: #{template}" do
        assert_behavior_preserved template
      end
    end

    # Specific edge cases for dates that exercise the distinct extraction
    # paths (e.g. beginning/end of year for week, midnight for time
    # fields, DST transitions).
    [
      'DateSerial(2025, 1, 1)',     # first day of year, Wednesday
      'DateSerial(2024, 12, 31)',   # last day of year, Tuesday
      'DateSerial(2024, 2, 29)',    # leap day
      %q{DateAdd("s", 23*60*60 + 59*60 + 58, DateSerial(2025, 6, 15))},  # near-midnight
    ].each do |specific_date|
      it "extracts Year/Month/Day for #{specific_date}" do
        %w(Year Month Day).each do |fn|
          assert_behavior_preserved "#{fn}(<Date>)", specific_date
        end
      end

      it "extracts Hour/Minute/Second for #{specific_date}" do
        %w(Hour Minute Second).each do |fn|
          assert_behavior_preserved "#{fn}(<Date>)", specific_date
        end
      end
    end

    it "returns null when the date argument is null (each getter)" do
      %w(Year Month Day Hour Minute Second Weekday DateValue TimeValue).each do |fn|
        assert_null_date_returns_null "#{fn}(<Date>)"
      end
    end

    it "returns null when the date argument is null (Weekday 2-arg)" do
      assert_null_date_returns_null %q{Weekday(<Date>, 2)}
    end

    it "raises when the date argument is non-Date (each getter)" do
      %w(Year Month Day Hour Minute Second Weekday DateValue TimeValue).each do |fn|
        assert_non_date_raises "#{fn}(<Date>)"
      end
    end

    it "raises when the date argument is non-Date (Weekday 2-arg)" do
      assert_non_date_raises %q{Weekday(<Date>, 2)}
    end
  end

  # Int(x) should return floor(x), not trunc(x). Stock Mondrian was
  # buggy for -1 < x < 0: truncation gives 0, but floor is -1.
  # https://jira.pentaho.com/browse/MONDRIAN-2730
  describe "Int MONDRIAN-2730 floor-for-small-negatives" do
    def int_of(number_expr)
      execute_result <<~MDX
        WITH MEMBER [Measures].[Result] AS Int(#{number_expr})
        SELECT {[Measures].[Result]} ON COLUMNS FROM [Sales]
      MDX
    end

    # Cases in the patched range (−1 < x < 0) — these were broken in stock.
    it "returns -1 for -0.5 (was 0 in stock)" do
      assert_equal(-1, int_of('-0.5'))
    end

    it "returns -1 for -0.001 (was 0 in stock)" do
      assert_equal(-1, int_of('-0.001'))
    end

    it "returns -1 for -0.9999 (was 0 in stock)" do
      assert_equal(-1, int_of('-0.9999'))
    end

    # Regression cases — behavior must be unchanged by the patch.
    it "returns 0 for 0" do
      assert_equal 0, int_of('0')
    end

    it "returns 0 for 0.5 (positive, truncates to 0)" do
      assert_equal 0, int_of('0.5')
    end

    it "returns 2 for 2.7 (positive)" do
      assert_equal 2, int_of('2.7')
    end

    it "returns -1 for exact -1.0" do
      assert_equal(-1, int_of('-1.0'))
    end

    it "returns -3 for -2.7 (standard negative case)" do
      assert_equal(-3, int_of('-2.7'))
    end

    it "returns -1 for exact -1 (no fractional part)" do
      assert_equal(-1, int_of('-1'))
    end
  end
end

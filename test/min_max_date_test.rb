# frozen_string_literal: true

require_relative "test_helper"
require 'date'

describe "Min and Max with date expressions" do
  before(:all) do
    create_olap_connection
  end

  # Use CASE to assign different dates to USA state children
  def date_measure_expression
    "CASE [Customers].CurrentMember" \
    " WHEN [Customers].[USA].[CA] THEN DateSerial(2020, 1, 15)" \
    " WHEN [Customers].[USA].[OR] THEN DateSerial(2020, 6, 15)" \
    " WHEN [Customers].[USA].[WA] THEN DateSerial(2020, 12, 15)" \
    " END"
  end

  it "should return the latest date from Max" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[Max Date]').as(
        "Max([Customers].[USA].Children, [Measures].[Test Date])"
      ).
      columns('[Measures].[Max Date]').execute
    assert_kind_of java.util.Date, result.values[0]
    # WA = Dec 2020, the latest date
    assert_equal Date.new(2020, 12, 15), Date.parse(result.values[0].to_s)
  end

  it "should return the earliest date from Min" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[Min Date]').as(
        "Min([Customers].[USA].Children, [Measures].[Test Date])"
      ).
      columns('[Measures].[Min Date]').execute
    assert_kind_of java.util.Date, result.values[0]
    # CA = Jan 2020, the earliest date
    assert_equal Date.new(2020, 1, 15), Date.parse(result.values[0].to_s)
  end

  it "should return the latest date from Max with Filter" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[Max Date]').as(
        "Max(Filter([Customers].[USA].Children, [Measures].[Unit Sales] > 0), [Measures].[Test Date])"
      ).
      columns('[Measures].[Max Date]').execute
    assert_kind_of java.util.Date, result.values[0]
    assert_equal Date.new(2020, 12, 15), Date.parse(result.values[0].to_s)
  end

  it "should return the earliest date from Min with Filter" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[Min Date]').as(
        "Min(Filter([Customers].[USA].Children, [Measures].[Unit Sales] > 0), [Measures].[Test Date])"
      ).
      columns('[Measures].[Min Date]').execute
    assert_kind_of java.util.Date, result.values[0]
    assert_equal Date.new(2020, 1, 15), Date.parse(result.values[0].to_s)
  end

  it "should skip null date values and return extreme of non-null" do
    # Only CA and OR get dates, WA is null
    partial_date_expression =
      "CASE [Customers].CurrentMember" \
      " WHEN [Customers].[USA].[CA] THEN DateSerial(2020, 1, 15)" \
      " WHEN [Customers].[USA].[OR] THEN DateSerial(2020, 6, 15)" \
      " END"
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(partial_date_expression).
      with_member('[Measures].[Max Date]').as(
        "Max([Customers].[USA].Children, [Measures].[Test Date])"
      ).
      with_member('[Measures].[Min Date]').as(
        "Min([Customers].[USA].Children, [Measures].[Test Date])"
      ).
      columns('[Measures].[Max Date]', '[Measures].[Min Date]').execute
    assert_equal Date.new(2020, 6, 15), Date.parse(result.values[0].to_s)
    assert_equal Date.new(2020, 1, 15), Date.parse(result.values[1].to_s)
  end

  it "should return nil when all date values are null" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as("CASE WHEN 1=2 THEN DateSerial(2020, 1, 1) END").
      with_member('[Measures].[Max Date]').as(
        "Max([Customers].[USA].Children, [Measures].[Test Date])"
      ).
      with_member('[Measures].[Min Date]').as(
        "Min([Customers].[USA].Children, [Measures].[Test Date])"
      ).
      columns('[Measures].[Max Date]', '[Measures].[Min Date]').execute
    assert_nil result.values[0]
    assert_nil result.values[1]
  end

  it "should return date from single-element set" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[Max Date]').as(
        "Max({[Customers].[USA].[OR]}, [Measures].[Test Date])"
      ).
      with_member('[Measures].[Min Date]').as(
        "Min({[Customers].[USA].[OR]}, [Measures].[Test Date])"
      ).
      columns('[Measures].[Max Date]', '[Measures].[Min Date]').execute
    assert_equal Date.new(2020, 6, 15), Date.parse(result.values[0].to_s)
    assert_equal Date.new(2020, 6, 15), Date.parse(result.values[1].to_s)
  end

  it "should return nil from Max and Min with empty set" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as("DateSerial(2020, 1, 15)").
      with_member('[Measures].[Max Date]').as(
        "Max(Filter([Customers].[USA].Children, 1=2), [Measures].[Test Date])"
      ).
      with_member('[Measures].[Min Date]').as(
        "Min(Filter([Customers].[USA].Children, 1=2), [Measures].[Test Date])"
      ).
      columns('[Measures].[Max Date]', '[Measures].[Min Date]').execute
    assert_nil result.values[0]
    assert_nil result.values[1]
  end

  it "should not leak format from Filter predicate when numeric value expression has no format member" do
    result = @olap.from('Sales').
      with_member('[Measures].[Formatted]').as('[Measures].[Unit Sales]', format_string: '$#,##0.0000').
      with_member('[Measures].[max result]').as(
        "Max(Filter([Customers].[USA].Children, [Measures].[Formatted] > 0), 42.5)"
      ).
      with_member('[Measures].[min result]').as(
        "Min(Filter([Customers].[USA].Children, [Measures].[Formatted] > 0), 42.5)"
      ).
      columns('[Measures].[max result]', '[Measures].[min result]').execute
    # The value expression is a literal — no format-bearing member to inherit.
    # FormatAwareFunDef explicitly picked arg 1 for format inference; when no
    # format is found there, the result must use default formatting rather than
    # falling back to the Filter predicate.
    # With the leak: "$42.5000" (picked up $#,##0.0000 from [Formatted] in Filter).
    # Correct: default numeric format (#,##0) rounds 42.5 to "43".
    assert_equal '43', result.formatted_values[0]
    assert_equal '43', result.formatted_values[1]
  end

  it "should not leak format from Filter predicate when date value expression has no format member" do
    # MDX has no date literal syntax; DateSerial(...) is the closest equivalent
    # and, like a numeric literal, contains no MemberExpr for FormatFinder.
    result = @olap.from('Sales').
      with_member('[Measures].[Formatted]').as('[Measures].[Unit Sales]', format_string: '$#,##0.0000').
      with_member('[Measures].[max result]').as(
        "Max(Filter([Customers].[USA].Children, [Measures].[Formatted] > 0), DateSerial(2020, 6, 15))"
      ).
      with_member('[Measures].[min result]').as(
        "Min(Filter([Customers].[USA].Children, [Measures].[Formatted] > 0), DateSerial(2020, 6, 15))"
      ).
      columns('[Measures].[max result]', '[Measures].[min result]').execute
    # Underlying value is a Date — preserved regardless of format inference.
    assert_kind_of java.util.Date, result.values[0]
    assert_kind_of java.util.Date, result.values[1]
    # With the leak: "$43,998.0000". Correct: default numeric format renders
    # the Date's serial number as "43,998". The calculated member is statically
    # typed as Numeric, so without an explicit date format_string the default
    # numeric format is applied — this is pre-existing Mondrian behavior.
    assert_equal '43,998', result.formatted_values[0]
    assert_equal '43,998', result.formatted_values[1]
  end

  it "should inherit date format from value expression, not from Filter condition" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression, format_string: 'dd.mm.yyyy').
      with_member('[Measures].[Max Date]').as(
        "Max(Filter([Customers].[USA].Children, [Measures].[Unit Sales] > 0), [Measures].[Test Date])"
      ).
      with_member('[Measures].[Min Date]').as(
        "Min(Filter([Customers].[USA].Children, [Measures].[Unit Sales] > 0), [Measures].[Test Date])"
      ).
      columns('[Measures].[Max Date]', '[Measures].[Min Date]').execute
    # Should be formatted as date (from Test Date's format), not as number (from Unit Sales' format)
    assert_equal '15.12.2020', result.formatted_values[0]
    assert_equal '15.01.2020', result.formatted_values[1]
  end

  it "should still work with numeric expressions" do
    result = @olap.from('Sales').
      with_member('[Measures].[Max Sales]').as(
        "Max([Customers].[USA].Children, [Measures].[Unit Sales])"
      ).
      with_member('[Measures].[Min Sales]').as(
        "Min([Customers].[USA].Children, [Measures].[Unit Sales])"
      ).
      columns('[Measures].[Max Sales]', '[Measures].[Min Sales]').execute
    assert_kind_of Numeric, result.values[0]
    assert_kind_of Numeric, result.values[1]
    assert_operator result.values[0], :>, result.values[1]
  end

  it "should work with arithmetic numeric expressions" do
    result = @olap.from('Sales').
      with_member('[Measures].[Max Sales]').as(
        "Max([Customers].[USA].Children, [Measures].[Unit Sales] * 2)"
      ).
      columns('[Measures].[Max Sales]').execute
    assert_kind_of Numeric, result.values[0]

    simple_max = @olap.from('Sales').
      with_member('[Measures].[Max Sales]').as(
        "Max([Customers].[USA].Children, [Measures].[Unit Sales])"
      ).
      columns('[Measures].[Max Sales]').execute.values[0]
    assert_equal simple_max * 2, result.values[0]
  end

  it "should work inside IIf with 2-arg numeric form" do
    result = @olap.from('Sales').
      with_member('[Measures].[result]').as(
        "IIf([Measures].[Unit Sales] > 0, " \
        "Min([Customers].[USA].Children, [Measures].[Unit Sales]), 0)"
      ).
      columns('[Measures].[result]').execute
    assert_kind_of Numeric, result.values[0]
  end

  it "should work inside IIf with 1-arg numeric form" do
    result = @olap.from('Sales').
      with_member('[Measures].[result]').as(
        "IIf([Measures].[Unit Sales] > 0, " \
        "Max([Customers].[USA].Children), 0)"
      ).
      columns('[Measures].[result]').execute
    assert_kind_of Numeric, result.values[0]
  end

  it "should work inside IIf with 2-arg numeric form in both branches" do
    result = @olap.from('Sales').
      with_member('[Measures].[result]').as(
        "IIf([Measures].[Unit Sales] > 0, " \
        "Min([Customers].[USA].Children, [Measures].[Unit Sales]), " \
        "Max([Customers].[USA].Children, [Measures].[Unit Sales]))"
      ).
      columns('[Measures].[result]').execute
    assert_kind_of Numeric, result.values[0]
  end

  it "should return correct numeric values for 1-arg form over real set" do
    result = @olap.from('Sales').
      with_member('[Measures].[Max Sales]').as(
        "Max([Customers].[USA].Children)"
      ).
      with_member('[Measures].[Min Sales]').as(
        "Min([Customers].[USA].Children)"
      ).
      columns('[Measures].[Max Sales]', '[Measures].[Min Sales]', '[Measures].[Unit Sales]').
      rows('[Customers].[USA]').execute
    max_val = result.values[0][0]
    min_val = result.values[0][1]
    total = result.values[0][2]
    assert_kind_of Numeric, max_val
    assert_kind_of Numeric, min_val
    assert_operator max_val, :>, min_val
    assert_operator max_val, :<=, total
  end

  it "should return correct numeric values with Filter" do
    result = @olap.from('Sales').
      with_member('[Measures].[Max Sales]').as(
        "Max(Filter([Customers].[USA].Children, [Measures].[Unit Sales] > 0), [Measures].[Unit Sales])"
      ).
      with_member('[Measures].[Min Sales]').as(
        "Min(Filter([Customers].[USA].Children, [Measures].[Unit Sales] > 0), [Measures].[Unit Sales])"
      ).
      columns('[Measures].[Max Sales]', '[Measures].[Min Sales]').execute
    assert_kind_of Numeric, result.values[0]
    assert_kind_of Numeric, result.values[1]
    assert_operator result.values[0], :>, result.values[1]
  end

  it "should inherit numeric format from value expression when Filter is involved" do
    result = @olap.from('Sales').
      with_member('[Measures].[custom]').as('[Measures].[Store Sales]', format_string: '#,##0.0000').
      with_member('[Measures].[Max Custom]').as(
        "Max(Filter([Customers].[USA].Children, [Measures].[Unit Sales] > 0), [Measures].[custom])"
      ).
      columns('[Measures].[Max Custom]').execute
    # Should inherit #,##0.0000 format from [custom], not default format from [Unit Sales] in Filter
    assert_match(/\A[\d,]+\.\d{4}\z/, result.formatted_values[0])
  end

  it "should work inside IIf with date Min/Max and null fallback" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[result]').as(
        "IIf([Measures].[Unit Sales] > 0, " \
        "Min([Customers].[USA].Children, [Measures].[Test Date]), NULL)"
      ).
      columns('[Measures].[result]').execute
    assert_kind_of java.util.Date, result.values[0]
    assert_equal Date.new(2020, 1, 15), Date.parse(result.values[0].to_s)
  end

  it "should support nested Min inside Max with dates" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[result]').as(
        "Max([Customers].[USA].Children, " \
        "Min({[Customers].CurrentMember}, [Measures].[Test Date]))"
      ).
      columns('[Measures].[result]').execute
    assert_kind_of java.util.Date, result.values[0]
    # Each inner Min over a single member returns that member's date,
    # then outer Max picks the latest — same as plain Max
    assert_equal Date.new(2020, 12, 15), Date.parse(result.values[0].to_s)
  end

  it "should return correct dates across multiple rows with 2-arg form" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[Max Date]').as(
        "Max({[Customers].CurrentMember}, [Measures].[Test Date])"
      ).
      with_member('[Measures].[Min Date]').as(
        "Min({[Customers].CurrentMember}, [Measures].[Test Date])"
      ).
      columns('[Measures].[Max Date]', '[Measures].[Min Date]').
      rows('[Customers].[USA].Children').execute
    # Each row evaluates Max/Min over a single-member set, so both return that member's date.
    # Verifies evaluator savepoint/restore works correctly across row iterations.
    result.values.each do |row|
      assert_kind_of java.util.Date, row[0]
      assert_kind_of java.util.Date, row[1]
      assert_equal row[0].to_s, row[1].to_s
    end
    dates = result.values.map { |row| Date.parse(row[0].to_s) }.sort
    assert_equal [Date.new(2020, 1, 15), Date.new(2020, 6, 15), Date.new(2020, 12, 15)], dates
  end

  it "should support 1-arg form with date measure as set member" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[max result]').as(
        "Max({[Measures].[Test Date]})"
      ).
      with_member('[Measures].[min result]').as(
        "Min({[Measures].[Test Date]})"
      ).
      columns('[Measures].[max result]', '[Measures].[min result]').
      rows('[Customers].[USA].Children').execute
    # Each row gets Max/Min of a single-element set — both return the same date
    result.values.each do |row|
      row.each do |v|
        assert_kind_of java.util.Date, v
      end
      assert_equal row[0].to_s, row[1].to_s
    end
    # CA = Jan, OR = Jun, WA = Dec
    dates = result.values.map { |row| Date.parse(row[0].to_s) }
    assert_includes dates, Date.new(2020, 1, 15)
    assert_includes dates, Date.new(2020, 12, 15)
  end

  it "should inherit format string for 1-arg form from the measure in the set" do
    result = @olap.from('Sales').
      with_member('[Measures].[custom]').as('[Measures].[Store Sales]', format_string: '#,##0.0000').
      with_member('[Measures].[result]').as(
        "Max({[Measures].[custom]})"
      ).
      columns('[Measures].[result]').
      rows('[Customers].[USA].[CA]').execute
    # Should inherit #,##0.0000 format from the measure in the set
    assert_match(/\A[\d,]+\.\d{4}\z/, result.formatted_values[0][0])
  end

  it "should return date value for 1-arg form and inherit date format from source measure" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression, format_string: 'yyyy-mm-dd').
      with_member('[Measures].[result]').as(
        "Max({[Measures].[Test Date]})"
      ).
      columns('[Measures].[result]').
      rows('[Customers].[USA].[WA]').execute
    assert_kind_of java.util.Date, result.values[0][0]
    # Output format should match the input: date in, date out.
    assert_match(/\A\d{4}-\d{2}-\d{2}\z/, result.formatted_values[0][0])
  end

  it "should not bleed format strings between multiple date measures" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression, format_string: 'dd.mm.yyyy').
      with_member('[Measures].[Max 1]').as(
        "Max([Customers].[USA].Children, [Measures].[Test Date])"
      ).
      with_member('[Measures].[Test Date 2]').as(date_measure_expression, format_string: 'yyyy-mm-dd').
      with_member('[Measures].[Min 2]').as(
        "Min([Customers].[USA].Children, [Measures].[Test Date 2])"
      ).
      columns('[Measures].[Max 1]', '[Measures].[Min 2]').execute
    # Max 1 should use dd.mm.yyyy from Test Date, Min 2 should use yyyy-mm-dd from Test Date 2
    assert_equal '15.12.2020', result.formatted_values[0]
    assert_equal '2020-01-15', result.formatted_values[1]
  end

  it "should work inside CoalesceEmpty" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[max result]').as(
        "CoalesceEmpty(" \
        "Max(Filter([Customers].[USA].Children, 1=2), [Measures].[Test Date]), " \
        "DateSerial(1970, 1, 1))"
      ).
      with_member('[Measures].[min result]').as(
        "CoalesceEmpty(" \
        "Min(Filter([Customers].[USA].Children, 1=2), [Measures].[Test Date]), " \
        "DateSerial(1970, 1, 1))"
      ).
      columns('[Measures].[max result]', '[Measures].[min result]').execute
    # Max/Min over empty set returns null, CoalesceEmpty falls back to DateSerial
    assert_kind_of java.util.Date, result.values[0]
    assert_kind_of java.util.Date, result.values[1]
    assert_equal Date.new(1970, 1, 1), Date.parse(result.values[0].to_s)
    assert_equal Date.new(1970, 1, 1), Date.parse(result.values[1].to_s)
  end

  it "should work with CrossJoin producing tuples" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[max result]').as(
        "Max(CrossJoin({[Customers].[USA].[CA], [Customers].[USA].[WA]}, " \
        "{[Gender].[All Gender]}), [Measures].[Test Date])"
      ).
      with_member('[Measures].[min result]').as(
        "Min(CrossJoin({[Customers].[USA].[CA], [Customers].[USA].[WA]}, " \
        "{[Gender].[All Gender]}), [Measures].[Test Date])"
      ).
      columns('[Measures].[max result]', '[Measures].[min result]').execute
    assert_kind_of java.util.Date, result.values[0]
    assert_kind_of java.util.Date, result.values[1]
    assert_equal Date.new(2020, 12, 15), Date.parse(result.values[0].to_s)
    assert_equal Date.new(2020, 1, 15), Date.parse(result.values[1].to_s)
  end

  it "should work as argument to VBA date functions" do
    # VBA functions like DateDiff call compileDateTime(), which casts to
    # DateTimeCalc. Without AbstractDateTimeCalc this would ClassCastException.
    # Use DateSerial directly (statically typed DateTime) so the resolver
    # picks the DateTime signature for Min/Max.
    result = @olap.from('Sales').
      with_member('[Measures].[result]').as(
        "DateDiff(\"d\", " \
        "Min([Customers].[USA].Children, DateSerial(2020, 1, 15)), " \
        "Max([Customers].[USA].Children, DateSerial(2020, 12, 15)))"
      ).
      columns('[Measures].[result]').execute
    # All members evaluate to the same date in each Min/Max, so diff = 335 days
    assert_kind_of Numeric, result.values[0]
    assert_equal 335, result.values[0].to_i
  end

  # Mondrian limitation: calculated members are always statically typed as
  # Numeric regardless of their runtime return type. The validator resolves
  # types bottom-up, so outer functions see <Numeric Expression> even when
  # the value is a Date at runtime. Core Mondrian operators (IIf,
  # comparison) lack DateTime signatures and so cannot consume such a
  # result. Vba date functions, however, accept Object and coerce at
  # runtime via Vba.castToDate — see the positive tests further below.

  it "should not support IIf with date branches on both sides (no DateTime IIf signature)" do
    assert_raises(Mondrian::OLAP::Error) do
      @olap.from('Sales').
        with_member('[Measures].[Test Date]').as(date_measure_expression).
        with_member('[Measures].[result]').as(
          "IIf([Measures].[Unit Sales] > 0, " \
          "Max([Customers].[USA].Children, [Measures].[Test Date]), " \
          "DateSerial(1970, 1, 1))"
        ).
        columns('[Measures].[result]').execute
    end
  end

  it "should not support comparison operators with date Min/Max result" do
    assert_raises(Mondrian::OLAP::Error) do
      @olap.from('Sales').
        with_member('[Measures].[Test Date]').as(date_measure_expression).
        with_member('[Measures].[result]').as(
          "Max([Customers].[USA].Children, [Measures].[Test Date]) > DateSerial(2020, 6, 1)"
        ).
        columns('[Measures].[result]').execute
    end
  end

  # Vba date functions take Object and coerce via Vba.castToDate, so a
  # Min/Max result that is statically typed Numeric but holds a Date at
  # runtime can be passed in. Coverage below pins down each Vba function
  # the patch widened.

  it "should compute DateDiff between Min and Max calc-member dates" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[result]').as(
        "DateDiff(\"d\", " \
        "Min([Customers].[USA].Children, [Measures].[Test Date]), " \
        "Max([Customers].[USA].Children, [Measures].[Test Date]))"
      ).
      columns('[Measures].[result]').execute
    # Min = Jan 15 2020, Max = Dec 15 2020 → 335 days.
    assert_kind_of Numeric, result.values[0]
    assert_equal 335, result.values[0].to_i
  end

  it "should compute DateAdd over Max calc-member date" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[result]').as(
        "DateAdd(\"d\", 30, " \
        "Max([Customers].[USA].Children, [Measures].[Test Date]))"
      ).
      columns('[Measures].[result]').execute
    # Max = Dec 15 2020, +30 days → Jan 14 2021.
    assert_kind_of java.util.Date, result.values[0]
    assert_equal Date.new(2021, 1, 14), Date.parse(result.values[0].to_s)
  end

  it "should compute DateAdd over Min calc-member date" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[result]').as(
        "DateAdd(\"m\", 2, " \
        "Min([Customers].[USA].Children, [Measures].[Test Date]))"
      ).
      columns('[Measures].[result]').execute
    # Min = Jan 15 2020, +2 months → Mar 15 2020.
    assert_kind_of java.util.Date, result.values[0]
    assert_equal Date.new(2020, 3, 15), Date.parse(result.values[0].to_s)
  end

  it "should extract Year, Month and Day from Min and Max calc-member dates" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[Min Year]').as(
        "Year(Min([Customers].[USA].Children, [Measures].[Test Date]))"
      ).
      with_member('[Measures].[Max Month]').as(
        "Month(Max([Customers].[USA].Children, [Measures].[Test Date]))"
      ).
      with_member('[Measures].[Min Day]').as(
        "Day(Min([Customers].[USA].Children, [Measures].[Test Date]))"
      ).
      columns('[Measures].[Min Year]', '[Measures].[Max Month]', '[Measures].[Min Day]').execute
    # Min = Jan 15 2020, Max = Dec 15 2020.
    assert_equal 2020, result.values[0].to_i
    assert_equal 12, result.values[1].to_i
    assert_equal 15, result.values[2].to_i
  end

  it "should compute Weekday from Min and Max calc-member dates" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[Min Weekday]').as(
        "Weekday(Min([Customers].[USA].Children, [Measures].[Test Date]))"
      ).
      with_member('[Measures].[Max Weekday Mon]').as(
        "Weekday(Max([Customers].[USA].Children, [Measures].[Test Date]), 2)"
      ).
      columns('[Measures].[Min Weekday]', '[Measures].[Max Weekday Mon]').execute
    # Min = Wed Jan 15 2020; default firstDayOfWeek=Sunday → 4.
    # Max = Tue Dec 15 2020; firstDayOfWeek=Monday(2) → 2.
    assert_equal 4, result.values[0].to_i
    assert_equal 2, result.values[1].to_i
  end

  it "should compute DatePart over Min and Max calc-member dates" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[Max Quarter]').as(
        "DatePart(\"q\", Max([Customers].[USA].Children, [Measures].[Test Date]))"
      ).
      with_member('[Measures].[Min Year]').as(
        "DatePart(\"yyyy\", Min([Customers].[USA].Children, [Measures].[Test Date]))"
      ).
      columns('[Measures].[Max Quarter]', '[Measures].[Min Year]').execute
    # Max = Dec 15 2020 → Q4; Min = Jan 15 2020 → year 2020.
    assert_equal 4, result.values[0].to_i
    assert_equal 2020, result.values[1].to_i
  end

  it "should accept Vba date functions inside Min/Max value expression" do
    # Vba date functions returning Date can be the 2-arg value expression
    # via the same Object-widened path. Resolver still picks Numeric for
    # the outer Min/Max (calc-member-shaped scalar), but the Date passes
    # through extremeValue's instanceof check.
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[Max Shifted]').as(
        "Max([Customers].[USA].Children, " \
        "DateAdd(\"d\", 30, [Measures].[Test Date]))"
      ).
      columns('[Measures].[Max Shifted]').execute
    # CA+30 = Feb 14 2020; OR+30 = Jul 15 2020; WA+30 = Jan 14 2021. Max = Jan 14 2021.
    assert_kind_of java.util.Date, result.values[0]
    assert_equal Date.new(2021, 1, 14), Date.parse(result.values[0].to_s)
  end

  it "should propagate null through Vba date functions when Min/Max is over an empty set" do
    # Min/Max over an empty set → null. Vba.castToDate returns null for
    # null input, so DateAdd returns null too (rather than raising).
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[result]').as(
        "DateAdd(\"d\", 30, " \
        "Max(Filter([Customers].[USA].Children, 1=2), [Measures].[Test Date]))"
      ).
      columns('[Measures].[result]').execute
    assert_nil result.values[0]
  end

end

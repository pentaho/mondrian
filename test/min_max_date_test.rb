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
      with_member('[Measures].[Min Sales]').as(
        "Min([Customers].[USA].Children, [Measures].[Unit Sales] * 2)"
      ).
      columns('[Measures].[Max Sales]', '[Measures].[Min Sales]').execute
    assert_kind_of Numeric, result.values[0]
    assert_kind_of Numeric, result.values[1]

    simple_result = @olap.from('Sales').
      with_member('[Measures].[Max Sales]').as(
        "Max([Customers].[USA].Children, [Measures].[Unit Sales])"
      ).
      with_member('[Measures].[Min Sales]').as(
        "Min([Customers].[USA].Children, [Measures].[Unit Sales])"
      ).
      columns('[Measures].[Max Sales]', '[Measures].[Min Sales]').execute
    assert_equal simple_result.values[0] * 2, result.values[0]
    assert_equal simple_result.values[1] * 2, result.values[1]
  end

  it "should work inside IIf with 2-arg numeric form" do
    result = @olap.from('Sales').
      with_member('[Measures].[min result]').as(
        "IIf([Measures].[Unit Sales] > 0, " \
        "Min([Customers].[USA].Children, [Measures].[Unit Sales]), 0)"
      ).
      with_member('[Measures].[max result]').as(
        "IIf([Measures].[Unit Sales] > 0, " \
        "Max([Customers].[USA].Children, [Measures].[Unit Sales]), 0)"
      ).
      columns('[Measures].[min result]', '[Measures].[max result]').execute
    assert_kind_of Numeric, result.values[0]
    assert_kind_of Numeric, result.values[1]
  end

  it "should work inside IIf with 1-arg numeric form" do
    result = @olap.from('Sales').
      with_member('[Measures].[max result]').as(
        "IIf([Measures].[Unit Sales] > 0, " \
        "Max([Customers].[USA].Children), 0)"
      ).
      with_member('[Measures].[min result]').as(
        "IIf([Measures].[Unit Sales] > 0, " \
        "Min([Customers].[USA].Children), 0)"
      ).
      columns('[Measures].[max result]', '[Measures].[min result]').execute
    assert_kind_of Numeric, result.values[0]
    assert_kind_of Numeric, result.values[1]
  end

  it "should work inside IIf with 2-arg numeric form in both branches" do
    result = @olap.from('Sales').
      with_member('[Measures].[min taken]').as(
        "IIf([Measures].[Unit Sales] > 0, " \
        "Min([Customers].[USA].Children, [Measures].[Unit Sales]), " \
        "Max([Customers].[USA].Children, [Measures].[Unit Sales]))"
      ).
      with_member('[Measures].[max taken]').as(
        "IIf([Measures].[Unit Sales] < 0, " \
        "Min([Customers].[USA].Children, [Measures].[Unit Sales]), " \
        "Max([Customers].[USA].Children, [Measures].[Unit Sales]))"
      ).
      columns('[Measures].[min taken]', '[Measures].[max taken]').execute
    assert_kind_of Numeric, result.values[0]
    assert_kind_of Numeric, result.values[1]
    # Inverted conditions take opposite branches; Max > Min confirms each
    # branch evaluated its own function rather than both hitting the same one.
    assert_operator result.values[1], :>, result.values[0]
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
      with_member('[Measures].[Min Custom]').as(
        "Min(Filter([Customers].[USA].Children, [Measures].[Unit Sales] > 0), [Measures].[custom])"
      ).
      columns('[Measures].[Max Custom]', '[Measures].[Min Custom]').execute
    # Should inherit #,##0.0000 format from [custom], not default format from [Unit Sales] in Filter
    assert_match(/\A[\d,]+\.\d{4}\z/, result.formatted_values[0])
    assert_match(/\A[\d,]+\.\d{4}\z/, result.formatted_values[1])
  end

  it "should work inside IIf with date Min/Max and null fallback" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[min result]').as(
        "IIf([Measures].[Unit Sales] > 0, " \
        "Min([Customers].[USA].Children, [Measures].[Test Date]), NULL)"
      ).
      with_member('[Measures].[max result]').as(
        "IIf([Measures].[Unit Sales] > 0, " \
        "Max([Customers].[USA].Children, [Measures].[Test Date]), NULL)"
      ).
      columns('[Measures].[min result]', '[Measures].[max result]').execute
    assert_kind_of java.util.Date, result.values[0]
    assert_kind_of java.util.Date, result.values[1]
    assert_equal Date.new(2020, 1, 15), Date.parse(result.values[0].to_s)
    assert_equal Date.new(2020, 12, 15), Date.parse(result.values[1].to_s)
  end

  it "should support nesting Min and Max with dates in both directions" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[max of min]').as(
        "Max([Customers].[USA].Children, " \
        "Min({[Customers].CurrentMember}, [Measures].[Test Date]))"
      ).
      with_member('[Measures].[min of max]').as(
        "Min([Customers].[USA].Children, " \
        "Max({[Customers].CurrentMember}, [Measures].[Test Date]))"
      ).
      columns('[Measures].[max of min]', '[Measures].[min of max]').execute
    # Each inner Min/Max over a single member returns that member's date,
    # then the outer aggregate picks its extreme — same as the plain form
    assert_kind_of java.util.Date, result.values[0]
    assert_kind_of java.util.Date, result.values[1]
    assert_equal Date.new(2020, 12, 15), Date.parse(result.values[0].to_s)
    assert_equal Date.new(2020, 1, 15), Date.parse(result.values[1].to_s)
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
      with_member('[Measures].[max result]').as(
        "Max({[Measures].[custom]})"
      ).
      with_member('[Measures].[min result]').as(
        "Min({[Measures].[custom]})"
      ).
      columns('[Measures].[max result]', '[Measures].[min result]').
      rows('[Customers].[USA].[CA]').execute
    # Should inherit #,##0.0000 format from the measure in the set
    assert_match(/\A[\d,]+\.\d{4}\z/, result.formatted_values[0][0])
    assert_match(/\A[\d,]+\.\d{4}\z/, result.formatted_values[0][1])
  end

  it "should return date value for 1-arg form and inherit date format from source measure" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression, format_string: 'yyyy-mm-dd').
      with_member('[Measures].[max result]').as(
        "Max({[Measures].[Test Date]})"
      ).
      with_member('[Measures].[min result]').as(
        "Min({[Measures].[Test Date]})"
      ).
      columns('[Measures].[max result]', '[Measures].[min result]').
      rows('[Customers].[USA].[WA]').execute
    assert_kind_of java.util.Date, result.values[0][0]
    assert_kind_of java.util.Date, result.values[0][1]
    # Output format should match the input: date in, date out.
    assert_match(/\A\d{4}-\d{2}-\d{2}\z/, result.formatted_values[0][0])
    assert_match(/\A\d{4}-\d{2}-\d{2}\z/, result.formatted_values[0][1])
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

  %w(Min Max).each do |min_or_max|
    it "should not support IIf with date #{min_or_max} branch (no DateTime IIf signature)" do
      assert_raises(Mondrian::OLAP::Error) do
        @olap.from('Sales').
          with_member('[Measures].[Test Date]').as(date_measure_expression).
          with_member('[Measures].[result]').as(
            "IIf([Measures].[Unit Sales] > 0, " \
            "#{min_or_max}([Customers].[USA].Children, [Measures].[Test Date]), " \
            "DateSerial(1970, 1, 1))"
          ).
          columns('[Measures].[result]').execute
      end
    end

    it "should not support comparison operators with date #{min_or_max} result" do
      assert_raises(Mondrian::OLAP::Error) do
        @olap.from('Sales').
          with_member('[Measures].[Test Date]').as(date_measure_expression).
          with_member('[Measures].[result]').as(
            "#{min_or_max}([Customers].[USA].Children, [Measures].[Test Date]) > DateSerial(2020, 6, 1)"
          ).
          columns('[Measures].[result]').execute
      end
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
      with_member('[Measures].[Min Shifted]').as(
        "Min([Customers].[USA].Children, " \
        "DateAdd(\"d\", 30, [Measures].[Test Date]))"
      ).
      columns('[Measures].[Max Shifted]', '[Measures].[Min Shifted]').execute
    # CA+30 = Feb 14 2020; OR+30 = Jul 15 2020; WA+30 = Jan 14 2021.
    assert_kind_of java.util.Date, result.values[0]
    assert_kind_of java.util.Date, result.values[1]
    assert_equal Date.new(2021, 1, 14), Date.parse(result.values[0].to_s)
    assert_equal Date.new(2020, 2, 14), Date.parse(result.values[1].to_s)
  end

  it "should propagate null through Vba date functions when Min/Max is over an empty set" do
    # Min/Max over an empty set → null. Vba.castToDate returns null for
    # null input, so DateAdd returns null too (rather than raising).
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[max result]').as(
        "DateAdd(\"d\", 30, " \
        "Max(Filter([Customers].[USA].Children, 1=2), [Measures].[Test Date]))"
      ).
      with_member('[Measures].[min result]').as(
        "DateAdd(\"d\", 30, " \
        "Min(Filter([Customers].[USA].Children, 1=2), [Measures].[Test Date]))"
      ).
      columns('[Measures].[max result]', '[Measures].[min result]').execute
    assert_nil result.values[0]
    assert_nil result.values[1]
  end

  it "should resolve Min/Max(set, NULL) as Numeric so the result composes with arithmetic" do
    # NullType converts to any scalar via validator.canConvert. Pre-DateTime
    # overload the only 2-arg form was Numeric, so Max(set, NULL) trivially
    # picked it. With the DateTime overload tried first, NullType also
    # converts to DateTime — without the static-type gate, the resolver
    # would steal the legacy case and statically type the result as Date,
    # breaking numeric composition (Date has no `+ Numeric` or `* Numeric`
    # operator overload, so parsing fails).
    result = @olap.from('Sales').
      with_member('[Measures].[max plus]').as("Max([Customers].[USA].Children, NULL) + 1").
      with_member('[Measures].[min plus]').as("Min([Customers].[USA].Children, NULL) + 1").
      with_member('[Measures].[max times]').as("Max([Customers].[USA].Children, NULL) * 2").
      with_member('[Measures].[min times]').as("Min([Customers].[USA].Children, NULL) * 2").
      columns('[Measures].[max plus]', '[Measures].[min plus]',
        '[Measures].[max times]', '[Measures].[min times]').execute
    # MDX addition treats null as 0 (null + 1 → 1), multiplication
    # preserves null.
    assert_kind_of Numeric, result.values[0]
    assert_kind_of Numeric, result.values[1]
    assert_equal 1, result.values[0].to_i
    assert_equal 1, result.values[1].to_i
    assert_nil result.values[2]
    assert_nil result.values[3]
  end

  it "should treat DateTime-branch Min/Max over an empty set as empty for CoalesceEmpty" do
    # Nested scalar consumers like CoalesceEmpty call Calc.evaluate() directly
    # (not evaluateDateTime). If the DateTime branch returns the Util.nullValue
    # sentinel rather than Java null, CoalesceEmpty's Java-null check misses
    # it and the fallback never fires.
    result = @olap.from('Sales').
      with_member('[Measures].[max result]').as(
        "CoalesceEmpty(" \
        "Max(Filter([Customers].[USA].Children, 1=2), DateSerial(2020, 1, 1)), " \
        "DateSerial(1970, 1, 1))"
      ).
      with_member('[Measures].[min result]').as(
        "CoalesceEmpty(" \
        "Min(Filter([Customers].[USA].Children, 1=2), DateSerial(2020, 1, 1)), " \
        "DateSerial(1970, 1, 1))"
      ).
      columns('[Measures].[max result]', '[Measures].[min result]').execute
    assert_kind_of java.util.Date, result.values[0]
    assert_kind_of java.util.Date, result.values[1]
    assert_equal Date.new(1970, 1, 1), Date.parse(result.values[0].to_s)
    assert_equal Date.new(1970, 1, 1), Date.parse(result.values[1].to_s)
  end

  it "should treat Min/Max over an empty set as empty for IsEmpty via the Numeric branch" do
    # IsEmpty only registers Numeric and String signatures, so the
    # calc-member (Numeric branch) form is the only IsEmpty composition
    # available; it relies on the empty-set result being Java null.
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[max empty]').as(
        "IsEmpty(Max(Filter([Customers].[USA].Children, 1=2), [Measures].[Test Date]))"
      ).
      with_member('[Measures].[min empty]').as(
        "IsEmpty(Min(Filter([Customers].[USA].Children, 1=2), [Measures].[Test Date]))"
      ).
      with_member('[Measures].[max non-empty]').as(
        "IsEmpty(Max([Customers].[USA].Children, [Measures].[Test Date]))"
      ).
      with_member('[Measures].[min non-empty]').as(
        "IsEmpty(Min([Customers].[USA].Children, [Measures].[Test Date]))"
      ).
      columns('[Measures].[max empty]', '[Measures].[min empty]',
        '[Measures].[max non-empty]', '[Measures].[min non-empty]').execute
    assert_equal true, result.values[0]
    assert_equal true, result.values[1]
    assert_equal false, result.values[2]
    assert_equal false, result.values[3]
  end

  %w(Min Max).each do |min_or_max|
    it "should not support IsEmpty over DateTime-branch #{min_or_max} (no DateTime IsEmpty signature)" do
      assert_raises(Mondrian::OLAP::Error) do
        @olap.from('Sales').
          with_member('[Measures].[result]').as(
            "IsEmpty(#{min_or_max}(Filter([Customers].[USA].Children, 1=2), DateSerial(2020, 1, 1)))"
          ).
          columns('[Measures].[result]').execute
      end
    end
  end

  it "should return nil from DateTime-branch Max and Min with empty set" do
    # The calc-member empty-set test above goes through the Numeric branch;
    # this pins the same boundary behavior for the statically-typed DateTime
    # branch without any wrapper function.
    result = @olap.from('Sales').
      with_member('[Measures].[Max Date]').as(
        "Max(Filter([Customers].[USA].Children, 1=2), DateSerial(2020, 1, 1))"
      ).
      with_member('[Measures].[Min Date]').as(
        "Min(Filter([Customers].[USA].Children, 1=2), DateSerial(2020, 1, 1))"
      ).
      columns('[Measures].[Max Date]', '[Measures].[Min Date]').execute
    assert_nil result.values[0]
    assert_nil result.values[1]
  end

  it "should propagate null through Vba date functions when DateTime-branch Min/Max is over an empty set" do
    # Mirrors the calc-member propagation test above, but through the
    # DateTime branch. JavaFunDef's calc short-circuits to a null cell when
    # an argument evaluates to Java null — which only works because the
    # DateTime branch normalizes the empty-set sentinel to null.
    result = @olap.from('Sales').
      with_member('[Measures].[max result]').as(
        "DateAdd(\"d\", 30, " \
        "Max(Filter([Customers].[USA].Children, 1=2), DateSerial(2020, 1, 1)))"
      ).
      with_member('[Measures].[min result]').as(
        "DateAdd(\"d\", 30, " \
        "Min(Filter([Customers].[USA].Children, 1=2), DateSerial(2020, 1, 1)))"
      ).
      columns('[Measures].[max result]', '[Measures].[min result]').execute
    assert_nil result.values[0]
    assert_nil result.values[1]
  end

  it "should return null cell for Max and Min over set with NULL value expression" do
    # Companion to the composition tests above: the bare cell value of the
    # Numeric-resolved NULL form is null (all evaluated values are null, so
    # extremeValue sees an empty value list).
    result = @olap.from('Sales').
      with_member('[Measures].[max]').as("Max([Customers].[USA].Children, NULL)").
      with_member('[Measures].[min]').as("Min([Customers].[USA].Children, NULL)").
      columns('[Measures].[max]', '[Measures].[min]').execute
    assert_nil result.values[0]
    assert_nil result.values[1]
  end

  # The statically-typed DateTime branch is exercised elsewhere only with a
  # constant DateSerial, where Min and Max trivially agree. An inline CASE of
  # DateSerial branches is statically DateTime too but varies per member, so
  # the tests below put the DateTime branch through the same paces as the
  # calc-member (Numeric branch) tests above: distinct values, partial nulls,
  # multiple rows, tuples.

  %w(Min Max).each do |min_or_max|
    it "should not support arithmetic over DateTime-branch #{min_or_max} result" do
      # Also guards that an inline CASE date expression really resolves via
      # the DateTime branch: `Date + Numeric` has no operator overload, so
      # validation fails. Contrast with the calc-member form, which is
      # statically Numeric and silently yields null in arithmetic (see the
      # arithmetic test below).
      assert_raises(Mondrian::OLAP::Error) do
        @olap.from('Sales').
          with_member('[Measures].[result]').as(
            "#{min_or_max}([Customers].[USA].Children, #{date_measure_expression}) + 1"
          ).
          columns('[Measures].[result]').execute
      end
    end
  end

  it "should select distinct per-member dates through the DateTime branch" do
    result = @olap.from('Sales').
      with_member('[Measures].[Max Date]').as(
        "Max([Customers].[USA].Children, #{date_measure_expression})"
      ).
      with_member('[Measures].[Min Date]').as(
        "Min([Customers].[USA].Children, #{date_measure_expression})"
      ).
      columns('[Measures].[Max Date]', '[Measures].[Min Date]').execute
    assert_kind_of java.util.Date, result.values[0]
    assert_kind_of java.util.Date, result.values[1]
    assert_equal Date.new(2020, 12, 15), Date.parse(result.values[0].to_s)
    assert_equal Date.new(2020, 1, 15), Date.parse(result.values[1].to_s)
  end

  it "should skip null date values through the DateTime branch" do
    # Only CA and OR get dates, WA evaluates to null
    partial_date_expression =
      "CASE [Customers].CurrentMember" \
      " WHEN [Customers].[USA].[CA] THEN DateSerial(2020, 1, 15)" \
      " WHEN [Customers].[USA].[OR] THEN DateSerial(2020, 6, 15)" \
      " END"
    result = @olap.from('Sales').
      with_member('[Measures].[Max Date]').as(
        "Max([Customers].[USA].Children, #{partial_date_expression})"
      ).
      with_member('[Measures].[Min Date]').as(
        "Min([Customers].[USA].Children, #{partial_date_expression})"
      ).
      columns('[Measures].[Max Date]', '[Measures].[Min Date]').execute
    assert_equal Date.new(2020, 6, 15), Date.parse(result.values[0].to_s)
    assert_equal Date.new(2020, 1, 15), Date.parse(result.values[1].to_s)
  end

  it "should return correct dates across multiple rows through the DateTime branch" do
    result = @olap.from('Sales').
      with_member('[Measures].[Max Date]').as(
        "Max({[Customers].CurrentMember}, #{date_measure_expression})"
      ).
      with_member('[Measures].[Min Date]').as(
        "Min({[Customers].CurrentMember}, #{date_measure_expression})"
      ).
      columns('[Measures].[Max Date]', '[Measures].[Min Date]').
      rows('[Customers].[USA].Children').execute
    result.values.each do |row|
      assert_kind_of java.util.Date, row[0]
      assert_kind_of java.util.Date, row[1]
      assert_equal row[0].to_s, row[1].to_s
    end
    dates = result.values.map { |row| Date.parse(row[0].to_s) }.sort
    assert_equal [Date.new(2020, 1, 15), Date.new(2020, 6, 15), Date.new(2020, 12, 15)], dates
  end

  it "should work with CrossJoin producing tuples through the DateTime branch" do
    result = @olap.from('Sales').
      with_member('[Measures].[max result]').as(
        "Max(CrossJoin({[Customers].[USA].[CA], [Customers].[USA].[WA]}, " \
        "{[Gender].[All Gender]}), #{date_measure_expression})"
      ).
      with_member('[Measures].[min result]').as(
        "Min(CrossJoin({[Customers].[USA].[CA], [Customers].[USA].[WA]}, " \
        "{[Gender].[All Gender]}), #{date_measure_expression})"
      ).
      columns('[Measures].[max result]', '[Measures].[min result]').execute
    assert_equal Date.new(2020, 12, 15), Date.parse(result.values[0].to_s)
    assert_equal Date.new(2020, 1, 15), Date.parse(result.values[1].to_s)
  end

  it "should return nil from 1-arg Max and Min over an empty set" do
    result = @olap.from('Sales').
      with_member('[Measures].[max result]').as(
        "Max(Filter([Customers].[USA].Children, 1=2))"
      ).
      with_member('[Measures].[min result]').as(
        "Min(Filter([Customers].[USA].Children, 1=2))"
      ).
      columns('[Measures].[max result]', '[Measures].[min result]').execute
    assert_nil result.values[0]
    assert_nil result.values[1]
  end

  it "should return nil from 1-arg Max and Min when the measure value is null" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[max result]').as(
        "Max({[Measures].[Test Date]})"
      ).
      with_member('[Measures].[min result]').as(
        "Min({[Measures].[Test Date]})"
      ).
      columns('[Measures].[max result]', '[Measures].[min result]').
      rows('[Customers].[Canada]').execute
    # date_measure_expression has no branch for Canada, so Test Date is null
    assert_nil result.values[0][0]
    assert_nil result.values[0][1]
  end

  %w(Min Max).each do |min_or_max|
    it "should raise for #{min_or_max} over values mixing dates and numbers" do
      # Pins current behavior rather than a contract: extremeValue picks the
      # comparison branch from the first evaluated value's runtime type and
      # casts the rest, so a mixed value list fails (ClassCastException
      # surfaced as a query error) regardless of which type comes first.
      # Both calc members are statically Numeric, so the mixed CASE passes
      # validation and the failure is purely a runtime one.
      ['THEN [Measures].[Date Calc] ELSE 42', 'THEN 42 ELSE [Measures].[Date Calc]'].each do |case_branches|
        assert_raises(Mondrian::OLAP::Error) do
          @olap.from('Sales').
            with_member('[Measures].[Date Calc]').as("DateSerial(2020, 1, 15)").
            with_member('[Measures].[Mixed]').as(
              "CASE [Customers].CurrentMember" \
              " WHEN [Customers].[USA].[CA] #{case_branches} END"
            ).
            with_member('[Measures].[result]').as(
              "#{min_or_max}([Customers].[USA].Children, [Measures].[Mixed])"
            ).
            columns('[Measures].[result]').execute
        end
      end
    end
  end

  it "should silently yield null when date-returning calc-member Min/Max is used in arithmetic" do
    # Documents current (lossy) behavior rather than a contract: the
    # calc-member form is statically Numeric, so arithmetic compiles the
    # aggregate via evaluateDouble, which has no double representation for a
    # Date and returns null. Same shape as the NULL matrix above: addition
    # coerces null to 0, multiplication preserves null. The statically-typed
    # DateTime form instead fails at validation (see the arithmetic
    # limitation tests above).
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[max plus]').as("Max([Customers].[USA].Children, [Measures].[Test Date]) + 1").
      with_member('[Measures].[min plus]').as("Min([Customers].[USA].Children, [Measures].[Test Date]) + 1").
      with_member('[Measures].[max times]').as("Max([Customers].[USA].Children, [Measures].[Test Date]) * 2").
      with_member('[Measures].[min times]').as("Min([Customers].[USA].Children, [Measures].[Test Date]) * 2").
      columns('[Measures].[max plus]', '[Measures].[min plus]',
        '[Measures].[max times]', '[Measures].[min times]').execute
    assert_equal 1, result.values[0].to_i
    assert_equal 1, result.values[1].to_i
    assert_nil result.values[2]
    assert_nil result.values[3]
  end

  it "should distinguish dates by time of day" do
    # All three values fall on the same day; only the time component differs.
    time_expression =
      "CASE [Customers].CurrentMember" \
      " WHEN [Customers].[USA].[CA] THEN DateAdd(\"h\", 9, DateSerial(2020, 1, 15))" \
      " WHEN [Customers].[USA].[OR] THEN DateAdd(\"h\", 17, DateSerial(2020, 1, 15))" \
      " WHEN [Customers].[USA].[WA] THEN DateAdd(\"h\", 1, DateSerial(2020, 1, 15))" \
      " END"
    result = @olap.from('Sales').
      with_member('[Measures].[Test Time]').as(time_expression).
      with_member('[Measures].[max result]').as(
        "Max([Customers].[USA].Children, [Measures].[Test Time])"
      ).
      with_member('[Measures].[min result]').as(
        "Min([Customers].[USA].Children, [Measures].[Test Time])"
      ).
      columns('[Measures].[max result]', '[Measures].[min result]').execute
    assert_kind_of java.util.Date, result.values[0]
    assert_kind_of java.util.Date, result.values[1]
    assert_equal Date.new(2020, 1, 15), Date.parse(result.values[0].to_s)
    # Max = 17:00, Min = 01:00 — 16 hours apart.
    assert_equal 16 * 60 * 60 * 1000, result.values[0].time - result.values[1].time
  end

  it "should compare dates before 1970 correctly" do
    epoch_expression =
      "CASE [Customers].CurrentMember" \
      " WHEN [Customers].[USA].[CA] THEN DateSerial(1965, 6, 15)" \
      " WHEN [Customers].[USA].[OR] THEN DateSerial(1975, 6, 15)" \
      " END"
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(epoch_expression).
      with_member('[Measures].[max result]').as(
        "Max([Customers].[USA].Children, [Measures].[Test Date])"
      ).
      with_member('[Measures].[min result]').as(
        "Min([Customers].[USA].Children, [Measures].[Test Date])"
      ).
      columns('[Measures].[max result]', '[Measures].[min result]').execute
    assert_equal Date.new(1975, 6, 15), Date.parse(result.values[0].to_s)
    assert_equal Date.new(1965, 6, 15), Date.parse(result.values[1].to_s)
    # 1965 precedes the Unix epoch, so its getTime() is negative — pins that
    # the comparison doesn't assume positive epoch millis.
    assert_operator result.values[1].time, :<, 0
  end

  # The query builder's rows() appends to the axis, so each ordering case
  # below builds its own query.
  def min_max_per_member_query(rows_expression)
    @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[Max Date]').as(
        "Max({[Customers].CurrentMember}, [Measures].[Test Date])"
      ).
      with_member('[Measures].[Min Date]').as(
        "Min({[Customers].CurrentMember}, [Measures].[Test Date])"
      ).
      columns('[Measures].[Max Date]').
      rows(rows_expression).execute
  end

  it "should order members by date Min/Max calculated members" do
    result = min_max_per_member_query(
      "Order([Customers].[USA].Children, [Measures].[Max Date], BDESC)")
    assert_equal %w(WA OR CA), result.row_names
    result = min_max_per_member_query(
      "Order([Customers].[USA].Children, [Measures].[Min Date], BASC)")
    assert_equal %w(CA OR WA), result.row_names
  end

  it "should pick members with extreme dates via TopCount and BottomCount over Min/Max calculated members" do
    result = min_max_per_member_query(
      "TopCount([Customers].[USA].Children, 1, [Measures].[Max Date])")
    assert_equal %w(WA), result.row_names
    result = min_max_per_member_query(
      "BottomCount([Customers].[USA].Children, 1, [Measures].[Min Date])")
    assert_equal %w(CA), result.row_names
  end

end

describe "Min and Max with date column properties" do
  before(:all) do
    # Mondrian measures only allow String/Numeric/Integer datatypes, so real
    # date columns reach MDX as typed member properties. Unlike the main
    # describe block, where every date is constructed via DateSerial, these
    # values come back from SQL as java.sql.Timestamp. Properties("...") on
    # a Timestamp property with a literal name deduces Category.DateTime, so
    # this also exercises the statically-typed DateTime branch with real
    # data. One store row has null dates, so real-data null skipping is
    # exercised too.
    schema = Mondrian::OLAP::Schema.define do
      cube 'Store' do
        table 'store'
        dimension 'Store' do
          hierarchy has_all: true do
            level 'Store Country', column: 'store_country', unique_members: true
            level 'Store Name', column: 'store_name', unique_members: false do
              property 'First Opened', column: 'first_opened_date', type: 'Timestamp'
              property 'Last Remodel', column: 'last_remodel_date', type: 'Timestamp'
            end
          end
        end
        measure 'Store Sqft', column: 'store_sqft', aggregator: 'sum'
      end
    end
    @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS.except(:catalog).merge(schema: schema))
  end

  it "should return extreme dates from Min and Max over a Timestamp property" do
    result = @olap.from('Store').
      with_member('[Measures].[Min Opened]').as(
        "Min([Store].[Store Name].Members, [Store].CurrentMember.Properties(\"First Opened\"))"
      ).
      with_member('[Measures].[Max Opened]').as(
        "Max([Store].[Store Name].Members, [Store].CurrentMember.Properties(\"First Opened\"))"
      ).
      columns('[Measures].[Min Opened]', '[Measures].[Max Opened]').execute
    assert_kind_of java.util.Date, result.values[0]
    assert_kind_of java.util.Date, result.values[1]
    assert_equal Date.new(1951, 1, 24), Date.parse(result.values[0].to_s)
    assert_equal Date.new(1994, 9, 27), Date.parse(result.values[1].to_s)
  end

  it "should return extreme dates from Min and Max over another Timestamp property" do
    result = @olap.from('Store').
      with_member('[Measures].[Min Remodel]').as(
        "Min([Store].[Store Name].Members, [Store].CurrentMember.Properties(\"Last Remodel\"))"
      ).
      with_member('[Measures].[Max Remodel]').as(
        "Max([Store].[Store Name].Members, [Store].CurrentMember.Properties(\"Last Remodel\"))"
      ).
      columns('[Measures].[Min Remodel]', '[Measures].[Max Remodel]').execute
    assert_equal Date.new(1958, 1, 7), Date.parse(result.values[0].to_s)
    assert_equal Date.new(1997, 11, 10), Date.parse(result.values[1].to_s)
  end

  it "should compose property-sourced Min and Max with Vba date functions" do
    result = @olap.from('Store').
      with_member('[Measures].[result]').as(
        "DateDiff(\"d\", " \
        "Min([Store].[Store Name].Members, [Store].CurrentMember.Properties(\"First Opened\")), " \
        "Max([Store].[Store Name].Members, [Store].CurrentMember.Properties(\"First Opened\")))"
      ).
      columns('[Measures].[result]').execute
    assert_kind_of Numeric, result.values[0]
    assert_equal (Date.new(1994, 9, 27) - Date.new(1951, 1, 24)).to_i, result.values[0].to_i
  end

end

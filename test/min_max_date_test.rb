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
      columns('[Measures].[Max Date]').execute
    assert_nil result.values[0]
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

  it "should return nil from Max with empty set" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as("DateSerial(2020, 1, 15)").
      with_member('[Measures].[Max Date]').as(
        "Max(Filter([Customers].[USA].Children, 1=2), [Measures].[Test Date])"
      ).
      columns('[Measures].[Max Date]').execute
    assert_nil result.values[0]
  end

  it "should inherit date format from value expression, not from Filter condition" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression, format_string: 'dd.mm.yyyy').
      with_member('[Measures].[Max Date]').as(
        "Max(Filter([Customers].[USA].Children, [Measures].[Unit Sales] > 0), [Measures].[Test Date])"
      ).
      columns('[Measures].[Max Date]').execute
    # Should be formatted as date (from Test Date's format), not as number (from Unit Sales' format)
    assert_equal '15.12.2020', result.formatted_values[0]
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

  it "should support 1-arg form with date measure as set member" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[result]').as(
        "Max({[Measures].[Test Date]})"
      ).
      columns('[Measures].[result]').
      rows('[Customers].[USA].Children').execute
    # Each row gets Max of a single-element set containing the date measure
    result.values.flatten.each do |v|
      assert_kind_of java.util.Date, v
    end
    # CA = Jan, OR = Jun, WA = Dec
    dates = result.values.flatten.map { |v| Date.parse(v.to_s) }
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
    assert_match(/\d+\.\d{4}/, result.formatted_values[0][0])
  end

  it "should return date value for 1-arg form but format as numeric by default" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression, format_string: 'yyyy-mm-dd').
      with_member('[Measures].[result]').as(
        "Max({[Measures].[Test Date]})"
      ).
      columns('[Measures].[result]').
      rows('[Customers].[USA].[WA]').execute
    # The value is a Date
    assert_kind_of java.util.Date, result.values[0][0]
    # But the 1-arg form (Category.Numeric) defaults to numeric formatting;
    # explicit FORMAT_STRING is needed for date display
    assert_match(/[\d,]+/, result.formatted_values[0][0])
  end

  it "should not bleed format strings between multiple date measures" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression, format_string: 'dd.mm.yyyy').
      with_member('[Measures].[Max 1]').as(
        "Max([Customers].[USA].Children, [Measures].[Test Date])"
      ).
      with_member('[Measures].[Test Date 2]').as(date_measure_expression, format_string: 'yyyy-mm-dd').
      with_member('[Measures].[Max 2]').as(
        "Max([Customers].[USA].Children, [Measures].[Test Date 2])"
      ).
      columns('[Measures].[Max 1]', '[Measures].[Max 2]').execute
    # Max 1 should use dd.mm.yyyy, Max 2 should use yyyy-mm-dd
    assert_equal '15.12.2020', result.formatted_values[0]
    assert_equal '2020-12-15', result.formatted_values[1]
  end

  it "should work inside CoalesceEmpty" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[result]').as(
        "CoalesceEmpty(" \
        "Max(Filter([Customers].[USA].Children, 1=2), [Measures].[Test Date]), " \
        "DateSerial(1970, 1, 1))"
      ).
      columns('[Measures].[result]').execute
    # Max over empty set returns null, CoalesceEmpty falls back to DateSerial
    assert_kind_of java.util.Date, result.values[0]
    assert_equal Date.new(1970, 1, 1), Date.parse(result.values[0].to_s)
  end

  it "should work with CrossJoin producing tuples" do
    result = @olap.from('Sales').
      with_member('[Measures].[Test Date]').as(date_measure_expression).
      with_member('[Measures].[result]').as(
        "Max(CrossJoin({[Customers].[USA].[CA], [Customers].[USA].[WA]}, " \
        "{[Gender].[All Gender]}), [Measures].[Test Date])"
      ).
      columns('[Measures].[result]').execute
    assert_kind_of java.util.Date, result.values[0]
    assert_equal Date.new(2020, 12, 15), Date.parse(result.values[0].to_s)
  end

  # Pre-existing Mondrian limitations: IIf and comparison operators lack
  # DateTime signatures. Not caused by the Min/Max date support changes.

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

  it "should not support comparison operators with date Min/Max result (no DateTime comparison signature)" do
    assert_raises(Mondrian::OLAP::Error) do
      @olap.from('Sales').
        with_member('[Measures].[Test Date]').as(date_measure_expression).
        with_member('[Measures].[result]').as(
          "Max([Customers].[USA].Children, [Measures].[Test Date]) > DateSerial(2020, 6, 1)"
        ).
        columns('[Measures].[result]').execute
    end
  end
end

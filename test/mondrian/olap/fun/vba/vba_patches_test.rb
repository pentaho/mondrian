# This software is subject to the terms of the Eclipse Public License v1.0
# Agreement, available at the following URL:
# http://www.eclipse.org/legal/epl-v10.html.
# You must accept the terms of that agreement to use this software.
#
# Copyright (C) 2026 eazyBI
# All Rights Reserved.

# frozen_string_literal: true

require_relative "../../../../test_helper"

# Tests for local patches to mondrian.olap.fun.vba.Vba. Each describe block
# covers one VBA date function whose signature was widened from (Date) to
# (Object) so that calculated members (which Mondrian's validator always
# types as Numeric, regardless of runtime return type) can be passed as
# date arguments. The coercion is done by a shared castToDate helper.
describe "Vba patches" do
  DATE_EXPRESSION = 'DateAdd("s", 12*60*60 + 13*60 + 14, DateSerial(2025, 9, 30))'
  NULL_EXPRESSION = 'CASE WHEN 1=2 THEN Now() END'
  NON_DATE_EXPRESSION = '123'

  def execute_result(olap, mdx)
    olap.execute(mdx).values.first
  end

  def execute_on_date_measure(olap, date_formula, result_formula)
    execute_result olap, <<~MDX
      WITH
        MEMBER [Measures].[Date] AS #{date_formula}
        MEMBER [Measures].[Result] AS #{result_formula}
      SELECT {[Measures].[Result]} ON COLUMNS FROM [Sales]
    MDX
  end

  before(:all) do
    create_olap_connection
  end

  after(:all) do
    @olap&.close
  end

  describe "DateAdd with Object date" do
    # DateSerial(2025, 9, 30) + 12h13m14s + 1 day = 2025-10-01 12:13:14 local.
    EXPECTED_TIME = Time.new(2025, 10, 1, 12, 13, 14)

    it "accepts a Date-typed expression (baseline stock Mondrian behaviour)" do
      # DateSerial returns a statically Date-typed value. This path must
      # continue to work after the signature widening.
      result = execute_result(@olap, <<~MDX)
        WITH MEMBER [Measures].[Result] AS DateAdd("d", 1, #{DATE_EXPRESSION})
        SELECT {[Measures].[Result]} ON COLUMNS FROM [Sales]
      MDX
      assert_kind_of Java::JavaUtil::Date, result
      assert_equal EXPECTED_TIME.to_i * 1000, result.getTime
    end

    it "accepts a calculated-member date (Numeric-typed, evaluates to Date)" do
      # The point of the patch: calc member is statically Numeric, so stock
      # Vba.dateAdd(Date) would not resolve. With Object, it does, and
      # runtime castToDate unwraps the Date.
      result = execute_on_date_measure(@olap, DATE_EXPRESSION,
        'DateAdd("d", 1, [Measures].[Date])')
      assert_kind_of Java::JavaUtil::Date, result
      assert_equal EXPECTED_TIME.to_i * 1000, result.getTime
    end

    it "returns null when the date expression is null" do
      result = execute_on_date_measure(@olap, NULL_EXPRESSION,
        'DateAdd("d", 1, [Measures].[Date])')
      assert_nil result
    end

    it "raises when the date expression is a non-Date, non-null value" do
      # Mondrian wraps VBA exceptions into the cell value rather than
      # propagating them to the MDX client.
      result = execute_on_date_measure(@olap, NON_DATE_EXPRESSION,
        'DateAdd("d", 1, [Measures].[Date])')
      assert_match(/must be of type Date/, result.message)
    end
  end
end

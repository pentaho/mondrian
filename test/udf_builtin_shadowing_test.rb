# frozen_string_literal: true

require_relative "test_helper"
require 'date'

# RolapSchemaFunctionTable.defineFunctions registers schema UDFs before the
# builtin resolvers and skips any builtin that a UDF shadows by (name, syntax).
# FunTableImpl keys all resolvers case-insensitively (makeResolverKey
# upper-cases the name), so the shadow check must be case-insensitive too —
# otherwise the builtin stays registered alongside the UDF and resolution
# becomes ambiguous or picks the builtin.
describe "Schema UDF shadowing builtin functions" do
  # Minimal FoodMart cube with an optional UDF block appended to the schema.
  def create_connection(&schema_block)
    schema = Mondrian::OLAP::Schema.define do
      cube 'Sales' do
        table 'sales_fact_1997'
        dimension 'Gender', foreign_key: 'customer_id' do
          hierarchy has_all: true, all_member_name: 'All Genders', primary_key: 'customer_id' do
            table 'customer'
            level 'Gender', column: 'gender', unique_members: true
          end
        end
        measure 'Unit Sales', column: 'unit_sales', aggregator: 'sum'
      end
      instance_eval(&schema_block) if schema_block
    end
    Mondrian::OLAP::Connection.create(CONNECTION_PARAMS.except(:catalog).merge(schema: schema))
  end

  def connection_with_min_udf(udf_name)
    create_connection do
      # Same signature as the builtin Min 2-arg numeric form (fnxn), but a
      # recognizable constant result so the test can tell which one resolved.
      user_defined_function udf_name do
        ruby do
          parameters :set, :numeric
          returns :numeric
          def call(set, value)
            42
          end
        end
      end
    end
  end

  def min_query_result(olap, function_name = 'Min')
    olap.from('Sales').
      with_member('[Measures].[m]').as(
        "#{function_name}([Gender].[Gender].Members, [Measures].[Unit Sales])"
      ).
      columns('[Measures].[m]').execute.values[0]
  end

  # FoodMart 1997 Unit Sales by gender: F = 131558, M = 135215.
  BUILTIN_MIN_UNIT_SALES = 131_558

  it "should shadow the builtin when the UDF name matches exactly" do
    olap = connection_with_min_udf('Min')
    assert_equal 42, min_query_result(olap)
  end

  it "should shadow the builtin when the UDF name differs only in case" do
    olap = connection_with_min_udf('min')
    assert_equal 42, min_query_result(olap)
  end

  it "should resolve the shadowing UDF regardless of call-site case" do
    olap = connection_with_min_udf('Min')
    assert_equal 42, min_query_result(olap, 'mIn')
  end

  it "should shadow the stock VBA Now() without the skipJavaFunDefs property" do
    # The motivating production case: eazyBI defines a Now UDF, and without
    # shadowing the stock VBA Now() triggers an ambiguous-match error (the
    # skipJavaFunDefs property exists solely to suppress it). Also proves
    # shadowing works for SimpleResolver-wrapped JavaFunDefs, not just the
    # MultiResolver builtins covered by the Min tests.
    assert_nil java.lang.System.getProperty("mondrian.olap.fun.skipJavaFunDefs")
    olap = create_connection do
      user_defined_function 'Now' do
        ruby do
          returns :date_time
          def call
            Java::JavaUtil::GregorianCalendar.new(2020, 0, 15).getTime
          end
        end
      end
    end
    result = olap.from('Sales').
      with_member('[Measures].[m]').as("Now()").
      columns('[Measures].[m]').execute
    assert_kind_of java.util.Date, result.values[0]
    assert_equal Date.new(2020, 1, 15), Date.parse(result.values[0].to_s)
  end

  it "should not shadow the builtin when the UDF has a different syntax" do
    # The shadow key is (name, syntax): a Property-syntax UDF named Min must
    # not suppress the Function-syntax builtin — both should resolve.
    olap = create_connection do
      user_defined_function 'Min' do
        ruby do
          parameters :member
          returns :numeric
          syntax :property
          def call(member)
            42
          end
        end
      end
    end
    assert_equal BUILTIN_MIN_UNIT_SALES, min_query_result(olap).to_i
    property_result = olap.from('Sales').
      with_member('[Measures].[m]').as("[Gender].[All Genders].Min").
      columns('[Measures].[m]').execute.values[0]
    assert_equal 42, property_result
  end

  it "should leave builtin functions intact when a UDF does not collide" do
    olap = create_connection do
      user_defined_function 'Factorial' do
        ruby do
          parameters :numeric
          returns :numeric
          def call(n)
            n <= 1 ? 1 : n * call(n - 1)
          end
        end
      end
    end
    assert_equal BUILTIN_MIN_UNIT_SALES, min_query_result(olap).to_i
    factorial_result = olap.from('Sales').
      with_member('[Measures].[m]').as("Factorial(4)").
      columns('[Measures].[m]').execute.values[0]
    assert_equal 24, factorial_result.to_i
  end

  it "should not leak shadowing to other schemas in the same process" do
    shadowed_olap = connection_with_min_udf('Min')
    plain_olap = create_connection
    assert_equal 42, min_query_result(shadowed_olap)
    assert_equal BUILTIN_MIN_UNIT_SALES, min_query_result(plain_olap).to_i
  end
end

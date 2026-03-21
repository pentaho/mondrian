# frozen_string_literal: true

require_relative "test_helper"

describe "Connection" do
  describe "create" do
    before do
      @olap = Mondrian::OLAP::Connection.new(CONNECTION_PARAMS)
    end

    it "should not be connected before connection" do
      assert_equal false, @olap.connected?
    end

    it "should be successful" do
      assert_equal true, @olap.connect
    end
  end

  describe "properties" do
    before(:all) do
      @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS)
    end

    it "should be connected" do
      assert_equal true, @olap.connected?
    end

    it "should use corresponding Mondrian dialect" do
      schema_field = @olap.raw_schema.getClass.getDeclaredField("schema")
      schema_field.setAccessible(true)
      private_schema = schema_field.get(@olap.raw_schema)
      expected_dialect =
        case MONDRIAN_DRIVER
        when 'mysql' then 'mondrian.spi.impl.MySqlDialect'
        when 'postgresql' then 'mondrian.spi.impl.PostgreSqlDialect'
        when 'oracle' then 'mondrian.spi.impl.OracleDialect'
        when 'sqlserver' then 'mondrian.spi.impl.MicrosoftSqlServerDialect'
        when 'clickhouse' then 'mondrian.spi.impl.ClickHouseDialect'
        end
      assert_equal expected_dialect, private_schema.getDialect.java_class.name
    end
  end

  describe "close" do
    before(:all) do
      @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS)
    end

    it "should not be connected after close" do
      @olap.close
      assert_equal false, @olap.connected?
    end
  end
end

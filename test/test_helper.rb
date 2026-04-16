# frozen_string_literal: true

require_relative 'support/database_setup'

require 'minitest/autorun'
require 'minitest/reporters'
require 'minitest/hooks/default'
require 'pry'

require_relative 'support/query_helper'

Minitest::Reporters.use! Minitest::Reporters::DefaultReporter.new(color: true)

class Minitest::Spec
  include QueryHelper

  def assert_like(expected, actual, msg = nil)
    expected_normalized = expected.gsub(/>\s*\n\s*/, '> ').gsub(/\s+/, ' ').strip
    actual_normalized = actual.gsub(/>\s*\n\s*/, '> ').gsub(/\s+/, ' ').strip
    assert_equal expected_normalized, actual_normalized, msg
  end

  def new_olap_connection
    @olap = Mondrian::OLAP::Connection.new(CONNECTION_PARAMS)
  end

  def create_olap_connection
    @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS)
  end
end

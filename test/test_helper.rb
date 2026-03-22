# frozen_string_literal: true

require_relative 'support/database_setup'

require 'minitest/autorun'
require 'minitest/reporters'
require 'minitest/hooks/default'
require 'pry'

Minitest::Reporters.use! Minitest::Reporters::DefaultReporter.new(color: true)

class Minitest::Spec
  def assert_like(expected, actual, msg = nil)
    expected_normalized = expected.gsub(/>\s*\n\s*/, '> ').gsub(/\s+/, ' ').strip
    actual_normalized = actual.gsub(/>\s*\n\s*/, '> ').gsub(/\s+/, ' ').strip
    assert_equal expected_normalized, actual_normalized, msg
  end
end

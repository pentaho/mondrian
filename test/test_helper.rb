# frozen_string_literal: true

require_relative 'support/database_setup'

require 'minitest/autorun'
require 'minitest/reporters'
require 'minitest/hooks/default'
require 'pry'

Minitest::Reporters.use! Minitest::Reporters::DefaultReporter.new(color: true)

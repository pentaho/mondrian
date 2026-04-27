# This software is subject to the terms of the Eclipse Public License v1.0
# Agreement, available at the following URL:
# http://www.eclipse.org/legal/epl-v10.html.
# You must accept the terms of that agreement to use this software.
#
# Copyright (C) 2026 eazyBI
# All Rights Reserved.

# frozen_string_literal: true

require_relative "../../../test_helper"

# Verifies that the mondrian.olap.fun.SkipJavaFunDefs property suppresses
# registration of specific Vba/Excel function definitions in a fresh
# BuiltinFunTable. The mechanism lets a schema (or external extension)
# register a UserDefinedFunction with the same name as a stock Vba/Excel
# built-in without causing an ambiguous-match error.
describe "BuiltinFunTable SkipJavaFunDefs filter" do
  PROPERTY = "mondrian.olap.fun.SkipJavaFunDefs"

  # Construct a fresh BuiltinFunTable instance bypassing the singleton
  # so the test can observe registration with a specific property value.
  def fresh_builtin_fun_table
    cls = Java::MondrianOlapFun::BuiltinFunTable.java_class
    ctor = cls.getDeclaredConstructor
    ctor.setAccessible(true)
    table = ctor.newInstance
    table.init
    table
  end

  def function_names(table, syntax = Java::MondrianOlap::Syntax::Function)
    table.getResolvers.select { |r| r.getSyntax == syntax }.map(&:getName).to_a
  end

  def with_property(value)
    original = java.lang.System.getProperty(PROPERTY)
    if value.nil?
      java.lang.System.clearProperty(PROPERTY)
    else
      java.lang.System.setProperty(PROPERTY, value)
    end
    yield
  ensure
    if original.nil?
      java.lang.System.clearProperty(PROPERTY)
    else
      java.lang.System.setProperty(PROPERTY, original)
    end
  end

  it "registers Vba Now() when the property is empty (baseline)" do
    with_property(nil) do
      table = fresh_builtin_fun_table
      assert_includes function_names(table), "Now"
    end
  end

  it "skips Vba Now() when the property lists 'Now'" do
    with_property("Now") do
      table = fresh_builtin_fun_table
      refute_includes function_names(table), "Now"
    end
  end

  it "honors comma-separated values and trims whitespace" do
    with_property("Now, Hour") do
      table = fresh_builtin_fun_table
      names = function_names(table)
      refute_includes names, "Now"
      refute_includes names, "Hour"
      # Unrelated function still present
      assert_includes names, "DateAdd"
    end
  end

  it "leaves other Vba functions untouched when property lists 'Now'" do
    with_property("Now") do
      table = fresh_builtin_fun_table
      names = function_names(table)
      %w(DateAdd DateDiff DateSerial Year Month Day).each do |fn|
        assert_includes names, fn, "#{fn} should still be registered"
      end
    end
  end
end

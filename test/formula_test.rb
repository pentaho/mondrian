# frozen_string_literal: true

require_relative "test_helper"

describe "Formula" do
  def create_connection_with_calculated_members(members_xml)
    foodmart_xml = File.read(CATALOG_FILE)
    modified_xml = foodmart_xml.sub('</Cube>', "#{members_xml}</Cube>")
    Mondrian::OLAP::Connection.create(
      CONNECTION_PARAMS.reject { |k, _| k == :catalog }.merge(catalog_content: modified_xml)
    )
  end

  it "should detect self-referencing schema-defined calculated measure" do
    # Calc A references Calc B and Calc B references itself.
    # During schema loading, FormatFinder infers format strings by
    # traversing calculated member expressions. Without the fix,
    # hasCyclicReference fails to detect the self-reference because
    # Util.lookup creates a new MemberExpr on each call and
    # identity-based MemberExpr tracking never matches.
    begin
      olap = create_connection_with_calculated_members(<<~XML)
        <CalculatedMember name="Calc A" dimension="Measures">
          <Formula>[Measures].[Calc B]</Formula>
        </CalculatedMember>
        <CalculatedMember name="Calc B" dimension="Measures">
          <Formula>[Measures].[Calc B]</Formula>
        </CalculatedMember>
      XML
    rescue Java::JavaLang::StackOverflowError
      flunk "StackOverflowError: infinite recursion in FormatFinder.hasCyclicReference"
    end

    result = olap.execute <<~MDX
      SELECT
        {[Measures].[Calc A]} ON COLUMNS,
        {[Product].Children} ON ROWS
      FROM Sales
    MDX

    refute_nil result
  end

  it "should resolve format string through non-cyclic calculated member chain" do
    olap = create_connection_with_calculated_members(<<~XML)
      <CalculatedMember name="Double Profit" dimension="Measures">
        <Formula>([Measures].[Store Sales] - [Measures].[Store Cost]) * 2</Formula>
      </CalculatedMember>
    XML

    result = olap.execute <<~MDX
      SELECT
        {[Measures].[Store Sales], [Measures].[Double Profit]} ON COLUMNS,
        {[Product].Children} ON ROWS
      FROM Sales
    MDX

    refute_nil result
    refute_empty result.values
  end
end

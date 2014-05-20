/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2014 Pentaho
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.*;
import mondrian.rolap.*;
import mondrian.rolap.aggmatcher.*;
import mondrian.spi.*;
import mondrian.spi.PropertyFormatter;
import mondrian.util.*;

import junit.framework.Assert;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;
import org.apache.log4j.varia.LevelRangeFilter;

import org.olap4j.OlapConnection;
import org.olap4j.impl.ArrayMap;
import org.olap4j.metadata.Catalog;
import org.olap4j.metadata.Database;
import org.olap4j.metadata.MetadataElement;
import org.olap4j.metadata.NamedList;

import java.io.*;
import java.sql.*;
import java.sql.Connection;
import java.util.*;

/**
 * Unit tests for various schema features.
 *
 * @see SchemaVersionTest
 * @see mondrian.rolap.SharedDimensionTest
 * @see HangerDimensionTest
 *
 * @author jhyde
 * @since August 7, 2006
 */
public class SchemaTest extends FoodMartTestCase {

    public static final String MINIMAL_SALES_CUBE =
        "<Cube name='Sales'>\n"
        + "  <MeasureGroups>\n"
        + "    <MeasureGroup table='sales_fact_1997'>\n"
        + "      <Measures>\n"
        + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='Standard'/>\n"
        + "      </Measures>\n"
        + "    </MeasureGroup>\n"
        + "  </MeasureGroups>\n"
        + "  <Dimensions/>\n"
        + "</Cube>\n";

    public SchemaTest(String name) {
        super(name);
    }

/*

=== Schema futures ===

Add ForeignKeyLink@name. Would allow multiple uses of the same dimension.

Add ForeignKeyLink@attribute. Would allow join to non-key attribute.

It is an error to define a dimension with the same name in a cube and at schema
level. In future, maybe allow schema Dimension to be refined as a cube
dimension. Would also make it more explicit what dimensions are in the cube.

It is an error if two measure groups in the same cube import dimensions with the
same name and they are not based on the same schema dimension.

Attribute hierarchies. (Add boolean attribute Attribute@hasHierarchy?)

Attribute dependencies.

Attribute relationships. (Allows levels to have properties. Not very important
since attributes available directly on the dimension.)

Add back Property (and Level@properties).

Add back Measure@datatype?

Add Column@type (similar to old Level@type)

Rename Attribute@levelType.

Add back Level@formatter. (Should it go to Attribute?)

Add back Property@formatter. (Should it go to Attribute? Obsolete
PropertyFormatter class?)

Add back Table.aggTables, Table.aggExcludes. But where?

Test that if attribute name is not specified and key is composite,
the name is the last column of the key.

Test that get error if attribute name is not specified and key is of non-text
type.

Test that get error if there is more than one Key, Name, Ordinal, Caption
element in Attribute.

Test that get error if Level@attribute is not specified.

Test that get error if Level@attribute is not a valid attribute in this
Dimension.

Test Attribute with Closure. (Currently ignored.)

Test Attribute with no key (no keyColumn attribute or Key subelement). Should
fail.

Test Attribute with composite key and no nameColun/Name subelement. Should fail.

Change Dimension@type (and Dimension.getType()) to use org.olap4j.Dimension.Type
values.

Change Level@type (and Level.getLevelType()) to use org.olap4j.Level.Type
values.

Probably broke level member count on sybase (which does not support compound
distinct-count). See old SqlMemberSource.makeLevelMemberCountSql.

Write test to ensure that RolapSchema.createMemberReader returns the same
MemberReader for instances of the same shared hierarchy.

Test that Dimension@name is mandatory for schema dimensions.

Test that Dimension@name is optional for cube dimensions, and name defaults to
source.

Test that source must not be speicied in schema dimensions.

Test that source is specified in cube dimensions, and is the name of a schema
dimension. (Could be one declared after the cube.)

Test uniqueness; get error if more than one object of given name in parent:
- cube in schema
- schema dimension in schema
- cube dimension in cube
- todo

Test that if a table has two keys with same name, get error. Likewise if one is
called 'primary' and the other is nameless, or both are nameless.

Test bad value for Level@formatter, make sure error has good position.

Move Level@formatter to Attribute.

Test Dimension@type with illegal value.

Test Dimension@type with value "standard" even though there are time
levels. (What should happen?)

Test Dimension@type with value "time" even though there are no time
levels. (What should happen?)

Create Measure@datatype, unify with Attribute@datatype (formerly Property@type)
and Level@type (obsolete).

Test that cube contains MeasureGroups element, and that it has at least one
MeasureGroup.

Test that get an error if a measure is in a different table than its measure
group.

Test that a measure can be based on a column (as attribute or element) but not
an expression.

Test that get an error if MeasureGroup@table is not set, or is not a valid table
name.

Test that get an error if DimensionLink@dimension is not valid.

Test MeasureGroup@ignoreUnrelatedDimensions. (Maybe there are existing tests;
the attribute used to be on CubeUsage@ignoreUnrelatedDimensions.)

Test Property based on attribute with a composite key and a different name. Also
test query of same.

Is it possible for a level's KEY to be unique within its parent and its
NAME not to be unique? Or vice versa? And if so, how to model it? We no longer
allow level-key-not-unique (because you have to provide the whole key) but
should we have a level-name-is-only-unique-within-parent flag? Would we even
use such a flag?

Test that name expression must be specified if key is composite.

Properties (i.e. relationships to other attributes in the same dimension)
that have composite keys and/or key not the same as name.

Test that duplicate attribute within dimension gives error.

Test that level referencing non-existent attribute gives error.

ForeignKeyLink: foreignKeyColumn and ForeignKey gives error

ForeignKeyLink: Key and ForeignKey different cardinality gives error

ForeignKeyLink: using foreignKeyColumn if dimension's key is composite
gives error

ForeignKeyLink with Key different to key attribute's key (should we allow
this?)

Test what happens if a dimension's key is not unique. (We should either reject
it or wrap in 'select distinct'.)

ForeignKeyLink, contains ForeignKey, contains Column, whose table
attribute is set and does not equal the alias of the fact table. Should give
error.

ForeignKeyLink references invalid dimension. Should give error.

ForeignKeyLink references invalid, but there is a schema dimension of
that name. Should give more informative error than previous test.

Dimension does not have key attribute set. Should give error.

Dimension's key attribute is not the name of an attribute of the dimension.
Should give error.

If dimension that is usage of schema dimension (i.e. source is set)
has 'key' set, should give warning. E.g. <Dimension source="xx" key="yy"/>

Key of attribute is composite, and columns come from different relations.
Should give error. (Current error comes when validating Dimension.key, should
come earlier, when validating Key.)

If a table is the target of a ForeignKeyLink, and has no defined keys,
should give error that table used in such a way should have a key.
(A bit redundant with 'key does not match known
key of table', but more useful.)

Invalid table in Column in SQL in ExpressionView in ColumnDefs in Table
(similar to defn of 'warehouse_profit') gives error.

Missing table in Column in SQL in ExpressionView in ColumnDefs in Table
(similar to defn of 'warehouse_profit') defaults to table being defined.

Test that get error if add a level to a time dimension whose type is not
years, quarters etc.

Test attribute Attribute@allMemberName.

Test that get error if a dimension has more than one hierarchy with same name.

=== Obsolete ===


*/

    // Tests follow...

    public void testSolveOrderInCalculatedMember() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            null,
            "<CalculatedMember\n"
            + "      name='QuantumProfit'\n"
            + "      dimension='Measures'>\n"
            + "    <Formula>[Measures].[Store Sales] / [Measures].[Store Cost]</Formula>\n"
            + "    <CalculatedMemberProperty name='FORMAT_STRING' value='$#,##0.00'/>\n"
            + "  </CalculatedMember>\n"
            + "<CalculatedMember\n"
            + "      name='foo'\n"
            + "      hierarchy='Gender'>\n"
            + "    <Formula>Sum(Gender.Members)</Formula>\n"
            + "    <CalculatedMemberProperty name='FORMAT_STRING' value='$#,##0.00'/>\n"
            + "    <CalculatedMemberProperty name='SOLVE_ORDER' value=\'2000\'/>\n"
            + "  </CalculatedMember>");

        testContext.assertQueryReturns(
            "select {[Measures].[QuantumProfit]} on 0, {(Gender.foo)} on 1 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[QuantumProfit]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[foo]}\n"
            + "Row #0: $7.52\n");
    }

    public void testHierarchyDefaultMember() {
        checkHierarchyDefaultMember(
            "[Promotion with default].[Media Type].[All Media Types].[TV]")
            .assertQueryReturns(
                "select {[Promotion with default].[Media Type]} on columns from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Promotion with default].[Media Type].[TV]}\n"
                + "Row #0: \n");

        checkHierarchyDefaultMember(
            "[Promotion with default].[Media Type].[All Media Type].[TV]")
            .assertSchemaError(
                TestContext.fragment(
                    "Can not find Default Member with name \"\\[Promotion with default\\].\\[Media Type\\].\\[All Media Type\\].\\[TV\\]\" \\(in Hierarchy 'Media Type'\\) \\(at ${pos}\\)",
                    "<Hierarchy name='Media Type' defaultMember='[Promotion with default].[Media Type].[All Media Type].[TV]'>"));
    }

    private TestContext checkHierarchyDefaultMember(String s) {
        return getTestContext().createSubstitutingCube(
            "Sales",
            "<Dimension name='Promotion with default' table='promotion' key='Promotion Id'>\n"
            + "    <Attributes>\n"
            + "        <Attribute name='Promotion Id' keyColumn='promotion_id'/>\n"
            + "        <Attribute name='Media Type' keyColumn='media_type' hasHierarchy='false'/>\n"
            + "        <Attribute name='Promotion Name' keyColumn='promotion_name'/>\n"
            + "    </Attributes>\n"
            + "    <Hierarchies>\n"
            // Define a default member's whose unique name includes the
            // 'all' member.
            + "        <Hierarchy name='Media Type' defaultMember='"
            + s
            + "'>\n"
            + "            <Level attribute='Media Type'/>\n"
            + "        </Hierarchy>\n"
            + "    </Hierarchies>\n"
            + "</Dimension>\n");
    }

    public void testDimensionRequiresHierarchy() {
        final TestContext testContext =
            getTestContext().createSubstitutingCube(
                "Sales",
                "<Dimension name='Store2' key='Store Id'>\n"
                + "  <Attributes>\n"
                + "    <Attribute name='Store Id' table='store' keyColumn='store_id' hasHierarchy='false'/>\n"
                + "  </Attributes>\n"
                + "</Dimension>");
        testContext.assertErrorList().containsError(
            "Dimension 'Store2' must have at least one hierarchy \\(or attribute hierarchy\\).*",
            "<Dimension name='Store2' key='Store Id'>");
    }

    /** Test that get error if cube has two dimensions with same name. */
    public void testDuplicateDimension() {
        final TestContext testContext =
            getTestContext().createSubstitutingCube(
                "Sales",
                "<Dimension source='Time'/>\n",
                null,
                null,
                null,
                ArrayMap.of(
                    "Sales",
                    "<ForeignKeyLink dimension='Customer2' "
                    + "foreignKeyColumn='customer_id'/>"));
        testContext.assertErrorList().containsError(
            "Duplicate dimension 'Time' \\(in Dimension 'Time'\\) \\(at ${pos}\\)",
            "<Dimension source='Time'/>");
    }

    /** Test that get error if dimension has two attributes with same name. */
    public void testDuplicateAttribute() {
        final TestContext testContext =
            getTestContext().createSubstitutingCube(
                "Sales",
                "<Dimension name='Store' key='Store Id'>\n"
                + "  <Attributes>\n"
                + "    <Attribute name='Store Id' table='store' keyColumn='store_id'/>\n"
                + "    <Attribute name='Store Id'/>\n"
                + "  </Attributes>\n"
                + "</Dimension>");
        testContext.assertErrorList().containsError(
            "Duplicate attribute 'Store Id' in dimension 'Store' \\(in "
            + "Attribute 'Store Id'\\) \\(at ${pos}\\)",
            "<Attribute name='Store Id'/>");
    }

    /** Test that get error if hierarchy has two levels with same name. */
    public void testDuplicateLevel() {
        final TestContext testContext =
            getTestContext().createSubstitutingCube(
                "Sales",
                "<Dimension name='Store' key='Store Id' table='store'>\n"
                + "  <Attributes>\n"
                + "    <Attribute name='Store Id' keyColumn='store_id'/>\n"
                + "    <Attribute name='State' keyColumn='store_state'/>\n"
                + "  </Attributes>\n"
                + "  <Hierarchies>\n"
                + "    <Hierarchy name='Stores'>\n"
                + "      <Level attribute='Store Id'/>\n"
                + "      <Level attribute='State' name='Store Id'/>\n"
                + "    </Hierarchy>\n"
                + "  </Hierarchies>\n"
                + "</Dimension>");
        testContext.assertErrorList().containsError(
            "mondrian.olap.MondrianException: Mondrian Error:"
            + "Level names within hierarchy '\\[Store\\].\\[Stores\\]' are not "
            + "unique; there is more than one level with name 'Store Id'. "
            + "\\(in Level 'Store Id'\\) \\(at ${pos}\\)",
            "<Level attribute='State' name='Store Id'/>");
    }

    /** Test that get error if two cubes have same name. */
    public void testDuplicateCube() {
        final TestContext testContext =
            getTestContext().withSchema(
                "<Schema metamodelVersion='4.0' name='foo'>\n"
                + "<PhysicalSchema>"
                + "  <Table name='sales_fact_1997'/>\n"
                + "</PhysicalSchema>"
                + "<Cube name='Sales'>\n"
                + "  <MeasureGroups>\n"
                + "    <MeasureGroup table='sales_fact_1997'>\n"
                + "      <Measures>\n"
                + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum'/>\n"
                + "      </Measures>\n"
                + "    </MeasureGroup>\n"
                + "  </MeasureGroups>\n"
                + "</Cube>\n"
                + "<Cube  name='Sales'>\n"
                + "  <MeasureGroups>\n"
                + "    <MeasureGroup table='sales_fact_1997'>\n"
                + "      <Measures>\n"
                + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum'/>\n"
                + "      </Measures>\n"
                + "    </MeasureGroup>\n"
                + "  </MeasureGroups>\n"
                + "</Cube>\n"
                + "</Schema>\n");
        testContext.assertErrorList().contains(
            testContext.pattern(
                "Duplicate cube 'Sales' \\(in Cube 'Sales'\\) \\(at ${pos}\\)",
                "<Cube  name='Sales'>"));
    }

    /** Test that get error if two measures in a cube have same name. */
    public void testDuplicateMeasure() {
        final TestContext testContext =
            getTestContext().withSchema(
                "<Schema metamodelVersion='4.0' name='foo'>\n"
                + "<PhysicalSchema>"
                + "  <Table name='sales_fact_1997'/>\n"
                + "</PhysicalSchema>"
                + "<Cube name='Sales'>\n"
                + "  <MeasureGroups>\n"
                + "    <MeasureGroup table='sales_fact_1997'>\n"
                + "      <Measures>\n"
                + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum'/>\n"
                + "        <Measure name='Unit Sales' column='store_cost' aggregator='sum'/>\n"
                + "      </Measures>\n"
                + "    </MeasureGroup>\n"
                + "  </MeasureGroups>\n"
                + "</Cube>\n"
                + "</Schema>\n");
        testContext.assertErrorList().containsError(
            "Duplicate measure 'Unit Sales' in cube 'Sales' "
            + "\\(in Measure 'Unit Sales'\\) \\(at ${pos}\\)",
            "<Measure name='Unit Sales' column='store_cost' aggregator='sum'/>");
    }

    /**
     * Test case for the issue described in
     * <a href="http://forums.pentaho.com/showthread.php?p=190737">Pentaho
     * forum post 'wrong unique name for default member when hasAll=false'</a>.
     */
    public void testDefaultMemberName() {
        final TestContext testContext =
            getTestContext().createSubstitutingCube(
                "Sales",
                "<Dimension name='Product with no all' key='Product Id'>"
                + "        <Attributes>\n"
                + "            <Attribute name='Product Subcategory' table='product_class'>\n"
                + "                <Key>\n"
                + "                    <Column name='product_family'/>\n"
                + "                    <Column name='product_department'/>\n"
                + "                    <Column name='product_category'/>\n"
                + "                    <Column name='product_subcategory'/>\n"
                + "                </Key>\n"
                + "                <Name>\n"
                + "                    <Column name='product_subcategory'/>\n"
                + "                </Name>\n"
                + "                <OrderBy>\n"
                + "                    <Column name='product_family'/>\n"
                + "                    <Column name='product_department'/>\n"
                + "                    <Column name='product_category'/>\n"
                + "                    <Column name='product_subcategory'/>\n"
                + "                </OrderBy>\n"
                + "            </Attribute>\n"
                + "            <Attribute name='Brand Name' table='product'>\n"
                + "                <Key>\n"
                + "                    <Column table='product_class' name='product_family'/>\n"
                + "                    <Column table='product_class' name='product_department'/>\n"
                + "                    <Column table='product_class' name='product_category'/>\n"
                + "                    <Column table='product_class' name='product_subcategory'/>\n"
                + "                    <Column name='brand_name'/>\n"
                + "                </Key>\n"
                + "                <Name>\n"
                + "                    <Column name='brand_name'/>\n"
                + "                </Name>\n"
                + "            </Attribute>\n"
                + "            <Attribute name='Product Name' table='product'\n"
                + "                keyColumn='product_id' nameColumn='product_name'/>\n"
                + "            <Attribute name='Product Id' table='product' keyColumn='product_id'/>"
                + "        </Attributes>\n"
                + "        <Hierarchies>\n"
                + "            <Hierarchy name='Products' allMemberName='All Products' hasAll='false'>\n"
                + "                <Level attribute='Product Subcategory'/>\n"
                + "                <Level attribute='Brand Name'/>\n"
                + "                <Level attribute='Product Name'/>\n"
                + "            </Hierarchy>\n"
                + "        </Hierarchies>\n"
                + "    </Dimension>\n",
                null,
                null,
                null,
                ArrayMap.of(
                    "Sales",
                    "<ForeignKeyLink dimension='Product with no all' "
                    + "foreignKeyColumn='product_id'/>"));

        // note that default member name has no 'all' and has a name not an id
        testContext.assertQueryReturns(
            "select {[Product with no all].[Products]} on columns from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product with no all].[Products].[Beer]}\n"
            + "Row #0: 1,683\n");
    }

    public void testHierarchyAbbreviatedDefaultMember() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "  <Dimension name='Gender with default' table='customer' key='Id'>\n"
            + "  <Attributes>\n"
            + "    <Attribute name='Gender' keyColumn='gender' hierarchyDefaultMember='F'/>\n"
            + "    <Attribute name='Id' keyColumn='customer_id'/>\n"
            + "  </Attributes>\n"
            + "</Dimension>",
            null,
            null,
            null,
            ArrayMap.of(
                "Sales",
                "<ForeignKeyLink dimension='Gender with default' foreignKeyColumn='customer_id'/>"));
        testContext.assertQueryReturns(
            "select {[Gender with default].[Gender]} on columns from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            // Note that the 'all' member is named according to the rule
            // '[<hierarchy>].[All <hierarchy>s]'.
            + "{[Gender with default].[Gender].[F]}\n"
            + "Row #0: 131,558\n");
    }

    public void testHierarchyNoLevelsFails() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "  <Dimension name='Gender no levels' table='customer' key='Name'>\n"
            + " <Attributes>"
            + "<Attribute name='Name' keyColumn='customer_id' nameColumn='full_name' orderByColumn='full_name' hasHierarchy='false'/>"
            + " <Attribute name='Gender' keyColumn='gender'/>"
            + " </Attributes>"
            + "  <Hierarchies>    "
            + "    <Hierarchy name=\"Gender\" hasAll='true' >\n"
            + "    </Hierarchy>"
            + "  </Hierarchies>\n"
            + "</Dimension>");
        testContext.assertQueryThrows(
            "select {[Gender no levels]} on columns from [Sales]",
            "Hierarchy '[Gender no levels].[Gender]' must have at least one level.");
    }

    public void testHierarchyNonUniqueLevelsFails() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "  <Dimension name='Gender dup levels' foreignKey='customer_id' table='customer'>\n"
            + "  <Attributes>"
            + "    <Attribute name='Gender' keyColumn='gender'/>"
            + "  </Attributes>"
            + "  <Hierarchies>    "
            + "    <Hierarchy hasAll='true' primaryKey='customer_id' name=\"Gender\">\n"
            + "      <Level attribute=\"Gender\"/>\n"
            + "      <Level attribute=\"Gender\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Hierarchies>"
            + "</Dimension>");
        testContext.assertQueryThrows(
            "select {[Gender dup levels]} on columns from [Sales]",
            "Level names within hierarchy '[Gender dup levels].[Gender]' are not unique; there is more than one level with name 'Gender'.");
    }

    /**
     * Tests a measure based on 'count'.
     */
    public void testCountMeasure() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            null,
            "<Measure name='Fact Count' aggregator='count'/>\n", null, null);
        testContext.assertQueryReturns(
            "select {[Measures].[Fact Count], [Measures].[Unit Sales]} on 0,\n"
            + "[Gender].members on 1\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Fact Count]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[All Gender]}\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Row #0: 86,837\n"
            + "Row #0: 266,773\n"
            + "Row #1: 42,831\n"
            + "Row #1: 131,558\n"
            + "Row #2: 44,006\n"
            + "Row #2: 135,215\n");
    }

    public void testBadMeasure1() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            null,
            "<Measure name='Bad Measure' aggregator='sum' formatString='Standard'/>\n",
            null, null);
        Throwable throwable = null;
        try {
            testContext.assertSimpleQuery();
        } catch (Throwable e) {
            throwable = e;
        }
        // neither a source column or source expression specified
        TestContext.checkThrowable(
            throwable,
            "must contain either a source column or a source expression, but not both");
    }

    public void testBadMeasure2() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            null,
            "<Measure name='Bad Measure' column='unit_sales' aggregator='sum' formatString='Standard'>\n"
            + "  <Arguments>\n"
            + "    <Column name='unit_sales'/>\n"
            + "  </Arguments>"
            + "</Measure>", null, null);
        testContext.assertErrorList().containsError(
            "must not specify both column and Arguments \\(in Arguments\\) \\(at ${pos}\\)",
            "<Arguments>");
    }

    /**
     * Tests that an error occurs if a hierarchy is based on a non-existent
     * table.
     */
    public void testHierarchyTableNotFound() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "<Dimension name='Yearly Income3' foreignKey='product_id' table='customer_not_found'>\n"
            + "  <Attributes>"
            + "    <Attribute name='Yearly Income' keyColumn='yearly_income'/>"
            + "  </Attributes>"
            + "</Dimension>");
        testContext.assertQueryThrows(
            "select from [Sales]",
            "table 'customer_not_found' not found (in Dimension 'Yearly Income3')");
    }

    public void testAttributeTableNotFound() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "<Dimension name='Yearly Income4' foreignKey='product_id' table='customer'>\n"
            + "  <Attributes>"
            + "    <Attribute name='Yearly Income' keyColumn='yearly_income' table='customer_not_found'/>"
            + "  </Attributes>"
            + "</Dimension>");
        testContext.assertErrorList().containsError(
            "table 'customer_not_found' not found \\(in Attribute 'Yearly Income'\\) \\(at ${pos}\\)",
            "<Attribute name='Yearly Income' keyColumn='yearly_income' table='customer_not_found'/>");
    }

    /**
     * WG: Note, this no longer throws an exception with the new RolapCubeMember
     * functionality.
     *
     * <p>Tests that an error is issued if two dimensions use the same table via
     * different drill-paths and do not use a different alias. If this error is
     * not issued, the generated SQL can be missing a join condition, as in
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-236">
     * Bug MONDRIAN-236, "Mondrian generates invalid SQL"</a>.
     */
    public void testDuplicateTableAlias() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "<Dimension name='Yearly Income2' table='customer' key='Name'>\n"
            + "  <Attributes>"
            + "    <Attribute name='Name' keyColumn='customer_id' nameColumn='full_name' orderByColumn='full_name' hasHierarchy='false'/>"
            + "    <Attribute name='Yearly Income' keyColumn='yearly_income' hierarchyAllMemberName='All Yearly Incomes'/>\n"
            + "  </Attributes>"
            + "</Dimension>");

        testContext.assertQueryReturns(
            "select {[Yearly Income2]} on columns, {[Measures].[Unit Sales]} on rows from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Yearly Income2].[Yearly Income].[All Yearly Incomes]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 266,773\n");
    }

    /**
     * This result is somewhat peculiar. If two dimensions share a foreign key,
     * what is the expected result?  Also, in this case, they share the same
     * table without an alias, and the system doesn't complain.
     */
    public void testDuplicateTableAliasSameForeignKey() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "<Dimension name='Yearly Income2' table='customer' key='Id'>\n"
            + "  <Attributes>"
            + "    <Attribute name='Yearly Income' keyColumn='yearly_income'/>"
            + "    <Attribute name='Id' keyColumn='customer_id'/>"
            + "  </Attributes>"
            + "</Dimension>",
            null,
            null,
            null,
            ArrayMap.of(
                "Sales",
                "<ForeignKeyLink dimension='Yearly Income2' foreignKeyColumn='customer_id'/>"));
        testContext.assertQueryReturns(
            "select from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "266,773");

        // NonEmptyCrossJoin Fails
        if (false) {
            testContext.assertQueryReturns(
                "select NonEmptyCrossJoin({[Yearly Income2].[Yearly Income].[All Yearly Income]},{[Customers].[All Customers]}) on rows,"
                + "NON EMPTY {[Measures].[Unit Sales]} on columns"
                + " from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "266,773");
        }
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * Without the table alias, generates SQL which is missing a join condition.
     * See {@link #testDuplicateTableAlias()}.
     */
    public void testDimensionsShareTable() {
        final TestContext testContext = getTestContext().insertDimension(
            "Sales",
            "<Dimension name='Yearly Income2' key='id' table='customerx'>\n"
            + "    <Attributes>"
            + "      <Attribute name='Yearly Income' keyColumn='yearly_income'/>"
            + "      <Attribute name='id' keyColumn='customer_id' hasHierarchy='false'/>"
            + "     </Attributes>"
            + "</Dimension>"
            + "<Dimension name='Yearly Income' key='id' table='customer'>\n"
            + "    <Attributes>"
            + "      <Attribute name='Yearly Income' keyColumn='yearly_income'/>"
            + "      <Attribute name='id' keyColumn='customer_id' hasHierarchy='false'/>"
            + "     </Attributes>"
            + "</Dimension>")
            .insertPhysTable("<Table name='customer' alias='customerx'/>")
            .insertDimensionLinks(
                "Sales",
                ArrayMap.of(
                    "Sales",
                    "<ForeignKeyLink dimension='Yearly Income2' foreignKeyColumn='product_id'/>"
                    + "<ForeignKeyLink dimension='Yearly Income' foreignKeyColumn='customer_id'/>"))
            .ignoreMissingLink();

        testContext.assertQueryReturns(
            "select {[Yearly Income].[Yearly Income].[$10K - $30K]} on columns,"
            + "{[Yearly Income2].[Yearly Income].[$150K +]} on rows from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Yearly Income].[Yearly Income].[$10K - $30K]}\n"
            + "Axis #2:\n"
            + "{[Yearly Income2].[Yearly Income].[$150K +]}\n"
            + "Row #0: 918\n");

        testContext.assertQueryReturns(
            "select NON EMPTY {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "NON EMPTY Crossjoin({[Yearly Income].[All Yearly Income].Children},\n"
            + "                     [Yearly Income2].[All Yearly Income].Children) ON ROWS\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Yearly Income].[Yearly Income].[$10K - $30K], [Yearly Income2].[Yearly Income].[$10K - $30K]}\n"
            + "{[Yearly Income].[Yearly Income].[$10K - $30K], [Yearly Income2].[Yearly Income].[$110K - $130K]}\n"
            + "{[Yearly Income].[Yearly Income].[$10K - $30K], [Yearly Income2].[Yearly Income].[$130K - $150K]}\n"
            + "{[Yearly Income].[Yearly Income].[$10K - $30K], [Yearly Income2].[Yearly Income].[$150K +]}\n"
            + "{[Yearly Income].[Yearly Income].[$10K - $30K], [Yearly Income2].[Yearly Income].[$30K - $50K]}\n"
            + "{[Yearly Income].[Yearly Income].[$10K - $30K], [Yearly Income2].[Yearly Income].[$50K - $70K]}\n"
            + "{[Yearly Income].[Yearly Income].[$10K - $30K], [Yearly Income2].[Yearly Income].[$70K - $90K]}\n"
            + "{[Yearly Income].[Yearly Income].[$10K - $30K], [Yearly Income2].[Yearly Income].[$90K - $110K]}\n"
            + "{[Yearly Income].[Yearly Income].[$110K - $130K], [Yearly Income2].[Yearly Income].[$10K - $30K]}\n"
            + "{[Yearly Income].[Yearly Income].[$110K - $130K], [Yearly Income2].[Yearly Income].[$110K - $130K]}\n"
            + "{[Yearly Income].[Yearly Income].[$110K - $130K], [Yearly Income2].[Yearly Income].[$130K - $150K]}\n"
            + "{[Yearly Income].[Yearly Income].[$110K - $130K], [Yearly Income2].[Yearly Income].[$150K +]}\n"
            + "{[Yearly Income].[Yearly Income].[$110K - $130K], [Yearly Income2].[Yearly Income].[$30K - $50K]}\n"
            + "{[Yearly Income].[Yearly Income].[$110K - $130K], [Yearly Income2].[Yearly Income].[$50K - $70K]}\n"
            + "{[Yearly Income].[Yearly Income].[$110K - $130K], [Yearly Income2].[Yearly Income].[$70K - $90K]}\n"
            + "{[Yearly Income].[Yearly Income].[$110K - $130K], [Yearly Income2].[Yearly Income].[$90K - $110K]}\n"
            + "{[Yearly Income].[Yearly Income].[$130K - $150K], [Yearly Income2].[Yearly Income].[$10K - $30K]}\n"
            + "{[Yearly Income].[Yearly Income].[$130K - $150K], [Yearly Income2].[Yearly Income].[$110K - $130K]}\n"
            + "{[Yearly Income].[Yearly Income].[$130K - $150K], [Yearly Income2].[Yearly Income].[$130K - $150K]}\n"
            + "{[Yearly Income].[Yearly Income].[$130K - $150K], [Yearly Income2].[Yearly Income].[$150K +]}\n"
            + "{[Yearly Income].[Yearly Income].[$130K - $150K], [Yearly Income2].[Yearly Income].[$30K - $50K]}\n"
            + "{[Yearly Income].[Yearly Income].[$130K - $150K], [Yearly Income2].[Yearly Income].[$50K - $70K]}\n"
            + "{[Yearly Income].[Yearly Income].[$130K - $150K], [Yearly Income2].[Yearly Income].[$70K - $90K]}\n"
            + "{[Yearly Income].[Yearly Income].[$130K - $150K], [Yearly Income2].[Yearly Income].[$90K - $110K]}\n"
            + "{[Yearly Income].[Yearly Income].[$150K +], [Yearly Income2].[Yearly Income].[$10K - $30K]}\n"
            + "{[Yearly Income].[Yearly Income].[$150K +], [Yearly Income2].[Yearly Income].[$110K - $130K]}\n"
            + "{[Yearly Income].[Yearly Income].[$150K +], [Yearly Income2].[Yearly Income].[$130K - $150K]}\n"
            + "{[Yearly Income].[Yearly Income].[$150K +], [Yearly Income2].[Yearly Income].[$150K +]}\n"
            + "{[Yearly Income].[Yearly Income].[$150K +], [Yearly Income2].[Yearly Income].[$30K - $50K]}\n"
            + "{[Yearly Income].[Yearly Income].[$150K +], [Yearly Income2].[Yearly Income].[$50K - $70K]}\n"
            + "{[Yearly Income].[Yearly Income].[$150K +], [Yearly Income2].[Yearly Income].[$70K - $90K]}\n"
            + "{[Yearly Income].[Yearly Income].[$150K +], [Yearly Income2].[Yearly Income].[$90K - $110K]}\n"
            + "{[Yearly Income].[Yearly Income].[$30K - $50K], [Yearly Income2].[Yearly Income].[$10K - $30K]}\n"
            + "{[Yearly Income].[Yearly Income].[$30K - $50K], [Yearly Income2].[Yearly Income].[$110K - $130K]}\n"
            + "{[Yearly Income].[Yearly Income].[$30K - $50K], [Yearly Income2].[Yearly Income].[$130K - $150K]}\n"
            + "{[Yearly Income].[Yearly Income].[$30K - $50K], [Yearly Income2].[Yearly Income].[$150K +]}\n"
            + "{[Yearly Income].[Yearly Income].[$30K - $50K], [Yearly Income2].[Yearly Income].[$30K - $50K]}\n"
            + "{[Yearly Income].[Yearly Income].[$30K - $50K], [Yearly Income2].[Yearly Income].[$50K - $70K]}\n"
            + "{[Yearly Income].[Yearly Income].[$30K - $50K], [Yearly Income2].[Yearly Income].[$70K - $90K]}\n"
            + "{[Yearly Income].[Yearly Income].[$30K - $50K], [Yearly Income2].[Yearly Income].[$90K - $110K]}\n"
            + "{[Yearly Income].[Yearly Income].[$50K - $70K], [Yearly Income2].[Yearly Income].[$10K - $30K]}\n"
            + "{[Yearly Income].[Yearly Income].[$50K - $70K], [Yearly Income2].[Yearly Income].[$110K - $130K]}\n"
            + "{[Yearly Income].[Yearly Income].[$50K - $70K], [Yearly Income2].[Yearly Income].[$130K - $150K]}\n"
            + "{[Yearly Income].[Yearly Income].[$50K - $70K], [Yearly Income2].[Yearly Income].[$150K +]}\n"
            + "{[Yearly Income].[Yearly Income].[$50K - $70K], [Yearly Income2].[Yearly Income].[$30K - $50K]}\n"
            + "{[Yearly Income].[Yearly Income].[$50K - $70K], [Yearly Income2].[Yearly Income].[$50K - $70K]}\n"
            + "{[Yearly Income].[Yearly Income].[$50K - $70K], [Yearly Income2].[Yearly Income].[$70K - $90K]}\n"
            + "{[Yearly Income].[Yearly Income].[$50K - $70K], [Yearly Income2].[Yearly Income].[$90K - $110K]}\n"
            + "{[Yearly Income].[Yearly Income].[$70K - $90K], [Yearly Income2].[Yearly Income].[$10K - $30K]}\n"
            + "{[Yearly Income].[Yearly Income].[$70K - $90K], [Yearly Income2].[Yearly Income].[$110K - $130K]}\n"
            + "{[Yearly Income].[Yearly Income].[$70K - $90K], [Yearly Income2].[Yearly Income].[$130K - $150K]}\n"
            + "{[Yearly Income].[Yearly Income].[$70K - $90K], [Yearly Income2].[Yearly Income].[$150K +]}\n"
            + "{[Yearly Income].[Yearly Income].[$70K - $90K], [Yearly Income2].[Yearly Income].[$30K - $50K]}\n"
            + "{[Yearly Income].[Yearly Income].[$70K - $90K], [Yearly Income2].[Yearly Income].[$50K - $70K]}\n"
            + "{[Yearly Income].[Yearly Income].[$70K - $90K], [Yearly Income2].[Yearly Income].[$70K - $90K]}\n"
            + "{[Yearly Income].[Yearly Income].[$70K - $90K], [Yearly Income2].[Yearly Income].[$90K - $110K]}\n"
            + "{[Yearly Income].[Yearly Income].[$90K - $110K], [Yearly Income2].[Yearly Income].[$10K - $30K]}\n"
            + "{[Yearly Income].[Yearly Income].[$90K - $110K], [Yearly Income2].[Yearly Income].[$110K - $130K]}\n"
            + "{[Yearly Income].[Yearly Income].[$90K - $110K], [Yearly Income2].[Yearly Income].[$130K - $150K]}\n"
            + "{[Yearly Income].[Yearly Income].[$90K - $110K], [Yearly Income2].[Yearly Income].[$150K +]}\n"
            + "{[Yearly Income].[Yearly Income].[$90K - $110K], [Yearly Income2].[Yearly Income].[$30K - $50K]}\n"
            + "{[Yearly Income].[Yearly Income].[$90K - $110K], [Yearly Income2].[Yearly Income].[$50K - $70K]}\n"
            + "{[Yearly Income].[Yearly Income].[$90K - $110K], [Yearly Income2].[Yearly Income].[$70K - $90K]}\n"
            + "{[Yearly Income].[Yearly Income].[$90K - $110K], [Yearly Income2].[Yearly Income].[$90K - $110K]}\n"
            + "Row #0: 12,824\n"
            + "Row #1: 2,822\n"
            + "Row #2: 2,933\n"
            + "Row #3: 918\n"
            + "Row #4: 18,381\n"
            + "Row #5: 10,436\n"
            + "Row #6: 6,777\n"
            + "Row #7: 2,859\n"
            + "Row #8: 2,432\n"
            + "Row #9: 532\n"
            + "Row #10: 566\n"
            + "Row #11: 177\n"
            + "Row #12: 3,877\n"
            + "Row #13: 2,131\n"
            + "Row #14: 1,319\n"
            + "Row #15: 527\n"
            + "Row #16: 3,331\n"
            + "Row #17: 643\n"
            + "Row #18: 703\n"
            + "Row #19: 187\n"
            + "Row #20: 4,497\n"
            + "Row #21: 2,629\n"
            + "Row #22: 1,681\n"
            + "Row #23: 721\n"
            + "Row #24: 1,123\n"
            + "Row #25: 224\n"
            + "Row #26: 257\n"
            + "Row #27: 109\n"
            + "Row #28: 1,924\n"
            + "Row #29: 1,026\n"
            + "Row #30: 675\n"
            + "Row #31: 291\n"
            + "Row #32: 19,067\n"
            + "Row #33: 4,078\n"
            + "Row #34: 4,235\n"
            + "Row #35: 1,569\n"
            + "Row #36: 28,160\n"
            + "Row #37: 15,368\n"
            + "Row #38: 10,329\n"
            + "Row #39: 4,504\n"
            + "Row #40: 9,708\n"
            + "Row #41: 2,353\n"
            + "Row #42: 2,243\n"
            + "Row #43: 748\n"
            + "Row #44: 14,469\n"
            + "Row #45: 7,966\n"
            + "Row #46: 5,272\n"
            + "Row #47: 2,208\n"
            + "Row #48: 7,320\n"
            + "Row #49: 1,630\n"
            + "Row #50: 1,602\n"
            + "Row #51: 541\n"
            + "Row #52: 10,550\n"
            + "Row #53: 5,843\n"
            + "Row #54: 3,997\n"
            + "Row #55: 1,562\n"
            + "Row #56: 2,722\n"
            + "Row #57: 597\n"
            + "Row #58: 568\n"
            + "Row #59: 193\n"
            + "Row #60: 3,800\n"
            + "Row #61: 2,192\n"
            + "Row #62: 1,324\n"
            + "Row #63: 523\n");
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * native non empty cross join sql generation returns empty query.
     * note that this works when native cross join is disabled
     */
    public void testDimensionsShareTableNativeNonEmptyCrossJoin() {
        final TestContext testContext = getTestContext().insertDimension(
            "Sales",
            "<Dimension name='Yearly Income2' key='id' table='customerx'>\n"
            + "    <Attributes>"
            + "      <Attribute name='Yearly Income' keyColumn='yearly_income'/>"
            + "      <Attribute name='id' keyColumn='customer_id' hasHierarchy='false'/>"
            + "     </Attributes>"
            + "</Dimension>")
            .insertPhysTable("<Table name='customer' alias='customerx'/>")
            .insertDimensionLinks(
                "Sales",
                ArrayMap.of(
                    "Sales",
                    "<ForeignKeyLink dimension='Yearly Income2' foreignKeyColumn='customer_id'/>"))
            .ignoreMissingLink();

        testContext.assertQueryReturns(
            "select NonEmptyCrossJoin({[Yearly Income2].[Yearly Income].[All Yearly Income]},{[Customers].[All Customers]}) on rows,"
            + "NON EMPTY {[Measures].[Unit Sales]} on columns"
            + " from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Yearly Income2].[Yearly Income].[All Yearly Income], [Customer].[Customers].[All Customers]}\n"
            + "Row #0: 266,773\n");
    }

    /**
     * Tests two dimensions using same table with same foreign key
     * one table uses an alias.
     */
    public void testDimensionsShareTableSameForeignKeys() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "<Dimension name='Yearly Income2' table='customer' alias='customerx' key='Name'>\n"
            + "  <Attributes>"
            + "    <Attribute name='Yearly Income2' keyColumn='yearly_income' uniqueMembers='true'/>"
            + "    <Attribute name='Name' keyColumn='customer_id' nameColumn='full_name' orderByColumn='full_name' hasHierarchy='false'/>"
            + "  </Attributes>"
            + "</Dimension>",
            null,
            null,
            null,
            ArrayMap.of(
                "Sales",
                "<ForeignKeyLink dimension='Yearly Income2' foreignKeyColumn='customer_id'/>"));

        testContext.assertQueryReturns(
            "select {[Yearly Income].[$10K - $30K]} on columns,"
            + "{[Yearly Income2].[$150K +]} on rows from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Yearly Income].[$10K - $30K]}\n"
            + "Axis #2:\n"
            + "{[Yearly Income2].[Yearly Income2].[$150K +]}\n"
            + "Row #0: \n");

        testContext.assertQueryReturns(
            "select NON EMPTY {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "NON EMPTY Crossjoin({[Yearly Income].[All Yearly Incomes].Children},\n"
            + "                     [Yearly Income2].[All Yearly Income2].Children) ON ROWS\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Yearly Income].[$10K - $30K], [Yearly Income2].[Yearly Income2].[$10K - $30K]}\n"
            + "{[Customer].[Yearly Income].[$110K - $130K], [Yearly Income2].[Yearly Income2].[$110K - $130K]}\n"
            + "{[Customer].[Yearly Income].[$130K - $150K], [Yearly Income2].[Yearly Income2].[$130K - $150K]}\n"
            + "{[Customer].[Yearly Income].[$150K +], [Yearly Income2].[Yearly Income2].[$150K +]}\n"
            + "{[Customer].[Yearly Income].[$30K - $50K], [Yearly Income2].[Yearly Income2].[$30K - $50K]}\n"
            + "{[Customer].[Yearly Income].[$50K - $70K], [Yearly Income2].[Yearly Income2].[$50K - $70K]}\n"
            + "{[Customer].[Yearly Income].[$70K - $90K], [Yearly Income2].[Yearly Income2].[$70K - $90K]}\n"
            + "{[Customer].[Yearly Income].[$90K - $110K], [Yearly Income2].[Yearly Income2].[$90K - $110K]}\n"
            + "Row #0: 57,950\n"
            + "Row #1: 11,561\n"
            + "Row #2: 14,392\n"
            + "Row #3: 5,629\n"
            + "Row #4: 87,310\n"
            + "Row #5: 44,967\n"
            + "Row #6: 33,045\n"
            + "Row #7: 11,919\n");
    }

    /**
     * test hierarchy with slightly different join path to fact table than
     * first hierarchy. tables from first and second hierarchy should contain
     * the same join aliases to the fact table.
     */
    public void testSnowflakeHierarchyValidationNotNeeded() {
        if (!Bug.BugMondrian1324Fixed) {
            return;
        }
        final String tableDefs =
            "<Table name='region' keyColumn='region_id'/>"
            + "<Link target='store' source='region' foreignKeyColumn='region_id'/>"
            + "<Link target='customer' source='region' foreignKeyColumn='customer_region_id'/>";
        final String cubeDef =
            "<Cube name='AliasedDimensionsTesting' defaultMeasure='Unit Sales'>\n"
            + "  <Dimensions>"
            + "    <Dimension name='Store' key='store_id'>"
            + "      <Attributes>"
            + "        <Attribute name='Store Country' table='store' keyColumn='store_country' uniqueMembers='true'/>\n"
            + "        <Attribute name='Store Region' table='region' keyColumn='sales_region' />\n"
            + "        <Attribute name='Store Name' table='store' keyColumn='store_name' />\n"
            + "        <Attribute name='store_id' table='store' keyColumn='store_id' />\n"
            + "      </Attributes>"
            + "      <Hierarchies>"
            + "        <Hierarchy name='Store' hasAll='true'>\n"
            + "          <Level attribute='Store Country'/>\n"
            + "          <Level attribute='Store Region'/>\n"
            + "          <Level attribute='Store Name'/>\n"
            + "        </Hierarchy>"
            + "        <Hierarchy name='MyHierarchy' hasAll='true'>\n"
            + "          <Level attribute='Store Country'/>\n"
            + "          <Level attribute='Store Region'/>\n"
            + "          <Level attribute='Store Name'/>\n"
            + "        </Hierarchy>\n"
            + "      </Hierarchies>"
            + "  </Dimension>\n"
            + "  <Dimension name='Customers' key='Name'>\n"
            + "      <Attributes>"
            + "    <Attribute name='Country' table='customer' keyColumn='country' uniqueMembers='true'/>\n"
            + "    <Attribute name='Region' table='region' keyColumn='sales_region' uniqueMembers='true'/>\n"
            + "    <Attribute name='City' table='customer' keyColumn='city' uniqueMembers='false'/>\n"
            + "    <Attribute name='Name' table='customer' keyColumn='customer_id' type='Numeric' uniqueMembers='true'/>\n"
            + "      </Attributes>"
            + "      <Hierarchies>"
            + "    <Hierarchy name='Customers' hasAll='true' allMemberName='All Customers'>\n"
            + "    <Level attribute='Country'/>\n"
            + "    <Level attribute='Region'/>\n"
            + "    <Level attribute='City'/>\n"
            + "    <Level attribute='Name'/>\n"
            + "  </Hierarchy>\n"
            + "     </Hierarchies>"
            + "</Dimension>\n"
            + "</Dimensions>"
            + "  <MeasureGroups>"
            + "     <MeasureGroup table='sales_fact_1997'>"
            + "       <Measures>"
            + "       <Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='Standard'/>\n"
            + "        </Measures>"
            + "        <DimensionLinks>"
            + "           <ForeignKeyLink dimension='Store' foreignKeyColumn='store_id'/>"
            + "           <ForeignKeyLink dimension='Customers' foreignKeyColumn='customer_id'/>"
            + "        </DimensionLinks>"
            + "      </MeasureGroup>"
            + "    </MeasureGroups>"
            + "</Cube>";
        final TestContext testContext = getTestContext()
            .insertCube(cubeDef)
            .insertPhysTable(tableDefs);

        testContext.assertQueryReturns(
            "select  {[Store.MyHierarchy].[USA].[South West]} on rows,"
            + "{[Customers].[USA].[South West]} on columns"
            + " from "
            + "AliasedDimensionsTesting",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[USA].[South West]}\n"
            + "Axis #2:\n"
            + "{[Store].[MyHierarchy].[USA].[South West]}\n"
            + "Row #0: 72,631\n");
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * both using a table alias.
     */
    public void testDimensionsShareJoinTable() {
        if (!Bug.BugMondrian1324Fixed) {
            return;
        }
        final String tableDefs =
            "<Table name='region' keyColumn='region_id'/>"
            + "<Link target='store' source='region' foreignKeyColumn='region_id'/>"
            + "<Link target='customer' source='region' foreignKeyColumn='customer_region_id'/>";
        final String cubeDef =
            "<Cube name='AliasedDimensionsTesting' defaultMeasure='Unit Sales'>\n"
            + "  <Dimensions>"
            + "    <Dimension name='Store' key='store_id'>"
            + "      <Attributes>\n"
            + "        <Attribute name='Store Country' table='store' keyColumn='store_country' hasHierarchy='false'/>"
            + "        <Attribute name='Store Region' table='region' keyColumn='sales_region' hasHierarchy='false'/>"
            + "        <Attribute name='Store Name' table='store' keyColumn='store_name' hasHierarchy='false'/>"
            + "        <Attribute name='store_id' table='store' keyColumn='store_id' hasHierarchy='false'/>"
            + "      </Attributes>"
            + "      <Hierarchies>"
            + "        <Hierarchy name='Store' hasAll='true' primaryKeyTable='store' primaryKey='store_id'>\n"
            + "          <Level attribute='Store Country'/>\n"
            + "          <Level attribute='Store Region'/>\n"
            + "          <Level attribute='Store Name'/>\n"
            + "        </Hierarchy>"
            + "      </Hierarchies>\n"
            + "    </Dimension>\n"
            + "    <Dimension name='Customers' key='Name'>"
            + "      <Attributes>"
            + "        <Attribute name='Country' table='customer' keyColumn='country'/>\n"
            + "        <Attribute name='Region'  table='region'   keyColumn='sales_region'/>\n"
            + "        <Attribute name='City'    table='customer' keyColumn='city'/>\n"
            + "        <Attribute name='Name'    table='customer' keyColumn='customer_id'/>"
            + "      </Attributes>\n"
            + "      <Hierarchies>"
            + "        <Hierarchy name='Customers' hasAll='true' allMemberName='All Customers' primaryKeyTable='customer' primaryKey='customer_id'>\n"
            + "          <Level attribute='Country'/>\n"
            + "          <Level attribute='Region'/>\n"
            + "          <Level attribute='City'/>\n"
            + "          <Level attribute='Name'/>\n"
            + "        </Hierarchy>\n"
            + "      </Hierarchies>"
            + "    </Dimension>\n"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup table='sales_fact_1997'>"
            + "      <Measures>"
            + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='Standard'/>\n"
            + "        <Measure name='Store Sales' column='store_sales' aggregator='sum' formatString='#,###.00'/>\n"
            + "      </Measures>"
            + "      <DimensionLinks>"
            + "        <ForeignKeyLink dimension='Store' foreignKeyColumn='store_id'/>"
            + "        <ForeignKeyLink dimension='Customers' foreignKeyColumn='customer_id'/>"
            + "      </DimensionLinks>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>"
            + "</Cube>";
        final TestContext testContext = getTestContext()
            .insertCube(cubeDef)
            .insertPhysTable(tableDefs);

        testContext.assertQueryReturns(
            "select  {[Store].[USA].[South West]} on rows,"
            + "{[Customers].[USA].[South West]} on columns"
            + " from "
            + "AliasedDimensionsTesting",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[USA].[South West]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[South West]}\n"
            + "Row #0: 72,631\n");
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * both using a table alias.
     */
    public void testDimensionsShareJoinTableOneAlias() {
        final String cubeDef =
            "<Cube name='AliasedDimensionsTesting' defaultMeasure='Unit Sales'>\n"
            + "  <Dimensions>"
            + "    <Dimension name='Store' key='store_id'>\n"
            + "      <Attributes>"
            + "        <Attribute name='Store Country' table='store'  keyColumn='store_country'/>\n"
            + "        <Attribute name='Store Region'  table='region' keyColumn='sales_region'/>\n"
            + "        <Attribute name='Store Name'    table='store'  keyColumn='store_name'/>\n"
            + "        <Attribute name='store_id'      table='store'  keyColumn='store_id'/>\n"
            + "      </Attributes>"
            + "      <Hierarchies>"
            + "        <Hierarchy hasAll='true' name='Store'>\n"
            + "          <Level attribute='Store Country'/>\n"
            + "          <Level attribute='Store Region'/>\n"
            + "          <Level attribute='Store Name'/>\n"
            + "        </Hierarchy>\n"
            + "      </Hierarchies>"
            + "    </Dimension>\n"
            + "    <Dimension name='Customers' key='Name'>\n"
            + "      <Attributes>"
            + "        <Attribute name='Country' table='customer' keyColumn='country' uniqueMembers='true'/>\n"
            + "        <Attribute name='Region'  table='customer_region'   keyColumn='sales_region' uniqueMembers='true'/>\n"
            + "        <Attribute name='City'    table='customer' keyColumn='city' uniqueMembers='false'/>\n"
            + "        <Attribute name='Name'    table='customer' keyColumn='customer_id' type='Numeric' uniqueMembers='true'/>\n"
            + "      </Attributes>"
            + "      <Hierarchies>"
            + "        <Hierarchy hasAll='true' allMemberName='All Customers' name='Customers'>\n"
            + "          <Level attribute='Country'/>\n"
            + "          <Level attribute='Region'/>\n"
            + "          <Level attribute='City'/>\n"
            + "          <Level attribute='Name'/>\n"
            + "        </Hierarchy>"
            + "      </Hierarchies>\n"
            + "    </Dimension>\n"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup table='sales_fact_1997'>"
            + "      <Measures>"
            + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='Standard'/>\n"
            + "        <Measure name='Store Sales' column='store_sales' aggregator='sum' formatString='#,###.00'/>\n"
            + "      </Measures>"
            + "      <DimensionLinks>"
            + "        <ForeignKeyLink dimension='Store' foreignKeyColumn='store_id'/>"
            + "        <ForeignKeyLink dimension='Customers' foreignKeyColumn='customer_id'/>"
            + "      </DimensionLinks>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>"
            + "</Cube>";
        final String tableDefs =
            "<Table name='region' keyColumn='region_id' alias='customer_region'/>"
            + "<Table name='region' keyColumn='region_id'/>"
            + "<Link target='store' source='region' foreignKeyColumn='region_id'/>"
            + "<Link target='customer' source='customer_region' foreignKeyColumn='customer_region_id'/>";
        final TestContext testContext = getTestContext()
            .insertCube(cubeDef)
            .insertPhysTable(tableDefs);

        testContext.assertQueryReturns(
            "select  {[Store].[USA].[South West]} on rows,"
            + "{[Customers].[USA].[South West]} on columns"
            + " from "
            + "AliasedDimensionsTesting",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customers].[Customers].[USA].[South West]}\n"
            + "Axis #2:\n"
            + "{[Store].[Store].[USA].[South West]}\n"
            + "Row #0: 72,631\n");
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * both using a table alias.
     */
    public void testDimensionsShareJoinTableTwoAliases() {
        final String cubeDef =
            "<Cube name='AliasedDimensionsTesting' defaultMeasure='Unit Sales'>\n"
            + "  <Dimensions>"
            + "    <Dimension name='Store' key='store_id'>\n"
            + "      <Attributes>"
            + "        <Attribute name='Store Country' table='store'  keyColumn='store_country'/>\n"
            + "        <Attribute name='Store Region'  table='store_region' keyColumn='sales_region'/>\n"
            + "        <Attribute name='Store Name'    table='store'  keyColumn='store_name'/>\n"
            + "        <Attribute name='store_id'      table='store'  keyColumn='store_id'/>\n"
            + "      </Attributes>"
            + "      <Hierarchies>"
            + "        <Hierarchy hasAll='true' name='Store'>\n"
            + "          <Level attribute='Store Country'/>\n"
            + "          <Level attribute='Store Region'/>\n"
            + "          <Level attribute='Store Name'/>\n"
            + "        </Hierarchy>\n"
            + "      </Hierarchies>"
            + "    </Dimension>\n"
            + "    <Dimension name='Customers' key='Name'>\n"
            + "      <Attributes>"
            + "        <Attribute name='Country' table='customer' keyColumn='country' uniqueMembers='true'/>\n"
            + "        <Attribute name='Region'  table='customer_region'   keyColumn='sales_region' uniqueMembers='true'/>\n"
            + "        <Attribute name='City'    table='customer' keyColumn='city' uniqueMembers='false'/>\n"
            + "        <Attribute name='Name'    table='customer' keyColumn='customer_id' type='Numeric' uniqueMembers='true'/>\n"
            + "      </Attributes>"
            + "      <Hierarchies>"
            + "        <Hierarchy hasAll='true' allMemberName='All Customers' name='Customers'>\n"
            + "          <Level attribute='Country'/>\n"
            + "          <Level attribute='Region'/>\n"
            + "          <Level attribute='City'/>\n"
            + "          <Level attribute='Name'/>\n"
            + "        </Hierarchy>"
            + "      </Hierarchies>\n"
            + "    </Dimension>\n"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup table='sales_fact_1997'>"
            + "      <Measures>"
            + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='Standard'/>\n"
            + "        <Measure name='Store Sales' column='store_sales' aggregator='sum' formatString='#,###.00'/>\n"
            + "      </Measures>"
            + "      <DimensionLinks>"
            + "        <ForeignKeyLink dimension='Store' foreignKeyColumn='store_id'/>"
            + "        <ForeignKeyLink dimension='Customers' foreignKeyColumn='customer_id'/>"
            + "      </DimensionLinks>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>"
            + "</Cube>";
        final String tableDefs =
            "<Table name='region' keyColumn='region_id' alias='customer_region'/>"
            + "<Table name='region' keyColumn='region_id' alias='store_region'/>"
            + "<Link target='store' source='store_region' foreignKeyColumn='region_id'/>"
            + "<Link target='customer' source='customer_region' foreignKeyColumn='customer_region_id'/>";
        final TestContext testContext = getTestContext()
            .insertCube(cubeDef)
            .insertPhysTable(tableDefs);

        testContext.assertQueryReturns(
            "select  {[Store].[USA].[South West]} on rows,"
            + "{[Customers].[USA].[South West]} on columns"
            + " from "
            + "AliasedDimensionsTesting",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customers].[Customers].[USA].[South West]}\n"
            + "Axis #2:\n"
            + "{[Store].[Store].[USA].[South West]}\n"
            + "Row #0: 72,631\n");
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * both using a table alias.
     */
    public void testTwoAliasesDimensionsShareTable() {
        final String cubeDef =
            "<Cube name='AliasedDimensionsTesting' defaultMeasure='Supply Time'>\n"
            + "  <Dimensions>"
            + "    <Dimension name='StoreA' key='id' table='storea'>"
            + "      <Attributes>"
            + "        <Attribute name='Store Country' keyColumn='store_country'/>"
            + "        <Attribute name='Store Name' keyColumn='store_name'/>"
            + "        <Attribute name='id' keyColumn='store_id' hasHierarchy='false'/>"
            + "      </Attributes>"
            + "    </Dimension>"
            + "    <Dimension name='StoreB' key='id' table='storeb'>"
            + "      <Attributes>"
            + "        <Attribute name='Store Country' keyColumn='store_country'/>"
            + "        <Attribute name='Store Name' keyColumn='store_name'/>"
            + "        <Attribute name='id' keyColumn='store_id' hasHierarchy='false'/>"
            + "      </Attributes>"
            + "    </Dimension>"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup table='inventory_fact_1997'>"
            + "      <Measures>"
            + "        <Measure name='Store Invoice' column='store_invoice' aggregator='sum'/>\n"
            + "        <Measure name='Supply Time' column='supply_time' aggregator='sum'/>\n"
            + "        <Measure name='Warehouse Cost' column='warehouse_cost' aggregator='sum'/>\n"
            + "      </Measures>"
            + "      <DimensionLinks>"
            + "        <ForeignKeyLink dimension='StoreA' foreignKeyColumn='store_id'/>"
            + "        <ForeignKeyLink dimension='StoreB' foreignKeyColumn='store_id'/>"
            + "      </DimensionLinks>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>"
            + "</Cube>";
        final String tableDefs =
            "<Table name='store' alias='storea'/>"
            + "<Table name='store' alias='storeb'/>";
        final TestContext testContext = getTestContext()
            .insertCube(cubeDef)
            .insertPhysTable(tableDefs);

        testContext.assertQueryReturns(
            "select {[StoreA].[USA]} on rows,"
            + "{[StoreB].[USA]} on columns"
            + " from "
            + "AliasedDimensionsTesting",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[StoreB].[Store Country].[USA]}\n"
            + "Axis #2:\n"
            + "{[StoreA].[Store Country].[USA]}\n"
            + "Row #0: 10,425\n");
    }

    /**
     * Test Multiple DimensionUsages on same Dimension.
     * Alias the fact table to avoid issues with aggregation rules
     * and multiple column names
     */
    public void testMultipleDimensionUsages() {
        final TestContext testContext = getTestContext().insertCube(
            "<Cube name='Sales Two Dimensions'>\n"
            + "  <Dimensions>"
            + "    <Dimension name='Time' source='Time'/>\n"
            + "    <Dimension name='Time2' source='Time'/>\n"
            + "    <Dimension name='Store' source='Store'/>\n"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup name='Sales Two Dimensions' table='sales_fact_1997'>"
            + "      <Measures>"
            + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum' "
            + "         formatString='Standard'/>\n"
            + "        <Measure name='Store Cost' column='store_cost' aggregator='sum'"
            + "         formatString='#,###.00'/>\n"
            + "      </Measures>"
            + "      <DimensionLinks>\n"
            + "        <ForeignKeyLink dimension='Time' foreignKeyColumn='time_id'/>\n"
            + "        <ForeignKeyLink dimension='Time2' foreignKeyColumn='product_id'/>\n"
            + "        <ForeignKeyLink dimension='Store' foreignKeyColumn='store_id'/>\n"
            + "      </DimensionLinks>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>"
            + "</Cube>");

        testContext.assertQueryReturns(
            "select\n"
            + " {[Time2].[1997]} on columns,\n"
            + " {[Time].[1997].[Q3]} on rows\n"
            + "From [Sales Two Dimensions]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time2].[Time].[1997]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "Row #0: 16,266\n");
    }

    /**
     * Test Multiple DimensionUsages on same Dimension.
     * Alias the fact table to avoid issues with aggregation rules
     * and multiple column names
     */
    public void testMultipleDimensionHierarchyCaptionUsages() {
        final TestContext testContext = getTestContext().insertCube(
            "<Cube name='Sales Two Dimensions'>\n"
            + "  <Dimensions>"
            + "    <Dimension name='Store' source='Store' caption='First Store'/>\n"
            + "    <Dimension name='Time' source='Time' caption='TimeOne'/>\n"
            + "    <Dimension name='Time2' source='Time' caption='TimeTwo'/>\n"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup name='Sales Two Dimensions' table='sales_fact_1997' alias='sales_fact_1997_mdu'>"
            + "      <Measures>"
            + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum' "
            + "         formatString='Standard'/>\n"
            + "        <Measure name='Store Cost' column='store_cost' aggregator='sum'"
            + "         formatString='#,###.00'/>\n"
            + "      </Measures>"
            + "      <DimensionLinks>\n"
            + "        <ForeignKeyLink dimension='Store' foreignKeyColumn='store_id'/>"
            + "        <ForeignKeyLink dimension='Time' foreignKeyColumn='time_id'/>"
            + "        <ForeignKeyLink dimension='Time2' foreignKeyColumn='product_id'/>"
            + "      </DimensionLinks>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>"
            + "</Cube>");

        String query =
            "select\n"
            + " {[Time2].[1997]} on columns,\n"
            + " {[Time].[1997].[Q3]} on rows\n"
            + "From [Sales Two Dimensions]";

        Result result = testContext.executeQuery(query);

        // Time2.1997 Member
        Member member1 =
            result.getAxes()[0].getPositions().iterator().next().iterator()
                .next();

        // NOTE: The caption is modified at the dimension, not the hierarchy
        assertEquals("TimeTwo", member1.getLevel().getDimension().getCaption());

        Member member2 =
            result.getAxes()[1].getPositions().iterator().next().iterator()
                .next();
        assertEquals("TimeOne", member2.getLevel().getDimension().getCaption());
    }


    /**
     * Test DimensionUsage level attribute
     */
    public void testDimensionUsageLevel() {
        final TestContext testContext = getTestContext().create(
            null,
            "  <Cube name='Customer Usage Level'>"
            + "  <Dimensions>"
            + "    <Dimension source='Store'/>"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup name='Customer Usage Level' table='customer'>"
            + "        <Measures>"
            + "          <Measure name='Cars' column='num_cars_owned' aggregator='sum'/>"
            + "          <Measure name='Children' column='total_children' aggregator='sum'/>"
            + "        </Measures>"
            + "        <DimensionLinks>\n"
            + "          <ForeignKeyLink dimension='Store' attribute='Store State' foreignKeyColumn='state_province'/>\n"
            + "        </DimensionLinks>"
            + "     </MeasureGroup>"
            + "  </MeasureGroups>"
            + "</Cube>", null, null, null, null);

        testContext.assertQueryReturns(
            "select\n"
            + " {[Store].[Store State].members} on columns \n"
            + "From [Customer Usage Level]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[Canada].[BC]}\n"
            + "{[Store].[Stores].[Mexico].[DF]}\n"
            + "{[Store].[Stores].[Mexico].[Guerrero]}\n"
            + "{[Store].[Stores].[Mexico].[Jalisco]}\n"
            + "{[Store].[Stores].[Mexico].[Veracruz]}\n"
            + "{[Store].[Stores].[Mexico].[Yucatan]}\n"
            + "{[Store].[Stores].[Mexico].[Zacatecas]}\n"
            + "{[Store].[Stores].[USA].[CA]}\n"
            + "{[Store].[Stores].[USA].[OR]}\n"
            + "{[Store].[Stores].[USA].[WA]}\n"
            + "Row #0: 7,700\n"
            + "Row #0: 1,492\n"
            + "Row #0: 228\n"
            + "Row #0: 206\n"
            + "Row #0: 195\n"
            + "Row #0: 229\n"
            + "Row #0: 1,209\n"
            + "Row #0: 46,965\n"
            + "Row #0: 4,686\n"
            + "Row #0: 32,767\n");

        // BC.children should return an empty list, considering that we've
        // joined Store at the State level.
        if (false) {
            testContext.assertQueryReturns(
                "select\n"
                + " {[Store].[All Stores].[Canada].[BC].children} on columns \n"
                + "From [Customer Usage Level]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n");
        }
    }

    /**
     * Test to verify naming of all member with
     * dimension usage name is different then source name
     */
    public void testAllMemberMultipleDimensionUsages() {
        final TestContext testContext = getTestContext().create(
            null,
            "<Cube name='Sales Two Sales Dimensions'>\n"
            + "  <Dimensions>"
            + "    <Dimension name='Store' source='Store' caption='First Store'/>\n"
            + "    <Dimension name='Store2' source='Store' caption='Second Store'/>\n"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup name='Sales Two Dimensions' table='sales_fact_1997'>"
            + "      <Measures>"
            + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum' "
            + "         formatString='Standard'/>\n"
            + "        <Measure name='Store Cost' column='store_cost' aggregator='sum'"
            + "         formatString='#,###.00'/>\n"
            + "      </Measures>"
            + "      <DimensionLinks>\n"
            + "        <ForeignKeyLink dimension='Store' foreignKeyColumn='store_id'/>"
            + "        <ForeignKeyLink dimension='Store2' foreignKeyColumn='product_id'/>"
            + "      </DimensionLinks>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>"
            + "</Cube>",
            null,
            null,
            null,
            null);

        // If SsasCompatibleNaming (the new behavior), the usages of the
        // [Store] dimension create dimensions called [Store]
        // and [Store2], each with a hierarchy called [Store].
        // Therefore Store2's all member is [Store2].[Store].[All Stores],
        // or [Store2].[All Stores] for short.
        //
        // Under the old behavior, the member is called [Store2].[All Store2s].
        final String store2AllMember = "[Store2].[All Stores]";
        testContext.assertQueryReturns(
            "select\n"
            + " {[Store].[Stores].[All Stores]} on columns,\n"
            + " {" + store2AllMember + "} on rows\n"
            + "From [Sales Two Sales Dimensions]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[All Stores]}\n"
            + "Axis #2:\n"
            + "{[Store2].[Stores].[All Stores]}\n"
            + "Row #0: 266,773\n");

        final Result result = testContext.executeQuery(
            "select ([Store].[All Stores], " + store2AllMember + ") on 0\n"
            + "from [Sales Two Sales Dimensions]");
        final Axis axis = result.getAxes()[0];
        final Position position = axis.getPositions().get(0);
        assertEquals(
            "First Store", position.get(0).getDimension().getCaption());
        assertEquals(
            "Second Store", position.get(1).getDimension().getCaption());
    }

    /**
     * This test displays an informative error message if someone uses
     * an unaliased name instead of an aliased name
     */
    public void testNonAliasedDimensionUsage() {
        final TestContext testContext = getTestContext().create(
            null,

            "<Cube name='Sales Two Dimensions'>\n"
            + "  <Dimensions>"
            + "    <Dimension name='Time2' source='Time'/>\n"
            + "    <Dimension name='Store' source='Store'/>\n"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup name='Sales Two Dimensions' table='sales_fact_1997'>"
            + "      <Measures>"
            + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum' "
            + "         formatString='Standard'/>\n"
            + "        <Measure name='Store Cost' column='store_cost' aggregator='sum'"
            + "         formatString='#,###.00'/>\n"
            + "      </Measures>"
            + "      <DimensionLinks>\n"
            + "        <ForeignKeyLink dimension='Store' foreignKeyColumn='store_id'/>"
            + "        <ForeignKeyLink dimension='Time2' foreignKeyColumn='time_id'/>"
            + "      </DimensionLinks>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>"
            + "</Cube>", null, null, null, null);

        final String query = "select\n"
                             + " {[Time].[1997]} on columns \n"
                             + "From [Sales Two Dimensions]";
        // In new behavior, resolves to the hierarchy name [Time] even if
        // not qualified by dimension name [Time2].
        testContext.assertQueryReturns(
            query,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time2].[Time].[1997]}\n"
            + "Row #0: 266,773\n");
    }

    public void testDimensionUsageWithInvalidForeignKey() {
        final TestContext testContext = getTestContext().create(
            null,
            "<Cube name='Sales77'>\n"
            + "  <Dimensions>"
            + "    <Dimension source='Time'/>"
            + "    <Dimension source='Store'/>"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup name='Sales77' table='sales_fact_1997'>"
            + "      <Measures>"
            + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum' "
            + "         formatString='Standard'/>\n"
            + "      </Measures>"
            + "      <DimensionLinks>\n"
            + "        <ForeignKeyLink dimension='Store' foreignKeyColumn='invalid_column'/>"
            + "        <ForeignKeyLink dimension='Time' foreignKeyColumn='time_id'/>"
            + "      </DimensionLinks>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>"
            + "</Cube>", null, null, null, null);
        testContext.assertErrorList().containsError(
            "Column 'invalid_column' not found in relation 'sales_fact_1997' \\(in ForeignKeyLink\\) \\(at ${pos}\\)",
            "<ForeignKeyLink dimension='Store' foreignKeyColumn='invalid_column'/>");
    }

    /**
     * Tests a cube whose fact table is a &lt;View&gt; element.
     */
    public void testViewFactTable() {
        final TestContext testContext = getTestContext()
            .insertCube(
                "<Cube name='Warehouse (based on view)'>\n"
                + "  <Dimensions>"
                + "    <Dimension name='Time' source='Time'/>\n"
                + "    <Dimension name='Product' source='Product'/>\n"
                + "    <Dimension name='Store' source='Store'/>\n"
                + "    <Dimension name='Warehouse' table='warehouse' key='Id'>\n"
                + "      <Attributes>"
                + "        <Attribute name='Country' keyColumn='warehouse_country'/>"
                + "        <Attribute name='State Province' keyColumn='warehouse_state_province'/>"
                + "        <Attribute name='City' keyColumn='warehouse_city'/>"
                + "        <Attribute name='Warehouse Name' keyColumn='warehouse_name'/>"
                + "        <Attribute name='Id' keyColumn='warehouse_id'/>"
                + "      </Attributes>"
                + "    </Dimension>\n"
                + "  </Dimensions>"
                + "  <MeasureGroups>"
                + "    <MeasureGroup table='FACT'>"
                + "      <Measures>"
                + "        <Measure name='Warehouse Cost' column='warehouse_cost' aggregator='sum'/>\n"
                + "        <Measure name='Warehouse Sales' column='warehouse_sales' aggregator='sum'/>\n"
                + "      </Measures>"
                + "      <DimensionLinks>\n"
                + "        <ForeignKeyLink dimension='Time' foreignKeyColumn='time_id'/>"
                + "        <ForeignKeyLink dimension='Product' foreignKeyColumn='product_id'/>"
                + "        <ForeignKeyLink dimension='Store' foreignKeyColumn='store_id'/>"
                + "        <ForeignKeyLink dimension='Warehouse' foreignKeyColumn='warehouse_id'/>"
                + "      </DimensionLinks>"
                + "    </MeasureGroup>"
                + "  </MeasureGroups>"
                + "</Cube>")
            .insertPhysTable(
                "<Query name='FACT' alias='FACT'>\n"
                + "  <ExpressionView>\n"
                + "    <SQL dialect='generic'>\n"
                + "     <![CDATA[select * from 'inventory_fact_1997' as 'FOOBAR']]>\n"
                + "    </SQL>\n"
                + "    <SQL dialect='oracle'>\n"
                + "     <![CDATA[select * from 'inventory_fact_1997' 'FOOBAR']]>\n"
                + "    </SQL>\n"
                + "    <SQL dialect='mysql'>\n"
                + "     <![CDATA[select * from `inventory_fact_1997` as `FOOBAR`]]>\n"
                + "    </SQL>\n"
                + "    <SQL dialect='infobright'>\n"
                + "     <![CDATA[select * from `inventory_fact_1997` as `FOOBAR`]]>\n"
                + "    </SQL>\n"
                + "  </ExpressionView>\n"
                + "</Query>");

        testContext.assertQueryReturns(
            "select\n"
            + " {[Time].[1997], [Time].[1997].[Q3]} on columns,\n"
            + " {[Store].[USA].Children} on rows\n"
            + "From [Warehouse (based on view)]\n"
            + "where [Warehouse].[USA]",
            "Axis #0:\n"
            + "{[Warehouse].[Country].[USA]}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1997]}\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA].[CA]}\n"
            + "{[Store].[Stores].[USA].[OR]}\n"
            + "{[Store].[Stores].[USA].[WA]}\n"
            + "Row #0: 25,789.086\n"
            + "Row #0: 8,624.791\n"
            + "Row #1: 17,606.904\n"
            + "Row #1: 3,812.023\n"
            + "Row #2: 45,647.262\n"
            + "Row #2: 12,664.162\n");
    }

    /**
     * Tests a cube whose fact table is a &lt;View&gt; element, and which
     * has dimensions based on the fact table.
     */
    public void testViewFactTable2() {
        String cubeDefs =
            "<Cube name='Store2'>\n"
            + "  <!-- We could have used the shared dimension 'Store Type', but we\n"
            + "     want to test private dimensions without primary key. -->\n"
            + "  <Dimensions>"
            + "    <Dimension name='Store' table='store' key='Id'>\n"
            + "      <Attributes>"
            + "        <Attribute name='Store Type' keyColumn='store_type'/>"
            + "        <Attribute name='Id' keyColumn='store_id'/>"
            + "      </Attributes>"
            + "    </Dimension>\n"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup table='FACT'>"
            + "      <Measures>"
            + "        <Measure name='Store Sqft' column='store_sqft' aggregator='sum'\n"
            + "         formatString='#,###'/>\n"
            + "        <Measure name='Grocery Sqft' column='grocery_sqft' aggregator='sum'\n"
            + "         formatString='#,###'/>\n"
            + "      </Measures>"
            + "      <DimensionLinks>"
            + "        <ForeignKeyLink dimension='Store' foreignKeyColumn='store_id'/>"
            + "      </DimensionLinks>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>"
            + "</Cube>";
        String tableDef =
            "<Query name='FACT' alias='FACT'>\n"
            + "  <ExpressionView>\n"
            + "    <SQL dialect='generic'>\n"
            + "     <![CDATA[select * from 'store' as 'FOOBAR']]>\n"
            + "    </SQL>\n"
            + "    <SQL dialect='oracle'>\n"
            + "     <![CDATA[select * from 'store' 'FOOBAR']]>\n"
            + "    </SQL>\n"
            + "    <SQL dialect='mysql'>\n"
            + "     <![CDATA[select * from `store` as `FOOBAR`]]>\n"
            + "    </SQL>\n"
            + "    <SQL dialect='infobright'>\n"
            + "     <![CDATA[select * from `store` as `FOOBAR`]]>\n"
            + "    </SQL>\n"
            + "  </ExpressionView>\n"
            + "</Query>";
        final TestContext testContext = getTestContext()
            .insertCube(cubeDefs)
            .insertPhysTable(tableDef);
        testContext.assertQueryReturns(
            "select {[Store].[Store Type].Children} on columns from [Store2]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Store Type].[Deluxe Supermarket]}\n"
            + "{[Store].[Store Type].[Gourmet Supermarket]}\n"
            + "{[Store].[Store Type].[HeadQuarters]}\n"
            + "{[Store].[Store Type].[Mid-Size Grocery]}\n"
            + "{[Store].[Store Type].[Small Grocery]}\n"
            + "{[Store].[Store Type].[Supermarket]}\n"
            + "Row #0: 146,045\n"
            + "Row #0: 47,447\n"
            + "Row #0: \n"
            + "Row #0: 109,343\n"
            + "Row #0: 75,281\n"
            + "Row #0: 193,480\n");
    }

    /**
     * Tests a cube whose fact table is a &lt;View&gt; element, and which
     * has dimensions based on the fact table.
     */
    public void testViewFactTableInvalid() {
        String cubeDefs =
            "<Cube name='Store2'>\n"
            + "  <Dimensions>"
            + "    <Dimension name='Store Type'>\n"
            + "      <Attributes>"
            + "        <Attribute name='Store Type' column='store_type'/>"
            + "      </Attributes>"
            + "    </Dimension>\n"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup name='Store2' table='FACT'>"
            + "      <Measures>"
            + "        <Measure name='Store Sqft' column='store_sqft' aggregator='sum'\n"
            + "         formatString='#,###'/>\n"
            + "      </Measures>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>"
            + "</Cube>";
        String tableDef =
            "<Query name='FACT' alias='FACT'>\n"
            + "  <ExpressionView>\n"
            + "    <SQL dialect='generic'>\n"
            + "     <![CDATA[select wrong from wronger]]>\n"
            + "    </SQL>\n"
            + "  </ExpressionView>\n"
            + "</Query>";
        TestContext testContext = getTestContext()
            .insertCube(cubeDefs)
            .insertPhysTable(tableDef);
        testContext.assertQueryThrows(
            "select {[Store Type].Children} on columns from [Store2]",
            "View is invalid: ");
    }

    /**
     * Tests that the deprecated "distinct count" value for the
     * Measure@aggregator attribute still works. The preferred value these days
     * is "distinct-count".
     */
    public void testDeprecatedDistinctCountAggregator() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            null,
            "  <Measure name='Customer Count2' column='customer_id'\n"
            + "      aggregator='distinct count' formatString='#,###'/>\n",
            "  <CalculatedMember\n"
            + "      name='Half Customer Count'\n"
            + "      dimension='Measures'\n"
            + "      visible='false'\n"
            + "      formula='[Measures].[Customer Count2] / 2'>\n"
            + "  </CalculatedMember>", null);
        testContext.assertQueryReturns(
            "select {[Measures].[Unit Sales],"
            + "    [Measures].[Customer Count], "
            + "    [Measures].[Customer Count2], "
            + "    [Measures].[Half Customer Count]} on 0,\n"
            + " {[Store].[USA].Children} ON 1\n"
            + "FROM [Sales]\n"
            + "WHERE ([Gender].[M])",
            "Axis #0:\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Customer Count]}\n"
            + "{[Measures].[Customer Count2]}\n"
            + "{[Measures].[Half Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA].[CA]}\n"
            + "{[Store].[Stores].[USA].[OR]}\n"
            + "{[Store].[Stores].[USA].[WA]}\n"
            + "Row #0: 37,989\n"
            + "Row #0: 1,389\n"
            + "Row #0: 1,389\n"
            + "Row #0: 695\n"
            + "Row #1: 34,623\n"
            + "Row #1: 536\n"
            + "Row #1: 536\n"
            + "Row #1: 268\n"
            + "Row #2: 62,603\n"
            + "Row #2: 901\n"
            + "Row #2: 901\n"
            + "Row #2: 451\n");
    }

    /**
     * Tests that an invalid aggregator causes an error.
     */
    public void testInvalidAggregator() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            null,
            "  <Measure name='Customer Count3' column='customer_id'\n"
            + "      aggregator='invalidAggregator' formatString='#,###'/>\n"
            + "  <CalculatedMember\n"
            + "      name='Half Customer Count'\n"
            + "      dimension='Measures'\n"
            + "      visible='false'\n"
            + "      formula='[Measures].[Customer Count2] / 2'>\n"
            + "  </CalculatedMember>", null, null);
        testContext.assertQueryThrows(
            "select from [Sales]",
            "Unknown aggregator 'invalidAggregator'; valid aggregators are: 'sum', 'count', 'min', 'max', 'avg', 'distinct-count'");
    }

    /**
     * Testcase for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-291">
     * Bug MONDRIAN-291, "'unknown usage' messages"</a>.
     */
    public void testUnknownUsages() {
        if (!MondrianProperties.instance().ReadAggregates.get()) {
            return;
        }
        final Logger logger = Logger.getLogger(AggTableManager.class);
        propSaver.setAtLeast(logger, org.apache.log4j.Level.WARN);
        final StringWriter sw = new StringWriter();
        final Appender appender =
            new WriterAppender(new SimpleLayout(), sw);
        final LevelRangeFilter filter = new LevelRangeFilter();
        filter.setLevelMin(org.apache.log4j.Level.WARN);
        appender.addFilter(filter);
        logger.addAppender(appender);
        try {
            final TestContext testContext = getTestContext().withSchema(
                "<?xml version='1.0'?>\n"
                + "<Schema name='FoodMart'>\n"
                + "<Cube name='Sales Degen'>\n"
                + "  <Table name='sales_fact_1997'>\n"
                + "    <AggExclude pattern='agg_c_14_sales_fact_1997'/>\n"
                + "    <AggExclude pattern='agg_l_05_sales_fact_1997'/>\n"
                + "    <AggExclude pattern='agg_g_ms_pcat_sales_fact_1997'/>\n"
                + "    <AggExclude pattern='agg_ll_01_sales_fact_1997'/>\n"
                + "    <AggExclude pattern='agg_c_special_sales_fact_1997'/>\n"
                + "    <AggExclude pattern='agg_l_03_sales_fact_1997'/>\n"
                + "    <AggExclude pattern='agg_l_04_sales_fact_1997'/>\n"
                + "    <AggExclude pattern='agg_pl_01_sales_fact_1997'/>\n"
                + "    <AggExclude pattern='agg_lc_06_sales_fact_1997'/>\n"
                + "    <AggExclude pattern='agg_lc_100_sales_fact_1997'/>\n"
                + "    <AggName name='agg_c_10_sales_fact_1997'>\n"
                + "      <AggFactCount column='fact_count'/>\n"
                + "      <AggMeasure name='[Measures].[Store Cost]' column='store_cost' />\n"
                + "      <AggMeasure name='[Measures].[Store Sales]' column='store_sales' />\n"
                + "     </AggName>\n"
                + "  </Table>\n"
                + "  <Dimension name='Time' type='TimeDimension' foreignKey='time_id'>\n"
                + "    <Hierarchy hasAll='false' primaryKey='time_id'>\n"
                + "      <Table name='time_by_day'/>\n"
                + "      <Level name='Year' column='the_year' type='Numeric' uniqueMembers='true'\n"
                + "          levelType='TimeYears'/>\n"
                + "      <Level name='Quarter' column='quarter' uniqueMembers='false'\n"
                + "          levelType='TimeQuarters'/>\n"
                + "      <Level name='Month' column='month_of_year' uniqueMembers='false' type='Numeric'\n"
                + "          levelType='TimeMonths'/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <Dimension name='Time Degenerate'>\n"
                + "    <Hierarchy hasAll='true' primaryKey='time_id'>\n"
                + "      <Level name='day' column='time_id'/>\n"
                + "      <Level name='month' column='product_id' type='Numeric'/>\n"
                + "    </Hierarchy>"
                + "  </Dimension>"
                + "  <Measure name='Store Cost' column='store_cost' aggregator='sum'\n"
                + "      formatString='#,###.00'/>\n"
                + "  <Measure name='Store Sales' column='store_sales' aggregator='sum'\n"
                + "      formatString='#,###.00'/>\n"
                + "</Cube>\n"
                + "</Schema>");
            testContext.assertQueryReturns(
                "select from [Sales Degen]",
                "Axis #0:\n"
                + "{}\n"
                + "225,627.23");
        } finally {
            logger.removeAppender(appender);
        }
        // Note that 'product_id' is NOT one of the columns with unknown usage.
        // It is used as a level in the degenerate dimension [Time Degenerate].
        TestContext.assertEqualsVerbose(
            "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_c_10_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'customer_count' with unknown usage.\n"
            + "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_c_10_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'month_of_year' with unknown usage.\n"
            + "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_c_10_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'quarter' with unknown usage.\n"
            + "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_c_10_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'the_year' with unknown usage.\n"
            + "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_c_10_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'unit_sales' with unknown usage.\n",
            sw.toString());
    }

    public void testUnknownUsages1() {
        if (!MondrianProperties.instance().ReadAggregates.get()) {
            return;
        }
        final Logger logger = Logger.getLogger(AggTableManager.class);
        propSaver.setAtLeast(logger, org.apache.log4j.Level.WARN);
        final StringWriter sw = new StringWriter();
        final Appender appender =
            new WriterAppender(new SimpleLayout(), sw);
        final LevelRangeFilter filter = new LevelRangeFilter();
        filter.setLevelMin(org.apache.log4j.Level.WARN);
        appender.addFilter(filter);
        logger.addAppender(appender);
        try {
            final TestContext testContext = getTestContext().withSchema(
                "<?xml version='1.0'?>\n"
                + "<Schema name='FoodMart'>\n"
                + "<Cube name='Denormalized Sales'>\n"
                + "  <Table name='sales_fact_1997'>\n"
                + "    <AggExclude pattern='agg_c_14_sales_fact_1997'/>\n"
                + "    <AggExclude pattern='agg_l_05_sales_fact_1997'/>\n"
                + "    <AggExclude pattern='agg_g_ms_pcat_sales_fact_1997'/>\n"
                + "    <AggExclude pattern='agg_ll_01_sales_fact_1997'/>\n"
                + "    <AggExclude pattern='agg_c_special_sales_fact_1997'/>\n"
                + "    <AggExclude pattern='agg_l_04_sales_fact_1997'/>\n"
                + "    <AggExclude pattern='agg_pl_01_sales_fact_1997'/>\n"
                + "    <AggExclude pattern='agg_c_10_sales_fact_1997'/>\n"
                + "    <AggExclude pattern='agg_lc_06_sales_fact_1997'/>\n"
                + "    <AggName name='agg_l_03_sales_fact_1997'>\n"
                + "      <AggFactCount column='fact_count'/>\n"
                + "      <AggMeasure name='[Measures].[Store Cost]' column='store_cost' />\n"
                + "      <AggMeasure name='[Measures].[Store Sales]' column='store_sales' />\n"
                + "      <AggMeasure name='[Measures].[Unit Sales]' column='unit_sales' />\n"
                + "      <AggLevel name='[Customer].[Customer ID]' column='customer_id' />\n"
                + "      <AggForeignKey factColumn='time_id' aggColumn='time_id' />\n"
                + "     </AggName>\n"
                + "  </Table>\n"
                + "  <Dimension name='Time' type='TimeDimension' foreignKey='time_id'>\n"
                + "    <Hierarchy hasAll='false' primaryKey='time_id'>\n"
                + "      <Table name='time_by_day'/>\n"
                + "      <Level name='Year' column='the_year' type='Numeric' uniqueMembers='true'\n"
                + "          levelType='TimeYears'/>\n"
                + "      <Level name='Quarter' column='quarter' uniqueMembers='false'\n"
                + "          levelType='TimeQuarters'/>\n"
                + "      <Level name='Month' column='month_of_year' uniqueMembers='false' type='Numeric'\n"
                + "          levelType='TimeMonths'/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <Dimension name='Customer'>\n"
                + "    <Hierarchy hasAll='true' primaryKey='customer_id'>\n"
                + "      <Level name='Customer ID' column='customer_id'/>\n"
                + "    </Hierarchy>"
                + "  </Dimension>"
                + "  <Dimension name='Product'>\n"
                + "    <Hierarchy hasAll='true' primaryKey='product_id'>\n"
                + "      <Level name='Product ID' column='product_id'/>\n"
                + "    </Hierarchy>"
                + "  </Dimension>"
                + "  <Measure name='Store Cost' column='store_cost' aggregator='sum'\n"
                + "      formatString='#,###.00'/>\n"
                + "  <Measure name='Store Sales' column='store_sales' aggregator='sum'\n"
                + "      formatString='#,###.00'/>\n"
                + "  <Measure name='Unit Sales' column='unit_sales' aggregator='sum'\n"
                + "      formatString='#,###'/>\n"
                + "</Cube>\n"
                + "</Schema>");
            testContext.assertQueryReturns(
                "select from [Denormalized Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "225,627.23");
        } finally {
            logger.removeAppender(appender);
        }
        TestContext.assertEqualsVerbose(
            "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_l_03_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'time_id' with unknown usage.\n",
            sw.toString());
    }

    public void testDegenerateDimension() {
        TestContext testContext = getTestContext().create(
            null,
            "<Cube name='Store with Degenerate'>\n"
            + "  <Dimensions>"
            + "    <Dimension name='store' table='store' key='country'>"
            + "      <Attributes> "
            + "        <Attribute name='country' keyColumn='store_country'/>"
            + "        </Attributes>"
            + "    </Dimension>"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup name='store' table='store'>"
            + "      <Measures>"
            + "        <Measure name='Store Sqft' column='store_sqft' aggregator='sum'\n"
            + "         formatString='#,###'/>\n"
            + "      </Measures>"
            + "      <DimensionLinks>"
            + "        <FactLink dimension='store' foreignKeyColumn='store_id'/>"
            + "      </DimensionLinks>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>"
            + "</Cube>",
            null,
            null,
            null,
            null);
        testContext.assertQueryReturns(
            "select [store].[country].members on 0 from [Store with Degenerate]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[store].[country].[All country]}\n"
            + "{[store].[country].[Canada]}\n"
            + "{[store].[country].[Mexico]}\n"
            + "{[store].[country].[USA]}\n"
            + "Row #0: 571,596\n"
            + "Row #0: 57,564\n"
            + "Row #0: 243,012\n"
            + "Row #0: 271,020\n");
    }

    public void testPropertyFormatter() {
        final TestContext testContext =
            getTestContext().createSubstitutingCube(
                "Sales",
                "<Dimension name='Store2' table='store' key='Store Type'>\n"
                + "  <Attributes>"
                + "    <Attribute name='Store Type' keyColumn='store_id'>"
                + "      <Property name='Store Type' attribute='Store Type' column='store_type' formatter='"
                + DummyPropertyFormatter.class.getName()
                + "'/>"
                + "      <Property attribute='Store Manager' column='store_manager'/>"
                + "    </Attribute>"
                + "    <Attribute name='id' keyColumn='store_id'/>"
                + "  </Attributes>"
                + "</Dimension>\n");
        try {
            testContext.assertSimpleQuery();
            fail("expected exception");
        } catch (RuntimeException e) {
            TestContext.checkThrowable(
                e,
                "Failed to load formatter class 'mondrian.test.SchemaTest$DummyPropertyFormatter' for property 'Store Type'.");
        }
    }

    /**
     * Bug <a href="http://jira.pentaho.com/browse/MONDRIAN-233">MONDRIAN-233,
     * "ClassCastException in AggQuerySpec"</a> occurs when two cubes
     * have the same fact table, distinct aggregate tables, and measures with
     * the same name.
     *
     * <p>This test case attempts to reproduce this issue by creating that
     * environment, but it found a different issue: a measure came back with a
     * cell value which was from a different measure. The root cause is
     * probably the same: when measures are registered in a star, they should
     * be qualified by cube name.
     */
    public void testBugMondrian233() {
        final TestContext testContext =
            getTestContext().create(
                null,
                "  <Cube name='Sales2' defaultMeasure='Unit Sales'>"
                + "  <Dimensions>"
                + "    <Dimension source='Time' foreignKey='time_id'/>\n"
                + "    <Dimension source='Product' foreignKey='product_id'/>"
                + "  </Dimensions>\n"
                + "  <MeasureGroups>"
                + "    <MeasureGroup name='Sales2' table='sales_fact_1997'>"
                + "      <Measures>"
                + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum'\n"
                + "         formatString='Standard'/>\n"
                + "        <Measure name='Store Cost' column='store_cost' aggregator='sum'\n"
                + "         formatString='#,###.00'/>"
                + "      </Measures>"
                + "      <DimensionLinks>\n"
                + "        <ForeignKeyLink dimension='Product' foreignKeyColumn='product_id'/>"
                + "        <ForeignKeyLink dimension='Time' foreignKeyColumn='time_id'/>"
                + "        </DimensionLinks>"
                + "    </MeasureGroup>"
                + "  </MeasureGroups>\n"
                + "</Cube>",
                null,
                null,
                null,
                null);

        // With bug, and with aggregates enabled, query against Sales returns
        // 565,238, which is actually the total for [Store Sales]. I think the
        // aggregate tables are getting crossed.
        final String expected =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 266,773\n";
        testContext.assertQueryReturns(
            "select {[Measures]} on 0 from [Sales2]",
            expected);
        testContext.assertQueryReturns(
            "select {[Measures]} on 0 from [Sales]", expected);
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-303">
     * MONDRIAN-303, "Property column shifting when use captionColumn"</a>.
     */
    public void testBugMondrian303() {
        // In order to reproduce the problem a dimension specifying
        // captionColumn and Properties were required.
        String dimDef =
            "<Dimension name='Store2' table='store' key='Store Id'>\n"
            + "  <Attributes>"
            + "    <Attribute name='Store Type' keyColumn='store_type'/>"
            + "    <Attribute name='Store Manager' keyColumn='store_manager'/>"
            + "    <Attribute name='Store Id' keyColumn='store_id' captionColumn='store_name'>"
            + "      <Property attribute='Store Type'/>"
            + "      <Property attribute='Store Manager'/>"
            + "    </Attribute>"
            + "  </Attributes>"
            + "</Dimension>\n";
        Map<String, String> dimLinks = ArrayMap.of(
            "Sales",
            "<ForeignKeyLink dimension='Store2' "
            + "foreignKeyColumn='store_id'/>");

        TestContext testContext =
            getTestContext()
                .insertDimension("Sales", dimDef)
                .insertDimensionLinks("Sales", dimLinks)
                .ignoreMissingLink();

        // In the query below Mondrian (prior to the fix) would
        // return the store name instead of the store type.
        testContext.assertQueryReturns(
            "WITH\n"
            + "   MEMBER [Measures].[StoreType] AS \n"
            + "   '[Store2].[Store Id].CurrentMember.Properties(\"Store Type\")'\n"
            + "SELECT\n"
            + "   NonEmptyCrossJoin({[Store2].[Store Id].[All Store Id].children}, {[Product].[All Products]}) ON ROWS,\n"
            + "   { [Measures].[Store Sales], [Measures].[StoreType]} ON COLUMNS\n"
            + "FROM Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[StoreType]}\n"
            + "Axis #2:\n"
            + "{[Store2].[Store Id].[2], [Product].[Products].[All Products]}\n"
            + "{[Store2].[Store Id].[3], [Product].[Products].[All Products]}\n"
            + "{[Store2].[Store Id].[6], [Product].[Products].[All Products]}\n"
            + "{[Store2].[Store Id].[7], [Product].[Products].[All Products]}\n"
            + "{[Store2].[Store Id].[11], [Product].[Products].[All Products]}\n"
            + "{[Store2].[Store Id].[13], [Product].[Products].[All Products]}\n"
            + "{[Store2].[Store Id].[14], [Product].[Products].[All Products]}\n"
            + "{[Store2].[Store Id].[15], [Product].[Products].[All Products]}\n"
            + "{[Store2].[Store Id].[16], [Product].[Products].[All Products]}\n"
            + "{[Store2].[Store Id].[17], [Product].[Products].[All Products]}\n"
            + "{[Store2].[Store Id].[22], [Product].[Products].[All Products]}\n"
            + "{[Store2].[Store Id].[23], [Product].[Products].[All Products]}\n"
            + "{[Store2].[Store Id].[24], [Product].[Products].[All Products]}\n"
            + "Row #0: 4,739.23\n"
            + "Row #0: Small Grocery\n"
            + "Row #1: 52,896.30\n"
            + "Row #1: Supermarket\n"
            + "Row #2: 45,750.24\n"
            + "Row #2: Gourmet Supermarket\n"
            + "Row #3: 54,545.28\n"
            + "Row #3: Supermarket\n"
            + "Row #4: 55,058.79\n"
            + "Row #4: Supermarket\n"
            + "Row #5: 87,218.28\n"
            + "Row #5: Deluxe Supermarket\n"
            + "Row #6: 4,441.18\n"
            + "Row #6: Small Grocery\n"
            + "Row #7: 52,644.07\n"
            + "Row #7: Supermarket\n"
            + "Row #8: 49,634.46\n"
            + "Row #8: Supermarket\n"
            + "Row #9: 74,843.96\n"
            + "Row #9: Deluxe Supermarket\n"
            + "Row #10: 4,705.97\n"
            + "Row #10: Small Grocery\n"
            + "Row #11: 24,329.23\n"
            + "Row #11: Mid-Size Grocery\n"
            + "Row #12: 54,431.14\n"
            + "Row #12: Supermarket\n");
    }

    public void testCubeWithOneDimensionOneMeasure() {
        final TestContext testContext = getTestContext().create(
            null,
            "<Cube name='OneDim' defaultMeasure='Unit Sales'>\n"
            + "  <Dimensions>"
            + "    <Dimension name='Promotion' table = 'promotion' key='Promotion Id'>\n"
            + "      <Attributes>"
            + "        <Attribute name='Media Type' keyColumn='media_type' hierarchyAllMemberName='All Media'/>"
            + "        <Attribute name='Promotion Id' keyColumn='promotion_id'/>"
            + "      </Attributes>"
            + "    </Dimension>"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup name='OneDim' table='sales_fact_1997'>"
            + "      <Measures>"
            + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum'\n"
            + "         formatString='Standard'/>"
            + "      </Measures>"
            + "      <DimensionLinks>\n"
            + "        <ForeignKeyLink dimension='Promotion' foreignKeyColumn='promotion_id'/>"
            + "      </DimensionLinks>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);
        testContext.assertQueryReturns(
            "select {[Promotion].[Media Type]} on columns from [OneDim]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Promotion].[Media Type].[All Media]}\n"
            + "Row #0: 266,773\n");
    }

    public void testCubeWithOneDimensionUsageOneMeasure() {
        final TestContext testContext = getTestContext().insertCube(
            "<Cube name='OneDimUsage' defaultMeasure='Unit Sales'>\n"
            + "  <Dimensions>"
            + "    <Dimension source='Product'/>"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup table='sales_fact_1997'>"
            + "      <Measures>"
            + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum'\n"
            + "         formatString='Standard'/>\n"
            + "      </Measures>"
            + "      <DimensionLinks>\n"
            + "        <ForeignKeyLink dimension='Product' foreignKeyColumn='product_id'/>"
            + "      </DimensionLinks>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>"
            + "</Cube>");

        testContext.assertQueryReturns(
            "select {[Product].Children} on columns from [OneDimUsage]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Products].[Drink]}\n"
            + "{[Product].[Products].[Food]}\n"
            + "{[Product].[Products].[Non-Consumable]}\n"
            + "Row #0: 24,597\n"
            + "Row #0: 191,940\n"
            + "Row #0: 50,236\n");
    }

    public void testCubeHasFact() {
        final TestContext testContext = getTestContext().create(
            null,
            "<Cube name='Cube with caption' caption='Cube with name'/>\n",
            null, null, null, null);
        testContext.assertErrorList().containsError(
            "Cube definition must contain a MeasureGroups element, and at least one MeasureGroup \\(in Cube 'Cube with caption'\\) \\(at ${pos}\\)",
            "<Cube name='Cube with caption' caption='Cube with name'/>");
    }

    public void testCubeCaption() throws SQLException {
        final TestContext testContext = getTestContext().insertCube(
            "<Cube name='Cube with caption' caption='Cube with name'>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup table='sales_fact_1997'>"
            + "      <Measures>"
            + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum'\n"
            + "         formatString='Standard'/>\n"
            + "      </Measures>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>"
            + "</Cube>\n");
        final NamedList<org.olap4j.metadata.Cube> cubes =
            testContext.getOlap4jConnection().getOlapSchema().getCubes();
        final org.olap4j.metadata.Cube cube = cubes.get("Cube with caption");
        assertEquals("Cube with name", cube.getCaption());
    }

    public void testCubeWithNoDimensions() {
        final TestContext testContext = getTestContext().insertCube(
            "<Cube name='NoDim' defaultMeasure='Unit Sales'>\n"
            + "  <MeasureGroups>"
            + "    <MeasureGroup table='sales_fact_1997'>"
            + "      <Measures>"
            + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum'\n"
            + "         formatString='Standard'/>\n"
            + "      </Measures>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>"
            + "</Cube>");

        testContext.assertQueryReturns(
            "select {[Measures].[Unit Sales]} on columns from [NoDim]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 266,773\n");
    }

    /** Tests that it is an error if a cube has no declared measures. The
     * [Fact Count] measures implicit for each measure group do not count. */
    public void testCubeWithNoMeasures() {
        final TestContext testContext = getTestContext().create(
            null,
            "<Cube name='NoMeasures'>\n"
            + "  <MeasureGroups>\n"
            + "    <MeasureGroup table='sales_fact_1997'/>\n"
            + "  </MeasureGroups>\n"
            + "</Cube>",
            null, null, null, null);
        testContext.assertErrorList().containsError(
            "Cube 'NoMeasures' must have at least one measure "
            + "\\(in Cube 'NoMeasures'\\) \\(at ${pos}\\)",
            "<Cube name='NoMeasures'>");
    }

    public void testCubeWithOneCalcMeasure() {
        final TestContext testContext = getTestContext().insertCube(
            "<Cube name='OneCalcMeasure' defaultMeasure='[Measures].[Fact Count]'>\n"
            + "  <Dimensions>"
            + "    <Dimension name='Promotion' table='promotion' key='id'>\n"
            + "      <Attributes>"
            + "        <Attribute name='Media Type' keyColumn='media_type' uniqueMembers='true'/>"
            + "        <Attribute name='id' keyColumn='promotion_id'/>"
            + "      </Attributes>"
            + "    </Dimension>\n"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup table='sales_fact_1997'>"
            + "      <Measures>"
            + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='Standard'/>\n"
            + "      </Measures>"
            + "      <DimensionLinks>\n"
            + "        <ForeignKeyLink dimension='Promotion' foreignKeyColumn='promotion_id'/>\n"
            + "      </DimensionLinks>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>"
            + "  <CalculatedMembers>"
            + "    <CalculatedMember name='One' dimension='Measures' formula='1'/>\n"
            + "  </CalculatedMembers>"
            + "</Cube>");

        testContext.assertQueryReturns(
            "select {[Measures]} on columns from [OneCalcMeasure]\n"
            + "where [Promotion].[Media Type].[TV]",
            "Axis #0:\n"
            + "{[Promotion].[Media Type].[TV]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Fact Count]}\n"
            + "Row #0: 1,171\n");
    }

    /**
     * Test case for feature
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-960">MONDRIAN-960,
     * "Ability to define non-measure calculated members in a cube under a
     * specific parent"</a>.
     */
    public void testCalcMemberInCube() {
        final TestContext testContext =
            getTestContext().insertCalculatedMembers(
                "Sales",
                "<CalculatedMember\n"
                + "      name='SF and LA'\n"
                + "      hierarchy='[Store].[Stores]'\n"
                + "      parent='[Store].[USA].[CA]'>\n"
                + "  <Formula>\n"
                + "    [Store].[USA].[CA].[San Francisco]\n"
                + "    + [Store].[USA].[CA].[Los Angeles]\n"
                + "  </Formula>\n"
                + "</CalculatedMember>");

        // Because there are no explicit stored measures, the default measure is
        // the implicit stored measure, [Fact Count]. Stored measures, even
        // non-visible ones, come before calculated measures.
        testContext.assertQueryReturns(
            "select {[Store].[USA].[CA].[SF and LA]} on columns from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[CA].[SF and LA]}\n"
            + "Row #0: 27,780\n");

        // Now access the same member using a path that is not its unique name.
        testContext.assertQueryReturns(
            "select {[Store].[All Stores].[USA].[CA].[SF and LA]} on columns from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[CA].[SF and LA]}\n"
            + "Row #0: 27,780\n");

        // Test where hierarchy & dimension both specified. should fail
        try {
            final TestContext testContextFail1 =
                getTestContext().insertCalculatedMembers(
                    "Sales",
                    "<CalculatedMember\n"
                    + "      name='SF and LA'\n"
                    + "      hierarchy='[Store].[Stores]'\n"
                    + "      dimension='[Store]'\n"
                    + "      parent='[Store].[USA].[CA]'>\n"
                    + "  <Formula>\n"
                    + "    [Store].[USA].[CA].[San Francisco]\n"
                    + "    + [Store].[USA].[CA].[Los Angeles]\n"
                    + "  </Formula>\n"
                    + "</CalculatedMember>");
            testContextFail1.assertQueryReturns(
                "select {[Store].[All Stores].[USA].[CA].[SF and LA]} on columns from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Store].[USA].[CA].[SF and LA]}\n"
                + "Row #0: 27,780\n");
            fail();
        } catch (MondrianException e) {
            assertTrue(
                e.getMessage().contains(
                    "Cannot specify both a dimension and hierarchy"
                    + " for calculated member 'SF and LA' in cube 'Sales'"));
        }

        // test where hierarchy is not uname of valid hierarchy. should fail
        try {
            final TestContext testContextFail1 =
                getTestContext().insertCalculatedMembers(
                    "Sales",
                    "<CalculatedMember\n"
                    + "      name='SF and LA'\n"
                    + "      hierarchy='[Bacon]'\n"
                    + "      parent='[Store].[USA].[CA]'>\n"
                    + "  <Formula>\n"
                    + "    [Store].[USA].[CA].[San Francisco]\n"
                    + "    + [Store].[USA].[CA].[Los Angeles]\n"
                    + "  </Formula>\n"
                    + "</CalculatedMember>");
            testContextFail1.assertQueryReturns(
                "select {[Store].[All Stores].[USA].[CA].[SF and LA]} on columns from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Store].[USA].[CA].[SF and LA]}\n"
                + "Row #0: 27,780\n");
            fail();
        } catch (MondrianException e) {
            assertTrue(
                e.getMessage().contains(
                    "Unknown dimension '[Bacon]' for calculated member"
                    + " 'SF and LA' in cube 'Sales'"));
        }

        // test where formula is invalid. should fail
        try {
            final TestContext testContextFail1 =
                getTestContext().insertCalculatedMembers(
                    "Sales",
                    "<CalculatedMember\n"
                    + "      name='SF and LA'\n"
                    + "      hierarchy='[Store].[Stores]'\n"
                    + "      parent='[Store].[USA].[CA]'>\n"
                    + "  <Formula>\n"
                    + "    Baconating!\n"
                    + "  </Formula>\n"
                    + "</CalculatedMember>");
            testContextFail1.assertQueryReturns(
                "select {[Store].[All Stores].[USA].[CA].[SF and LA]} on columns from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Store].[USA].[CA].[SF and LA]}\n"
                + "Row #0: 27,780\n");
            fail();
        } catch (MondrianException e) {
            assertTrue(
                e.getMessage().contains(
                    "Calculated member or named set in cube 'Sales' has bad formula"));
        }

        // Test where parent is invalid. should fail
        try {
            final TestContext testContextFail1 =
                getTestContext().insertCalculatedMembers(
                    "Sales",
                    "<CalculatedMember\n"
                    + "      name='SF and LA'\n"
                    + "      hierarchy='[Store].[Stores]'\n"
                    + "      parent='[Store].[USA].[CA].[Baconville]'>\n"
                    + "  <Formula>\n"
                    + "    [Store].[USA].[CA].[San Francisco]\n"
                    + "    + [Store].[USA].[CA].[Los Angeles]\n"
                    + "  </Formula>\n"
                    + "</CalculatedMember>");
            testContextFail1.assertQueryReturns(
                "select {[Store].[Stores].[All Stores].[USA].[CA].[SF and LA]} on columns from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Store].[USA].[CA].[SF and LA]}\n"
                + "Row #0: 27,780\n");
            fail();
        } catch (MondrianException e) {
            assertTrue(
                e.getMessage().contains(
                    "Cannot find a parent with name '[Store].[USA].[CA]"
                    + ".[Baconville]' for calculated member 'SF and LA'"
                    + " in cube 'Sales'"));
        }

        // test where parent is not in same hierarchy as hierarchy. should fail
        try {
            final TestContext testContextFail1 =
                getTestContext().insertCalculatedMembers(
                    "Sales",
                    "<CalculatedMember\n"
                    + "      name='SF and LA'\n"
                    + "      hierarchy='[Store].[Store Type]'\n"
                    + "      parent='[Store].[USA].[CA]'>\n"
                    + "  <Formula>\n"
                    + "    [Store].[USA].[CA].[San Francisco]\n"
                    + "    + [Store].[USA].[CA].[Los Angeles]\n"
                    + "  </Formula>\n"
                    + "</CalculatedMember>");
            testContextFail1.assertQueryReturns(
                "select {[Store].[All Stores].[USA].[CA].[SF and LA]} on columns from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Store].[USA].[CA].[SF and LA]}\n"
                + "Row #0: 27,780\n");
            fail();
        } catch (MondrianException e) {
            assertTrue(
                e.getMessage().contains(
                    "The calculated member 'SF and LA' in cube 'Sales'"
                    + " is defined for hierarchy '[Store].[Store Type]' but its"
                    + " parent member is not part of that hierarchy"));
        }

        // test where calc member has no formula (formula attribute or
        //   embedded element); should fail
        try {
            final TestContext testContextFail1 =
                getTestContext().insertCalculatedMembers(
                    "Sales",
                    "<CalculatedMember\n"
                    + "      name='SF and LA'\n"
                    + "      hierarchy='[Store].[Stores]'\n"
                    + "      parent='[Store].[USA].[CA]'>\n"
                    + "  <Formula>\n"
                    + "  </Formula>\n"
                    + "</CalculatedMember>");
            testContextFail1.assertQueryReturns(
                "select {[Store].[All Stores].[USA].[CA].[SF and LA]} on columns from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Store].[USA].[CA].[SF and LA]}\n"
                + "Row #0: 27,780\n");
            fail();
        } catch (MondrianException e) {
            assertTrue(
                e.getMessage().contains(
                    "Calculated member or named set in cube 'Sales' has bad formula"));
        }
    }

    /** Calculated members must have precisely one of:  formula attribute and
     * Formula element. Also tests ability to return multiple exceptions (per
     * {@link mondrian.rolap.RolapConnectionProperties#Ignore} and
     * {@link mondrian.rolap.RolapSchemaLoader.Handler#check()}. */
    public void testCalculatedMemberFormulaBothOrNeither() {
        TestContext testContext = getTestContext()
            .withIgnore(true)
            .insertCalculatedMembers(
                "Sales",
                "<CalculatedMember name='Profit bad 1' hierarchy='Measures'/>\n"
                + "<CalculatedMember name='Profit good 1' hierarchy='Measures' formula='1'/>\n"
                + "<CalculatedMember name='Profit bad 2' hierarchy='Measures' formula='2'>\n"
                + "  <Formula>3</Formula>\n"
                + "</CalculatedMember>");
        // since we carry on parsing, there will be 2 errors
        testContext.assertErrorList().containsError(
            "Must specify either 'formula' attribute or 'Formula' child element; ignoring member '\\[Measures\\].\\[Measures\\].\\[Profit bad 1\\]' \\(in CalculatedMember 'Profit bad 1'\\) \\(at ${pos}\\).*",
            "<CalculatedMember name='Profit bad 1' hierarchy='Measures'/>");
        testContext.assertErrorList().containsError(
            "Must not specify both 'formula' attribute and 'Formula' child element; ignoring member '\\[Measures\\].\\[Measures\\].\\[Profit bad 2\\]' \\(in CalculatedMember 'Profit bad 2'\\) \\(at ${pos}\\).*",
            "<CalculatedMember name='Profit bad 2' hierarchy='Measures' formula='2'>");
    }

    /** Unit test with various calculated members and various
     * combinations of hierarchy and dimension attribute. */
    public void testCalculatedMemberDimensionAttribute() {
        checkCalculatedMemberDimensionAttribute("dimension='Measures'");
        checkCalculatedMemberDimensionAttribute("dimension='[Measures]'");
        checkCalculatedMemberDimensionAttribute("hierarchy='Measures'");
        checkCalculatedMemberDimensionAttribute("hierarchy='[Measures]'");
        checkCalculatedMemberDimensionAttribute(
            "hierarchy='[Measures].[Measures]'");
    }

    private void checkCalculatedMemberDimensionAttribute(String s) {
        getTestContext()
            .insertCalculatedMembers(
                "Sales",
                "<CalculatedMember " + s + " name='One' formula='1'/>")
            .assertExprReturns("[Measures].[One]", "1");
    }

    /**
     * Tests a hierarchy whose default member is a calculated member.
     * (Difficult to bootstrap, since calculated members are validated by
     * running a query, which naturally happens AFTER calculated members have
     * been assigned.)
     */
    public void testCalculatedMemberAsDefaultMember() {
        final TestContext testContext = getTestContext()
            .insertHierarchy(
                "Sales",
                "Customer",
                "<Hierarchy name='Customer with default member' defaultMember='[Customer].[Customer with default member].[USA].[CA].[SF and LA]'>\n"
                + "  <Level attribute='Country'/>\n"
                + "  <Level attribute='State Province'/>\n"
                + "  <Level attribute='City'/>\n"
                + "  <Level attribute='Name'/>\n"
                + "</Hierarchy>\n")
            .insertCalculatedMembers(
                "Sales",
                "<CalculatedMember\n"
                + "      name='SF and LA'\n"
                + "      hierarchy='[Customer].[Customer with default member]'\n"
                + "      parent='[Customer].[Customer with default member].[USA].[CA]'>\n"
                + "  <Formula>\n"
                + "    [Customer].[Customer with default member].[USA].[CA].[San Francisco]\n"
                + "    + [Customer].[Customer with default member].[USA].[CA].[Los Angeles]\n"
                + "  </Formula>\n"
                + "</CalculatedMember>");
        testContext.assertAxisReturns(
            "[Customer].[Customer with default member]",
            "[Customer].[Customer with default member].[USA].[CA].[SF and LA]");
    }

    /**
     * this test triggers an exception out of the aggregate table manager
     */
    public void testAggTableSupportOfSharedDims() {
        if (!Bug.BugMondrian361Fixed) {
            return;
        }
        final TestContext testContext = getTestContext().create(
            null,
            "<Cube name='Sales Two Dimensions'>\n"
            + "  <Table name='sales_fact_1997'/>\n"
            + "  <DimensionUsage name='Time' source='Time' foreignKey='time_id'/>\n"
            + "  <DimensionUsage name='Time2' source='Time' foreignKey='product_id'/>\n"
            + "  <DimensionUsage name='Store' source='Store' foreignKey='store_id'/>\n"
            + "  <Measure name='Unit Sales' column='unit_sales' aggregator='sum' "
            + "   formatString='Standard'/>\n"
            + "  <Measure name='Store Cost' column='store_cost' aggregator='sum'"
            + "   formatString='#,###.00'/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);

        testContext.assertQueryReturns(
            "select\n"
            + " {[Time2].[1997]} on columns,\n"
            + " {[Time].[1997].[Q3]} on rows\n"
            + "From [Sales Two Dimensions]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time2].[1997]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "Row #0: 16,266\n");

        // turn off caching
        propSaver.set(propSaver.props.DisableCaching, true);

        // re-read aggregates
        propSaver.set(propSaver.props.UseAggregates, true);
        propSaver.set(propSaver.props.ReadAggregates, false);
        propSaver.set(propSaver.props.ReadAggregates, true);
    }

    /**
     * Verifies that RolapHierarchy.tableExists() supports views.
     */
    public void testLevelTableAttributeAsView() {
        String tableDef =
            "<Query name='gender2' alias='gender2'>\n"
            + "  <ExpressionView>\n"
            + "      <SQL dialect='generic'>\n"
            + "        <![CDATA[SELECT * FROM customer]]>\n"
            + "      </SQL>\n"
            + "      <SQL dialect='oracle'>\n"
            + "        <![CDATA[SELECT * FROM 'customer']]>\n"
            + "      </SQL>\n"
            + "      <SQL dialect='derby'>\n"
            + "        <![CDATA[SELECT * FROM 'customer']]>\n"
            + "      </SQL>\n"
            + "      <SQL dialect='hsqldb'>\n"
            + "        <![CDATA[SELECT * FROM 'customer']]>\n"
            + "      </SQL>\n"
            + "      <SQL dialect='luciddb'>\n"
            + "        <![CDATA[SELECT * FROM 'customer']]>\n"
            + "      </SQL>\n"
            + "      <SQL dialect='neoview'>\n"
            + "        <![CDATA[SELECT * FROM 'customer']]>\n"
            + "      </SQL>\n"
            + "      <SQL dialect='netezza'>\n"
            + "        <![CDATA[SELECT * FROM 'customer']]>\n"
            + "      </SQL>\n"
            + "      <SQL dialect='db2'>\n"
            + "        <![CDATA[SELECT * FROM 'customer']]>\n"
            + "      </SQL>\n"
            + "  </ExpressionView>\n"
            + "</Query>";

        String cubeDefs =
            "<Cube name='GenderCube'>\n"
            + "  <Dimensions>"
            + "    <Dimension name='Gender2' table='gender2' key='Gender'>\n"
            + "      <Attributes>"
            + "        <Attribute name='Gender' keyColumn='gender'/>"
            + "      </Attributes>"
            + "    </Dimension>"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup table='sales_fact_1997'>"
            + "      <Measures>"
            + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum'\n"
            + "         formatString='Standard'/>\n"
            + "      </Measures>"
            + "      <DimensionLinks>\n"
            + "        <ForeignKeyLink dimension='Gender2' foreignKeyColumn='customer_id'/>\n"
            + "      </DimensionLinks>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>"
            + "</Cube>";
        final TestContext testContext =
            getTestContext()
                .insertPhysTable(tableDef)
                .insertCube(cubeDefs);

        if (!testContext.getDialect().allowsFromQuery()) {
            return;
        }

        Result result = testContext.executeQuery(
            "select {[Gender2].members} on columns from [GenderCube]");

        TestContext.assertEqualsVerbose(
            "[Gender2].[Gender].[All Gender]\n"
            + "[Gender2].[Gender].[F]\n"
            + "[Gender2].[Gender].[M]",
            TestContext.toString(
                result.getAxes()[0].getPositions()));
    }

    public void testInvalidSchemaAccess() {
        final TestContext testContext = getTestContext().create(
            null,
            null,
            null,
            null,
            null,
            "<Role name='Role1'>\n"
            + "  <SchemaGrant access='invalid'/>\n"
            + "</Role>")
            .withRole("Role1");
        testContext.assertQueryThrows(
            "select from [Sales]",
            "In Schema: In Role: In SchemaGrant: "
            + "Value 'invalid' of attribute 'access' has illegal value 'invalid'.  "
            + "Legal values: {all, custom, none, all_dimensions}");
    }

    public void testAllMemberNoStringReplace() {
        final TestContext testContext = getTestContext().create(
            null,
            "<Cube name='Sales Special Time'>\n"
            + "  <Dimensions>"
            + "    <Dimension name='TIME' key='Id' type='TIME' table='time_by_day'>"
            + "      <Attributes>"
            + "         <Attribute name='Years' keyColumn='the_year'/>"
            + "         <Attribute name='Quarters' keyColumn='quarter'/>"
            + "         <Attribute name='Months' keyColumn='month_of_year'/>"
            + "         <Attribute name='Id' keyColumn='time_id'/>"
            + "       </Attributes>"
            + "       <Hierarchies>"
            + "         <Hierarchy name='CALENDAR' hasAll='true' allMemberName='All TIME(CALENDAR)' primaryKey='time_id'>"
            + "           <Level attribute='Years'/>"
            + "           <Level attribute='Quarters'/>"
            + "           <Level attribute='Months'/>"
            + "         </Hierarchy>"
            + "       </Hierarchies>"
            + "     </Dimension>"
            + "     <Dimension name='Store' source='Store' foreignKey='store_id'/>\n"
            + "   </Dimensions>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup table='sales_fact_1997'>"
            + "      <Measures>"
            + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum' "
            + "         formatString='Standard'/>\n"
            + "        <Measure name='Store Cost' column='store_cost' aggregator='sum'"
            + "         formatString='#,###.00'/>\n"
            + "      </Measures>"
            + "      <DimensionLinks>\n"
            + "        <ForeignKeyLink dimension='Store' foreignKeyColumn='store_id'/>\n"
            + "        <ForeignKeyLink dimension='TIME' foreignKeyColumn='time_id'/>\n"
            + "      </DimensionLinks>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>"
            + "</Cube>",
            null,
            null,
            null,
            null);

        testContext.assertQueryReturns(
            "select [TIME.CALENDAR].[All TIME(CALENDAR)] on columns\n"
            + "from [Sales Special Time]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[TIME].[CALENDAR].[All TIME(CALENDAR)]}\n"
            + "Row #0: 266,773\n");
    }

    public void testUnionRole() {
        final TestContext testContext = getTestContext().create(
            null,
            null,
            null,
            null,
            null,
            "<Role name='Role1'>\n"
            + "  <SchemaGrant access='all'/>\n"
            + "</Role>\n"
            + "<Role name='Role2'>\n"
            + "  <SchemaGrant access='all'/>\n"
            + "</Role>\n"
            + "<Role name='Role1Plus2'>\n"
            + "  <Union>\n"
            + "    <RoleUsage roleName='Role1'/>\n"
            + "    <RoleUsage roleName='Role2'/>\n"
            + "  </Union>\n"
            + "</Role>\n"
            + "<Role name='Role1Plus2Plus1'>\n"
            + "  <Union>\n"
            + "    <RoleUsage roleName='Role1Plus2'/>\n"
            + "    <RoleUsage roleName='Role1'/>\n"
            + "  </Union>\n"
            + "</Role>\n").withRole("Role1Plus2Plus1");
        testContext.assertQueryReturns(
            "select from [Sales]", "Axis #0:\n"
            + "{}\n"
            + "266,773");
    }

    public void testUnionRoleContainsGrants() {
        final TestContext testContext = getTestContext().create(
            null, null, null, null, null,
            "<Role name='Role1'>\n"
            + "  <SchemaGrant access='all'/>\n"
            + "</Role>\n"
            + "<Role name='Role1Plus2'>\n"
            + "  <SchemaGrant access='all'/>\n"
            + "  <Union>\n"
            + "    <RoleUsage roleName='Role1'/>\n"
            + "    <RoleUsage roleName='Role1'/>\n"
            + "  </Union>\n"
            + "</Role>\n").withRole("Role1Plus2");
        testContext.assertQueryThrows(
            "select from [Sales]", "Union role must not contain grants");
    }

    /**
     * Tests that mondrian gives an error if role names are not distinct.
     */
    public void testRoleDuplicate() {
        final TestContext testContext = getTestContext().create(
            null,
            null,
            null,
            null,
            null,
            "<Role name='Role1'>\n"
            + "  <SchemaGrant access='all'/>\n"
            + "</Role>\n"
            + "<Role name='Role1Plus2'>\n"
            + "  <Union>\n"
            + "    <RoleUsage roleName='Role1'/>\n"
            + "  </Union>\n"
            + "</Role>\n"
            + "<Role name='Role1' >\n"
            + "  <SchemaGrant access='all'/>\n"
            + "</Role>").withRole("Role1Plus2");
        testContext.assertErrorList().containsError(
            "Duplicate role 'Role1' \\(in Role 'Role1'\\) \\(at ${pos}\\)",
            "<Role name='Role1' >");
    }

    /**
     * Tests that mondrian gives an error if a union role is cyclic.
     */
    public void testUnionRoleCyclic() {
        // Role 2 is cyclic (contains Role 3, which contains Role 2).
        final TestContext testContext = getTestContext().create(
            null,
            null,
            null,
            null,
            null,
            "<Role name='Role1'>\n"
            + "  <SchemaGrant access='all'/>\n"
            + "</Role>\n"
            + "<Role name='Role2'>\n"
            + "  <Union>\n"
            + "    <RoleUsage roleName='Role1'/>\n"
            + "    <RoleUsage roleName='Role3'/>\n"
            + "  </Union>\n"
            + "</Role>\n"
            + "<Role name='Role3'>\n"
            + "  <Union>\n"
            + "    <RoleUsage roleName='Role4'/>\n"
            + "    <RoleUsage roleName='Role2'/>\n"
            + "  </Union>\n"
            + "</Role>\n"
            + "<Role name='Role4'>\n"
            + "  <SchemaGrant access='all'/>\n"
            + "</Role>\n"
            + "<Role name='Role5'>\n"
            + "  <Union>\n"
            + "    <RoleUsage roleName='Role1'/>\n"
            + "    <RoleUsage roleName='Role4'/>\n"
            + "  </Union>\n"
            + "</Role>")
            .withRole("Role1");
        testContext.assertErrorList().containsError(
            "Role 'Role3' has cyclic dependencies on other roles \\(in Role "
            + "'Role3'\\) \\(at ${pos}\\)",
            "<Role name='Role3'>");
    }

    public void _testValidatorFindsNumericLevel() {
        // In the real foodmart, the level has type="Numeric"
        final TestContext testContext =
            getTestContext().createSubstitutingCube(
                "Sales",
                "  <Dimension name='Store Size in SQFT'>\n"
                + "    <Hierarchy hasAll='true' primaryKey='store_id'>\n"
                + "      <Table name='store'/>\n"
                + "      <Level name='Store Sqft' column='store_sqft' type='Numeric' uniqueMembers='true'/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>");
        final List<Exception> exceptionList = testContext.getSchemaWarnings();
        testContext.assertContains(
            exceptionList, "todo xxxxx");
    }

    public void testInvalidRoleError() {
        String schema = TestContext.getRawSchema(TestContext.DataSet.FOODMART);
        schema =
            schema.replaceFirst(
                "<Schema name='FoodMart'",
                "<Schema name='FoodMart' defaultRole='Unknown'");
        getTestContext().withSchema(schema).assertErrorList().containsError(
            "Role 'Unknown' not found \\(in Schema 'FoodMart'\\) \\(at ${pos}\\)",
            "<Schema name='FoodMart' defaultRole='Unknown' metamodelVersion='4.0'>");
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-413">
     * MONDRIAN-413, "RolapMember causes ClassCastException in compare()"</a>,
     * caused by binary column value.
     */
    public void testBinaryLevelKey() {
        if (!Bug.BugMondrian1330Fixed) {
            return;
        }
        switch (getTestContext().getDialect().getDatabaseProduct()) {
        case DERBY:
        case MYSQL:
            break;
        default:
            // Not all databases support binary literals (e.g. X'AB01'). Only
            // Derby returns them as byte[] values from its JDBC driver and
            // therefore experiences bug MONDRIAN-413.
            return;
        }
        String tableDef =
            "<InlineTable alias='binary'>\n"
            + "  <ColumnDefs>\n"
            + "    <ColumnDef name='id' type='Integer'/>\n"
            + "    <ColumnDef name='bin' type='Numeric'/>\n"
            + "    <ColumnDef name='name' type='String'/>\n"
            + "  </ColumnDefs>\n"
            + "  <Rows>\n"
            + "    <Row>\n"
            + "      <Value column='id'>2</Value>\n"
            + "      <Value column='bin'>X'4546'</Value>\n"
            + "      <Value column='name'>Ben</Value>\n"
            + "    </Row>\n"
            + "    <Row>\n"
            + "      <Value column='id'>3</Value>\n"
            + "      <Value column='bin'>X'424344'</Value>\n"
            + "      <Value column='name'>Bill</Value>\n"
            + "    </Row>\n"
            + "    <Row>\n"
            + "      <Value column='id'>4</Value>\n"
            + "      <Value column='bin'>X'424344'</Value>\n"
            + "      <Value column='name'>Bill</Value>\n"
            + "    </Row>\n"
            + "  </Rows>\n"
            + "</InlineTable>\n";
        TestContext testContext = getTestContext()
            .insertDimension(
                "Sales",
                "<Dimension name='Binary' key='Level2' table='binary'>\n"
                + "  <Attributes>"
                + "    <Attribute name='Level1' keyColumn='bin' nameColumn='name' ordinalColumn='name'/>"
                + "    <Attribute name='Level2' keyColumn='id'/>"
                + "  </Attributes>"
                + "  <Hierarchies>"
                + "    <Hierarchy name='foo' hasAll='false' primaryKey='id'>"
                + "      <Level attribute='Level1'/>"
                + "      <Level attribute='Level2'/>"
                + "    </Hierarchy>"
                + "  </Hierarchies>"
                + "</Dimension>\n")
            .insertDimensionLinks(
                "Sales",
                ArrayMap.of(
                    "Sales",
                    "<ForeignKeyLink dimension='Binary' "
                    + "foreignKeyColumn='promotion_id'/>"))
            .insertPhysTable(tableDef)
            .ignoreMissingLink();
        testContext.assertQueryReturns(
            "select {[Binary].[foo].members} on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Binary].[Ben]}\n"
            + "{[Binary].[Ben].[2]}\n"
            + "{[Binary].[Bill]}\n"
            + "{[Binary].[Bill].[3]}\n"
            + "{[Binary].[Bill].[4]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n");
        testContext.assertQueryReturns(
            "select hierarchize({[Binary].members}) on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Binary].[Ben]}\n"
            + "{[Binary].[Ben].[2]}\n"
            + "{[Binary].[Bill]}\n"
            + "{[Binary].[Bill].[3]}\n"
            + "{[Binary].[Bill].[4]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n");
    }

    /**
     * Test case for the Level@internalType attribute.
     *
     * <p>See bug <a href="http://jira.pentaho.com/browse/MONDRIAN-896">
     * MONDRIAN-896, "Oracle integer columns overflow if value &gt;>2^31"</a>.
     */
    public void testLevelInternalType() {
        // One of the keys is larger than Integer.MAX_VALUE (2 billion), so
        // will only work if we use long values.
        String tableDef =
            "<InlineTable alias='t'>\n"
            + "  <ColumnDefs>\n"
            + "    <ColumnDef name='id' type='Integer' internalType='int'/>\n"
            + "    <ColumnDef name='big_num' type='Integer' internalType='long'/>\n"
            + "    <ColumnDef name='name' type='String'/>\n"
            + "  </ColumnDefs>\n"
            + "  <Rows>\n"
            + "    <Row>\n"
            + "      <Value column='id'>0</Value>\n"
            + "      <Value column='big_num'>1234</Value>\n"
            + "      <Value column='name'>Ben</Value>\n"
            + "    </Row>\n"
            + "    <Row>\n"
            + "      <Value column='id'>519</Value>\n"
            + "      <Value column='big_num'>1234567890123</Value>\n"
            + "      <Value column='name'>Bill</Value>\n"
            + "    </Row>\n"
            + "  </Rows>\n"
            + "</InlineTable>\n";
        String dimDef =
            "<Dimension name='Big numbers' foreignKey='promotion_id' table='t' key='Level2'>\n"
            + "  <Attributes>"
            + "    <Attribute name='Level1' keyColumn='big_num'/>\n"
            + "    <Attribute name='Level2' keyColumn='id'/>\n"
            + "  </Attributes>"
            + "  <Hierarchies>"
            + "    <Hierarchy name='foo' hasAll='false' primaryKey='id'>"
            + "       <Level attribute='Level1'/>"
            + "       <Level attribute='Level2'/>"
            + "     </Hierarchy>"
            + "   </Hierarchies>"
            + "</Dimension>\n";
        Map<String, String> dimLInks = ArrayMap.of(
            "Sales",
            "<ForeignKeyLink dimension='Big numbers' "
            + "foreignKeyColumn='promotion_id'/>");
        TestContext testContext = getTestContext()
            .insertPhysTable(tableDef)
            .insertDimension("Sales", dimDef)
            .insertDimensionLinks("Sales", dimLInks)
            .ignoreMissingLink();
        testContext.assertQueryReturns(
            "select {[Big numbers].[foo].members} on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Big numbers].[foo].[1234]}\n"
            + "{[Big numbers].[foo].[1234].[0]}\n"
            + "{[Big numbers].[foo].[1234567890123]}\n"
            + "{[Big numbers].[foo].[1234567890123].[519]}\n"
            + "Row #0: 195,448\n"
            + "Row #0: 195,448\n"
            + "Row #0: 739\n"
            + "Row #0: 739\n");
    }

    /**
     * Negative test for Level@internalType attribute.
     */
    public void testLevelInternalTypeErr() {
        String tableDef =
            "<InlineTable alias='t'>\n"
            + "  <ColumnDefs>\n"
            + "    <ColumnDef name='id' type='Integer'/>\n"
            + "    <ColumnDef name='big_num' internalType='char' type='Integer'/>\n"
            + "    <ColumnDef name='name' type='String'/>\n"
            + "  </ColumnDefs>\n"
            + "  <Rows>\n"
            + "    <Row>\n"
            + "      <Value column='id'>0</Value>\n"
            + "      <Value column='big_num'>1234</Value>\n"
            + "      <Value column='name'>Ben</Value>\n"
            + "    </Row>\n"
            + "  </Rows>\n"
            + "</InlineTable>\n";
        Map<String, String> dimLInks = ArrayMap.of(
            "Sales",
            "<ForeignKeyLink dimension='Big numbers' "
            + "foreignKeyColumn='promotion_id'/>");
        String dimDef =
            "<Dimension name='Big numbers' foreignKey='promotion_id' table='t' key='Level2'>\n"
            + "  <Attributes>"
            + "    <Attribute name='Level1' keyColumn='big_num' type='Integer'/>"
            + "    <Attribute name='Level2' keyColumn='id'/>"
            + "  </Attributes>"
            + "  <Hierarchies>"
            + "    <Hierarchy name='foo' hasAll='false' primaryKey='id'>\n"
            + "      <Level attribute='Level1'/>\n"
            + "      <Level attribute='Level2'/>\n"
            + "    </Hierarchy>\n"
            + "  </Hierarchies>"
            + "</Dimension>\n";
        TestContext testContext = getTestContext()
            .insertPhysTable(tableDef)
            .insertDimension("Sales", dimDef)
            .insertDimensionLinks("Sales", dimLInks)
            .ignoreMissingLink();
        testContext.assertQueryThrows(
            "select {[Big numbers].members} on 0 from [Sales]",
            "Invalid value 'char' for attribute 'internalType' of element 'Level'. Valid values are: [int, double, Object, String, long]");
    }

    public void testAllLevelName() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "<Dimension name='Gender4' table='customer' key='Name'>\n"
            + "    <Attributes>\n"
            + "        <Attribute name='Gender' keyColumn='gender'/>\n"
            + "        <Attribute name='Name' keyColumn='customer_id' nameColumn='full_name' orderByColumn='full_name'/>\n"
            + "    </Attributes>\n"
            + "    <Hierarchies>\n"
            + "        <Hierarchy name='Gender2' allMemberName='All Gender' allLevelName='GenderLevel'>\n"
            + "            <Level attribute='Gender'/>\n"
            + "        </Hierarchy>\n"
            + "    </Hierarchies>\n"
            + "</Dimension>");
        String mdx =
            "select {[Gender4].[Gender2].[All Gender]} on columns from Sales";
        assertTrue(testContext.getSchemaWarnings().isEmpty());
        Result result = testContext.executeQuery(mdx);
        Axis axis0 = result.getAxes()[0];
        Position pos0 = axis0.getPositions().get(0);
        Member allGender = pos0.get(0);
        String caption = allGender.getLevel().getName();
        Assert.assertEquals(caption, "GenderLevel");
    }

    public void testAllMemberCaption() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "<Dimension name='Gender3' table='customer' key='Name'>\n"
            + "    <Attributes>\n"
            + "        <Attribute name='Gender' keyColumn='gender'/>\n"
            + "        <Attribute name='Name' keyColumn='customer_id' nameColumn='full_name' orderByColumn='full_name'/>\n"
            + "    </Attributes>\n"
            + "    <Hierarchies>\n"
            + "        <Hierarchy name='Gender2' allMemberName='All Gender' allMemberCaption='Frauen und Maenner'>\n"
            + "            <Level attribute='Gender'/>\n"
            + "        </Hierarchy>\n"
            + "    </Hierarchies>\n"
            + "</Dimension>");
        String mdx =
            "select {[Gender3].[Gender2].[All Gender]} on columns from Sales";
        assertTrue(testContext.getSchemaWarnings().isEmpty());
        Result result = testContext.executeQuery(mdx);
        Axis axis0 = result.getAxes()[0];
        Position pos0 = axis0.getPositions().get(0);
        Member allGender = pos0.get(0);
        String caption = allGender.getCaption();
        Assert.assertEquals(caption, "Frauen und Maenner");
    }

    public void _testAttributeHierarchy() {
        // from email from peter tran dated 2008/9/8
        // TODO: schema syntax to create attribute hierarchy
        assertQueryReturns(
            "WITH \n"
            + " MEMBER\n"
            + "  Measures.SalesPerWorkingDay AS \n"
            + "    IIF(\n"
            + "     Count(\n"
            + "      Filter(\n"
            + "        Descendants(\n"
            + "          [Date].[Calendar].CurrentMember\n"
            + "          ,[Date].[Calendar].[Date]\n"
            + "          ,SELF)\n"
            + "       ,  [Date].[Day of Week].CurrentMember.Name <> '1'\n"
            + "      )\n"
            + "    ) = 0\n"
            + "     ,NULL\n"
            + "     ,[Measures].[Internet Sales Amount]\n"
            + "      /\n"
            + "       Count(\n"
            + "         Filter(\n"
            + "           Descendants(\n"
            + "             [Date].[Calendar].CurrentMember\n"
            + "             ,[Date].[Calendar].[Date]\n"
            + "             ,SELF)\n"
            + "          ,  [Date].[Day of Week].CurrentMember.Name <> '1'\n"
            + "         )\n"
            + "       )\n"
            + "    )\n"
            + "   '\n"
            + "SELECT [Measures].[SalesPerWorkingDay]  ON 0\n"
            + ", [Date].[Calendar].[Month].MEMBERS ON 1\n"
            + "FROM [Adventure Works]", "x");
    }

    /**
     * Testcase for a problem which involved a slowly changing dimension.
     * Not actually a slowly-changing dimension - we don't have such a thing in
     * the foodmart schema - but the same structure. The dimension is a two
     * table snowflake, and the table nearer to the fact table is not used by
     * any level.
     */
    public void testScdJoin() {
        final String dimDefs =
            "<Dimension name='Product truncated' key='product_id'>\n"
            + "  <Attributes>"
            + "    <Attribute name='Product Class' table='product_class' "
            + "     keyColumn='product_class_id' nameColumn='product_subcategory' datatype='Numeric'/>"
            + "    <Attribute name='product_id' table='product' keyColumn='product_id' hasHierarchy='false'/>"
            + "  </Attributes>"
            + "</Dimension>\n";
        final Map<String, String> dimLinks = ArrayMap.of(
            "Sales",
            "<ForeignKeyLink dimension='Product truncated' foreignKeyColumn='product_id'/>");

        final TestContext testContext = getTestContext()
            .insertDimension("Sales", dimDefs)
            .insertDimensionLinks("Sales", dimLinks)
            .ignoreMissingLink();

        testContext.assertQueryReturns(
            "select non empty {[Measures].[Unit Sales]} on 0,\n"
            + " non empty Filter({[Product truncated].Members}, [Measures].[Unit Sales] > 10000) on 1\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product truncated].[Product Class].[All Product Class]}\n"
            + "{[Product truncated].[Product Class].[Fresh Fruit]}\n"
            + "{[Product truncated].[Product Class].[Fresh Vegetables]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 11,767\n"
            + "Row #2: 20,739\n");
    }

    /**
     * Tests whether the agg mgr behaves correctly if a cell request causes
     * a column to be constrained multiple times. This happens if two levels
     * map to the same column via the same join-path. If the constraints are
     * inconsistent, no data will be returned.
     */
    public void testMultipleConstraintsOnSameColumn() {
        final String cubeName = "Sales_withCities";
        final TestContext testContext = getTestContext().create(
            null,
            "<Cube name='" + cubeName + "'>\n"
            + "  <Dimensions>\n"
            + "    <Dimension source='Time'/>\n"
            + "    <Dimension name='City' table='customer' key='Name'>\n"
            + "      <Attributes>\n"
            + "        <Attribute name='Name' keyColumn='customer_id' nameColumn='full_name' orderByColumn='full_name'/>\n"
            + "        <Attribute name='City' keyColumn='city'/>\n"
            + "      </Attributes>\n"
            + "      <Hierarchies>\n"
            + "        <Hierarchy name='Cities' hasAll='true' allMemberName='All Cities'>\n"
            + "          <Level attribute='City'/>\n"
            + "        </Hierarchy>\n"
            + "      </Hierarchies>\n"
            + "    </Dimension>\n"
            + "    <Dimension name='Customer' table='customer' key='Name'>\n"
            + "      <Attributes>\n"
            + "        <Attribute name='Country' keyColumn='country'/>\n"
            + "        <Attribute name='State Province' keyColumn='state_province'/>\n"
            + "        <Attribute name='City'>\n"
            + "          <Key>\n"
            + "            <Column name='state_province'/>\n"
            + "            <Column name='city'/>\n"
            + "          </Key>\n"
            + "          <Name>\n"
            + "            <Column name='city'/>\n"
            + "          </Name>\n"
            + "        </Attribute>\n"
            + "        <Attribute name='Name' keyColumn='customer_id' nameColumn='full_name' orderByColumn='full_name'/>\n"
            + "      </Attributes>\n"
            + "      <Hierarchies>\n"
            + "        <Hierarchy name='Customers' allMemberName='All Customers'>\n"
            + "          <Level attribute='Country'/>\n"
            + "          <Level attribute='State Province'/>\n"
            + "          <Level attribute='City'/>\n"
            + "          <Level attribute='Name'/>\n"
            + "        </Hierarchy>\n"
            + "      </Hierarchies>\n"
            + "    </Dimension>\n"
            + "  </Dimensions>\n"
            + "  <MeasureGroups>\n"
            + "    <MeasureGroup name='sales' table='sales_fact_1997'>\n"
            + "      <Measures>\n"
            + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='Standard' visible='false'/>\n"
            + "        <Measure name='Store Sales' column='store_sales' aggregator='sum' formatString='#,###.00'/>\n"
            + "      </Measures>\n"
            + "      <DimensionLinks>\n"
            + "        <ForeignKeyLink dimension='Time' foreignKeyColumn='time_id'/>\n"
            + "        <ForeignKeyLink dimension='Customer' foreignKeyColumn='customer_id'/>\n"
            + "        <ForeignKeyLink dimension='City' foreignKeyColumn='customer_id'/>\n"
            + "      </DimensionLinks>\n"
            + "    </MeasureGroup>\n"
            + "  </MeasureGroups>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);

        testContext.assertQueryReturns(
            "select {\n"
            + " [Customers].[USA],\n"
            + " [Customers].[USA].[OR],\n"
            + " [Customers].[USA].[CA],\n"
            + " [Customers].[USA].[CA].[Altadena],\n"
            + " [Customers].[USA].[CA].[Burbank],\n"
            + " [Customers].[USA].[CA].[Burbank].[Alma Son]} ON COLUMNS\n"
            + "from [" + cubeName + "] \n"
            + "where ([Cities].[All Cities].[Burbank], [Measures].[Store Sales])",
            "Axis #0:\n"
            + "{[City].[Cities].[Burbank], [Measures].[Store Sales]}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[USA]}\n"
            + "{[Customer].[Customers].[USA].[OR]}\n"
            + "{[Customer].[Customers].[USA].[CA]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Altadena]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Burbank]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Burbank].[Alma Son]}\n"
            + "Row #0: 6,577.33\n"
            + "Row #0: \n"
            + "Row #0: 6,577.33\n"
            + "Row #0: \n"
            + "Row #0: 6,577.33\n"
            + "Row #0: 36.50\n");
    }

    /**
     * Tests that get error if there is no DimensionLinks element.
     */
    public void testDimensionLinksMissing() {
        TestContext testContext =
            getTestContext()
                .create(
                    null,
                    "<Cube name='Sales Gen' factTable='sales_fact_1997'>\n"
                    + "  <Dimensions>\n"
                    + "    <Dimension name='Gender4' table='customer' key='Name'>\n"
                    + "      <Attributes>\n"
                    + "        <Attribute name='Gender' keyColumn='gender' hasHierarchy='true'/>\n"
                    + "        <Attribute name='Name' keyColumn='customer_id' nameColumn='full_name' orderByColumn='full_name'/>\n"
                    + "      </Attributes>\n"
                    + "    </Dimension>\n"
                    + "  </Dimensions>\n"
                    + "  <MeasureGroups>\n"
                    + "    <MeasureGroup name='Sales' table='sales_fact_1997'>\n"
                    + "      <Measures>\n"
                    + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='Standard'/>\n"
                    + "      </Measures>\n"
                    + "    </MeasureGroup>\n"
                    + "  </MeasureGroups>\n"
                    + "</Cube>", null, null, null, null);

        // No link for 'Customer_2'.
        testContext.assertErrorList().containsError(
            "No link for dimension 'Gender4' in measure group 'Sales' \\(in MeasureGroup 'Sales'\\) \\(at ${pos}\\)",
            "<MeasureGroup name='Sales' table='sales_fact_1997'>");
    }

    /**
     * Tests that get error if there is not a link for a particular dimension.
     *
     * @see mondrian.test.BasicQueryTest#testBug1630754()
     */
    public void testDimensionLinkMissing() {
        TestContext testContext =
            getTestContext()
                .withFlag(TestContext.Flag.AUTO_MISSING_LINK, false)
                .createSubstitutingCube(
                    "Sales",
                    "  <Dimension name='Store_2' source='Store'/>");

        // No link for 'Customer_2'.
        testContext.assertErrorList().containsError(
            "No link for dimension 'Store_2' in measure group 'Sales' \\(in MeasureGroup 'Sales'\\) \\(at ${pos}\\)",
            "<MeasureGroup name='Sales' table='sales_fact_1997'>");
    }

    public void testDimensionLinkMissingExplicitNoLink() {
        final TestContext testContext =
            getTestContext().createSubstitutingCube(
                "Sales",
                "<Dimension name='Store_2' source='Store'/>",
                null,
                null,
                null,
                ArrayMap.of(
                    "Sales",
                    "<NoLink dimension='Store_2'/>"));
        assertEquals(Collections.emptyList(), testContext.getSchemaWarnings());
        testContext.assertSimpleQuery();
    }

    public void testDimensionLinkDuplicate() {
        final TestContext testContext =
            getTestContext().createSubstitutingCube(
                "Sales",
                null,
                null,
                null,
                null,
                ArrayMap.of(
                    "Sales",
                    "<NoLink dimension='Store'/>"));
        testContext.assertErrorList().containsError(
            "More than one link for dimension 'Store' in measure group 'Sales' \\(in NoLink\\) \\(at ${pos}\\)",
            "<NoLink dimension='Store'/>");
    }

    public void testDimensionLinkIgnoreMissingLinks() {
        final TestContext testContext =
            getTestContext()
                .withFlag(TestContext.Flag.AUTO_MISSING_LINK, false)
                .createSubstitutingCube(
                    "Sales",
                    "<Dimension name='Store2' source='Store'/>")
                .withSubstitution(
                    new Util.Function1<String, String>() {
                        public String apply(String schema) {
                            final String find = "<Schema ";
                            int i = schema.indexOf(find) + find.length();
                            return schema.substring(0, i)
                                + "missingLink='ignore' "
                                + schema.substring(i);
                        }
                    });
        testContext.assertSimpleQuery();
    }

    public void testDimensionMultiUsesHierarchyName() throws SQLException {
        final TestContext testContext =
            getTestContext().createSubstitutingCube(
                "Sales",
                "<Dimension name='Store_2' source='Store'/>",
                null,
                null,
                null,
                ArrayMap.of(
                    "Sales",
                    "<NoLink dimension='Store_2'/>"));
        assertEquals(Collections.emptyList(), testContext.getSchemaWarnings());
        testContext.assertSimpleQuery();
        org.olap4j.metadata.Dimension dimensionStore2 =
            testContext.getOlap4jConnection().getOlapSchema()
                .getCubes().get("Sales")
                .getDimensions().get("Store_2");
        org.olap4j.metadata.Hierarchy hierarchyStores =
            dimensionStore2.getHierarchies().get("Stores");
        assertEquals("Store_2", hierarchyStores.getDimension().getName());
        assertEquals("[Store_2].[Stores]", hierarchyStores.getUniqueName());
    }

    // TODO: enable this test as part of PhysicalSchema work
    // TODO: also add a test that Table.alias, Join.leftAlias and
    // Join.rightAlias cannot be the empty string.
    public void _testNonUniqueAlias() {
        final TestContext testContext =
            getTestContext().createSubstitutingCube(
                "Sales",
                "  <Dimension name='Product truncated' foreignKey='product_id'>\n"
                + "    <Hierarchy hasAll='true' primaryKey='product_id' primaryKeyTable='product'>\n"
                + "      <Join leftKey='product_class_id' rightKey='product_class_id'>\n"
                + "        <Table name='product' alias='product_class'/>\n"
                + "        <Table name='product_class'/>\n"
                + "      </Join>\n"
                + "      <Level name='Product Class' table='product_class' nameColumn='product_subcategory'\n"
                + "          column='product_class_id' type='Numeric' uniqueMembers='true'/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n",
                null, null, null);
        Throwable throwable = null;
        try {
            testContext.assertSimpleQuery();
        } catch (Throwable e) {
            throwable = e;
        }
        // neither a source column or source expression specified
        TestContext.checkThrowable(
            throwable, "Alias not unique");
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-482">
     * MONDRIAN-482, "ClassCastException when obtaining RolapCubeLevel"</a>.
     */
    public void testBugMondrian482() {
        // until bug MONDRIAN-495, "Table filter concept does not support
        // dialects." is fixed, this test case only works on MySQL
        if (!Bug.BugMondrian495Fixed
            && getTestContext().getDialect().getDatabaseProduct()
            != Dialect.DatabaseProduct.MYSQL)
        {
            return;
        }

        // skip this test if using aggregates, the agg tables do not
        // enforce the SQL element in the fact table
        if (MondrianProperties.instance().UseAggregates.booleanValue()) {
            return;
        }

        // In order to reproduce the problem it was necessary to only have one
        // non empty member under USA. In the cube definition below we create a
        // cube with only CA data to achieve this.
        String tableDef =
            "<Query name='FACT' alias='FACT'>\n"
            + "  <ExpressionView>\n"
            + "    <SQL dialect='mysql'>\n"
            + "     <![CDATA[select * from `sales_fact_1997` where `sales_fact_1997`.`store_id` in (select distinct `store_id` from `store` where `store`.`store_state` = 'CA')]]>\n\n"
            + "    </SQL>\n"
            + "  </ExpressionView>\n"
            + "</Query>";
        String salesCube1 =
            "<Cube name='Sales2' defaultMeasure='Unit Sales'>\n"
            + "  <Dimensions>"
            + "  <Dimension source='Store' foreignKey='store_id'/>\n"
            + "  <Dimension source='Product' foreignKey='product_id'/>\n"
            + "   </Dimensions>"
            + "   <MeasureGroups>"
            + "      <MeasureGroup table='FACT'>"
            + "       <Measures>"
            + "  <Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='Standard'/>\n"
            + "  <Measure name='Store Sales' column='store_sales' aggregator='sum' formatString='Standard'/>\n"
            + "   </Measures>"
            + "   <DimensionLinks>"
            + "     <ForeignKeyLink dimension='Store' foreignKeyColumn='store_id'/>"
            + "     <ForeignKeyLink dimension='Product' foreignKeyColumn='product_id'/>"
            + "   </DimensionLinks>"
            + "</MeasureGroup>"
            + "</MeasureGroups>"
            + "</Cube>\n";

        final TestContext testContext = getTestContext()
            .insertCube(salesCube1)
            .insertPhysTable(tableDef);

        // First query all children of the USA. This should only return CA since
        // all the other states were filtered out. CA will be put in the member
        // cache
        String query1 =
            "WITH SET [#DataSet#] as "
            + "'NonEmptyCrossjoin({[Product].[All Products]}, {[Store].[All Stores].[USA].Children})' "
            + "SELECT {[Measures].[Unit Sales]} on columns, "
            + "NON EMPTY Hierarchize({[#DataSet#]}) on rows FROM [Sales2]";

        testContext.assertQueryReturns(
            query1,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[All Products], [Store].[Stores].[USA].[CA]}\n"
            + "Row #0: 74,748\n");

        // Now query the children of CA using the descendants function
        // This is where the ClassCastException occurs
        String query2 =
            "WITH SET [#DataSet#] as "
            + "'{Descendants([Store].[All Stores], 3)}' "
            + "SELECT {[Measures].[Unit Sales]} on columns, "
            + "NON EMPTY Hierarchize({[#DataSet#]}) on rows FROM [Sales2]";

        testContext.assertQueryReturns(
            query2,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[Stores].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Diego]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Francisco]}\n"
            + "Row #0: 21,333\n"
            + "Row #1: 25,663\n"
            + "Row #2: 25,635\n"
            + "Row #3: 2,117\n");
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-355">Bug MONDRIAN-355,
     * "adding hours/mins as levelType for level of type Dimension"</a>.
     */
    public void testBugMondrian355() {
        checkBugMondrian355("TimeHalfYears");

        // make sure that the deprecated name still works
        checkBugMondrian355("TimeHalfYear");
    }

    public void checkBugMondrian355(String timeHalfYear) {
        final String dimDef =
            "<Dimension name='Time2' key='id' type='TIME' table='time_by_day'>\n"
            + "  <Attributes>"
            + "    <Attribute name='id' keyColumn='time_id'/>"
            + "    <Attribute name='Years' keyColumn='the_year' type='Numeric' levelType='TimeHours'/>"
            + "    <Attribute name='Half year' keyColumn='quarter' uniqueMembers='false' levelType='"
            + timeHalfYear
            + "'/>"
            + "    <Attribute name='Hours' keyColumn='month_of_year' uniqueMembers='false' type='Numeric' levelType='TimeHours'/>\n"
            + "    <Attribute name='Quarter hours' keyColumn='time_id' uniqueMembers='false' type='Numeric' levelType='TimeUndefined'/>\n"
            + "  </Attributes>"
            + "  <Hierarchies>"
            + "    <Hierarchy name='Time2'>"
            + "      <Level name='Years' visible='true' attribute='Years' hideMemberIf='Never'/>"
            + "      <Level name='Half year' visible='true' attribute='Half year' hideMemberIf='Never'/>"
            + "      <Level name='Hours' visible='true' attribute='Hours' hideMemberIf='Never'/>"
            + "      <Level name='Quarter hours' visible='true' attribute='Quarter hours' hideMemberIf='Never'/>"
            + "    </Hierarchy>"
            + "  </Hierarchies>"
            + "</Dimension>";
        final Map<String, String> dimLinks =
            ArrayMap.of(
                "Sales",
                "<ForeignKeyLink dimension='Time2' foreignKeyColumn='time_id'/>");
        TestContext testContext = getTestContext()
            .insertDimension("Sales", dimDef)
            .insertDimensionLinks("Sales", dimLinks)
            .ignoreMissingLink();

        testContext.assertQueryReturns(
            "select Head([Time2].[Time2].[Quarter hours].Members, 3) on columns\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time2].[Time2].[1997].[Q1].[1].[367]}\n"
            + "{[Time2].[Time2].[1997].[Q1].[1].[368]}\n"
            + "{[Time2].[Time2].[1997].[Q1].[1].[369]}\n"
            + "Row #0: 348\n"
            + "Row #0: 635\n"
            + "Row #0: 589\n");

        // Check that can apply ParallelPeriod to a TimeUndefined level.
        testContext.assertAxisReturns(
            "PeriodsToDate([Time2].[Time2].[Quarter hours], [Time2].[Time2].[1997].[Q1].[1].[368])",
            "[Time2].[Time2].[1997].[Q1].[1].[368]");

        testContext.assertAxisReturns(
            "PeriodsToDate([Time2].[Time2].[Half year], [Time2].[Time2].[1997].[Q1].[1].[368])",
            "[Time2].[Time2].[1997].[Q1].[1].[367]\n"
            + "[Time2].[Time2].[1997].[Q1].[1].[368]");

        // Check that get an error if give invalid level type
        try {
            getTestContext()
                .createSubstitutingCube(
                    "Sales",
                    Util.replace(dimDef, "TimeUndefined", "TimeUnspecified"))
                .assertSimpleQuery();
            fail("expected error");
        } catch (Throwable e) {
            TestContext.checkThrowable(
                e,
                "Value 'TimeUnspecified' of attribute 'levelType' has illegal value 'TimeUnspecified'.  Legal values: {Regular, TimeYears, ");
        }
    }

    /**
     * Test for descriptions, captions and annotations of various schema
     * elements.
     */
    public void testCaptionDescriptionAndAnnotation() {
        final String salesCubeName = "DescSales";
        final TestContext testContext = getTestContext().withSchema(
            "<Schema name='Description schema' missingLink='ignore' metamodelVersion='4.0'>"
            + " <PhysicalSchema>"
            + "  <Table name='time_by_day' alias='time_by_day' keyColumn='time_id'/>"
            + "  <Table name='warehouse' alias='warehouse' keyColumn='warehouse_id'/>"
            + "  <Table name='sales_fact_1997' alias='sales_fact_1997'/>"
            + "  <Table name='store' alias='store' keyColumn='store_id'/>"
            + "  <Table name='region' alias='region' keyColumn='region_id'/>"
            + "  <Table name='promotion' alias='promotion' keyColumn='promotion_id'/>"
            + "  <Table name='inventory_fact_1997' alias='inventory_fact_1997'/>"
            + "  <Link source='promotion' target='region' foreignKeyColumn='sales_district_id'/>"
            + "  <Link source='region' target='store' foreignKeyColumn='region_id'/>"
            + " </PhysicalSchema>"
            + " <Dimension name='Time' visible='true' caption='Time shared caption' description='Time shared description' type='TIME' key='$Id'>"
            + "  <Hierarchies>"
            + "   <Hierarchy name='Time' visible='true' hasAll='false' caption='Time shared hierarchy caption' description='Time shared hierarchy description'>"
            + "    <Level name='Year' visible='true' attribute='Year' hideMemberIf='Never'>"
            + "    </Level>"
            + "    <Level name='Quarter' visible='true' attribute='Quarter' hideMemberIf='Never'>"
            + "    </Level>"
            + "    <Level name='Month' visible='true' attribute='Month' hideMemberIf='Never'>"
            + "    </Level>"
            + "   </Hierarchy>"
            + "  </Hierarchies>"
            + "  <Attributes>"
            + "   <Attribute name='Year' levelType='TimeYears' table='time_by_day' datatype='Numeric' hasHierarchy='false'>"
            + "    <Key>"
            + "     <Column table='time_by_day' name='the_year'>"
            + "     </Column>"
            + "    </Key>"
            + "   </Attribute>"
            + "   <Attribute name='Quarter' levelType='TimeQuarters' table='time_by_day' datatype='String' hasHierarchy='false'>"
            + "    <Key>"
            + "     <Column table='time_by_day' name='the_year'>"
            + "     </Column>"
            + "     <Column table='time_by_day' name='quarter'>"
            + "     </Column>"
            + "    </Key>"
            + "    <Name>"
            + "     <Column table='time_by_day' name='quarter'>"
            + "     </Column>"
            + "    </Name>"
            + "   </Attribute>"
            + "   <Attribute name='Month' levelType='TimeMonths' table='time_by_day' datatype='Numeric' hasHierarchy='false'>"
            + "    <Key>"
            + "     <Column table='time_by_day' name='the_year'>"
            + "     </Column>"
            + "     <Column table='time_by_day' name='quarter'>"
            + "     </Column>"
            + "     <Column table='time_by_day' name='month_of_year'>"
            + "     </Column>"
            + "    </Key>"
            + "    <Name>"
            + "     <Column table='time_by_day' name='month_of_year'>"
            + "     </Column>"
            + "    </Name>"
            + "   </Attribute>"
            + "   <Attribute name='$Id' levelType='Regular' table='time_by_day' keyColumn='time_id' hasHierarchy='false'>"
            + "   </Attribute>"
            + "  </Attributes>"
            + " </Dimension>"
            + " <Dimension name='Warehouse' visible='true' key='$Id'>"
            + "  <Hierarchies>"
            + "   <Hierarchy name='Warehouse' visible='true' hasAll='true'>"
            + "    <Level name='Country' visible='true' attribute='Country' hideMemberIf='Never'>"
            + "    </Level>"
            + "    <Level name='State Province' visible='true' attribute='State Province' hideMemberIf='Never'>"
            + "    </Level>"
            + "    <Level name='City' visible='true' attribute='City' hideMemberIf='Never'>"
            + "    </Level>"
            + "    <Level name='Warehouse Name' visible='true' attribute='Warehouse Name' hideMemberIf='Never'>"
            + "    </Level>"
            + "   </Hierarchy>"
            + "  </Hierarchies>"
            + "  <Attributes>"
            + "   <Attribute name='Country' levelType='Regular' table='warehouse' datatype='String' hasHierarchy='false'>"
            + "    <Key>"
            + "     <Column table='warehouse' name='warehouse_country'>"
            + "     </Column>"
            + "    </Key>"
            + "   </Attribute>"
            + "   <Attribute name='State Province' levelType='Regular' table='warehouse' datatype='String' hasHierarchy='false'>"
            + "    <Key>"
            + "     <Column table='warehouse' name='warehouse_state_province'>"
            + "     </Column>"
            + "    </Key>"
            + "   </Attribute>"
            + "   <Attribute name='City' levelType='Regular' table='warehouse' datatype='String' hasHierarchy='false'>"
            + "    <Key>"
            + "     <Column table='warehouse' name='warehouse_state_province'>"
            + "     </Column>"
            + "     <Column table='warehouse' name='warehouse_city'>"
            + "     </Column>"
            + "    </Key>"
            + "    <Name>"
            + "     <Column table='warehouse' name='warehouse_city'>"
            + "     </Column>"
            + "    </Name>"
            + "   </Attribute>"
            + "   <Attribute name='Warehouse Name' levelType='Regular' table='warehouse' datatype='String' hasHierarchy='false'>"
            + "    <Key>"
            + "     <Column table='warehouse' name='warehouse_name'>"
            + "     </Column>"
            + "    </Key>"
            + "   </Attribute>"
            + "   <Attribute name='$Id' levelType='Regular' table='warehouse' keyColumn='warehouse_id' hasHierarchy='false'>"
            + "   </Attribute>"
            + "  </Attributes>"
            + " </Dimension>"
            + " <Cube name='DescSales' visible='true' description='Cube description' cache='true' enabled='true'>"
            + "  <Dimensions>"
            + "   <Dimension name='Store' visible='true' caption='Dimension caption' description='Dimension description' key='$Id'>"
            + "    <Hierarchies>"
            + "     <Hierarchy name='Store' visible='true' hasAll='true' caption='Hierarchy caption' description='Hierarchy description'>"
            + "      <Annotations>"
            + "       <Annotation name='a'>"
            + "        Hierarchy       </Annotation>"
            + "      </Annotations>"
            + "      <Level name='Store Country' visible='true' attribute='Store Country' hideMemberIf='Never' description='Level description' caption='Level caption'>"
            + "       <Annotations>"
            + "        <Annotation name='a'>"
            + "         Level        </Annotation>"
            + "       </Annotations>"
            + "      </Level>"
            + "      <Level name='Store Region' visible='true' attribute='Store Region' hideMemberIf='Never'>"
            + "      </Level>"
            + "      <Level name='Store Name' visible='true' attribute='Store Name' hideMemberIf='Never'>"
            + "      </Level>"
            + "     </Hierarchy>"
            + "    </Hierarchies>"
            + "    <Attributes>"
            + "     <Attribute name='Store Country' levelType='Regular' table='store' datatype='String' hasHierarchy='false'>"
            + "      <Key>"
            + "       <Column table='store' name='store_country'>"
            + "       </Column>"
            + "      </Key>"
            + "     </Attribute>"
            + "     <Attribute name='Store Region' levelType='Regular' table='region' datatype='String' hasHierarchy='false'>"
            + "      <Key>"
            + "       <Column table='store' name='store_country'>"
            + "       </Column>"
            + "       <Column table='region' name='sales_region'>"
            + "       </Column>"
            + "      </Key>"
            + "      <Name>"
            + "       <Column table='region' name='sales_region'>"
            + "       </Column>"
            + "      </Name>"
            + "     </Attribute>"
            + "     <Attribute name='Store Name' levelType='Regular' table='store' datatype='String' hasHierarchy='false'>"
            + "      <Key>"
            + "       <Column table='store' name='store_country'>"
            + "       </Column>"
            + "       <Column table='region' name='sales_region'>"
            + "       </Column>"
            + "       <Column table='store' name='store_name'>"
            + "       </Column>"
            + "      </Key>"
            + "      <Name>"
            + "       <Column table='store' name='store_name'>"
            + "       </Column>"
            + "      </Name>"
            + "     </Attribute>"
            + "     <Attribute name='$Id' levelType='Regular' table='store' keyColumn='store_id' hasHierarchy='false'>"
            + "     </Attribute>"
            + "    </Attributes>"
            + "    <Annotations>"
            + "     <Annotation name='a'>Dimension</Annotation>"
            + "    </Annotations>"
            + "   </Dimension>"
            + "   <Dimension name='Time1' source='Time' description='Time usage description' caption='Time usage caption'>"
            + "    <Annotations>"
            + "     <Annotation name='a'>Time usage</Annotation>"
            + "    </Annotations>"
            + "   </Dimension>"
            + "   <Dimension name='Time2' source='Time'/>"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "   <MeasureGroup name='DescSales' type='fact' table='sales_fact_1997'>"
            + "    <Measures>"
            + "     <Measure name='Unit Sales' formatString='Standard' aggregator='sum' caption='Measure caption' description='Measure description'>"
            + "      <Annotations>"
            + "       <Annotation name='a'>"
            + "        Measure       </Annotation>"
            + "      </Annotations>"
            + "      <Arguments>"
            + "       <Column table='sales_fact_1997' name='unit_sales'>"
            + "       </Column>"
            + "      </Arguments>"
            + "     </Measure>"
            + "    </Measures>"
            + "    <DimensionLinks>"
            + "     <ForeignKeyLink dimension='Store'>"
            + "      <ForeignKey>"
            + "       <Column table='sales_fact_1997' name='store_id'>"
            + "       </Column>"
            + "      </ForeignKey>"
            + "     </ForeignKeyLink>"
            + "     <ForeignKeyLink dimension='Time1'>"
            + "      <ForeignKey>"
            + "       <Column table='sales_fact_1997' name='time_id'>"
            + "       </Column>"
            + "      </ForeignKey>"
            + "     </ForeignKeyLink>"
            + "     <ForeignKeyLink dimension='Time2'>"
            + "      <ForeignKey>"
            + "       <Column table='sales_fact_1997' name='time_id'>"
            + "       </Column>"
            + "      </ForeignKey>"
            + "     </ForeignKeyLink>"
            + "    </DimensionLinks>"
            + "   </MeasureGroup>"
            + "  </MeasureGroups>"
            + "  <CalculatedMembers>"
            + "   <CalculatedMember name='Foo' caption='Calc member caption' description='Calc member description' dimension='Measures'>"
            + "    <Formula>"
            + "     [Measures].[Unit Sales] + 1    </Formula>"
            + "    <Annotations>"
            + "     <Annotation name='a'>"
            + "      Calc member     </Annotation>"
            + "    </Annotations>"
            + "    <CalculatedMemberProperty name='FORMAT_STRING' value='$#,##0.00'>"
            + "    </CalculatedMemberProperty>"
            + "   </CalculatedMember>"
            + "  </CalculatedMembers>"
            + "  <NamedSets>"
            + "   <NamedSet name='Top Periods' caption='Named set caption' description='Named set description'>"
            + "    <Formula>"
            + "     TopCount([Time1].MEMBERS, 5, [Measures].[Foo])    </Formula>"
            + "    <Annotations>"
            + "     <Annotation name='a'>"
            + "      Named set     </Annotation>"
            + "    </Annotations>"
            + "   </NamedSet>"
            + "  </NamedSets>"
            + "  <Annotations>"
            + "   <Annotation name='a'>"
            + "    Cube   </Annotation>"
            + "  </Annotations>"
            + " </Cube>"
            + " <Cube name='Warehouse' visible='true' cache='true' enabled='true'>"
            + "  <Dimensions>"
            + "   <Dimension name='Time' visible='true' caption='Time shared caption' type='TIME' key='$Id'>"
            + "    <Hierarchies>"
            + "     <Hierarchy name='Time' visible='true' hasAll='false' caption='Time shared hierarchy caption' description='Time shared hierarchy description'>"
            + "      <Level name='Year' visible='true' attribute='Year' hideMemberIf='Never'>"
            + "      </Level>"
            + "      <Level name='Quarter' visible='true' attribute='Quarter' hideMemberIf='Never'>"
            + "      </Level>"
            + "      <Level name='Month' visible='true' attribute='Month' hideMemberIf='Never'>"
            + "      </Level>"
            + "     </Hierarchy>"
            + "    </Hierarchies>"
            + "    <Attributes>"
            + "     <Attribute name='Year' levelType='TimeYears' table='time_by_day' datatype='Numeric' hasHierarchy='false'>"
            + "      <Key>"
            + "       <Column table='time_by_day' name='the_year'>"
            + "       </Column>"
            + "      </Key>"
            + "     </Attribute>"
            + "     <Attribute name='Quarter' levelType='TimeQuarters' table='time_by_day' datatype='String' hasHierarchy='false'>"
            + "      <Key>"
            + "       <Column table='time_by_day' name='the_year'>"
            + "       </Column>"
            + "       <Column table='time_by_day' name='quarter'>"
            + "       </Column>"
            + "      </Key>"
            + "      <Name>"
            + "       <Column table='time_by_day' name='quarter'>"
            + "       </Column>"
            + "      </Name>"
            + "     </Attribute>"
            + "     <Attribute name='Month' levelType='TimeMonths' table='time_by_day' datatype='Numeric' hasHierarchy='false'>"
            + "      <Key>"
            + "       <Column table='time_by_day' name='the_year'>"
            + "       </Column>"
            + "       <Column table='time_by_day' name='quarter'>"
            + "       </Column>"
            + "       <Column table='time_by_day' name='month_of_year'>"
            + "       </Column>"
            + "      </Key>"
            + "      <Name>"
            + "       <Column table='time_by_day' name='month_of_year'>"
            + "       </Column>"
            + "      </Name>"
            + "     </Attribute>"
            + "     <Attribute name='$Id' levelType='Regular' table='time_by_day' keyColumn='time_id' hasHierarchy='false'>"
            + "     </Attribute>"
            + "    </Attributes>"
            + "   </Dimension>"
            + "   <Dimension name='Warehouse' visible='true' key='$Id'>"
            + "    <Hierarchies>"
            + "     <Hierarchy name='Warehouse' visible='true' hasAll='true'>"
            + "      <Level name='Country' visible='true' attribute='Country' hideMemberIf='Never'>"
            + "      </Level>"
            + "      <Level name='State Province' visible='true' attribute='State Province' hideMemberIf='Never'>"
            + "      </Level>"
            + "      <Level name='City' visible='true' attribute='City' hideMemberIf='Never'>"
            + "      </Level>"
            + "      <Level name='Warehouse Name' visible='true' attribute='Warehouse Name' hideMemberIf='Never'>"
            + "      </Level>"
            + "     </Hierarchy>"
            + "    </Hierarchies>"
            + "    <Attributes>"
            + "     <Attribute name='Country' levelType='Regular' table='warehouse' datatype='String' hasHierarchy='false'>"
            + "      <Key>"
            + "       <Column table='warehouse' name='warehouse_country'>"
            + "       </Column>"
            + "      </Key>"
            + "     </Attribute>"
            + "     <Attribute name='State Province' levelType='Regular' table='warehouse' datatype='String' hasHierarchy='false'>"
            + "      <Key>"
            + "       <Column table='warehouse' name='warehouse_state_province'>"
            + "       </Column>"
            + "      </Key>"
            + "     </Attribute>"
            + "     <Attribute name='City' levelType='Regular' table='warehouse' datatype='String' hasHierarchy='false'>"
            + "      <Key>"
            + "       <Column table='warehouse' name='warehouse_state_province'>"
            + "       </Column>"
            + "       <Column table='warehouse' name='warehouse_city'>"
            + "       </Column>"
            + "      </Key>"
            + "      <Name>"
            + "       <Column table='warehouse' name='warehouse_city'>"
            + "       </Column>"
            + "      </Name>"
            + "     </Attribute>"
            + "     <Attribute name='Warehouse Name' levelType='Regular' table='warehouse' datatype='String' hasHierarchy='false'>"
            + "      <Key>"
            + "       <Column table='warehouse' name='warehouse_name'>"
            + "       </Column>"
            + "      </Key>"
            + "     </Attribute>"
            + "     <Attribute name='$Id' levelType='Regular' table='warehouse' keyColumn='warehouse_id' hasHierarchy='false'>"
            + "     </Attribute>"
            + "    </Attributes>"
            + "   </Dimension>"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "   <MeasureGroup name='Warehouse' type='fact' table='inventory_fact_1997'>"
            + "    <Measures>"
            + "     <Measure name='Units Shipped' formatString='#.0' aggregator='sum'>"
            + "      <Arguments>"
            + "       <Column table='inventory_fact_1997' name='units_shipped'>"
            + "       </Column>"
            + "      </Arguments>"
            + "     </Measure>"
            + "    </Measures>"
            + "    <DimensionLinks>"
            + "     <ForeignKeyLink dimension='Time'>"
            + "      <ForeignKey>"
            + "       <Column table='inventory_fact_1997' name='time_id'>"
            + "       </Column>"
            + "      </ForeignKey>"
            + "     </ForeignKeyLink>"
            + "     <ForeignKeyLink dimension='Warehouse'>"
            + "      <ForeignKey>"
            + "       <Column table='inventory_fact_1997' name='warehouse_id'>"
            + "       </Column>"
            + "      </ForeignKey>"
            + "     </ForeignKeyLink>"
            + "    </DimensionLinks>"
            + "   </MeasureGroup>"
            + "  </MeasureGroups>"
            + " </Cube>"
            + " <Annotations>"
            + "  <Annotation name='a'>Schema</Annotation>"
            + "  <Annotation name='b'>Xyz</Annotation>"
            + " </Annotations>"
            + "</Schema>");

        final Result result =
            testContext.executeQuery("select from [" + salesCubeName + "]");
        final Cube cube = result.getQuery().getCube();
        assertEquals("Cube description", cube.getDescription());
        checkAnnotations(cube.getAnnotationMap(), "a", "Cube");

        final Schema schema = cube.getSchema();
        checkAnnotations(schema.getAnnotationMap(), "a", "Schema", "b", "Xyz");

        final Dimension dimension = cube.getDimensionList().get(1);
        assertEquals("Dimension description", dimension.getDescription());
        assertEquals("Dimension caption", dimension.getCaption());
        checkAnnotations(dimension.getAnnotationMap(), "a", "Dimension");

        final Hierarchy hierarchy = dimension.getHierarchyList().get(0);
        assertEquals("Hierarchy description", hierarchy.getDescription());
        assertEquals("Hierarchy caption", hierarchy.getCaption());
        checkAnnotations(hierarchy.getAnnotationMap(), "a", "Hierarchy");

        final mondrian.olap.Level level = hierarchy.getLevelList().get(1);
        assertEquals("Level description", level.getDescription());
        assertEquals("Level caption", level.getCaption());
        checkAnnotations(level.getAnnotationMap(), "a", "Level");

        // Caption comes from the CAPTION member property, defaults to name.
        // Description comes from the DESCRIPTION member property.
        // Annotations are always empty for regular members.
        final List<Member> memberList =
            cube.getSchemaReader(null).withLocus()
                .getLevelMembers(level, false);
        final Member member = memberList.get(0);
        assertEquals("Canada", member.getName());
        assertEquals("Canada", member.getCaption());
        assertNull(member.getDescription());
        checkAnnotations(member.getAnnotationMap());

        // All member. Caption defaults to name; description is null.
        final Member allMember = member.getParentMember();
        assertEquals("All Stores", allMember.getName());
        assertEquals("All Stores", allMember.getCaption());
        assertNull(allMember.getDescription());

        // All level.
        final mondrian.olap.Level allLevel = hierarchy.getLevelList().get(0);
        assertEquals("(All)", allLevel.getName());
        assertNull(allLevel.getDescription());
        assertEquals(allLevel.getName(), allLevel.getCaption());
        checkAnnotations(allLevel.getAnnotationMap());

        // the first time dimension overrides the caption and description of the
        // shared time dimension
        final Dimension timeDimension = cube.getDimensionList().get(2);
        assertEquals("Time1", timeDimension.getName());
        assertEquals("Time usage description", timeDimension.getDescription());
        assertEquals("Time usage caption", timeDimension.getCaption());
        checkAnnotations(timeDimension.getAnnotationMap(), "a", "Time usage");

        // Time1 is a usage of a shared dimension Time.
        // Now look at the hierarchy usage within that dimension usage.
        // Because the dimension usage has a name, use that as a prefix for
        // name, caption and description of the hierarchy usage.
        final Hierarchy timeHierarchy = timeDimension.getHierarchyList().get(0);
        // The hierarchy in the shared dimension does not have a name, so the
        // hierarchy usage inherits the name of the dimension usage, Time1.
        assertEquals("Time", timeHierarchy.getName());
        assertEquals("Time1", timeHierarchy.getDimension().getName());
        // The description is prefixed by the dimension usage name.
        assertEquals(
            "Time1.Time shared hierarchy description",
            timeHierarchy.getDescription());
        // The hierarchy caption is prefixed by the caption of the dimension
        // usage.
        assertEquals(
            "Time1.Time shared hierarchy caption",
            timeHierarchy.getCaption());
        // No annotations.
        checkAnnotations(timeHierarchy.getAnnotationMap());

        // the second time dimension does not overrides caption and description
        final Dimension time2Dimension = cube.getDimensionList().get(3);
        assertEquals("Time2", time2Dimension.getName());
        assertEquals(
            "Time shared description", time2Dimension.getDescription());
        assertEquals("Time shared caption", time2Dimension.getCaption());
        checkAnnotations(time2Dimension.getAnnotationMap());

        final Hierarchy time2Hierarchy =
            time2Dimension.getHierarchyList().get(0);
        // The hierarchy in the shared dimension does not have a name, so the
        // hierarchy usage inherits the name of the dimension usage, Time2.
        assertEquals("Time", time2Hierarchy.getName());
        assertEquals("Time2", time2Hierarchy.getDimension().getName());
        // The description is prefixed by the dimension usage name (because
        // dimension usage has no caption).
        assertEquals(
            "Time2.Time shared hierarchy description",
            time2Hierarchy.getDescription());
        // The hierarchy caption is prefixed by the dimension usage name
        // (because the dimension usage has no caption.
        assertEquals(
            "Time2.Time shared hierarchy caption",
            time2Hierarchy.getCaption());
        // No annotations.
        checkAnnotations(time2Hierarchy.getAnnotationMap());

        final Dimension measuresDimension = cube.getDimensionList().get(0);
        final Hierarchy measuresHierarchy =
            measuresDimension.getHierarchyList().get(0);
        final mondrian.olap.Level measuresLevel =
            measuresHierarchy.getLevelList().get(0);
        final SchemaReader schemaReader = cube.getSchemaReader(null);
        final List<Member> measures =
            schemaReader.getLevelMembers(measuresLevel, true);
        final Member measure = measures.get(0);
        assertEquals("Unit Sales", measure.getName());
        assertEquals("Measure caption", measure.getCaption());
        assertEquals("Measure description", measure.getDescription());
        assertEquals(
            measure.getDescription(),
            measure.getPropertyValue(Property.DESCRIPTION));
        assertEquals(
            measure.getCaption(),
            measure.getPropertyValue(Property.CAPTION));
        assertEquals(
            measure.getCaption(),
            measure.getPropertyValue(Property.MEMBER_CAPTION));
        checkAnnotations(measure.getAnnotationMap(), "a", "Measure");

        // The implicitly created [Fact Count] measure
        Member factCountMeasure = null;
        if (cube instanceof RolapCube) {
            RolapMeasureGroup mg = ((RolapCube)cube).getMeasureGroups().get(0);
            factCountMeasure = mg.getFactCountMeasure();
        }
        assertNotNull(factCountMeasure);
        assertEquals("Fact Count", factCountMeasure.getName());
        assertEquals(
            false,
            factCountMeasure.getPropertyValue(Property.VISIBLE));

        final int fooIndex = 1;
        final Member calcMeasure = measures.get(fooIndex);
        assertEquals("Foo", calcMeasure.getName());
        assertEquals("Calc member caption", calcMeasure.getCaption());
        assertEquals("Calc member description", calcMeasure.getDescription());
        assertEquals(
            calcMeasure.getDescription(),
            calcMeasure.getPropertyValue(Property.DESCRIPTION));
        assertEquals(
            calcMeasure.getCaption(),
            calcMeasure.getPropertyValue(Property.CAPTION));
        assertEquals(
            calcMeasure.getCaption(),
            calcMeasure.getPropertyValue(Property.MEMBER_CAPTION));
        checkAnnotations(calcMeasure.getAnnotationMap(), "a", "Calc member");

        final NamedSet namedSet = cube.getNamedSets()[0];
        assertEquals("Top Periods", namedSet.getName());
        assertEquals("Named set caption", namedSet.getCaption());
        assertEquals("Named set description", namedSet.getDescription());
        checkAnnotations(namedSet.getAnnotationMap(), "a", "Named set");
    }

    private static void checkAnnotations(
        Map<String, Annotation> annotationMap,
        String... nameVal)
    {
        assertNotNull(annotationMap);
        assertEquals(0, nameVal.length % 2);
        assertEquals(nameVal.length / 2, annotationMap.size());
        int i = 0;
        for (Map.Entry<String, Annotation> entry : annotationMap.entrySet()) {
            assertEquals(nameVal[i++], entry.getKey());
            assertEquals(nameVal[i++], entry.getValue().getValue());
        }
    }

    public void testCaptionExpression() {
        TestContext testContext = getTestContext()
            .insertPhysTable(
                "<Table name='customer' alias='customer2'>\n"
                + "  <Key>\n"
                + "    <Column name='customer_id'/>\n"
                + "  </Key>\n"
                + "  <ColumnDefs>\n"
                + "    <CalculatedColumnDef name='foo' type='String'>\n"
                + "      <ExpressionView>\n"
                + "        <SQL dialect='generic'> 'foobar'</SQL>\n"
                + "      </ExpressionView>\n"
                + "    </CalculatedColumnDef>\n"
                + "  </ColumnDefs>\n"
                + "</Table>")
            .insertDimension(
                "Sales",
                "<Dimension name='Gender2' table='customer2' key='id'>\n"
                + "  <Attributes>"
                + "    <Attribute name='Gender' keyColumn='gender' captionColumn='foo'/>"
                + "    <Attribute name='id' keyColumn='customer_id' hasHierarchy='false'/>"
                + "  </Attributes>"
                + "</Dimension>")
            .insertDimensionLinks(
                "Sales",
                ArrayMap.of(
                    "Sales",
                    "<ForeignKeyLink dimension='Gender2' foreignKeyColumn='customer_id'/>"))
            .ignoreMissingLink();
        switch (testContext.getDialect().getDatabaseProduct()) {
        case POSTGRESQL:
            // Postgres fails with:
            //   Internal error: while building member cache; sql=[select
            //     "customer"."gender" as "c0", 'foobar' as "c1" from "customer"
            //     as "customer" group by "customer"."gender", 'foobar' order by
            //     "customer"."\ gender" ASC NULLS LAST]
            //   Caused by: org.postgresql.util.PSQLException: ERROR:
            //     non-integer constant in GROUP BY
            //
            // It's difficult for mondrian to spot that it's been given a
            // constant expression. We can live with this bug. Postgres
            // shouldn't be so picky, and people shouldn't be so daft.
            return;
        }
        Result result = testContext.executeQuery(
            "select {[Gender2].Children} on columns from [Sales]");
        assertEquals(
            "foobar",
            result.getAxes()[0].getPositions().get(0).get(0).getCaption());
    }

    /**
     * Implementation of {@link PropertyFormatter} that throws.
     */
    public static class DummyPropertyFormatter implements PropertyFormatter {
        public DummyPropertyFormatter() {
            throw new RuntimeException("oops");
        }

        public String formatProperty(
            Member member, String propertyName, Object propertyValue)
        {
            return null;
        }
    }

    /**
     * Unit test for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-747">
     * MONDRIAN-747, "When joining a shared dimension into a cube at a level
     * other than its leaf level, Mondrian gives wrong results"</a>.
     */
    public void testBugMondrian747() {
        // Test case requires a pecular inline view, and it works on dialects
        // that scalar subqery, viz oracle. I believe that the mondrian code
        // being works in all dialects.
        switch (getTestContext().getDialect().getDatabaseProduct()) {
        case ORACLE:
            break;
        default:
            return;
        }
        final TestContext testContext = getTestContext().withSchema(
            "<Schema name='Test_DimensionUsage'> \n"
            + "  <Dimension type='StandardDimension' name='Store'> \n"
            + "    <Hierarchy hasAll='true' primaryKey='store_id'> \n"
            + "      <Table name='store'> \n"
            + "      </Table> \n"
            + "      <Level name='country' column='store_country' type='String' uniqueMembers='false' levelType='Regular' hideMemberIf='Never'> \n"
            + "      </Level> \n"
            + "      <Level name='state' column='store_state' type='String' uniqueMembers='false' levelType='Regular' hideMemberIf='Never'> \n"
            + "      </Level> \n"
            + "      <Level name='city' column='store_city' type='String' uniqueMembers='false' levelType='Regular' hideMemberIf='Never'> \n"
            + "      </Level> \n"
            + "    </Hierarchy> \n"
            + "  </Dimension> \n"
            + "  <Dimension type='StandardDimension' name='Product'> \n"
            + "    <Hierarchy name='New Hierarchy 0' hasAll='true' primaryKey='product_id'> \n"
            + "      <Table name='product'> \n"
            + "      </Table> \n"
            + "      <Level name='product_name' column='product_name' type='String' uniqueMembers='false' levelType='Regular' hideMemberIf='Never'> \n"
            + "      </Level> \n"
            + "    </Hierarchy> \n"
            + "  </Dimension> \n"
            + "  <Cube name='cube1' cache='true' enabled='true'> \n"
            + "    <Table name='sales_fact_1997'> \n"
            + "    </Table> \n"
            + "    <DimensionUsage source='Store' name='Store' foreignKey='store_id'> \n"
            + "    </DimensionUsage> \n"
            + "    <DimensionUsage source='Product' name='Product' foreignKey='product_id'> \n"
            + "    </DimensionUsage> \n"
            + "    <Measure name='unitsales1' column='unit_sales' datatype='Numeric' aggregator='sum' visible='true'> \n"
            + "    </Measure> \n"
            + "  </Cube> \n"
            + "  <Cube name='cube2' cache='true' enabled='true'> \n"
//            + "    <Table name='sales_fact_1997_test'/> \n"
            + "    <View alias='sales_fact_1997_test'> \n"
            + "      <SQL dialect='generic'>select 'product_id', 'time_id', 'customer_id', 'promotion_id', 'store_id', 'store_sales', 'store_cost', 'unit_sales', (select 'store_state' from 'store' where 'store_id' = 'sales_fact_1997'.'store_id') as 'sales_state_province' from 'sales_fact_1997'</SQL>\n"
            + "    </View> \n"
            + "    <DimensionUsage source='Store' level='state' name='Store' foreignKey='sales_state_province'> \n"
            + "    </DimensionUsage> \n"
            + "    <DimensionUsage source='Product' name='Product' foreignKey='product_id'> \n"
            + "    </DimensionUsage> \n"
            + "    <Measure name='unitsales2' column='unit_sales' datatype='Numeric' aggregator='sum' visible='true'> \n"
            + "    </Measure> \n"
            + "  </Cube> \n"
            + "  <VirtualCube enabled='true' name='virtual_cube'> \n"
            + "    <VirtualCubeDimension name='Store'> \n"
            + "    </VirtualCubeDimension> \n"
            + "    <VirtualCubeDimension name='Product'> \n"
            + "    </VirtualCubeDimension> \n"
            + "    <VirtualCubeMeasure cubeName='cube1' name='[Measures].[unitsales1]' visible='true'> \n"
            + "    </VirtualCubeMeasure> \n"
            + "    <VirtualCubeMeasure cubeName='cube2' name='[Measures].[unitsales2]' visible='true'> \n"
            + "    </VirtualCubeMeasure> \n"
            + "  </VirtualCube> \n"
            + "</Schema>");

        if (!Bug.BugMondrian747Fixed
            && MondrianProperties.instance().EnableGroupingSets.get())
        {
            // With grouping sets enabled, MONDRIAN-747 behavior is even worse.
            return;
        }

        // [Store].[All Stores] and [Store].[USA] should be 266,773. A higher
        // value would indicate that there is a cartesian product going on --
        // because "store_state" is not unique in "store" table.
        final String x = !Bug.BugMondrian747Fixed
            ? "1,379,620"
            : "266,773";
        testContext.assertQueryReturns(
            "select non empty {[Measures].[unitsales2]} on 0,\n"
            + " non empty [Store].members on 1\n"
            + "from [cube2]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[unitsales2]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores]}\n"
            + "{[Store].[USA]}\n"
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[OR]}\n"
            + "{[Store].[USA].[WA]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: " + x + "\n"
            + "Row #2: 373,740\n"
            + "Row #3: 135,318\n"
            + "Row #4: 870,562\n");

        // No idea why, but this value comes out TOO LOW. FIXME.
        final String y = !Bug.BugMondrian747Fixed
            && MondrianProperties.instance().ReadAggregates.get()
            && MondrianProperties.instance().UseAggregates.get()
            ? "20,957"
            : "266,773";
        testContext.assertQueryReturns(
            "select non empty {[Measures].[unitsales1]} on 0,\n"
            + " non empty [Store].members on 1\n"
            + "from [cube1]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[unitsales1]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores]}\n"
            + "{[Store].[USA]}\n"
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[CA].[San Francisco]}\n"
            + "{[Store].[USA].[OR]}\n"
            + "{[Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[USA].[WA]}\n"
            + "{[Store].[USA].[WA].[Bellingham]}\n"
            + "{[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[USA].[WA].[Spokane]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "{[Store].[USA].[WA].[Walla Walla]}\n"
            + "{[Store].[USA].[WA].[Yakima]}\n"
            + "Row #0: " + y + "\n"
            + "Row #1: 266,773\n"
            + "Row #2: 74,748\n"
            + "Row #3: 21,333\n"
            + "Row #4: 25,663\n"
            + "Row #5: 25,635\n"
            + "Row #6: 2,117\n"
            + "Row #7: 67,659\n"
            + "Row #8: 26,079\n"
            + "Row #9: 41,580\n"
            + "Row #10: 124,366\n"
            + "Row #11: 2,237\n"
            + "Row #12: 24,576\n"
            + "Row #13: 25,011\n"
            + "Row #14: 23,591\n"
            + "Row #15: 35,257\n"
            + "Row #16: 2,203\n"
            + "Row #17: 11,491\n");

        testContext.assertQueryReturns(
            "select non empty {[Measures].[unitsales2], [Measures].[unitsales1]} on 0,\n"
            + " non empty [Store].[Stores].members on 1\n"
            + "from [virtual_cube]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[unitsales2]}\n"
            + "{[Measures].[unitsales1]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores]}\n"
            + "{[Store].[USA]}\n"
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[CA].[San Francisco]}\n"
            + "{[Store].[USA].[OR]}\n"
            + "{[Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[USA].[WA]}\n"
            + "{[Store].[USA].[WA].[Bellingham]}\n"
            + "{[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[USA].[WA].[Spokane]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "{[Store].[USA].[WA].[Walla Walla]}\n"
            + "{[Store].[USA].[WA].[Yakima]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: " + y + "\n"
            + "Row #1: 1,379,620\n"
            + "Row #1: 266,773\n"
            + "Row #2: 373,740\n"
            + "Row #2: 74,748\n"
            + "Row #3: \n"
            + "Row #3: 21,333\n"
            + "Row #4: \n"
            + "Row #4: 25,663\n"
            + "Row #5: \n"
            + "Row #5: 25,635\n"
            + "Row #6: \n"
            + "Row #6: 2,117\n"
            + "Row #7: 135,318\n"
            + "Row #7: 67,659\n"
            + "Row #8: \n"
            + "Row #8: 26,079\n"
            + "Row #9: \n"
            + "Row #9: 41,580\n"
            + "Row #10: 870,562\n"
            + "Row #10: 124,366\n"
            + "Row #11: \n"
            + "Row #11: 2,237\n"
            + "Row #12: \n"
            + "Row #12: 24,576\n"
            + "Row #13: \n"
            + "Row #13: 25,011\n"
            + "Row #14: \n"
            + "Row #14: 23,591\n"
            + "Row #15: \n"
            + "Row #15: 35,257\n"
            + "Row #16: \n"
            + "Row #16: 2,203\n"
            + "Row #17: \n"
            + "Row #17: 11,491\n");
    }

    /**
     * Unit test for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-463">
     * MONDRIAN-463, "Snowflake dimension with 3-way join."</a>.
     */
    public void testBugMondrian463() {
        if (!Bug.BugMondrian1335Fixed) {
            return;
        }
        if (!MondrianProperties.instance().FilterChildlessSnowflakeMembers
            .get())
        {
            // Similar to aggregates. If we turn off filtering,
            // we get wild stuff because of referential integrity.
            return;
        }
        // To build a dimension that is a 3-way snowflake, take the 2-way
        // product -> product_class join and convert to product -> store ->
        // product_class.
        //
        // It works because product_class_id covers the range 1 .. 110;
        // store_id covers every value in 0 .. 24;
        // region_id has 24 distinct values in the range 0 .. 106 (region_id 25
        // occurs twice).
        // Therefore in store, store_id -> region_id is a 25 to 24 mapping.
        checkBugMondrian463(
            getTestContext().insertDimension(
                "Sales",
                "<Dimension name='Product3' key='id'>"
                + "  <Attributes>\n"
                + "    <Attribute name='Product Family' table='product_class' keyColumn='product_family' hasHierarchy='false'/>\n"
                + "    <Attribute name='Product Department' table='product_class' keyColumn='product_department' hasHierarchy='false'/>\n"
                + "    <Attribute name='Product Category' table='product_class' keyColumn='product_category' hasHierarchy='false'/>\n"
                + "    <Attribute name='Product Subcategory' table='product_class' keyColumn='product_subcategory' hasHierarchy='false'/>\n"
                + "    <Attribute name='Product Class' table='store' keyColumn='store_id' datatype='Numeric' hasHierarchy='false'/>\n"
                + "    <Attribute name='Brand Name' table='product' keyColumn='brand_name' hasHierarchy='false'/>\n"
                + "    <Attribute name='Product Name' table='product' keyColumn='product_name' hasHierarchy='false'/>"
                + "    <Attribute name='id' table='product' keyColumn='product_id' hasHierarchy='false'/>"
                + "  </Attributes>"
                + "  <Hierarchies>"
                + "    <Hierarchy hasAll='true' name='Product'>\n"
                + "      <Level attribute='Product Family'/>\n"
                + "      <Level attribute='Product Department'/>\n"
                + "      <Level attribute='Product Category'/>\n"
                + "      <Level attribute='Product Subcategory'/>\n"
                + "      <Level attribute='Product Class'/>\n"
                + "      <Level attribute='Brand Name'/>\n"
                + "      <Level attribute='Product Name'/>\n"
                + "  </Hierarchy>\n"
                + "</Hierarchies>"
                + "</Dimension>")
                .insertDimensionLinks(
                    "Sales",
                    ArrayMap.of(
                        "Sales",
                        "<ForeignKeyLink dimension='Product3' foreignKeyColumn='product_id'/>"))
                .insertPhysTable(
                    "<Link target='product' source='store' foreignKeyColumn='product_class_id'/>"
                    + "<Link target='store' source='product_class' foreignKeyColumn='region_id'/>")
                .ignoreMissingLink()
                .remove(
                    "<Link target='product' source='product_class'>\n"
                    + "            <ForeignKey>\n"
                    + "                <Column name='product_class_id'/>\n"
                    + "            </ForeignKey>\n"
                    + "        </Link>"));

        // As above, but using shared dimension.
        if (MondrianProperties.instance().ReadAggregates.get()
            && MondrianProperties.instance().UseAggregates.get())
        {
            // With aggregates enabled, query gives different answer. This is
            // expected because some of the foreign keys have referential
            // integrity problems.
            return;
        }
        checkBugMondrian463(
            getTestContext().withSchema(
                "<?xml version='1.0'?>\n"
                + "<Schema name='FoodMart' metamodelVersion='4.0'>"
                + "<PhysicalSchema>"
                + "  <Table name='product' keyColumn='product_id'/>"
                + "  <Table name='product_class' keyColumn='product_class_id'/>"
                + "  <Table name='time_by_day' keyColumn='time_id'/>"
                + "  <Table name='store' keyColumn='store_id'/>"
                + "  <Table name='sales_fact_1997'/>"
                + "  <Link target='product' source='store' foreignKeyColumn='product_class_id'/>"
                + "  <Link target='store' source='product_class' foreignKeyColumn='region_id'/>"
                + "</PhysicalSchema>"
                + "<Dimension name='Product3' key='id'>"
                + "  <Attributes>"
                + "    <Attribute name='Product Family' table='product_class' keyColumn='product_family' hasHierarchy='false'/>\n"
                + "    <Attribute name='Product Department' table='product_class' keyColumn='product_department' hasHierarchy='false'/>\n"
                + "    <Attribute name='Product Category' table='product_class' keyColumn='product_category' hasHierarchy='false'/>\n"
                + "    <Attribute name='Product Subcategory' table='product_class' keyColumn='product_subcategory' hasHierarchy='false'/>\n"
                + "    <Attribute name='Product Class' table='store' keyColumn='store_id' type='Numeric' hasHierarchy='false'/>\n"
                + "    <Attribute name='Brand Name' table='product' keyColumn='brand_name' hasHierarchy='false'/>\n"
                + "    <Attribute name='Product Name' table='product' keyColumn='product_name' hasHierarchy='false'/>\n"
                + "    <Attribute name='id' table='product' keyColumn='product_id' hasHierarchy='false'/>\n"
                + "  </Attributes>"
                + "  <Hierarchies>"
                + "    <Hierarchy hasAll='true' name='Product'>\n"
                + "      <Level attribute='Product Family'/>\n"
                + "      <Level attribute='Product Department'/>\n"
                + "      <Level attribute='Product Category'/>\n"
                + "      <Level attribute='Product Subcategory'/>\n"
                + "      <Level attribute='Product Class'/>\n"
                + "      <Level attribute='Brand Name'/>\n"
                + "      <Level attribute='Product Name'/>\n"
                + "    </Hierarchy>\n"
                + "  </Hierarchies>"
                + "</Dimension>\n"
                + "<Cube name='Sales'>\n"
                + "  <Dimensions>"
                + "    <Dimension name='Time' type='TIME' table='time_by_day' key='id'>\n"
                + "      <Attributes>"
                + "        <Attribute name='Year' keyColumn='the_year' type='Numeric' levelType='TimeYears'/>\n"
                + "        <Attribute name='Quarter' keyColumn='quarter' levelType='TimeQuarters'/>\n"
                + "        <Attribute name='Month' keyColumn='month_of_year' type='Numeric' levelType='TimeMonths'/>\n"
                + "        <Attribute name='id' keyColumn='time_id'/>\n"
                + "      </Attributes>"
                + "      <Hierarchies>"
                + "        <Hierarchy hasAll='false' name='Time'>\n"
                + "          <Level attribute='Year'/>\n"
                + "          <Level attribute='Quarter'/>\n"
                + "          <Level attribute='Month'/>\n"
                + "        </Hierarchy>\n"
                + "      </Hierarchies>"
                + "    </Dimension>\n"
                + "    <Dimension source='Product3'/>\n"
                + "  </Dimensions>"
                + "  <MeasureGroups>"
                + "    <MeasureGroup table='sales_fact_1997'>"
                + "      <Measures>"
                + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='#,###'/>\n"
                + "      </Measures>"
                + "      <DimensionLinks>"
                + "        <ForeignKeyLink dimension='Product3' foreignKeyColumn='product_id'/>"
                + "        <ForeignKeyLink dimension='Time' foreignKeyColumn='time_id'/>"
                + "      </DimensionLinks>"
                + "    </MeasureGroup>"
                + "  </MeasureGroups>"
                + "</Cube>\n"
                + "</Schema>"));
    }

    private void checkBugMondrian463(TestContext testContext) {
        testContext.assertQueryReturns(
            "select [Measures] on 0,\n"
            + " head([Product3].members, 10) on 1\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product3].[Product].[All Product3s]}\n"
            + "{[Product3].[Product].[Drink]}\n"
            + "{[Product3].[Product].[Drink].[Baking Goods]}\n"
            + "{[Product3].[Product].[Drink].[Baking Goods].[Dry Goods]}\n"
            + "{[Product3].[Product].[Drink].[Baking Goods].[Dry Goods].[Coffee]}\n"
            + "{[Product3].[Product].[Drink].[Baking Goods].[Dry Goods].[Coffee].[24]}\n"
            + "{[Product3].[Product].[Drink].[Baking Goods].[Dry Goods].[Coffee].[24].[Amigo]}\n"
            + "{[Product3].[Product].[Drink].[Baking Goods].[Dry Goods].[Coffee].[24].[Amigo].[Amigo Lox]}\n"
            + "{[Product3].[Product].[Drink].[Baking Goods].[Dry Goods].[Coffee].[24].[Curlew]}\n"
            + "{[Product3].[Product].[Drink].[Baking Goods].[Dry Goods].[Coffee].[24].[Curlew].[Curlew Lox]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 2,647\n"
            + "Row #2: 835\n"
            + "Row #3: 835\n"
            + "Row #4: 835\n"
            + "Row #5: 835\n"
            + "Row #6: 175\n"
            + "Row #7: 175\n"
            + "Row #8: 186\n"
            + "Row #9: 186\n");
    }

    /**
     * Test for MONDRIAN-943 and MONDRIAN-465.
     */
    public void testCaptionWithOrdinalColumn() {
        String dimDef =
            "<Dimension name='Position2' key='employee_id' table='employee'>\n"
            + "  <Attributes>"
            + "    <Attribute name='Management Role' uniqueMembers='true' keyColumn='management_role'/>\n"
            + "    <Attribute name='Position Title' uniqueMembers='false' keyColumn='position_title' ordinalColumn='position_id' captionColumn='position_title'/>\n"
            + "    <Attribute name='employee_id' keyColumn='employee_id'/>\n"
            + "   </Attributes>"
            + "</Dimension>\n";
        Map<String, String> dimLinks = ArrayMap.of(
            "HR",
            "<ForeignKeyLink dimension='Position2' foreignKeyColumn='employee_id'/>");
        final TestContext tc =
            getTestContext()
                .insertDimension("HR", dimDef)
                .insertDimensionLinks("HR", dimLinks);
        String mdxQuery =
            "WITH SET [#DataSet#] as '{Descendants([Position].[All Position], 2)}' "
            + "SELECT {[Measures].[Org Salary]} on columns, "
            + "NON EMPTY Hierarchize({[#DataSet#]}) on rows FROM [HR]";
        Result result = tc.executeQuery(mdxQuery);
        Axis[] axes = result.getAxes();
        List<Position> positions = axes[1].getPositions();
        Member mall = positions.get(0).get(0);
        String caption = mall.getHierarchy().getCaption();
        assertEquals("Position", caption);
        String captionValue = mall.getCaption();
        assertEquals("HQ Information Systems", captionValue);
        mall = positions.get(14).get(0);
        captionValue = mall.getCaption();
        assertEquals("Store Manager", captionValue);
        mall = positions.get(15).get(0);
        captionValue = mall.getCaption();
        assertEquals("Store Assistant Manager", captionValue);
    }

    public void testCubesVisibility() throws Exception {
        for (Boolean testValue : new Boolean[] {true, false}) {
            String cubeDef =
                "<Cube name='Foo' visible='@REPLACE_ME@'>\n"
                + "  <Dimensions>"
                + "    <Dimension name='Store Type' key='id' table='store'>\n"
                + "      <Attributes>"
                + "        <Attribute name='Store Type' keyColumn='store_type' uniqueMembers='true'/>"
                + "        <Attribute name='id' keyColumn='store_id'/>"
                + "      </Attributes>"
                + "    </Dimension>\n"
                + "  </Dimensions>"
                + "  <MeasureGroups>"
                + "    <MeasureGroup table='store'>"
                + "      <Measures>"
                + "        <Measure name='Store Sqft' column='store_sqft' aggregator='sum'\n"
                + "         formatString='#,###'/>\n"
                + "      </Measures>"
                + "      <DimensionLinks>\n"
                + "        <ForeignKeyLink dimension='Store Type' foreignKeyColumn='store_id'/>\n"
                + "      </DimensionLinks>\n"
                + "    </MeasureGroup>"
                + "  </MeasureGroups>"
                + "</Cube>\n";
            cubeDef = cubeDef.replace(
                "@REPLACE_ME@",
                String.valueOf(testValue));
            final TestContext context =
                getTestContext().create(
                    null, cubeDef, null, null, null, null);
            final Cube cube =
                context.getConnection().getSchema()
                    .lookupCube("Foo", true);
            assertTrue(testValue.equals(cube.isVisible()));
        }
    }

    public void testDimensionVisibility() throws Exception {
        for (Boolean testValue : new Boolean[] {true, false}) {
            String cubeDef =
                "<Cube name='Foo'>\n"
                + "  <Dimensions>\n"
                + "    <Dimension name='Bar' table='store' visible='@REPLACE_ME@' key='id'>\n"
                + "      <Attributes>"
                + "        <Attribute name='Store Type' keyColumn='store_type' uniqueMembers='true'/>"
                + "        <Attribute name='id' keyColumn='store_id'/>"
                + "      </Attributes>"
                + "    </Dimension>\n"
                + "  </Dimensions>"
                + "  <MeasureGroups>"
                + "    <MeasureGroup table='store'>"
                + "      <Measures>"
                + "        <Measure name='Store Sqft' column='store_sqft' aggregator='sum'\n"
                + "         formatString='#,###'/>\n"
                + "        </Measures>"
                + "      <DimensionLinks>\n"
                + "        <ForeignKeyLink dimension='Bar' foreignKeyColumn='store_id'/>\n"
                + "      </DimensionLinks>\n"
                + "    </MeasureGroup>"
                + "  </MeasureGroups>"
                + "</Cube>\n";
            cubeDef = cubeDef.replace(
                "@REPLACE_ME@",
                String.valueOf(testValue));
            final TestContext context =
                getTestContext().create(
                    null, cubeDef, null, null, null, null);
            final Cube cube =
                context.getConnection().getSchema()
                    .lookupCube("Foo", true);
            Dimension dim = null;
            for (Dimension dimCheck : cube.getDimensionList()) {
                if (dimCheck.getName().equals("Bar")) {
                    dim = dimCheck;
                }
            }
            assertNotNull(dim);
            assertTrue(testValue.equals(dim.isVisible()));
        }
    }

    public void testDimensionUsageVisibility() throws Exception {
        for (Boolean testValue : new Boolean[] {true, false}) {
            String cubeDef =
                "<Cube name='Foo'>\n"
                + "  <Dimensions>"
                + "    <Dimension name='Bar' table='store' key='Store Id' visible='@REPLACE_ME@'>\n"
                + "      <Attributes>"
                + "        <Attribute name='Store Id' keyColumn='store_id'/>"
                + "       </Attributes>"
                + "     </Dimension>"
                + "  </Dimensions>\n"
                + "  <MeasureGroups>"
                + "    <MeasureGroup name='Foo' table='store'>"
                + "      <Measures>"
                + "        <Measure name='Store Sqft' column='store_sqft' aggregator='sum'\n"
                + "         formatString='#,###'/>"
                + "      </Measures>"
                + "      <DimensionLinks>\n"
                + "        <ForeignKeyLink dimension='Bar' foreignKeyColumn='store_id'/>\n"
                + "      </DimensionLinks>\n"
                + "    </MeasureGroup>"
                + "  </MeasureGroups>\n"
                + "</Cube>\n";
            String replaced =
                cubeDef.replace("@REPLACE_ME@", String.valueOf(testValue));
            final TestContext context =
                getTestContext().create(
                    null,
                    replaced,
                    null,
                    null,
                    null,
                    null);
            final Cube cube =
                context.getConnection().getSchema()
                    .lookupCube("Foo", true);
            Dimension dim = null;
            for (Dimension dimCheck : cube.getDimensionList()) {
                if (dimCheck.getName().equals("Bar")) {
                    dim = dimCheck;
                }
            }
            assertNotNull(dim);
            assertTrue(testValue.equals(dim.isVisible()));
        }
    }

    public void testHierarchyVisibility() throws Exception {
        for (Boolean testValue : new Boolean[] {true, false}) {
            String cubeDef =
                "<Cube name='Foo'>\n"
                + "  <Dimensions>"
                + "    <Dimension name='Bar' table='store' key='id'>\n"
                + "      <Attributes>"
                + "        <Attribute name='Store Type' keyColumn='store_type' uniqueMembers='true'/>"
                + "        <Attribute name='id' keyColumn='store_id'/>"
                + "      </Attributes>"
                + "      <Hierarchies>"
                + "        <Hierarchy name='Bacon' hasAll='true' visible='@REPLACE_ME@'>\n"
                + "          <Level attribute='Store Type'/>\n"
                + "        </Hierarchy>"
                + "      </Hierarchies>\n"
                + "    </Dimension>\n"
                + "  </Dimensions>"
                + "  <MeasureGroups>"
                + "    <MeasureGroup table='store'>"
                + "      <Measures>"
                + "        <Measure name='Store Sqft' column='store_sqft' aggregator='sum'\n"
                + "         formatString='#,###'/>\n"
                + "      </Measures>"
                + "      <DimensionLinks>"
                + "        <ForeignKeyLink dimension='Bar' foreignKeyColumn='store_id'/>"
                + "      </DimensionLinks>"
                + "    </MeasureGroup>"
                + "  </MeasureGroups>"
                + "</Cube>\n";
            cubeDef = cubeDef.replace(
                "@REPLACE_ME@",
                String.valueOf(testValue));
            final TestContext context = getTestContext().insertCube(cubeDef);
            final Cube cube =
                context.getConnection().getSchema()
                    .lookupCube("Foo", true);
            Dimension dim = null;
            for (Dimension dimCheck : cube.getDimensionList()) {
                if (dimCheck.getName().equals("Bar")) {
                    dim = dimCheck;
                }
            }
            assertNotNull(dim);
            final Hierarchy hier = dim.getHierarchy();
            assertNotNull(hier);
            assertEquals(
                "Bacon",
                hier.getName());
            assertTrue(testValue.equals(hier.isVisible()));
        }
    }

    public void testLevelVisibility() throws Exception {
        for (Boolean testValue : new Boolean[] {true, false}) {
            String cubeDef =
                "<Cube name='Foo'>\n"
                + "  <Dimensions>"
                + "    <Dimension name='Bar' key='id'>\n"
                + "      <Attributes>"
                + "        <Attribute name='Samosa' table='store' keyColumn='store_type' uniqueMembers='true' visible='@REPLACE_ME@'/>"
                + "        <Attribute name='id' table='store' keyColumn='store_id'/>"
                + "      </Attributes>"
                + "      <Hierarchies>"
                + "        <Hierarchy name='Bacon' hasAll='false'>\n"
                + "          <Level attribute='Samosa' visible='@REPLACE_ME@'/>\n"
                + "        </Hierarchy>"
                + "      </Hierarchies>\n"
                + "    </Dimension>\n"
                + "  </Dimensions>"
                + "  <MeasureGroups>"
                + "    <MeasureGroup table='store'>"
                + "      <Measures>"
                + "        <Measure name='Store Sqft' column='store_sqft' aggregator='sum'\n"
                + "         formatString='#,###'/>\n"
                + "      </Measures>"
                + "      <DimensionLinks>\n"
                + "        <ForeignKeyLink dimension='Bar' foreignKeyColumn='store_id'/>\n"
                + "      </DimensionLinks>\n"
                + "    </MeasureGroup>"
                + "  </MeasureGroups>"
                + "</Cube>\n";
            cubeDef = cubeDef.replace(
                "@REPLACE_ME@",
                String.valueOf(testValue));
            final TestContext context =
                getTestContext().create(
                    null, cubeDef, null, null, null, null);
            final Cube cube =
                context.getConnection().getSchema()
                    .lookupCube("Foo", true);
            Dimension dim = null;
            for (Dimension dimCheck : cube.getDimensionList()) {
                if (dimCheck.getName().equals("Bar")) {
                    dim = dimCheck;
                }
            }
            assertNotNull(dim);
            final Hierarchy hier = dim.getHierarchy();
            assertNotNull(hier);
            assertEquals(
                "Bacon",
                hier.getName());
            final mondrian.olap.Level level = hier.getLevelList().get(0);
            assertEquals("Samosa", level.getName());
            assertTrue(testValue.equals(level.isVisible()));
        }
    }

    public void testNonCollapsedAggregate() throws Exception {
        if (!MondrianProperties.instance().UseAggregates.get()
            && !MondrianProperties.instance().ReadAggregates.get())
        {
            return;
        }
        final String cube =
            "<Cube name='Foo' defaultMeasure='Unit Sales'>\n"
            + "  <Table name='sales_fact_1997'>\n"
            + "    <AggExclude name='agg_g_ms_pcat_sales_fact_1997'/>"
            + "    <AggExclude name='agg_c_14_sales_fact_1997'/>"
            + "    <AggExclude name='agg_pl_01_sales_fact_1997'/>"
            + "    <AggExclude name='agg_ll_01_sales_fact_1997'/>"
            + "    <AggName name='agg_l_05_sales_fact_1997'>"
            + "        <AggFactCount column='fact_count'/>\n"
            + "        <AggIgnoreColumn column='customer_id'/>\n"
            + "        <AggIgnoreColumn column='store_id'/>\n"
            + "        <AggIgnoreColumn column='promotion_id'/>\n"
            + "        <AggIgnoreColumn column='store_sales'/>\n"
            + "        <AggIgnoreColumn column='store_cost'/>\n"
            + "        <AggMeasure name='[Measures].[Unit Sales]' column='unit_sales' />\n"
            + "        <AggLevel name='[Product].[Product Id]' column='product_id' collapsed='false'/>\n"
            + "    </AggName>\n"
            + "</Table>\n"
            + "<Dimension foreignKey='product_id' name='Product'>\n"
            + "<Hierarchy hasAll='true' primaryKey='product_id' primaryKeyTable='product'>\n"
            + "  <Join leftKey='product_class_id' rightKey='product_class_id'>\n"
            + " <Table name='product'/>\n"
            + " <Table name='product_class'/>\n"
            + "  </Join>\n"
            + "  <Level name='Product Family' table='product_class' column='product_family'\n"
            + "   uniqueMembers='true'/>\n"
            + "  <Level name='Product Department' table='product_class' column='product_department'\n"
            + "   uniqueMembers='false'/>\n"
            + "  <Level name='Product Category' table='product_class' column='product_category'\n"
            + "   uniqueMembers='false'/>\n"
            + "  <Level name='Product Subcategory' table='product_class' column='product_subcategory'\n"
            + "   uniqueMembers='false'/>\n"
            + "  <Level name='Brand Name' table='product' column='brand_name' uniqueMembers='false'/>\n"
            + "  <Level name='Product Name' table='product' column='product_name'\n"
            + "   uniqueMembers='true'/>\n"
            + "  <Level name='Product Id' table='product' column='product_id'\n"
            + "   uniqueMembers='true'/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>\n"
            + "<Measure name='Unit Sales' column='unit_sales' aggregator='sum'\n"
            + "      formatString='Standard'/>\n"
            + "</Cube>\n";
        final TestContext context =
            getTestContext().legacy().create(
                null, cube, null, null, null, null);
        context.assertQueryReturns(
            "select {[Product].[Product Family].Members} on rows, {[Measures].[Unit Sales]} on columns from [Foo]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Product].[Drink]}\n"
            + "{[Product].[Product].[Food]}\n"
            + "{[Product].[Product].[Non-Consumable]}\n"
            + "Row #0: 24,597\n"
            + "Row #1: 191,940\n"
            + "Row #2: 50,236\n");
    }

    public void testNonCollapsedAggregateOnNonUniqueLevelFails()
        throws Exception
    {
        if (!MondrianProperties.instance().UseAggregates.get()
            && !MondrianProperties.instance().ReadAggregates.get())
        {
            return;
        }
        final String cube =
            "<Cube name='Foo' defaultMeasure='Unit Sales'>\n"
            + "  <Table name='sales_fact_1997'>\n"
            + "    <AggExclude name='agg_g_ms_pcat_sales_fact_1997'/>"
            + "    <AggExclude name='agg_c_14_sales_fact_1997'/>"
            + "    <AggExclude name='agg_pl_01_sales_fact_1997'/>"
            + "    <AggExclude name='agg_ll_01_sales_fact_1997'/>"
            + "    <AggName name='agg_l_05_sales_fact_1997'>"
            + "        <AggFactCount column='fact_count'/>\n"
            + "        <AggIgnoreColumn column='customer_id'/>\n"
            + "        <AggIgnoreColumn column='store_id'/>\n"
            + "        <AggIgnoreColumn column='promotion_id'/>\n"
            + "        <AggIgnoreColumn column='store_sales'/>\n"
            + "        <AggIgnoreColumn column='store_cost'/>\n"
            + "        <AggMeasure name='[Measures].[Unit Sales]' column='unit_sales' />\n"
            + "        <AggLevel name='[Product].[Product Name]' column='product_id' collapsed='false'/>\n"
            + "    </AggName>\n"
            + "</Table>\n"
            + "<Dimension foreignKey='product_id' name='Product'>\n"
            + "<Hierarchy hasAll='true' primaryKey='product_id' primaryKeyTable='product'>\n"
            + "  <Join leftKey='product_class_id' rightKey='product_class_id'>\n"
            + " <Table name='product'/>\n"
            + " <Table name='product_class'/>\n"
            + "  </Join>\n"
            + "  <Level name='Product Family' table='product_class' column='product_family'\n"
            + "   uniqueMembers='true'/>\n"
            + "  <Level name='Product Department' table='product_class' column='product_department'\n"
            + "   uniqueMembers='false'/>\n"
            + "  <Level name='Product Category' table='product_class' column='product_category'\n"
            + "   uniqueMembers='false'/>\n"
            + "  <Level name='Product Subcategory' table='product_class' column='product_subcategory'\n"
            + "   uniqueMembers='false'/>\n"
            + "  <Level name='Brand Name' table='product' column='brand_name' uniqueMembers='false'/>\n"
            + "  <Level name='Product Name' table='product' column='product_name'\n"
            + "   uniqueMembers='false'/>\n"
            + "  <Level name='Product Id' table='product' column='product_id'\n"
            + "   uniqueMembers='true'/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>\n"
            + "<Measure name='Unit Sales' column='unit_sales' aggregator='sum'\n"
            + "      formatString='Standard'/>\n"
            + "</Cube>\n";
        final TestContext context =
            getTestContext().legacy().create(
                null, cube, null, null, null, null);
        context.assertQueryThrows(
            "select {[Product].[Product Family].Members} on rows, {[Measures].[Unit Sales]} on columns from [Foo]",
            "mondrian.olap.MondrianException: Mondrian Error:Too many errors, '1', while loading/reloading aggregates.");
    }

    public void testTwoNonCollapsedAggregate() throws Exception {
        if (!MondrianProperties.instance().UseAggregates.get()
            && !MondrianProperties.instance().ReadAggregates.get())
        {
            return;
        }
        final String cube =
            "<Cube name='Foo' defaultMeasure='Unit Sales'>\n"
            + "  <Table name='sales_fact_1997'>\n"
            + "    <AggExclude name='agg_g_ms_pcat_sales_fact_1997'/>"
            + "    <AggExclude name='agg_c_14_sales_fact_1997'/>"
            + "    <AggExclude name='agg_pl_01_sales_fact_1997'/>"
            + "    <AggExclude name='agg_ll_01_sales_fact_1997'/>"
            + "    <AggName name='agg_l_05_sales_fact_1997'>"
            + "        <AggFactCount column='fact_count'/>\n"
            + "        <AggIgnoreColumn column='customer_id'/>\n"
            + "        <AggIgnoreColumn column='promotion_id'/>\n"
            + "        <AggIgnoreColumn column='store_sales'/>\n"
            + "        <AggIgnoreColumn column='store_cost'/>\n"
            + "        <AggMeasure name='[Measures].[Unit Sales]' column='unit_sales' />\n"
            + "        <AggLevel name='[Product].[Product Id]' column='product_id' collapsed='false'/>\n"
            + "        <AggLevel name='[Store].[Store Id]' column='store_id' collapsed='false'/>\n"
            + "    </AggName>\n"
            + "</Table>\n"
            + "<Dimension foreignKey='product_id' name='Product'>\n"
            + "<Hierarchy hasAll='true' primaryKey='product_id' primaryKeyTable='product'>\n"
            + "  <Join leftKey='product_class_id' rightKey='product_class_id'>\n"
            + " <Table name='product'/>\n"
            + " <Table name='product_class'/>\n"
            + "  </Join>\n"
            + "  <Level name='Product Family' table='product_class' column='product_family'\n"
            + "   uniqueMembers='true'/>\n"
            + "  <Level name='Product Department' table='product_class' column='product_department'\n"
            + "   uniqueMembers='false'/>\n"
            + "  <Level name='Product Category' table='product_class' column='product_category'\n"
            + "   uniqueMembers='false'/>\n"
            + "  <Level name='Product Subcategory' table='product_class' column='product_subcategory'\n"
            + "   uniqueMembers='false'/>\n"
            + "  <Level name='Brand Name' table='product' column='brand_name' uniqueMembers='false'/>\n"
            + "  <Level name='Product Name' table='product' column='product_name'\n"
            + "   uniqueMembers='true'/>\n"
            + "  <Level name='Product Id' table='product' column='product_id'\n"
            + "   uniqueMembers='true'/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>\n"
            + "  <Dimension name='Store' foreignKey='store_id' >\n"
            + "    <Hierarchy hasAll='true' primaryKey='store_id'\n"
            + "        primaryKeyTable='store'>\n"
            + "      <Join leftKey='region_id' rightKey='region_id'>\n"
            + "        <Table name='store'/>\n"
            + "        <Table name='region'/>\n"
            + "      </Join>\n"
            + "      <Level name='Store Region' table='region' column='sales_city'\n"
            + "          uniqueMembers='false'/>\n"
            + "      <Level name='Store Id' table='store' column='store_id'\n"
            + "          uniqueMembers='true'>\n"
            + "      </Level>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "<Measure name='Unit Sales' column='unit_sales' aggregator='sum'\n"
            + "      formatString='Standard'/>\n"
            + "</Cube>\n";
        final TestContext context =
            getTestContext().legacy().create(
                null, cube, null, null, null, null);
        context.assertQueryReturns(
            "select {Crossjoin([Product].[Product Family].Members, [Store].[Store Id].Members)} on rows, {[Measures].[Unit Sales]} on columns from [Foo]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[Acapulco].[1]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[Bellingham].[2]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[Beverly Hills].[6]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[Bremerton].[3]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[Camacho].[4]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[Guadalajara].[5]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[Hidalgo].[12]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[Hidalgo].[18]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[Los Angeles].[7]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[Merida].[8]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[Mexico City].[9]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[None].[0]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[Orizaba].[10]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[Portland].[11]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[Salem].[13]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[San Andres].[21]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[San Diego].[24]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[San Francisco].[14]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[Seattle].[15]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[Spokane].[16]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[Tacoma].[17]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[Vancouver].[19]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[Victoria].[20]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[Walla Walla].[22]}\n"
            + "{[Product].[Product].[Drink], [Store].[Store].[Yakima].[23]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[Acapulco].[1]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[Bellingham].[2]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[Beverly Hills].[6]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[Bremerton].[3]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[Camacho].[4]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[Guadalajara].[5]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[Hidalgo].[12]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[Hidalgo].[18]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[Los Angeles].[7]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[Merida].[8]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[Mexico City].[9]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[None].[0]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[Orizaba].[10]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[Portland].[11]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[Salem].[13]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[San Andres].[21]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[San Diego].[24]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[San Francisco].[14]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[Seattle].[15]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[Spokane].[16]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[Tacoma].[17]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[Vancouver].[19]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[Victoria].[20]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[Walla Walla].[22]}\n"
            + "{[Product].[Product].[Food], [Store].[Store].[Yakima].[23]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[Acapulco].[1]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[Bellingham].[2]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[Beverly Hills].[6]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[Bremerton].[3]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[Camacho].[4]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[Guadalajara].[5]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[Hidalgo].[12]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[Hidalgo].[18]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[Los Angeles].[7]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[Merida].[8]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[Mexico City].[9]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[None].[0]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[Orizaba].[10]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[Portland].[11]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[Salem].[13]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[San Andres].[21]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[San Diego].[24]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[San Francisco].[14]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[Seattle].[15]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[Spokane].[16]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[Tacoma].[17]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[Vancouver].[19]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[Victoria].[20]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[Walla Walla].[22]}\n"
            + "{[Product].[Product].[Non-Consumable], [Store].[Store].[Yakima].[23]}\n"
            + "Row #0: \n"
            + "Row #1: 208\n"
            + "Row #2: 1,945\n"
            + "Row #3: 2,288\n"
            + "Row #4: \n"
            + "Row #5: \n"
            + "Row #6: \n"
            + "Row #7: \n"
            + "Row #8: 2,422\n"
            + "Row #9: \n"
            + "Row #10: \n"
            + "Row #11: \n"
            + "Row #12: \n"
            + "Row #13: 2,371\n"
            + "Row #14: 3,735\n"
            + "Row #15: \n"
            + "Row #16: 2,560\n"
            + "Row #17: 175\n"
            + "Row #18: 2,213\n"
            + "Row #19: 2,238\n"
            + "Row #20: 3,092\n"
            + "Row #21: \n"
            + "Row #22: \n"
            + "Row #23: 191\n"
            + "Row #24: 1,159\n"
            + "Row #25: \n"
            + "Row #26: 1,587\n"
            + "Row #27: 15,438\n"
            + "Row #28: 17,809\n"
            + "Row #29: \n"
            + "Row #30: \n"
            + "Row #31: \n"
            + "Row #32: \n"
            + "Row #33: 18,294\n"
            + "Row #34: \n"
            + "Row #35: \n"
            + "Row #36: \n"
            + "Row #37: \n"
            + "Row #38: 18,632\n"
            + "Row #39: 29,905\n"
            + "Row #40: \n"
            + "Row #41: 18,369\n"
            + "Row #42: 1,555\n"
            + "Row #43: 18,159\n"
            + "Row #44: 16,925\n"
            + "Row #45: 25,453\n"
            + "Row #46: \n"
            + "Row #47: \n"
            + "Row #48: 1,622\n"
            + "Row #49: 8,192\n"
            + "Row #50: \n"
            + "Row #51: 442\n"
            + "Row #52: 3,950\n"
            + "Row #53: 4,479\n"
            + "Row #54: \n"
            + "Row #55: \n"
            + "Row #56: \n"
            + "Row #57: \n"
            + "Row #58: 4,947\n"
            + "Row #59: \n"
            + "Row #60: \n"
            + "Row #61: \n"
            + "Row #62: \n"
            + "Row #63: 5,076\n"
            + "Row #64: 7,940\n"
            + "Row #65: \n"
            + "Row #66: 4,706\n"
            + "Row #67: 387\n"
            + "Row #68: 4,639\n"
            + "Row #69: 4,428\n"
            + "Row #70: 6,712\n"
            + "Row #71: \n"
            + "Row #72: \n"
            + "Row #73: 390\n"
            + "Row #74: 2,140\n");
    }

    public void testCollapsedError() throws Exception {
        if (!MondrianProperties.instance().UseAggregates.get()
            && !MondrianProperties.instance().ReadAggregates.get())
        {
            return;
        }
        final String cube =
            "<Cube name='Foo' defaultMeasure='Unit Sales'>\n"
            + "  <Table name='sales_fact_1997'>\n"
            + "    <AggExclude name='agg_g_ms_pcat_sales_fact_1997'/>"
            + "    <AggExclude name='agg_c_14_sales_fact_1997'/>"
            + "    <AggExclude name='agg_pl_01_sales_fact_1997'/>"
            + "    <AggExclude name='agg_ll_01_sales_fact_1997'/>"
            + "    <AggName name='agg_l_05_sales_fact_1997'>"
            + "        <AggFactCount column='fact_count'/>\n"
            + "        <AggIgnoreColumn column='customer_id'/>\n"
            + "        <AggIgnoreColumn column='store_id'/>\n"
            + "        <AggIgnoreColumn column='promotion_id'/>\n"
            + "        <AggIgnoreColumn column='store_sales'/>\n"
            + "        <AggIgnoreColumn column='store_cost'/>\n"
            + "        <AggMeasure name='[Measures].[Unit Sales]' column='unit_sales' />\n"
            + "        <AggLevel name='[Product].[Product Id]' column='product_id' collapsed='true'/>\n"
            + "    </AggName>\n"
            + "</Table>\n"
            + "<Dimension foreignKey='product_id' name='Product'>\n"
            + "<Hierarchy hasAll='true' primaryKey='product_id' primaryKeyTable='product'>\n"
            + "  <Join leftKey='product_class_id' rightKey='product_class_id'>\n"
            + " <Table name='product'/>\n"
            + " <Table name='product_class'/>\n"
            + "  </Join>\n"
            + "  <Level name='Product Family' table='product_class' column='product_family'\n"
            + "   uniqueMembers='true'/>\n"
            + "  <Level name='Product Department' table='product_class' column='product_department'\n"
            + "   uniqueMembers='false'/>\n"
            + "  <Level name='Product Category' table='product_class' column='product_category'\n"
            + "   uniqueMembers='false'/>\n"
            + "  <Level name='Product Subcategory' table='product_class' column='product_subcategory'\n"
            + "   uniqueMembers='false'/>\n"
            + "  <Level name='Brand Name' table='product' column='brand_name' uniqueMembers='false'/>\n"
            + "  <Level name='Product Name' table='product' column='product_name'\n"
            + "   uniqueMembers='true'/>\n"
            + "  <Level name='Product Id' table='product' column='product_id'\n"
            + "   uniqueMembers='true'/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>\n"
            + "<Measure name='Unit Sales' column='unit_sales' aggregator='sum'\n"
            + "      formatString='Standard'/>\n"
            + "</Cube>\n";
        final TestContext context =
            getTestContext().legacy().create(
                null, cube, null, null, null, null);
        context.assertQueryThrows(
            "select {[Product].[Product Family].Members} on rows, {[Measures].[Unit Sales]} on columns from [Foo]",
            "Too many errors, '1', while loading/reloading aggregates.");
    }

    /**
     * Test case for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1047">MONDRIAN-1047,
     * "IllegalArgumentException when cube has closure tables and many
     * levels"</a>.
     */
    public void testBugMondrian1047() {
        // Test case only works under MySQL, due to how columns are quoted.
        switch (getTestContext().getDialect().getDatabaseProduct()) {
        case MYSQL:
            break;
        default:
            return;
        }
        TestContext testContext =
            getTestContext().createSubstitutingCube(
                "HR",
                TestContext.repeatString(
                    100,
                    "<Dimension name='Position %1$d' key='id' table='employee'>"
                    + "  <Attributes>"
                    + "    <Attribute name='id' keyColumn='employee_id' hasHierarchy='false'/>"
                    + "    <Attribute name='Position Title' keyColumn='position_title' ordinalColumn='position_id'/>"
                    + "  </Attributes>"
                    + "</Dimension>"));
        testContext.assertQueryReturns(
            "select from [HR]",
            "Axis #0:\n"
            + "{}\n"
            + "$39,431.67");
    }

    /**
     * Test case for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1065">MONDRIAN-1065,
     * Incorrect data column is used in the WHERE clause of the SQL when
     * using Oracle DB</a>.
     */
    public void testBugMondrian1065() {
        // Test case only works under Oracle
        switch (getTestContext().getDialect().getDatabaseProduct()) {
        case ORACLE:
            break;
        default:
            return;
        }
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "  <Dimension name='PandaSteak' foreignKey='promotion_id'>\n"
            + "    <Hierarchy hasAll='false' primaryKey='lvl_3_id'>\n"
            + "      <InlineTable alias='meatShack'>\n"
            + "        <ColumnDefs>\n"
            + "          <ColumnDef name='lvl_1_id' type='Integer'/>\n"
            + "          <ColumnDef name='lvl_1_name' type='String'/>\n"
            + "          <ColumnDef name='lvl_2_id' type='Integer'/>\n"
            + "          <ColumnDef name='lvl_2_name' type='String'/>\n"
            + "          <ColumnDef name='lvl_3_id' type='Integer'/>\n"
            + "          <ColumnDef name='lvl_3_name' type='String'/>\n"
            + "        </ColumnDefs>\n"
            + "        <Rows>\n"
            + "          <Row>\n"
            + "            <Value column='lvl_1_id'>1</Value>\n"
            + "            <Value column='lvl_1_name'>level 1</Value>\n"
            + "            <Value column='lvl_2_id'>1</Value>\n"
            + "            <Value column='lvl_2_name'>level 2 - 1</Value>\n"
            + "            <Value column='lvl_3_id'>112</Value>\n"
            + "            <Value column='lvl_3_name'>level 3 - 1</Value>\n"
            + "          </Row>\n"
            + "          <Row>\n"
            + "            <Value column='lvl_1_id'>1</Value>\n"
            + "            <Value column='lvl_1_name'>level 1</Value>\n"
            + "            <Value column='lvl_2_id'>1</Value>\n"
            + "            <Value column='lvl_2_name'>level 2 - 1</Value>\n"
            + "            <Value column='lvl_3_id'>114</Value>\n"
            + "            <Value column='lvl_3_name'>level 3 - 2</Value>\n"
            + "          </Row>\n"
            + "        </Rows>\n"
            + "      </InlineTable>\n"
            + "      <Level name='Level1' column='lvl_1_id' nameColumn='lvl_1_name' />\n"
            + "      <Level name='Level2' column='lvl_2_id' nameColumn='lvl_2_name' />\n"
            + "      <Level name='Level3' column='lvl_3_id' nameColumn='lvl_3_name' />\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n");
        testContext.assertQueryReturns(
            "select non empty crossjoin({[PandaSteak].[Level3].[level 3 - 1], [PandaSteak].[Level3].[level 3 - 2]}, {[Measures].[Unit Sales], [Measures].[Store Cost]}) on columns, {[Product].[Product Family].Members} on rows from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[PandaSteak].[level 1].[level 2 - 1].[level 3 - 1], [Measures].[Unit Sales]}\n"
            + "{[PandaSteak].[level 1].[level 2 - 1].[level 3 - 1], [Measures].[Store Cost]}\n"
            + "{[PandaSteak].[level 1].[level 2 - 1].[level 3 - 2], [Measures].[Unit Sales]}\n"
            + "{[PandaSteak].[level 1].[level 2 - 1].[level 3 - 2], [Measures].[Store Cost]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Row #0: 5\n"
            + "Row #0: 3.50\n"
            + "Row #0: 9\n"
            + "Row #0: 7.70\n"
            + "Row #1: 27\n"
            + "Row #1: 20.77\n"
            + "Row #1: 46\n"
            + "Row #1: 39.88\n"
            + "Row #2: 10\n"
            + "Row #2: 9.63\n"
            + "Row #2: 17\n"
            + "Row #2: 16.21\n");
    }

    /**
     * Tests that it is OK for a physical schema to have no tables.
     */
    public void testEmptyPhysicalSchema() throws SQLException {
        // Empty physical schema is OK.
        final TestContext testContext =
            getTestContext().withSchema(
                "<Schema metamodelVersion='4.0' name='foo' >"
                + "<PhysicalSchema/>"
                + "</Schema>");
        assertEquals(0, testContext.getSchemaWarnings().size());
        final org.olap4j.metadata.Schema olapSchema =
            testContext.getOlap4jConnection().getOlapSchema();
        assertEquals("foo", olapSchema.getName());
        assertEquals(0, olapSchema.getCubes().size());
    }

    /**
     * Tests that an error occurs if a Cube has no Table child element and
     * there is no physical schema.
     */
    public void testPhysicalSchemaRequired() {
        getTestContext()
            .withSchema(
                "<Schema metamodelVersion='4.0' name='foo' >"
                + "<Cube name='SalesPhys'/>"
                + "</Schema>")
            .assertErrorList()
            .containsError(
                "Physical schema required", null);
    }

    /**
     * Tests that an error occurs if a Cube's factTable attribute references
     * table that is not defined in the physical schema.
     */
    public void testCubeReferencesUnknownTableUsage() {
        final TestContext testContext =
            getTestContext().insertCube(
                "<Cube name='SalesPhys'>\n"
                + "<MeasureGroups>"
                + "   <MeasureGroup table='Foo'>"
                + "    <Measures>"
                + "     </Measures>"
                + "    </MeasureGroup>"
                + "</MeasureGroups>"
                + "</Cube>");
        testContext.assertErrorList().containsError(
            "Unknown fact table 'Foo' \\(in MeasureGroup 'Foo'\\) \\(at ${pos}\\)",
            "<MeasureGroup table='Foo'>");
    }

    /**
     * Tests that a table's alias obscures its name.
     */
    public void testCubeReferencesObscuredTable() {
        final TestContext testContext =
            getTestContext().withSchema(
                "<Schema name='FoodMart' metamodelVersion='4.0'>"
                + "  <PhysicalSchema>"
                + "    <Table name='sales_fact_1997' alias='foo'/>\n"
                + "  </PhysicalSchema>"
                + "  <Cube name='SalesPhys'>"
                + "    <MeasureGroups>"
                + "      <MeasureGroup table='sales_fact_1997'>"
                + "        <Measures>"
                + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum'\n"
                + "         formatString='Standard'/>\n"
                + "        </Measures>"
                + "      </MeasureGroup>"
                + "    </MeasureGroups>"
                + "  </Cube>"
                + "</Schema>");
        testContext.assertErrorList().containsError(
            "Unknown fact table 'sales_fact_1997'.*",
            "<MeasureGroup table='sales_fact_1997'>");
    }

    public void testJoinInvalidInPhysicalSchema() {
        // todo:
    }

    public void testViewInPhysicalSchema() {
        // todo:
    }

    public void testInlineTableInPhysicalSchema() {
        // todo:

        // todo: declare key in inline table

        // todo: test that get error if key references invalid columns

        // todo: test that get error if inline table does not have alias

        // todo: test that get error if alias is not unique within schema

        // todo: test that get error if column names are not unique

        // todo: test that get error if define calc column in inline table
    }

    public void testCubeRequiresFactTable() {
        final TestContext testContext =
            getTestContext().insertCube(
                "<Cube name='cube without fact table'/>");
        testContext.assertErrorList().containsError(
            "Cube definition must contain a MeasureGroups element, and at least "
            + "one MeasureGroup \\(in Cube 'cube without fact table'\\) \\(at ${pos}\\)",
            "<Cube name='cube without fact table'/>");
    }

    public void testPhysicalSchema() {
        final TestContext testContext =
            TestContext.instance().withSchema(
                "<Schema name='foo' metamodelVersion='4.0'>\n"
                + "<PhysicalSchema>\n"
                + "  <Table name='sales_fact_1997' />\n"
                + "  <Table name='customer'>\n"
/*
+ "    <ColumnDefs>\n"
+ "      <ColumnDef name='customer_id'/>\n"
+ "      <ColumnDef name='state_province'/>\n"
+ "      <ColumnDef name='country'/>\n"
+ "      <ColumnDef name='city'/>\n"
+ "      <CalculatedColumnDef name='name'>\n"
+ "        <ExpressionView>\n"
+ "          <SQL dialect='oracle'>\n"
+ "            <Column name='fname'/>  || ' ' || <Column name='lname'/>\n"
+ "          </SQL>\n"
+ "          <SQL dialect='access'>\n"
+ "            <Column name='fname'/>  + ' ' + <Column name='lname'/>\n"
+ "          </SQL>\n"
+ "          <SQL dialect='postgres'>\n"
+ "            <Column name='fname'/>  || ' ' || <Column name='lname'/>\n"
+ "          </SQL>\n"
+ "          <SQL dialect='mysql'>\n"
+ "            CONCAT(<Column name='fname'/>, ' ', <Column name='lname'/>)\n"
+ "          </SQL>\n"
+ "          <SQL dialect='mssql'>\n"
+ "            <Column name='fname'/> + ' ' + <Column name='lname'/>\n"
+ "          </SQL>\n"
+ "          <SQL dialect='derby'>\n"
+ "            <Column name='fullname'/>\n"
+ "          </SQL>\n"
+ "          <SQL dialect='db2'>\n"
+ "       CONCAT(CONCAT(<Column name='fname'/>, ' '), <Column name='lname'/>)\n"
+ "          </SQL>\n"
+ "          <SQL dialect='luciddb'>\n"
+ "            <Column name='fname'/>  || ' ' || <Column name='lname'/>\n"
+ "          </SQL>\n"
+ "          <SQL dialect='generic'>\n"
+ "            <Column name='fullname'/>\n"
+ "          </SQL>\n"
+ "        </ExpressionView>\n"
+ "      </CalculatedColumnDef>\n"
+ "    </ColumnDefs>\n"
*/
                + "  </Table>\n"
                + "</PhysicalSchema>\n"
                + MINIMAL_SALES_CUBE
                + "</Schema>");
        testContext.assertSimpleQuery();
    }

    /**
     * Tests a physical schema with a simple Query element.
     */
    public void testPhysicalSchemaQuery() {
        final TestContext testContext0 = getTestContext();
        final String catalogContent0 = testContext0.getRawSchema();
        final TestContext testContext =
            testContext0.withSchema(
                "<Schema name='foo' metamodelVersion='4.0'>\n"
                + "<PhysicalSchema>\n"
                + "  <Table name='sales_fact_1997' />\n"
                + "  <Query alias='customer'>\n"
                + "    <ExpressionView>\n"
                + "    <SQL dialect='mysql'>\n"
                + "      SELECT * FROM `customer`\n"
                + "    </SQL>\n"
                + "    </ExpressionView>\n"
                + "  </Query>\n"
                + "</PhysicalSchema>\n"
                + MINIMAL_SALES_CUBE
                + "</Schema>");
        testContext.assertSimpleQuery();

        // Replace the product_class table with an equivalent query.
        final String catalogContent1 =
            catalogContent0.replace(
                "<Table name='product_class' keyColumn='product_class_id'/>",
                "<Query alias='product_class' keyColumn='product_class_id'>\n"
                + "  <ExpressionView>\n"
                + "    <SQL dialect='mysql'>\n"
                + "      SELECT * FROM `product_class`\n"
                + "    </SQL>\n"
                + "  </ExpressionView>\n"
                + "</Query>\n");
        assertNotSame(catalogContent0, catalogContent1);
        simpleProductQuery(testContext0.withSchema(catalogContent1));

        // Now using an embedded Key element
        final String catalogContent2 =
            catalogContent0.replace(
                "<Table name='product_class' keyColumn='product_class_id'/>",
                "<Query alias='product_class'>\n"
                + "  <Key><Column name='product_class_id'/></Key>\n"
                + "  <ExpressionView>\n"
                + "    <SQL dialect='mysql'>\n"
                + "      SELECT * FROM `product_class`\n"
                + "    </SQL>\n"
                + "  </ExpressionView>\n"
                + "</Query>\n");
        assertNotSame(catalogContent0, catalogContent2);
        simpleProductQuery(testContext0.withSchema(catalogContent2));

        // If neither keyColumn nor Key, get error
        final String catalogContent3 =
            catalogContent0.replace(
                "<Table name='product_class' keyColumn='product_class_id'/>",
                "<Query alias='product_class'>\n"
                + "  <ExpressionView>\n"
                + "    <SQL dialect='mysql'>\n"
                + "      SELECT * FROM `product_class`\n"
                + "    </SQL>\n"
                + "  </ExpressionView>\n"
                + "</Query>\n");
        assertNotSame(catalogContent0, catalogContent3);
        testContext0.withSchema(catalogContent3)
            .assertSchemaError(
                "Source table 'product_class' of link has no key named 'primary'. \\(in Link\\) \\(at ${pos}\\)",
                "<Link target='product' source='product_class'>");
    }

    private void simpleProductQuery(TestContext testContext) {
        testContext
            .assertQueryReturns(
                "select [Product].[Products].Children on 0 from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Product].[Products].[Drink]}\n"
                + "{[Product].[Products].[Food]}\n"
                + "{[Product].[Products].[Non-Consumable]}\n"
                + "Row #0: 24,597\n"
                + "Row #0: 191,940\n"
                + "Row #0: 50,236\n");
    }

    /** Query that is missing an ExpressionView child. */
    public void testPhysicalSchemaQueryMissingExpressionView() {
        final TestContext testContext =
            TestContext.instance().withSchema(
                "<Schema name='foo' metamodelVersion='4.0'>\n"
                + "<PhysicalSchema>\n"
                + "  <Table name='sales_fact_1997' />\n"
                + "  <Query alias='customer'>\n"
                + "    <SQL dialect='mysql'>\n"
                + "      SELECT * FROM `customer`\n"
                + "    </SQL>\n"
                + "  </Query>\n"
                + "</PhysicalSchema>\n"
                + MINIMAL_SALES_CUBE
                + "</Schema>");
        testContext.assertErrorList().containsError(
            "Missing required child element ExpressionView \\(in Query\\) \\(at ${pos}\\)",
            "<Query alias='customer'>");
    }

    /**
     * Various tests concerning the Link element inside PhysicalSchema.
     * Tests equivalence of Link/ForeignKey/Column and Link.foreignKeyColumn.
     */
    public void testPhysicalSchemaLink() {
        String catalog =
            "<Schema name='foo' metamodelVersion='4.0'>\n"
            + "<PhysicalSchema>\n"
            + "  <Table name='sales_fact_1997' />\n"
            + "  <Table name='customer'/>\n"
            + "  <Table name='product' keyColumn='product_id'/>\n"
            + "  <Table name='product_class' keyColumn='product_class_id'/>\n"
            + "  <Link source='product_class' target='product' foreignKeyColumn='product_class_id'/>\n"
            + "</PhysicalSchema>\n"
            + MINIMAL_SALES_CUBE
            + "</Schema>";
        final TestContext testContext =
            TestContext.instance().withSchema(catalog);
        testContext.assertSimpleQuery();

        // Bad column in Table.
        getTestContext().withSchema(
            catalog.replace(
                "<Table name='product_class' keyColumn='product_class_id'/>",
                "<Table name='product_class' keyColumn='product_class_idz'/>"))
            .assertSchemaError(
                "Reference to unknown column 'product_class_idz' in table "
                + "'product_class', in key of table 'product_class'. "
                + "\\(in Table\\) \\(at ${pos}\\)",
                "<Table name='product_class' keyColumn='product_class_idz'/>");

        // Bad column in Link.
        getTestContext().withSchema(
            catalog.replace(
                "<Link source='product_class' target='product' foreignKeyColumn='product_class_id'/>",
                "<Link source='product_class' target='product' foreignKeyColumn='product_class_idz'/>"))
            .assertSchemaError(
                "Column 'product_class_idz' not found in relation 'product' "
                + "\\(in Link\\) \\(at ${pos}\\)",
                "<Link source='product_class' target='product' foreignKeyColumn='product_class_idz'/>");

        // Bad column in Link (nested).
        getTestContext().withSchema(
            catalog.replace(
                "<Link source='product_class' target='product' foreignKeyColumn='product_class_id'/>",
                "<Link source='product_class' target='product'>\n"
                + "  <ForeignKey>\n"
                + "    <Column name='product_class_idz'/>\n"
                + "  </ForeignKey>\n"
                + "</Link>"))
            .assertSchemaError(
                "Column 'product_class_idz' not found in relation 'product' "
                + "\\(in Column\\) \\(at ${pos}\\)",
                "<Column name='product_class_idz'/>");

        // Remove table's key. Is a problem because it is referenced from a
        // link.
        getTestContext().withSchema(
            catalog.replace(
                "<Table name='product_class' keyColumn='product_class_id'/>",
                "<Table name='product_class'/>"))
            .assertSchemaError(
                "Source table 'product_class' of link has no key named 'primary'. \\(in Link\\) \\(at ${pos}\\)",
                "<Link source='product_class' target='product' foreignKeyColumn='product_class_id'/>");

        // Switch from Table.keyColumn to Table/Key/Column. Should work fine.
        getTestContext().withSchema(
            catalog.replace(
                "<Table name='product_class' keyColumn='product_class_id'/>",
                "<Table name='product_class'>\n"
                + "  <Key>\n"
                + "    <Column name='product_class_id'/>\n"
                + "  </Key>\n"
                + "</Table>"))
            .assertSimpleQuery();
    }

    /**
     * Various positive and negative tests for columns and calculated
     * columns in a physical schema.
     */
    public void testPhysicalColumn() {
        final TestContext testContext =
            getTestContext().withIgnore(true).withSchema(
                "<Schema name='FoodMart' metamodelVersion='4.0'>"
                + "<PhysicalSchema>"
                + "  <Table name='sales_fact_1997' alias='myfact'>\n"
                + "    <ColumnDefs>\n"
                + "      <ColumnDef name='unit_sales'/>\n"
                + "      <ColumnDef name='store_sales'/>\n"
                + "    </ColumnDefs>\n"
                + "  </Table>\n"
                + "  <Table name='unknownTable'/>\n"
                + "  <Table name='customer' alias='myfact'/>\n"
                + "  <Table name='customer' alias='customer1'/>\n"
                + "  <Table name='customer' alias='compoundKeyNotSupported'>\n"
                + "    <Key>\n"
                + "      <Column name='customer_id'/>\n"
                + "      <Column name='customer_id'/>\n"
                + "    </Key>\n"
                + "  </Table>\n"
                + "  <Table name='customer' alias='emptyKeyNotSupported'>\n"
                + "    <Key/>\n"
                + "  </Table>\n"
                + "  <Table name='customer' alias='calcColumnInKeyNotSupported'>\n"
                + "    <ColumnDefs>\n"
                + "      <CalculatedColumnDef name='cidPlusOne'>\n"
                + "        <ExpressionView>\n"
                + "          <SQL dialect='generic'>\n"
                + "            <Column name='customer_id'/> + 1\n"
                + "          </SQL>"
                + "        </ExpressionView>\n"
                + "      </CalculatedColumnDef>\n"
                + "    </ColumnDefs>\n"
                + "    <Key>\n"
                + "      <Column name='cidPlusOne'/>\n"
                + "    </Key>\n"
                + "  </Table>\n"
                + "  <Table name='customer' alias='keyInOtherTable'>\n"
                + "    <ColumnDefs>\n"
                + "      <ColumnDef name='state_province'/>\n"
                + "    </ColumnDefs>\n"
                + "    <Key>\n"
                + "      <Column table='myfact' name='unit_sales'/>\n"
                + "    </Key>\n"
                + "  </Table>\n"
                + "  <Table name='customer' alias='customer2'>\n"
                + "    <ColumnDefs>\n"
                + "      <ColumnDef name='customer_id'/>\n"
                + "      <ColumnDef name='state_province'/>\n"
                + "      <ColumnDef name='country'/>\n"
                + "      <ColumnDef name='city'/>\n"
                // column 'nonexistent' does not exist
                + "      <CalculatedColumnDef name='err1'>\n"
                + "        <ExpressionView>\n"
                + "          <SQL dialect='generic'>\n"
                + "            <Column name='nonexistent'/>\n"
                + "          </SQL>\n"
                + "        </ExpressionView>\n"
                + "      </CalculatedColumnDef>\n"
                // column 'unit_sales' does not exist in 'customer' table
                + "      <CalculatedColumnDef name='err2'>\n"
                + "        <ExpressionView>\n"
                + "          <SQL dialect='generic'>\n"
                + "            <Column name='unit_sales'/>\n"
                + "          </SQL>\n"
                + "         </ExpressionView>\n"
                + "      </CalculatedColumnDef>\n"
                // ok
                + "      <CalculatedColumnDef name='store_sales'>\n"
                + "        <ExpressionView>\n"
                + "          <SQL dialect='generic'>\n"
                + "            <Column name='store_sales' table='myfact'/>\n"
                + "          </SQL>\n"
                + "        </ExpressionView>\n"
                + "      </CalculatedColumnDef>\n"
                // reference to table by its name, which is obscured
                + "      <CalculatedColumnDef name='err3'>\n"
                + "        <ExpressionView>\n"
                + "          <SQL dialect='generic'>\n"
                + "            <Column name='store_sales' table='sales_fact_1997'/>\n"
                + "          </SQL>\n"
                + "        </ExpressionView>\n"
                + "      </CalculatedColumnDef>\n"
                // ok
                + "      <CalculatedColumnDef name='customer_id2'>\n"
                + "        <ExpressionView>\n"
                + "          <SQL dialect='generic'>\n"
                + "            <Column name='customer_id'/>\n"
                + "          </SQL>\n"
                + "        </ExpressionView>\n"
                + "      </CalculatedColumnDef>\n"
                // calc column name clashes with other column name
                + "      <CalculatedColumnDef name='state_province'>\n"
                + "        <ExpressionView>\n"
                + "          <SQL dialect='generic'>\n"
                + "            <Column name='customer_id'/>\n"
                + "          </SQL>\n"
                + "        </ExpressionView>\n"
                + "      </CalculatedColumnDef>\n"
                // calc column references unknown table
                + "      <CalculatedColumnDef name='err4'>\n"
                + "        <ExpressionView>\n"
                + "          <SQL dialect='generic'>\n"
                + "            <Column table='unknownTable' name='customer_id'/>\n"
                + "          </SQL>\n"
                + "        </ExpressionView>\n"
                + "      </CalculatedColumnDef>\n"
                // calc column references unknown column in known table
                + "      <CalculatedColumnDef name='err5'>\n"
                + "        <ExpressionView>\n"
                + "          <SQL dialect='generic'>\n"
                + "            <Column table='myfact' name='unknownColumn'/>\n"
                + "          </SQL>\n"
                + "        </ExpressionView>\n"
                + "      </CalculatedColumnDef>\n"
                // calc column references calc column - ok
                + "      <CalculatedColumnDef name='customer_id2'>\n"
                + "        <ExpressionView>\n"
                + "          <SQL dialect='generic'>\n"
                + "            <Column name='customer_id'/>\n"
                + "          </SQL>\n"
                + "        </ExpressionView>\n"
                + "      </CalculatedColumnDef>\n"
                + "    </ColumnDefs>\n"
                + "    <Key>\n"
                + "      <Column name='customer_id'/>\n"
                + "    </Key>\n"
                + "  </Table>\n"
                + "  <Link source='myfact' target='customer2'/>\n"
                + "  <Link source='unknownSource' target='customer2'/>\n"
                + "  <Link source='myfact' target='unknownTarget'/>\n"
                + "</PhysicalSchema>"
                + "<Cube name='Sales'/>\n"
                + "</Schema>");
        final TestContext.ExceptionList assertList =
            testContext.assertErrorList();
        assertList.containsError(
            "Table 'unknownTable' does not exist in database.*",
            "<Table name='unknownTable'/>");
        assertList.containsError(
            "Reference to unknown column 'nonexistent' in table 'customer2', in definition of calculated column 'customer2'.'err1'.*",
            "<SQL dialect='generic'>");
        assertList.containsError(
            "Reference to unknown column 'unit_sales' in table 'customer2', in definition of calculated column 'customer2'.'err2'.*",
            "<SQL dialect='generic'>");
        assertList.containsError(
            "Unknown table 'sales_fact_1997', in definition of calculated column 'customer2'.'err3'.*",
            "<SQL dialect='generic'>");
        assertList.containsError(
            "Duplicate table alias 'myfact'.*",
            "<Table name='customer' alias='myfact'/>");

        assertList.containsError(
            "Duplicate column 'state_province' in table 'customer2'.*",
            "<CalculatedColumnDef name='state_province'>");
        assertList.containsError(
            "Link references unknown source table 'unknownSource'.*",
            "<Link source='unknownSource' target='customer2'/>");
        assertList.containsError(
            "Link references unknown target table 'unknownTarget'.*",
            "<Link source='myfact' target='unknownTarget'/>");
        assertList.containsError(
            "Source table 'myfact' of link has no key named 'primary'.*",
            "<Link source='myfact' target='customer2'/>");
        // We would like to support compound keys in future, but for now it's
        // an error.
        assertList.containsError(
            "Key must have precisely one column; "
            + "key \\[compoundKeyNotSupported.customer_id, compoundKeyNotSupported.customer_id\\] "
            + "in table 'compoundKeyNotSupported'.*",
            "<Key>");
        assertList.containsError(
            "Key must have precisely one column; key \\[\\] in table 'emptyKeyNotSupported'.*",
            "<Key/>");
        assertList.containsError(
            "Columns in primary key must belong to key table; in table 'keyInOtherTable'.*",
            "<Table name='customer' alias='keyInOtherTable'>");
        // We would like to support calculated columns in compound keys in
        // future, but for now it's an error.
        assertList.containsError(
            "Key must not contain calculated column; calculated column 'cidPlusOne' in table 'calcColumnInKeyNotSupported'.*",
            "<Table name='customer' alias='calcColumnInKeyNotSupported'>");
    }

    public void testPhysicalSchemaColumnRequiresTable() {
        final String physSchema =
            "<PhysicalSchema>"
            + "  <Table name='sales_fact_1997'/>\n"
            + "  <Table name='time_by_day'/>\n"
            + "</PhysicalSchema>";
        final String cube =
            "<Cube name='SalesPhys'>\n"
            + "  <Dimensions>\n"
            + "    <Dimension name='Time' type='TIME' key='Id'>\n"
            + "      <Attributes>\n"
            + "        <Attribute name='Id' table='time_by_day' keyColumn='time_id'/>\n"
            + "        <Attribute name='Year' levelType='TimeYears'>\n"
            + "          <Key>\n"
            + "            <Column name='the_year'/>\n"
            + "          </Key>\n"
            + "        </Attribute>\n"
            + "      </Attributes>\n"
            + "    </Dimension>\n"
            + "  </Dimensions>\n"
            + "  <MeasureGroups>\n"
            + "    <MeasureGroup table='sales_fact_1997'>\n"
            + "      <Measures>\n"
            + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum'/>\n"
            + "      </Measures>\n"
            + "      <DimensionLinks>\n"
            + "        <ForeignKeyLink dimension='Time' foreignKeyColumn='time_id'/>\n"
            + "      </DimensionLinks>\n"
            + "    </MeasureGroup>\n"
            + "  </MeasureGroups>\n"
            + "</Cube>\n";
        TestContext testContext =
            getTestContext().withSchema(
                "<Schema metamodelVersion='4.0' name='x'>\n"
                + physSchema
                + cube
                + "</Schema>");
        testContext.assertErrorList().containsError(
            "Table required. No table is specified or inherited when resolving "
            + "column 'the_year' \\(in Column\\) \\(at ${pos}\\)",
            "<Column name='the_year'/>");

        // As above, except attribute has table specified, is OK.
        testContext =
            getTestContext().withSchema(
                "<Schema metamodelVersion='4.0' name='x'>\n"
                + physSchema
                + cube.replace(
                    "<Column name='the_year'/>",
                    "<Column name='the_year' table='time_by_day'/>")
                + "</Schema>");
        testContext.assertErrorList().isEmpty();

        // As above, but specify table in Attribute
        testContext =
            getTestContext().withSchema(
                "<Schema metamodelVersion='4.0' name='x'>\n"
                + physSchema
                + cube.replace(
                    "<Attribute name='Year' ",
                    "<Attribute name='Year' table='time_by_day' ")
                + "</Schema>");
        testContext.assertErrorList().isEmpty();

        // As above, but specify table in Dimension
        testContext =
            getTestContext().withSchema(
                "<Schema metamodelVersion='4.0' name='x'>\n"
                + physSchema
                + cube.replace(
                    "<Dimension name='Time' ",
                    "<Dimension name='Time' table='time_by_day' ")
                + "</Schema>");
        testContext.assertErrorList().isEmpty();

        // todo: test for property that does not specify exactly one
        // of column name and expression

        // todo: test for property that does not specify table
    }

    public void testInvalidTableElements() {
        // test that it is invalid for <Table> to contain <Column> when
        // used as fact table

        // test that it is invalid for <Table> to contain <Key> when
        // used as fact table

        // test that it is invalid for <Table> to contain <Column> when
        // used in hierarchy

        // test that it is invalid for <Table> to contain <Key> when
        // used in hierarchy

        // test that it is invalid for a <Measure> to contain 'column'
        // attribute without 'table' attribute

        // test that it is invalid for a <Measure> to contain 'table'
        // attribute without 'column' attribute

        // test that it is invalid for a <Measure> to contain 'column'
        // attribute and also <MeasureExpression> child

        // test that it is invalid for a <Measure> to contain neither 'column'
        // attribute nor <MeasureExpression>
    }

    /**
     * Tests that mondrian gives an error if a level is not functionally
     * dependent on the level immediately below it.
     */
    public void testSnowflakeNotFunctionallyDependent() {
        if (!Bug.BugMondrian1333Fixed) {
            return;
        }
        final String tableDefs =
            "<Table name='region' keyColumn='region_id'/>"
            + "<Link target='store' source='region' foreignKeyColumn='region_id'/>"
            + "<Link target='customer' source='region' foreignKeyColumn='customer_region_id'/>";
        final String cubeDefs =
            "<Cube name='SalesNotFD' defaultMeasure='Unit Sales'>\n"
            + "  <Dimensions>"
            + "    <Dimension name='Store' key='id'>\n"
            + "      <Attributes>"
            + "        <Attribute name='Store Country' table='store' keyColumn='store_country' hasHierarchy='false'/>"
            + "        <Attribute name='Store State' table='store' keyColumn='store_state' hasHierarchy='false'/>"
            + "        <Attribute name='Store Region' table='region' keyColumn='sales_region' hasHierarchy='false'/>"
            + "        <Attribute name='Store Name' table='store' keyColumn='store_name' hasHierarchy='false'/>"
            + "        <Attribute name='id' table='store' keyColumn='store_id' hasHierarchy='false'/>"
            + "      </Attributes>"
            + "      <Hierarchies>"
            + "        <Hierarchy hasAll='true' name='Store'>\n"
            + "          <Level attribute='Store Country'/>\n"
            + "          <Level attribute='Store Region'/>\n"
            + "          <Level attribute='Store State'/>\n"
            + "          <Level attribute='Store Name'/>\n"
            + "        </Hierarchy>"
            + "      </Hierarchies>"
            + "    </Dimension>\n"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup table='sales_fact_1997'>"
            + "      <Measures>"
            + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='Standard'/>\n"
            + "      </Measures>"
            + "      <DimensionLinks>"
            + "        <ForeignKeyLink dimension='Store' foreignKeyColumn='store_id'/>"
            + "      </DimensionLinks>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>"
            + "</Cube>";
        final TestContext testContext = getTestContext()
            .insertCube(cubeDefs)
            .insertPhysTable(tableDefs);

        // TODO: convert this exception from fatal to warning
        testContext.assertQueryThrows(
            "select from [SalesNotFD]",
            "Key of level [Store].[Store Region] is not functionally dependent "
            + "on key of parent level: Needed to find exactly one path from "
            + "region to store, but found 0");
    }

    /**
     * Unit test for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-661">
     * MONDRIAN-661, "Name expressions in snowflake hierarchies do not work,
     * unfriendly exception occurs"</a>.
     *
     * <p>NOTE: bug is not marked fixed yet.</p>
     */
    public void testSnowFlakeNameExpressions() {
        final TestContext testContext = getTestContext()
            .replace(
                "<Table name='product' keyColumn='product_id'/>",
                "<Table name='product' keyColumn='product_id'>"
                + "  <ColumnDefs>\n"
                + "    <CalculatedColumnDef name='product_name_exp' type='String'>\n"
                + "      <ExpressionView>\n"
                + "        <SQL dialect='oracle'><Column name='product_name'/>  || '_bar'</SQL>"
                + "        <SQL dialect='mysql'>CONCAT(<Column name='product_name'/>, '_bar')</SQL>"
                + "      </ExpressionView>\n"
                + "    </CalculatedColumnDef>\n"
                + "  </ColumnDefs>"
                + "</Table>")
            .insertDimension(
                "Sales",
                "<Dimension name='Product with inline' key='product_id'>"
                + "  <Attributes>"
                + "    <Attribute name='Product Family' table='product_class' keyColumn='product_family' uniqueMembers='true'/>"
                + "    <Attribute name='Product Department' table='product_class' keyColumn='product_department' uniqueMembers='false'/>"
                + "    <Attribute name='Product Category' table='product_class' keyColumn='product_category' uniqueMembers='false'/>"
                + "    <Attribute name='Product Subcategory' table='product_class' keyColumn='product_subcategory' uniqueMembers='false'/>"
                + "    <Attribute name='Brand Name' table='product' >"
                + "       <Key>\n"
                + "        <Column table='product_class' name='product_family'/>\n"
                + "        <Column table='product_class' name='product_department'/>\n"
                + "        <Column table='product_class' name='product_category'/>\n"
                + "        <Column table='product_class' name='product_subcategory'/>\n"
                + "        <Column name='brand_name'/>\n"
                + "       </Key>\n"
                + "       <Name>\n"
                + "        <Column name='brand_name'/>\n"
                + "       </Name>\n"
                + "    </Attribute>\n"
                + "    <Attribute name='Product Name' table='product' keyColumn='product_name_exp' uniqueMembers='true'/>"
                + "    <Attribute name='product_id' table='product' keyColumn='product_id' hasHierarchy='false'/>"
                + "  </Attributes>"
                + "  <Hierarchies>"
                + "    <Hierarchy hasAll='true' name='Product' allMemberName='All Product'>"
                + "      <Level attribute='Product Family'/>"
                + "      <Level attribute='Product Department'/>"
                + "      <Level attribute='Product Category'/>"
                + "      <Level attribute='Product Subcategory'/>"
                + "      <Level attribute='Brand Name'/>"
                + "      <Level attribute='Product Name'>"
                + "      </Level>"
                + "    </Hierarchy>"
                + "  </Hierarchies>"
                + "</Dimension>")
            .insertDimensionLinks(
                "Sales",
                ArrayMap.of(
                    "Sales",
                    "<ForeignKeyLink dimension='Product with inline' foreignKeyColumn='product_id'/>"))
            .ignoreMissingLink();
        testContext.assertQueryReturns(
            "select {[Product with inline].[Product].[Drink].[Dairy].[Dairy].[Milk].[Club].Children} on columns from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product with inline].[Product].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club 1% Milk_bar]}\n"
            + "{[Product with inline].[Product].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club 2% Milk_bar]}\n"
            + "{[Product with inline].[Product].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club Buttermilk_bar]}\n"
            + "{[Product with inline].[Product].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club Chocolate Milk_bar]}\n"
            + "{[Product with inline].[Product].[Drink].[Dairy].[Dairy].[Milk].[Club].[Club Whole Milk_bar]}\n"
            + "Row #0: 155\n"
            + "Row #0: 145\n"
            + "Row #0: 140\n"
            + "Row #0: 159\n"
            + "Row #0: 168\n");
    }

    /** Error if Schema has 2 PhysicalSchema children. */
    public void testSchemaWithTwoPhysicalSchema() {
        getTestContext().withSchema(
            "<Schema metamodelVersion='4.0' name='foo'>\n"
            + "  <PhysicalSchema/>\n"
            + "  <PhysicalSchema />\n"
            + "</Schema>")
            .assertSchemaError(
                "More than one PhysicalSchema element found; ignoring all but "
                + "first \\(in PhysicalSchema\\) \\(at ${pos}\\)",
                "<PhysicalSchema />");
    }

    public void testCubeWithPhysSchema() {
        final TestContext testContext = getTestContext().withSchema(
            "<Schema metamodelVersion='4.0' name='foo'>\n"
            + "<PhysicalSchema>"
            + "  <Table name='sales_fact_1997' alias='fact'/>\n"
            + "  <Table name='customer'>\n"
            + "    <Key>\n"
            + "      <Column name='customer_id'/>\n"
            + "    </Key>\n"
            + "  </Table>\n"
            + "  <Table name='time_by_day' keyColumn='time_id'/>\n"
            + "  <Link source='customer' target='fact'>\n"
            + "    <ForeignKey>\n"
            + "      <Column name='customer_id'/>\n"
            + "    </ForeignKey>\n"
            + "  </Link>\n"
            + "  <Link source='time_by_day' target='fact'>\n"
            + "    <ForeignKey>\n"
            + "      <Column name='time_id'/>\n"
            + "    </ForeignKey>\n"
            + "  </Link>\n"
            + "</PhysicalSchema>"
            + "<Cube name='SalesPhys'>\n"
            + "  <Dimensions>\n"
            + "    <Dimension name='Time' type='TIME' table='time_by_day' key='Time Id'>\n"
            + "      <Attributes>\n"
            + "        <Attribute name='Year' levelType='TimeYears' keyColumn='the_year'/>\n"
            + "        <Attribute name='Time Id' keyColumn='time_id'/>\n"
            + "      </Attributes>\n"
            + "    </Dimension>\n"
            + "  </Dimensions>\n"
            + "  <MeasureGroups>\n"
            + "    <MeasureGroup table='fact'>\n"
            + "      <Measures>\n"
            + "        <Measure name='Unit Sales' aggregator='sum' formatString='Standard' column='unit_sales'/>\n"
            + "      </Measures>\n"
            + "      <DimensionLinks>\n"
            + "        <ForeignKeyLink dimension='Time' foreignKeyColumn='time_id'/>\n"
            + "      </DimensionLinks>\n"
            + "    </MeasureGroup>\n"
            + "  </MeasureGroups>\n"
            + "</Cube>"
            + "</Schema>");
        testContext.assertQueryReturns(
            "select [Time].[Year].Children on 0 from [SalesPhys]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Year].[1997]}\n"
            + "{[Time].[Year].[1998]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: \n");
    }

    public void testClosureWithoutDistanceColumn() {
        final TestContext testContext =
            getTestContext().withSchema(
                "<?xml version='1.0'?>\n"
                + "<Schema name='FoodMart' metamodelVersion='4.0'>\n"
                + "    <PhysicalSchema>\n"
                + "        <Table name='salary'/>\n"
                + "        <Table name='salary' alias='salary2'/>\n"
                + "        <Table name='department'>\n"
                + "            <Key>\n"
                + "                <Column name='department_id'/>\n"
                + "            </Key>\n"
                + "        </Table>\n"
                + "        <Table name='employee'>\n"
                + "            <Key>\n"
                + "                <Column name='employee_id'/>\n"
                + "            </Key>\n"
                + "        </Table>\n"
                + "        <Table name='employee_closure'>\n"
                + "            <Key>\n"
                + "                <Column name='employee_id'/>\n"
                + "            </Key>\n"
                + "        </Table>\n"
                + "        <Link source='employee' target='employee_closure'>\n"
                + "            <ForeignKey>\n"
                + "                <Column name='employee_id'/>\n"
                + "            </ForeignKey>\n"
                + "        </Link>\n"
                + "    </PhysicalSchema>\n"
                + "    <Cube name='HR'>\n"
                + "        <Dimensions>\n"
                + "            <Dimension name='Employee' key='Employee Id'>\n"
                + "                <Attributes>\n"
                + "                    <Attribute name='Manager Id' table='employee' keyColumn='supervisor_id'/>\n"
                + "                    <Attribute name='Employee Id' table='employee' keyColumn='employee_id' nameColumn='full_name'/>\n"
                + "                </Attributes>\n"
                + "                <Hierarchies>\n"
                + "                    <Hierarchy name='Employees' allMemberName='All Employees'>\n"
                + "                        <Level attribute='Employee Id' parentAttribute='Manager Id' nullParentValue='0'>\n"
                + "                            <Closure table='employee_closure' parentColumn='supervisor_id' childColumn='employee_id'/>\n"
                + "                        </Level>\n"
                + "                    </Hierarchy>\n"
                + "                </Hierarchies>\n"
                + "            </Dimension>\n"
                + "        </Dimensions>\n"
                + "        <MeasureGroups>\n"
                + "            <MeasureGroup name='HR' table='salary'>\n"
                + "                <Measures>\n"
                + "                    <Measure name='Org Salary' column='salary_paid' aggregator='sum'\n"
                + "                             formatString='Currency'/>\n"
                + "                    <Measure name='Count' column='employee_id' aggregator='count'\n"
                + "                             formatString='#,#'/>\n"
                + "                    <Measure name='Number of Employees' column='employee_id'\n"
                + "                             aggregator='distinct-count' formatString='#,#'/>\n"
                + "                </Measures>\n"
                + "                <DimensionLinks>\n"
                + "                    <ForeignKeyLink dimension='Employee' foreignKeyColumn='employee_id'/>\n"
                + "                </DimensionLinks>\n"
                + "            </MeasureGroup>\n"
                + "        </MeasureGroups>\n"
                + "        <CalculatedMembers>\n"
                + "            <CalculatedMember name='Employee Salary' dimension='Measures'\n"
                + "                              formatString='Currency'\n"
                + "                              formula='([Employees].currentmember.datamember, [Measures].[Org Salary])'/>\n"
                + "            <CalculatedMember name='Avg Salary' dimension='Measures'\n"
                + "                              formatString='Currency'\n"
                + "                              formula='[Measures].[Org Salary]/[Measures].[Number of Employees]'/>\n"
                + "        </CalculatedMembers>\n"
                + "    </Cube>\n"
                + "</Schema>\n");
        testContext.assertQueryReturns(
            "select {[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].Children} on columns from [HR]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker]}\n"
            + "{[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Laurie Borges]}\n"
            + "{[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo]}\n"
            + "Row #0: $10,256.30\n"
            + "Row #0: $35,487.69\n"
            + "Row #0: $29,121.55\n");
    }

    public void testClosureWithDistanceColumn() {
        final TestContext testContext =
            getTestContext().withSchema(
                "<?xml version='1.0'?>\n"
                + "<Schema name='FoodMart' metamodelVersion='4.0'>\n"
                + "    <PhysicalSchema>\n"
                + "        <Table name='salary'/>\n"
                + "        <Table name='salary' alias='salary2'/>\n"
                + "        <Table name='department'>\n"
                + "            <Key>\n"
                + "                <Column name='department_id'/>\n"
                + "            </Key>\n"
                + "        </Table>\n"
                + "        <Table name='employee'>\n"
                + "            <Key>\n"
                + "                <Column name='employee_id'/>\n"
                + "            </Key>\n"
                + "        </Table>\n"
                + "        <Table name='employee_closure'>\n"
                + "            <Key>\n"
                + "                <Column name='employee_id'/>\n"
                + "            </Key>\n"
                + "        </Table>\n"
                + "        <Link source='employee' target='employee_closure'>\n"
                + "            <ForeignKey>\n"
                + "                <Column name='employee_id'/>\n"
                + "            </ForeignKey>\n"
                + "        </Link>\n"
                + "    </PhysicalSchema>\n"
                + "    <Cube name='HR'>\n"
                + "        <Dimensions>\n"
                + "            <Dimension name='Employee' key='Employee Id'>\n"
                + "                <Attributes>\n"
                + "                    <Attribute name='Manager Id' table='employee' keyColumn='supervisor_id'/>\n"
                + "                    <Attribute name='Employee Id' table='employee' keyColumn='employee_id' nameColumn='full_name'/>\n"
                + "                </Attributes>\n"
                + "                <Hierarchies>\n"
                + "                    <Hierarchy name='Employees' allMemberName='All Employees'>\n"
                + "                        <Level attribute='Employee Id' parentAttribute='Manager Id' nullParentValue='0'>\n"
                + "                            <Closure table='employee_closure' parentColumn='supervisor_id' childColumn='employee_id' distanceColumn='distance'/>\n"
                + "                        </Level>\n"
                + "                    </Hierarchy>\n"
                + "                </Hierarchies>\n"
                + "            </Dimension>\n"
                + "        </Dimensions>\n"
                + "        <MeasureGroups>\n"
                + "            <MeasureGroup name='HR' table='salary'>\n"
                + "                <Measures>\n"
                + "                    <Measure name='Org Salary' column='salary_paid' aggregator='sum'\n"
                + "                             formatString='Currency'/>\n"
                + "                    <Measure name='Count' column='employee_id' aggregator='count'\n"
                + "                             formatString='#,#'/>\n"
                + "                    <Measure name='Number of Employees' column='employee_id'\n"
                + "                             aggregator='distinct-count' formatString='#,#'/>\n"
                + "                </Measures>\n"
                + "                <DimensionLinks>\n"
                + "                    <ForeignKeyLink dimension='Employee' foreignKeyColumn='employee_id'/>\n"
                + "                </DimensionLinks>\n"
                + "            </MeasureGroup>\n"
                + "        </MeasureGroups>\n"
                + "        <CalculatedMembers>\n"
                + "            <CalculatedMember name='Employee Salary' dimension='Measures'\n"
                + "                              formatString='Currency'\n"
                + "                              formula='([Employees].currentmember.datamember, [Measures].[Org Salary])'/>\n"
                + "            <CalculatedMember name='Avg Salary' dimension='Measures'\n"
                + "                              formatString='Currency'\n"
                + "                              formula='[Measures].[Org Salary]/[Measures].[Number of Employees]'/>\n"
                + "        </CalculatedMembers>\n"
                + "    </Cube>\n"
                + "</Schema>\n");
        testContext.assertQueryReturns(
            "select {[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].Children} on columns from [HR]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker]}\n"
            + "{[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Laurie Borges]}\n"
            + "{[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo]}\n"
            + "Row #0: $10,256.30\n"
            + "Row #0: $35,487.69\n"
            + "Row #0: $29,121.55\n");
    }

    public void testClosureWithNonExistingDistanceColumn() {
        final TestContext testContext =
            getTestContext().withSchema(
                "<?xml version='1.0'?>\n"
                + "<Schema name='FoodMart' metamodelVersion='4.0'>\n"
                + "    <PhysicalSchema>\n"
                + "        <Table name='salary'/>\n"
                + "        <Table name='salary' alias='salary2'/>\n"
                + "        <Table name='department'>\n"
                + "            <Key>\n"
                + "                <Column name='department_id'/>\n"
                + "            </Key>\n"
                + "        </Table>\n"
                + "        <Table name='employee'>\n"
                + "            <Key>\n"
                + "                <Column name='employee_id'/>\n"
                + "            </Key>\n"
                + "        </Table>\n"
                + "        <Table name='employee_closure'>\n"
                + "            <Key>\n"
                + "                <Column name='employee_id'/>\n"
                + "            </Key>\n"
                + "        </Table>\n"
                + "        <Link source='employee' target='employee_closure'>\n"
                + "            <ForeignKey>\n"
                + "                <Column name='employee_id'/>\n"
                + "            </ForeignKey>\n"
                + "        </Link>\n"
                + "    </PhysicalSchema>\n"
                + "    <Cube name='HR'>\n"
                + "        <Dimensions>\n"
                + "            <Dimension name='Employee' key='Employee Id'>\n"
                + "                <Attributes>\n"
                + "                    <Attribute name='Manager Id' table='employee' keyColumn='supervisor_id'/>\n"
                + "                    <Attribute name='Employee Id' table='employee' keyColumn='employee_id' nameColumn='full_name'/>\n"
                + "                </Attributes>\n"
                + "                <Hierarchies>\n"
                + "                    <Hierarchy name='Employees' allMemberName='All Employees'>\n"
                + "                        <Level attribute='Employee Id' parentAttribute='Manager Id' nullParentValue='0'>\n"
                + "                            <Closure table='employee_closure' parentColumn='supervisor_id' childColumn='employee_id' distanceColumn='bacon'/>\n"
                + "                        </Level>\n"
                + "                    </Hierarchy>\n"
                + "                </Hierarchies>\n"
                + "            </Dimension>\n"
                + "        </Dimensions>\n"
                + "        <MeasureGroups>\n"
                + "            <MeasureGroup name='HR' table='salary'>\n"
                + "                <Measures>\n"
                + "                    <Measure name='Org Salary' column='salary_paid' aggregator='sum'\n"
                + "                             formatString='Currency'/>\n"
                + "                    <Measure name='Count' column='employee_id' aggregator='count'\n"
                + "                             formatString='#,#'/>\n"
                + "                    <Measure name='Number of Employees' column='employee_id'\n"
                + "                             aggregator='distinct-count' formatString='#,#'/>\n"
                + "                </Measures>\n"
                + "                <DimensionLinks>\n"
                + "                    <ForeignKeyLink dimension='Employee' foreignKeyColumn='employee_id'/>\n"
                + "                </DimensionLinks>\n"
                + "            </MeasureGroup>\n"
                + "        </MeasureGroups>\n"
                + "        <CalculatedMembers>\n"
                + "            <CalculatedMember name='Employee Salary' dimension='Measures'\n"
                + "                              formatString='Currency'\n"
                + "                              formula='([Employees].currentmember.datamember, [Measures].[Org Salary])'/>\n"
                + "            <CalculatedMember name='Avg Salary' dimension='Measures'\n"
                + "                              formatString='Currency'\n"
                + "                              formula='[Measures].[Org Salary]/[Measures].[Number of Employees]'/>\n"
                + "        </CalculatedMembers>\n"
                + "    </Cube>\n"
                + "</Schema>\n");
        if (testContext.getDialect().getDatabaseProduct()
            .equals(Dialect.DatabaseProduct.MYSQL))
        {
            testContext.assertQueryThrows(
                "select {[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].Children} on columns from [HR]",
                "Unknown column 'employee_closure.bacon' in 'where clause'");
        }
    }

    public void testClosureWithNoPhysicalTableDefined() {
        final TestContext testContext =
            getTestContext().withSchema(
                "<?xml version='1.0'?>\n"
                + "<Schema name='FoodMart' metamodelVersion='4.0'>\n"
                + "    <PhysicalSchema>\n"
                + "        <Table name='salary'/>\n"
                + "        <Table name='salary' alias='salary2'/>\n"
                + "        <Table name='department'>\n"
                + "            <Key>\n"
                + "                <Column name='department_id'/>\n"
                + "            </Key>\n"
                + "        </Table>\n"
                + "        <Table name='employee'>\n"
                + "            <Key>\n"
                + "                <Column name='employee_id'/>\n"
                + "            </Key>\n"
                + "        </Table>\n"
                + "    </PhysicalSchema>\n"
                + "    <Cube name='HR'>\n"
                + "        <Dimensions>\n"
                + "            <Dimension name='Employee' key='Employee Id'>\n"
                + "                <Attributes>\n"
                + "                    <Attribute name='Manager Id' table='employee' keyColumn='supervisor_id'/>\n"
                + "                    <Attribute name='Employee Id' table='employee' keyColumn='employee_id' nameColumn='full_name'/>\n"
                + "                </Attributes>\n"
                + "                <Hierarchies>\n"
                + "                    <Hierarchy name='Employees' allMemberName='All Employees'>\n"
                + "                        <Level attribute='Employee Id' parentAttribute='Manager Id' nullParentValue='0'>\n"
                + "                            <Closure table='employee_closure' parentColumn='supervisor_id' childColumn='employee_id' distanceColumn='distance'/>\n"
                + "                        </Level>\n"
                + "                    </Hierarchy>\n"
                + "                </Hierarchies>\n"
                + "            </Dimension>\n"
                + "        </Dimensions>\n"
                + "        <MeasureGroups>\n"
                + "            <MeasureGroup name='HR' table='salary'>\n"
                + "                <Measures>\n"
                + "                    <Measure name='Org Salary' column='salary_paid' aggregator='sum'\n"
                + "                             formatString='Currency'/>\n"
                + "                    <Measure name='Count' column='employee_id' aggregator='count'\n"
                + "                             formatString='#,#'/>\n"
                + "                    <Measure name='Number of Employees' column='employee_id'\n"
                + "                             aggregator='distinct-count' formatString='#,#'/>\n"
                + "                </Measures>\n"
                + "                <DimensionLinks>\n"
                + "                    <ForeignKeyLink dimension='Employee' foreignKeyColumn='employee_id'/>\n"
                + "                </DimensionLinks>\n"
                + "            </MeasureGroup>\n"
                + "        </MeasureGroups>\n"
                + "        <CalculatedMembers>\n"
                + "            <CalculatedMember name='Employee Salary' dimension='Measures'\n"
                + "                              formatString='Currency'\n"
                + "                              formula='([Employees].currentmember.datamember, [Measures].[Org Salary])'/>\n"
                + "            <CalculatedMember name='Avg Salary' dimension='Measures'\n"
                + "                              formatString='Currency'\n"
                + "                              formula='[Measures].[Org Salary]/[Measures].[Number of Employees]'/>\n"
                + "        </CalculatedMembers>\n"
                + "    </Cube>\n"
                + "</Schema>\n");
        testContext.assertQueryThrows(
            "select {[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].Children} on columns from [HR]",
            "table 'employee_closure' not found (in Closure) (at line 26, column 28)");
    }

    // todo: add validation for schema that contains free SQL MeasureExpression,
    // and add test for same. See measure [Sales].[Promotion Sales] in the
    // original FoodMart, which references "sales_fact_1997"."store_sales".

    // todo: should we allow free SQL MeasureExpression?

    // todo: test cube where all measures are table-less (e.g. 1 or 't.x + t.y')

    // Attributes not applicable:
    // Dimension@foreignKey
    // Hierarchy@primaryKey
    // Level@type
    // Level@column

    // todo: test for inline table where datatype has invalid value
    // todo: test for inline table where value is inconsistent with datatype

    // todo: test for closure table in new format where closure table is
    //  defined in physical schema but there is no link to fact table. should
    //  give error

    // todo: test that get an error if a table has two keys with the same name

    // todo: test that get an error if a table has two unnamed keys

    // todo: test that get an error if a table has an unnamed key and one called
    // 'primary'

    // test: test a link that references a key by name, and the target has
    //   that key and an anonymous key (succeeds)

    // test: test a link that references a key called 'primary', and the target
    //   has an anonymous key (succeeds)

    public void testStoredMeasureMustHaveColumns() {
        // Old style cube
        final TestContext testContext = getTestContext().insertCube(
            "<Cube name='Warehouse-old'>\n"
            + "  <Dimensions>"
            + "    <Dimension source='Time'/>\n"
            + "    <Dimension source='Product'/>\n"
            + "    <Dimension source='Warehouse'/>\n"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup table='inventory_fact_1997'>"
            + "      <Measures>"
            + "        <Measure name='Units Ordered' column='units_ordered' aggregator='sum' formatString='#.0'/>\n"
            + "        <Measure name='Warehouse Profit' aggregator='sum'>\n"
            + "          <MeasureExpression>\n"
            + "            <SQL dialect='generic'>\n"
            + "             &quot;warehouse_sales&quot; - &quot;inventory_fact_1997&quot;.&quot;warehouse_cost&quot;\n"
            + "            </SQL>\n"
            + "          </MeasureExpression>\n"
            + "        </Measure>\n"
            + "      </Measures>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>"
            + "</Cube>");
        Throwable throwable = null;
        try {
            testContext.assertSimpleQuery();
        } catch (RuntimeException e) {
            throwable = e;
        }
        TestContext.checkThrowable(
            throwable,
            "Measure 'Warehouse Profit' must contain either a source column or a source expression, but not both");
    }

    public void testInvalidInlineTable() {
        String tblDef =
            "<InlineTable alias='foo2'>\n"
            + "  <ColumnDefs>\n"
            + "    <ColumnDef name='foo' type='Numeric'/>\n"
            + "  </ColumnDefs>\n"
            + "  <Rows/>\n"
            + "</InlineTable>\n";
        String cubeDef =
            "<Dimension name='Scenario' table='foo2' key='id' type='SCENARIO'>\n"
            + "  <Attributes>"
            + "    <Attribute name='Scenario' keyColumn='foo'/>"
            + "    <Attribute name='id' keyColumn='time_id'/>"
            + "  </Attributes>"
            + "</Dimension>";
        final TestContext testContext =
            getTestContext()
                .insertPhysTable(tblDef)
                .insertDimension("Sales", cubeDef);
            testContext.assertErrorList().containsError(
                "Column 'time_id' not found in relation 'foo2' \\(in Attribute 'id'\\) \\(at ${pos}\\)",
                "<Attribute name='id' keyColumn='time_id'/>");
    }

    public void testHierarchiesWithDifferentPrimaryKeysThrows() {
        final TestContext testContext =
            getTestContext().insertDimension(
                "Sales",
                "<Dimension name='Time2' table='time_by_day' type='TIME' key='id'>\n"
                + "  <Attributes>"
                + "    <Attribute name='Year' keyColumn='the_year' type='Numeric' levelType='TimeYears'/>"
                + "    <Attribute name='Quarter' keyColumn='quarter' type='Numeric' levelType='TimeQuarters'/>"
                + "    <Attribute name='Month' keyColumn='month_of_year' type='Numeric' levelType='TimeMonths'/>"
                + "    <Attribute name='Week' keyColumn='week_of_year' type='Numeric' levelType='TimeWeeks'/>"
                + "    <Attribute name='Day' keyColumn='day_of_month' type='Numeric' levelType='TimeDays'/>"
                + "    <Attribute name='id' keyColumn='time_id'/>"
                + "  </Attributes>"
                + "  <Hierarchy hasAll='false' primaryKey='time_id'>\n"
                + "      <Level attribute='Year'/>\n"
                + "      <Level attribute='Quarter'/>\n"
                + "      <Level attribute='Month'/>\n"
                + "    </Hierarchy>\n"
                + "    <Hierarchy hasAll='true' name='Weekly' primaryKey='store_id'>\n"
                + "      <Level attribute='Year'/>\n"
                + "      <Level attribute='Week'/>\n"
                + "      <Level attribute='Day'/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>")
                .insertDimensionLinks(
                    "Sales",
                    ArrayMap.of(
                        "Sales",
                        "<ForeignKeyLink dimension='Time2' "
                        + "foreignKeyColumn='time_id'/>"))
                .ignoreMissingLink();
        try {
            testContext.assertSimpleQuery();
        } catch (RuntimeException e) {
            TestContext.checkThrowable(
                e,
                "hierarchies in same dimension with different primary key");
        }
    }

    /**
     * Tests various kinds of attribute key: with nested Key elements, composite
     * keys, and keys specified using the keyColumn attribute. Does not run
     * any queries, just checks that schema validates.
     */
    public void testKeyAttribute() {
        final TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "<Dimension name='Customer2' table='customer' key='Key'>\n"
            + "  <Attributes>\n"
            + "    <Attribute name='State'>"
            + "      <Key>\n"
            + "        <Column name='state_province'/>\n"
            + "      </Key>\n"
            + "    </Attribute>"
            + "    <Attribute name='City'>\n"
            + "      <Key>\n"
            + "        <Column name='state_province'/>\n"
            + "        <Column name='city'/>\n"
            + "      </Key>\n"
            + "      <Name>\n"
            + "        <Column name='city'/>\n"
            + "      </Name>\n"
            + "    </Attribute>"
            + "    <Attribute name='Name' keyColumn='full_name'/>\n"
            + "    <Attribute name='Key' hidden='true' keyColumn='customer_id'/>\n"
            + "  </Attributes>\n"
            + "  <Hierarchies>\n"
            + "    <Hierarchy name='Customers'>\n"
            + "      <Level attribute='State'/>\n"
            + "      <Level attribute='City'/>\n"
            + "      <Level attribute='Name'/>\n"
            + "    </Hierarchy>\n"
            + "  </Hierarchies>\n"
            + "</Dimension>\n");
        testContext.assertSimpleQuery();
    }

    /**
     * Tests that it is not an error if no key attribute is specified
     * for a dimension. (It is only an error if that dimension links to
     * measure groups and they do not specify an explicit key; see
     * {@link #testDimensionMissingKey()}.)
     */
    public void _testKeyAttributeMissing() {
        // disabling as a part of queryBuilder merge.  TODO:  Jira case.
        getTestContext().insertDimension(
            "Sales",
            "<Dimension name='Customer3' table='customer'>\n"
            + "  <Attributes>\n"
            + "    <Attribute name='State' keyColumn='state_province'/>\n"
            + "  </Attributes>\n"
            + "</Dimension>\n")
            .insertDimensionLinks(
                "Sales",
                ArrayMap.of(
                    "Sales",
                    "<NoLink dimension='Customer3'/>"))
            .ignoreMissingLink()
            .assertSimpleQuery();
    }

    /** Tests that get error if dimension's key is the name of a non-existent
     * attribute. */
    public void testDimensionBadKey() {
        final TestContext testContext =
            getTestContext().createSubstitutingCube(
                "Sales",
                "<Dimension name='Store2' key='Non existent'>\n"
                + "  <Attributes>\n"
                + "    <Attribute name='Store Id' table='store' keyColumn='store_id'/>\n"
                + "  </Attributes>\n"
                + "</Dimension>");
        testContext.assertErrorList().containsError(
            "Key attribute 'Non existent' is not a valid attribute of this dimension \\(in Dimension 'Store2'\\) \\(at ${pos}\\)",
            "<Dimension name='Store2' key='Non existent'>");
    }

    /** Tests that get error if dimension has no key and the dimension is
     * used via a ForeignKeyLink. */
    public void testDimensionMissingKey() {
        final TestContext testContext =
            getTestContext().createSubstitutingCube(
                "Sales",
                "<Dimension name='Store2'>\n"
                + "  <Attributes>\n"
                + "    <Attribute name='Store Id' table='store' keyColumn='store_id'/>\n"
                + "    <Attribute name='Store Name' table='store' keyColumn='store_name'/>\n"
                + "  </Attributes>\n"
                + "</Dimension>",
                null, null, null,
                ArrayMap.of(
                    "Sales",
                    "<ForeignKeyLink dimension='Store2' foreignKeyColumn='store_id'/>"));
        testContext.assertErrorList().containsError(
            "mondrian.olap.MondrianException: Mondrian Error:Dimension 'Store2' omits a defined key, which is only valid for degenerate dimensions with a single attribute. \\(in Dimension 'Store2'\\) \\(at ${pos}\\)",
            "<Dimension name='Store2'>");
    }

    /**
     * Tests that mondrian gives error if an attribute hierarchy has the
     * same name as another hierarchy in the dimension. (This would typically
     * occur if an attribute has hasHierarchy=true and there is an explicit
     * hierarchy for the same attribute.)
     */
    public void testDuplicateHierarchyNameCausedByAttrHierarchy() {
        final TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "<Dimension name='Customer' table='customer' key='Key'>\n"
            + "  <Attributes>"
            + "    <Attribute name='State'>"
            + "      <Key>\n"
            + "        <Column name='state_province'/>\n"
            + "      </Key>\n"
            + "    </Attribute>"
            + "    <Attribute name='City' hasHierarchy='true'>\n"
            + "      <Key>\n"
            + "        <Column name='state_province'/>\n"
            + "        <Column name='city'/>\n"
            + "      </Key>\n"
            + "      <Name>\n"
            + "        <Column name='city'/>\n"
            + "      </Name>\n"
            + "    </Attribute>"
            + "    <Attribute name='Name' keyColumn='customer_id'/>\n"
            + "    <Attribute name='Key' keyColumn='customer_id' hidden='true'/>\n"
            + "  </Attributes>"
            + "  <Hierarchies>"
            + "    <Hierarchy name='Customers'>\n"
            + "      <Level attribute='State'/>\n"
            + "      <Level attribute='City'/>\n"
            + "      <Level attribute='Name'/>\n"
            + "    </Hierarchy>\n"
            + "    <Hierarchy name='City'>\n"
            + "      <Level attribute='City'/>\n"
            + "    </Hierarchy>\n"
            + "  </Hierarchies>"
            + "</Dimension>\n"
            + "<MeasureGroups>\n"
            + "  <MeasureGroup name='sales_fact'>\n"
            + "  </MeasureGroup>\n"
            + "</MeasureGroups>");
        testContext.assertErrorList().containsError(
            "Cannot create hierarchy for attribute 'City'; dimension already "
            + "has a hierarchy of that name \\(in Attribute 'City'\\) "
            + "\\(at ${pos}\\)",
            "<Attribute name='City' hasHierarchy='true'>");
    }

    /**
     * Tests that get error if a Column element does not have 'table'
     * attribute specified, and none is inherited from enclosing Attribute
     * or Dimension element.
     */
    public void testWarnIfNoRelation() {
        final TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "<Dimension name='Customer' key='Key'>\n"
            + "  <Attributes>"
            + "    <Attribute name='City' hasHierarchy='true'>\n"
            + "      <Key>\n"
            + "        <Column table='customer' name='state_province'/>\n"
            + "        <Column name='city'/>\n"
            + "      </Key>\n"
            + "      <Name>\n"
            + "        <Column table='customer' name='city'/>\n"
            + "      </Name>\n"
            + "    </Attribute>"
            + "    <Attribute name='Key' hidden='true'/>\n"
            + "  </Attributes>"
            + "</Dimension>\n"
            + "<MeasureGroups>\n"
            + "  <MeasureGroup name='sales_fact'>\n"
            + "  </MeasureGroup>\n"
            + "</MeasureGroups>");
        testContext.assertErrorList().containsError(
            "Table required. No table is specified or inherited when resolving "
            + "column 'city' \\(in Column\\) \\(at ${pos}\\)",
            "<Column name='city'/>");
    }

    private static final String SALES_GEN_CUBE =
        "<Cube name='Sales' factTable='sales_fact_1997'>\n"
        + "  <Dimensions>\n"
        + "    <Dimension name='Time' table='time_by_day_generated' type='TIME' key='Time Id'>\n"
        + "        <Attributes>\n"
        + "            <Attribute name='Year' keyColumn='the_year' levelType='TimeYears'/>\n"
        + "            <Attribute name='Quarter' levelType='TimeQuarters'>\n"
        + "                <Key>\n"
        + "                    <Column name='the_year'/>\n"
        + "                    <Column name='quarter'/>\n"
        + "                </Key>\n"
        + "                <Name>\n"
        + "                    <Column name='quarter'/>\n"
        + "                </Name>\n"
        + "            </Attribute>\n"
        + "            <Attribute name='Month' levelType='TimeMonths'>\n"
        + "                <Key>\n"
        + "                    <Column name='the_year'/>\n"
        + "                    <Column name='month_of_year'/>\n"
        + "                </Key>\n"
        + "                <Name>\n"
        + "                    <Column name='month_of_year'/>\n"
        + "                </Name>\n"
        + "            </Attribute>\n"
        + "            <Attribute name='Week' levelType='TimeWeeks'>\n"
        + "                <Key>\n"
        + "                    <Column name='the_year'/>\n"
        + "                    <Column name='week_of_year'/>\n"
        + "                </Key>\n"
        + "                <Name>\n"
        + "                    <Column name='week_of_year'/>\n"
        + "                </Name>\n"
        + "            </Attribute>\n"
        + "            <Attribute name='Day' levelType='TimeDays'>\n"
        + "                <Key>\n"
        + "                    <Column name='time_id'/>\n"
        + "                </Key>\n"
        + "                <Name>\n"
        + "                    <Column name='day_of_month'/>\n"
        + "                </Name>\n"
        + "            </Attribute>\n"
        + "            <Attribute name='Month Name'>\n"
        + "                <Key>\n"
        + "                    <Column name='the_year'/>\n"
        + "                    <Column name='month_of_year'/>\n"
        + "                </Key>\n"
        + "                <Name>\n"
        + "                    <Column name='the_month'/>\n"
        + "                </Name>\n"
        + "            </Attribute>\n"
        + "            <Attribute name='Date' keyColumn='the_date'/>\n"
        + "            <Attribute name='Time Id' keyColumn='time_id'/>\n"
        + "        </Attributes>\n"
        + "        <Hierarchies>\n"
        + "            <Hierarchy name='Time' hasAll='false'>\n"
        + "                <Level attribute='Year'/>\n"
        + "                <Level attribute='Quarter'/>\n"
        + "                <Level attribute='Month'/>\n"
        + "            </Hierarchy>\n"
        + "            <Hierarchy name='Weekly' hasAll='true'>\n"
        + "                <Level attribute='Year'/>\n"
        + "                <Level attribute='Week'/>\n"
        + "                <Level attribute='Day'/>\n"
        + "            </Hierarchy>\n"
        + "        </Hierarchies>\n"
        + "    </Dimension>\n"
        + "  </Dimensions>\n"
        + "  <MeasureGroups>\n"
        + "    <MeasureGroup name='Sales' table='sales_fact_1997'>\n"
        + "      <Measures>\n"
        + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='Standard'/>\n"
        + "      </Measures>\n"
        + "      <DimensionLinks>\n"
        + "        <ForeignKeyLink dimension='Time' foreignKeyColumn='time_id'/>\n"
        + "      </DimensionLinks>\n"
        + "    </MeasureGroup>\n"
        + "  </MeasureGroups>\n"
        + "</Cube>";

    public void testAutogeneratedDateTableInvalidRole() throws SQLException {
        final TestContext testContext = genTableSchema(
            "<PhysicalSchema>"
            + "  <AutoGeneratedDateTable name='time_by_day_generated' startDate='2011-12-30' endDate='2012-03-31'>\n"
            + "    <ColumnDefs>\n"
            + "      <ColumnDef name='the_year'>"
            + "          <TimeDomain role='BAD_ROLE'/>"
            + "        </ColumnDef>\n"
            + "    </ColumnDefs>\n"
            + "  </AutoGeneratedDateTable>\n"
            + "  <Table name='sales_fact_1997'/>\n"
            + "</PhysicalSchema>");
        testContext.assertErrorList().containsError(
            "Bad role 'TimeDomain\n"
            + "   role = \"BAD_ROLE\"\n"
            + "   epoch = null\n"
            + "', in column 'the_year'. Allowable roles are "
            + "\\[JULIAN, YYMMDD, YYYYMMDD, DATE, DAY_OF_WEEK, "
            + "DAY_OF_WEEK_IN_MONTH, DAY_OF_WEEK_NAME, MONTH_NAME, "
            + "YEAR, DAY_OF_MONTH, WEEK_OF_YEAR, MONTH, QUARTER\\]"
            + " \\(in ColumnDef\\) \\(at ${pos}\\)",
            "<ColumnDef name='the_year'>");
    }

    private TestContext genTableSchema(String physSchema) throws SQLException {
        TestContext testContext = getTestContext();
        doSql(testContext, "drop table time_by_day_generated");
        JdbcSchema.clearAllDBs();
        return testContext.withSchema(
            "<Schema name='FoodMart' metamodelVersion='4.0'>\n"
            + physSchema
            + SALES_GEN_CUBE
            + "</Schema>");
    }

    public void testAutogeneratedDateTableDefaultColumns() throws SQLException {
        final TestContext testContext = genTableSchema(
            "<PhysicalSchema>"
            + "  <AutoGeneratedDateTable name='time_by_day_generated' startDate='2011-09-30' endDate='2012-03-31'/>\n"
            + "  <Table name='sales_fact_1997'/>\n"
            + "</PhysicalSchema>");
        testContext.assertQueryReturns(
            "select [Time].[Time].Children on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[2011].[Q3]}\n"
            + "{[Time].[Time].[2011].[Q4]}\n"
            + "Row #0: \n"
            + "Row #0: \n");
    }

    public void testAutoGeneratedDateTableExplicitColumns()
        throws SQLException
    {
        // Note:
        // 1. TimeDomain.role may be lowercase 'year' or uppercase 'MONTH'.
        // 2. TimeDomain may be omitted if the column name is standard
        //    ('time_id'); in this case, ColumnDef.type may be omitted also.
        // 3. time_id_2's epoch is 3 days after startDate; therefore the first
        //    3 rows in the table have negative values.
        //
        // This test does not check the contents of the time_by_day_generated
        // table. Do that manually.
        final TestContext testContext = genTableSchema(
            "<PhysicalSchema>"
            + "  <AutoGeneratedDateTable name='time_by_day_generated' startDate='2011-06-28' endDate='2012-03-31'>\n"
            + "    <ColumnDefs>\n"
            + "      <ColumnDef name='time_id'/>\n"
            + "      <ColumnDef name='my_year'>\n"
            + "        <TimeDomain role='year'/>\n"
            + "      </ColumnDef>\n"
            + "      <ColumnDef name='my_month'>\n"
            + "        <TimeDomain role='MONTH'/>\n"
            + "      </ColumnDef>\n"
            + "      <ColumnDef name='time_id_2'>\n"
            + "        <TimeDomain role='JULIAN' epoch='2011-07-01'/>\n"
            + "      </ColumnDef>\n"
            + "      <ColumnDef name='the_year'/>\n"
            + "      <ColumnDef name='quarter'/>\n"
            + "      <ColumnDef name='month_of_year'/>\n"
            + "      <ColumnDef name='week_of_year'/>\n"
            + "      <ColumnDef name='day_of_month'/>\n"
            + "      <ColumnDef name='the_month'/>\n"
            + "      <ColumnDef name='the_date'/>\n"
            + "    </ColumnDefs>\n"
            + "    <Key>\n"
            + "      <Column name='time_id_2'/>\n"
            + "    </Key>\n"
            + "  </AutoGeneratedDateTable>\n"
            + "  <Table name='sales_fact_1997'/>\n"
            + "</PhysicalSchema>");

        // Note that [2001].[Q1] is missing, because date generation starts
        // in Q2 (June 28).
        testContext.assertQueryReturns(
            "select [Time].[Time].Children on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[2011].[Q2]}\n"
            + "{[Time].[Time].[2011].[Q3]}\n"
            + "{[Time].[Time].[2011].[Q4]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n");
    }

    /**
     * Tests an empty schema, also the schema's caption and description.
     *
     * <p>Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-1038">
     * MONDRIAN-1038, "Add caption to the Schema Name so that the Schema Name
     * can be localized."</a>.</p>
     *
     * @throws SQLException on error
     */
    public void testEmptySchema() throws SQLException {
        TestContext testContext = getTestContext().withSchema(
            "<Schema name='FoodMart' metamodelVersion='4.0' "
            + "caption='schema caption' description='schema description'>\n"
            + "<PhysicalSchema/>"
            + "</Schema>");
        final org.olap4j.metadata.Schema schema =
            testContext.getOlap4jConnection().getOlapSchema();
        assertTrue(schema instanceof MetadataElement);
        MetadataElement metadataElement = (MetadataElement) schema;
        assertEquals("FoodMart", metadataElement.getName());
        assertEquals("schema caption", metadataElement.getCaption());
        assertEquals("schema description", metadataElement.getDescription());
        assertTrue(metadataElement.isVisible());

        final Catalog catalog = schema.getCatalog();
        assertTrue(catalog instanceof MetadataElement);
        metadataElement = (MetadataElement) catalog;
        assertEquals("FoodMart", metadataElement.getName());
        assertEquals("FoodMart", metadataElement.getCaption());
        assertEquals("", metadataElement.getDescription());
        assertTrue(metadataElement.isVisible());

        final Database database = catalog.getDatabase();
        assertTrue(database instanceof MetadataElement);
        metadataElement = (MetadataElement) database;
        assertEquals("FoodMart", metadataElement.getName());
        assertEquals("FoodMart", metadataElement.getCaption());
        assertNull(metadataElement.getDescription());
        assertTrue(metadataElement.isVisible());
    }

    private void doSql(TestContext testContext, String sql)
        throws SQLException
    {
        Connection connection = null;
        Statement statement = null;
        try {
            connection =
                testContext.getConnection().getDataSource().getConnection();
            statement = connection.createStatement();
            try {
                statement.executeUpdate(sql);
            } catch (SQLException e) {
                // ignore
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    // TODO: test where 'datatype' field of a <Measure> is invalid

    // TODO: test that get error if 'Dimension.foreignKey' not specified

    // TODO: test that get error in modern cube with no MeasureGroups element

    // TODO: test that get error in modern cube if MeasureGroups does not
    // contain any MeasureGroup elements

    // TODO: test that get error in modern cube if MeasureGroup names are
    // not unique

    // TODO: test that get error in modern cube if MeasureGroup's fact is not
    // known table

    // TODO: test that get error in MeasureGroup.dimension is null

    // TODO: test that get error in MeasureGroup.dimension is non-existent dim

    /**
     * Tests that get error if ForeignKeyLink has different number
     * of foreign key columns (in fact table) than key columns (in dimension
     * table). Error "Number of foreign key columns N does not match ..."
     */
    public void testForeignKeyColumnCountMatch() {
        final TestContext testContext =
            getTestContext().insertDimension(
                "Sales",
                "<Dimension name='Product with no all' key='Product Subcategory'>"
                + "    <Attributes>\n"
                + "        <Attribute name='Product Subcategory' table='product_class'>\n"
                + "            <Key>\n"
                + "                <Column name='product_family'/>\n"
                + "                <Column name='product_department'/>\n"
                + "                <Column name='product_category'/>\n"
                + "                <Column name='product_subcategory'/>\n"
                + "            </Key>\n"
                + "        <Name>\n"
                + "                <Column name='product_subcategory'/>\n"
                + "            </Name>\n"
                + "        </Attribute>\n"
                + "        <Attribute name='Brand Name' table='product'>\n"
                + "            <Key>\n"
                + "                <Column table='product_class' name='product_family'/>\n"
                + "                <Column table='product_class' name='product_department'/>\n"
                + "                <Column table='product_class' name='product_category'/>\n"
                + "                <Column table='product_class' name='product_subcategory'/>\n"
                + "                <Column name='brand_name'/>\n"
                + "            </Key>\n"
                + "            <Name>\n"
                + "                <Column name='brand_name'/>\n"
                + "            </Name>\n"
                + "        </Attribute>\n"
                + "        <Attribute name='Product Name' table='product'\n"
                + "            keyColumn='product_id' nameColumn='product_name'/>\n"
                + "        <Attribute name='Product Id' table='product' keyColumn='product_id'/>"
                + "    </Attributes>\n"
                + "    <Hierarchies>\n"
                + "        <Hierarchy name='Products' allMemberName='All Products' hasAll='false'>\n"
                + "            <Level attribute='Product Subcategory'/>\n"
                + "            <Level attribute='Brand Name'/>\n"
                + "            <Level attribute='Product Name'/>\n"
                + "        </Hierarchy>\n"
                + "    </Hierarchies>\n"
                + "</Dimension>\n")
            .insertDimensionLinks(
                "Sales",
                ArrayMap.of(
                    "Sales",
                    "<ForeignKeyLink dimension='Product with no all' "
                    + "foreignKeyColumn='product_id'/>"))
            .ignoreMissingLink();

        testContext.assertErrorList().containsError(
            "Number of foreign key columns 1 does not match number of key columns "
            + "4 \\(in ForeignKeyLink\\) \\(at ${pos}\\)",
            "<ForeignKeyLink dimension='Product with no all' "
            + "foreignKeyColumn='product_id'/>");
    }

    // TODO: implement check on data types of columns in ForeignKeyLink,
    // and test it. (Currently will throw at runtime.)

    // TODO: test that cannot specify Key or ForeignKey if specify
    // ForeignKeyLink.factColumn.

    // TODO: test for error "foreign key columns come from different relations"
    // when compound key in ForeignKeyLink nominates columns in different
    // tables

    // TOxDO: test for error "Unknown table 'xxx'" when ForeignKeyLink
    // contains a foreign key whose table name is (a) not specified or (b) not
    // a valid table in the schema

    // TODOx: test for error when ForeignKeyLink has different number of
    // foreign and key columns. "Column count mismatch"

    // TODOx: test that get error on invalid key column name in
    // ForeignKeyLink. It must be in measure group's fact table.

    // TODO: test that get error  "Dimension has no hierarchies" on converting
    // old-style schema where one of the dimensions has no hierarchies

    // TODO: test that get error "Cannot convert schema: hierarchies in
    // dimension 'xx' do not have consistent primary keys" if primary keys are
    // different.

    // TODO: test with repeating element, e.g.
    //   <Cube><CalculatedMembers .../><CalculatedMEmbers ../>

    // TODO: test that <Property attribute='foo'/>, attribute 'foo' must exist

    // TODO: test that properties are unique within an attribute

    // TODO: test, and write documentation for, how to define a boolean
    // attribute. (See Store.Store.Has coffee bar for an example.) Does one
    // use ColumnDef.type='Boolean' or Attribute.datatype='Boolean'?
    // Is Attribute.datatype obsolete?

    // TODO: test that it is an error if you define a FactLink
    // and the dimension's fact table is not the same as the measure group's
    // fact table

    // TODO: Feature: If a dimension has only one attribute, you don't need
    // to specify Dimension.key

    // TODO: Feature: Default value for Attribute.hasHierarchy should be
    // true if the attribute is not included in any other hierarchies.

    // TODO: default value for MeasureGroup.name is .table.

    /** Test cube where Cube.defaultMeasure is non-existent. */
    public void testBadDefaultMeasure() {
        getTestContext()
            .withSubstitution(
                new Util.Function1<String, String>() {
                    public String apply(String param) {
                        return param.replace(
                            "<Cube name='Sales' defaultMeasure='Unit Sales'>",
                            "<Cube name='Sales' defaultMeasure='Bad Unit Sales'>");
                    }
                })
            .assertSchemaError(
                "Default measure 'Bad Unit Sales' not found \\(in Cube 'Sales'\\) \\(at ${pos}\\)",
                "<Cube name='Sales' defaultMeasure='Bad Unit Sales'>");
    }

    /** Test cube where Cube.defaultMeasure is a calculated member. */
    public void testDefaultMeasureIsCalculated() {
        getTestContext()
            .withSubstitution(
                new Util.Function1<String, String>() {
                    public String apply(String param) {
                        return param.replace(
                            "<Cube name='Sales' defaultMeasure='Unit Sales'>",
                            "<Cube name='Sales' defaultMeasure='Profit'>");
                    }
                })
            .assertQueryReturns(
                "select [Measures] on 0, [Product].Children on 1\n"
                + "from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Measures].[Profit]}\n"
                + "Axis #2:\n"
                + "{[Product].[Products].[Drink]}\n"
                + "{[Product].[Products].[Food]}\n"
                + "{[Product].[Products].[Non-Consumable]}\n"
                + "Row #0: $29,358.98\n"
                + "Row #1: $245,764.87\n"
                + "Row #2: $64,487.05\n");
    }

    // TODO: document NoLink

    // TODO: Test that need a link (possibly NoLink) in each measure group for
    // each dimension

    // TODO: Test that "MeasureGroup nolink=false" works.

    // TODO: Document ReferenceLink in MondrianSchema.xml

    // TODO: Implement ReferenceLink

    // TODO: Test ReferenceLink

    // TODO: document that the name of an attribute's property defaults to the
    // name of the source attribute

    // TODO: test that if name of MeasureGroup is missing, defaults to alias of
    // table

    // TODO: test that names of MeasureGroups in cube are enforced unique; even
    // if one of them is missing and therefore defaults to name of table

    // TODO: test that it's OK if a MeasureGroup contains no measures

    // TODO: test that mondrian gives error if MeasureRef occurs in a
    // MeasureGroup whose type is not aggregate.

    // TODO: measure defined using MeasureRef. Search "implement MeasureRef".

    // TODO: test MeasureRef@aggColumn references non-existent measure.

    // TODO: test that mondrian gives error if Column@aggColumn is specified in
    // a column that is in an expression or in a key.

    // TODO: test that mondrian gives error if Column@aggColumn is not specified
    // in a column in a CopyLink.

    // TODO: test CopyLink with invalid column

    // TODO: test for warning AggTableZeroSize "Zero size Aggregate table ..."

    // TODO: check that get an error if and only if ForeignKeyLink.attribute
    // is null and dimension has no key attribute

    // TODO: automatically load agg tables (don't require them to be in
    // PhysSchema). Remove agg tables from top of FoodMart.mondrian.xml.

    // TODO: Try to create a MeasureRef to a calculated measure. Should fail
    // (for now).

    // TODO: You should be able to create a <MeasureRef name='Fact Count'/>
    // in an aggregate MeasureGroup regardless of whether you created a
    // measure called "Fact Count" in the base measure group. (Design problem:
    // how to reliably deduce whether the measure group is a fact or an
    // aggregate, and if an aggregate, which is the base. Otherwise we will
    // tend to create a measure called "Fact Count" in aggregate measure groups
    // too, and the names will clash. Do we want that? In version 4
    // FoodMart.mondrian.xml, we solved the problem temporarily by
    // creating an explicit "Fact Count" measure in the base
    // MeasureGroup.)

    // TODO: Test Schema.measuresCaption. (Suspect it is not implemented.)

    // TODO: test cube with 0 measures (should give user error)

    // TODO: test that it is an error if you define a CalculatedMember
    // in the Measures hierarchy and attempt to give the member a parent.
    // (Measures are flat.)

    // TODO: Test that if an attribute has a composite OrderKey,
    // and is used in a multi-level hierarchy,
    // and the order key starts with the key or orderKey of its parent level,
    // then a reduced order key is used.

    // TODO: Allow OrderKey to be a child element of Level. It can have fewer
    // elements than the attribute's key; it probably does not need to be
    // composite. Warn if OrderKey
    // contains any of parent level's key. (e.g. if
    // orderKey of Customer contains city). OrderKey only needs to distinguish
    // between And test.

    // TODO: Test NamedSet with a Formula element (as opposed to
    // formula attribute)

    // TODO: Make sure that MeasureGroup, Attribute can have annotations.

    // TODO: test cube where MeasureGroups occurs before Dimensions

    // TODO: test cube where there is more than one Dimensions element
    // (should give error)


    /**
     * Unit test for:
     * Attribute.hierarchyHasAll
     * Attribute.hierarchyAllMemberName
     * Attribute.hierarchyAllMemberCaption
     * Attribute.hierarchyAllLevelName
     * Attribute.hierarchyDefaultMember
     */
    public void testAttributeHierarchy() throws SQLException {
        final TestContext testContext =
            getTestContext().createSubstitutingCube(
                "Sales",
                "<Dimension name='Customer2' table='customer' key='Name'>\n"
                + "    <Attributes>\n"
                + "        <Attribute name='Gender no hierarchy' keyColumn='gender' hasHierarchy='false'/>\n"
                + "        <Attribute name='Gender2' keyColumn='gender' "
                + "hasHierarchy='true' "
                + "hierarchyAllMemberName='abc' "
                + "hierarchyHasAll='false' "
                + "hierarchyAllMemberCaption='def' "
                + "hierarchyCaption='ghi' "
                + "hierarchyDefaultMember='[Customer2].[Gender2].[M]'/>\n"
                + "        <Attribute name='Name' keyColumn='customer_id' nameColumn='full_name' orderByColumn='full_name'/>\n"
                + "    </Attributes>\n"
                + "    <Hierarchies>\n"
                + "      <Hierarchy name='H'>\n"
                + "        <Level attribute='Gender no hierarchy' name='L'/>\n"
                + "      </Hierarchy>\n"
                + "    </Hierarchies>\n"
                + "</Dimension>\n",
                null,
                null,
                null,
                ArrayMap.of(
                    "Sales",
                    "<ForeignKeyLink dimension='Customer2' "
                    + "foreignKeyColumn='customer_id'/>"));

        // [Gender no hierarchy] attribute has no hierarchy
        testContext.assertAxisThrows(
            "[Customer2].[Gender no hierarchy].Members",
            "not found");

        // Explicit hierarchy based on [Gender no hierarchy] works OK
        testContext.assertAxisReturns(
            "[Customer2].[H]",
            "[Customer2].[H].[All Hs]");

        OlapConnection connection = testContext.getOlap4jConnection();
        org.olap4j.metadata.Hierarchy hierarchy =
            connection.getOlapSchema().getCubes().get("Sales")
                .getDimensions().get("Customer2")
                .getHierarchies().get("Gender2");
        assertEquals("ghi", hierarchy.getCaption());
        assertEquals("Gender2", hierarchy.getName());
        // no 'all' level, therefore 2 root members
        assertEquals(2, hierarchy.getRootMembers().size());
        assertEquals("[Customer2].[Gender2]", hierarchy.getUniqueName());
        assertEquals(false, hierarchy.hasAll());
        assertEquals(
            "[Customer2].[Gender2].[M]",
            hierarchy.getDefaultMember().getUniqueName());
        assertNull(
            hierarchy.getDefaultMember().getParentMember());
    }

    /**
     * Check if Sales Ragged is being loaded ok.
     */
    public void testRaggedHierarchy() {
        getTestContext().withSalesRagged().assertQueryReturns(
            "select {} on 0, [Geography].[USA].Children on 1\n"
            + "from [Sales Ragged]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "Axis #2:\n"
            + "{[Geography].[Geographies].[USA].[CA]}\n"
            + "{[Geography].[Geographies].[USA].[OR]}\n"
            + "{[Geography].[Geographies].[USA].[USA].[Washington]}\n"
            + "{[Geography].[Geographies].[USA].[WA]}\n");
    }

    /**
     * This is a test for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1390">MONDRIAN-1390</a>
     *
     * <p>Calling {@link SchemaReader#getLevelMembers(Level, boolean)}
     * directly would return the null members at the end, since it was
     * using TupleReader#readTuples instead of TupleReader#readMembers.
     */
    public void testMondrian1390() throws Exception {
        Schema schema = getConnection().getSchema();
        Cube salesCube = schema.lookupCube("Sales", true);
        SchemaReader sr = salesCube.getSchemaReader(null).withLocus();
        List<Member> members =
            sr.getLevelMembers(
                (Level) new NameResolver().resolve(
                    salesCube,
                    Util.toOlap4j(
                        Util.parseIdentifier(
                            "[Store].[Store Size in SQFT].[Store Sqft]")),
                    true,
                    Category.Level,
                    MatchType.EXACT,
                    sr.getNamespaces()),
                true);
        assertEquals(
            "[[Store].[Store Size in SQFT].[#null], "
            + "[Store].[Store Size in SQFT].[20319], "
            + "[Store].[Store Size in SQFT].[21215], "
            + "[Store].[Store Size in SQFT].[22478], "
            + "[Store].[Store Size in SQFT].[23112], "
            + "[Store].[Store Size in SQFT].[23593], "
            + "[Store].[Store Size in SQFT].[23598], "
            + "[Store].[Store Size in SQFT].[23688], "
            + "[Store].[Store Size in SQFT].[23759], "
            + "[Store].[Store Size in SQFT].[24597], "
            + "[Store].[Store Size in SQFT].[27694], "
            + "[Store].[Store Size in SQFT].[28206], "
            + "[Store].[Store Size in SQFT].[30268], "
            + "[Store].[Store Size in SQFT].[30584], "
            + "[Store].[Store Size in SQFT].[30797], "
            + "[Store].[Store Size in SQFT].[33858], "
            + "[Store].[Store Size in SQFT].[34452], "
            + "[Store].[Store Size in SQFT].[34791], "
            + "[Store].[Store Size in SQFT].[36509], "
            + "[Store].[Store Size in SQFT].[38382], "
            + "[Store].[Store Size in SQFT].[39696]]",
            members.toString());
    }

    public void testColumnDatatypeFromSchemaIsHonored() {
        // verifies that types set in the schema are loaded and used.
        TestContext context = getTestContext().withSchema(
            "<?xml version='1.0'?>\n"
            + "<Schema name='FoodMart' metamodelVersion='4.0'>"
            + "<PhysicalSchema>"
            + "  <Table name='store' keyColumn='store_id'>"
            + "   <ColumnDefs>"
            + "  <!-- explicitly set the type and internalType for grocery_sqft -->"
            + "    <ColumnDef name='grocery_sqft' type='Numeric' internalType='double'/>"
            + "  <!-- leave the type and internalType unset for frozen_sqft -->"
            + "    <ColumnDef name='frozen_sqft' />"
            + "    </ColumnDefs></Table>"
            + "  <Table name='sales_fact_1997'>"
            + "  </Table>"
            + "</PhysicalSchema>"
            + "<Cube name='Sales'>\n"
            + "  <Dimensions>"
            + "    <Dimension name='Store2' table='store' key='Store Id'>\n"
            + "      <Attributes>"
            + "       <Attribute name='Grocery Sqft' keyColumn='grocery_sqft' hasHierarchy='true'/>\n"
            + "       <Attribute name='Frozen Sqft' keyColumn='frozen_sqft' hasHierarchy='true'/>\n"
            + "       <Attribute name='Store Id' keyColumn='store_id' hasHierarchy='true'/>\n"
            + "      </Attributes>"
            + "    </Dimension>\n"
            + "  </Dimensions>"
            + "  <MeasureGroups>"
            + "    <MeasureGroup table='sales_fact_1997'>"
            + "      <Measures>"
            + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum' />\n"
            + "      </Measures>"
            + "      <DimensionLinks>"
            + "        <ForeignKeyLink dimension='Store2' foreignKeyColumn='store_id'/>"
            + "      </DimensionLinks>"
            + "    </MeasureGroup>"
            + "  </MeasureGroups>"
            + "</Cube>\n"
            + "</Schema>");
        RolapSchema schema = (RolapSchema)context.getConnection().getSchema();
        SqlStatement.Type sqlStatementType = schema.getCubeList().get(0)
            .getDimensionList().get(1)
            .getHierarchyList().get("Grocery Sqft")
            .getLevelList().get(1)
            .getAttribute().getNameExp().getInternalType();
        Dialect.Datatype  dialectType = schema.getCubeList().get(0)
            .getDimensionList().get(1)
            .getHierarchyList().get("Grocery Sqft")
            .getLevelList().get(1)
            .getAttribute().getNameExp().getDatatype();
        assertEquals(
            "internalType should match what was set in the schema",
            SqlStatement.Type.DOUBLE, sqlStatementType);
        assertEquals(
            "dialect type should match what was set in the schema",
            Dialect.Datatype.Numeric, dialectType);

        dialectType = schema.getCubeList().get(0)
            .getDimensionList().get(1)
            .getHierarchyList().get("Frozen Sqft")
            .getLevelList().get(1)
            .getAttribute().getNameExp().getDatatype();
        assertEquals(
            "The type has not been explicitly set in the schema, "
            + "so the default type should still be used.",
            Dialect.Datatype.Integer, dialectType);
    }

    public void testMultiByteSchemaReadFromFile() throws IOException {
        String rawSchema = getTestContext().getRawSchema().replace(
            "<Attribute name='Department Description' keyColumn='department_id'/>",
            "<Attribute name='Department Description' keyColumn='department_id' hasHierarchy='false'/>")
            .replace(
                "<Hierarchy name='Department'>",
                "<Hierarchy name='地域'>");
        File schemaFile = File.createTempFile("multiByteSchema", ",xml");
        schemaFile.deleteOnExit();
        FileOutputStream output = new FileOutputStream(schemaFile);
        IOUtils.write(rawSchema, output);
        IOUtils.closeQuietly(output);
        TestContext context = getTestContext();
        final Util.PropertyList properties =
            context.getConnectionProperties().clone();
        properties.put(
            RolapConnectionProperties.Catalog.name(),
            schemaFile.getAbsolutePath());
        context.withProperties(properties).assertQueryReturns(
            "select [Department].members on 0 from [HR]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Department].[地域].[All 地域s]}\n"
            + "{[Department].[地域].[1]}\n"
            + "{[Department].[地域].[2]}\n"
            + "{[Department].[地域].[3]}\n"
            + "{[Department].[地域].[4]}\n"
            + "{[Department].[地域].[5]}\n"
            + "{[Department].[地域].[11]}\n"
            + "{[Department].[地域].[14]}\n"
            + "{[Department].[地域].[15]}\n"
            + "{[Department].[地域].[16]}\n"
            + "{[Department].[地域].[17]}\n"
            + "{[Department].[地域].[18]}\n"
            + "{[Department].[地域].[19]}\n"
            + "Row #0: $39,431.67\n"
            + "Row #0: $2,376.00\n"
            + "Row #0: $428.76\n"
            + "Row #0: $739.80\n"
            + "Row #0: $72.36\n"
            + "Row #0: $832.68\n"
            + "Row #0: $5,984.28\n"
            + "Row #0: $874.80\n"
            + "Row #0: $9,190.80\n"
            + "Row #0: $5,765.23\n"
            + "Row #0: $5,873.04\n"
            + "Row #0: $5,641.52\n"
            + "Row #0: $1,652.40\n");
    }


    // TODO: test that there is an error if we try to define a property
    // that is not functionally dependent on the attribute (e.g. define week as
    // a property of month).

    // TODO: Test that get error if Query element has no child for current
    // SQL dialect, and no generic SQL element.

    // TODO: ForeignKeyLink with attribute and composite key. (E.g. reference
    // to the month attribute, requires a composite key.)
}

// End SchemaTest.java

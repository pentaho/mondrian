Mondrian 4.0.0 beta release notes
=================================

Introduction
------------

Mondrian 4.0.0 is a beta release of a new major version of the
leading open-source OLAP engine. To find out more about mondrian,
go to http://mondrian.pentaho.com.

Contents
--------

The distribution is contained in the file mondrian-<version>.zip.
Each distribution contains the full Mondrian source code, as
mondrian-<version>-src.zip, documentation including generated API
documentation.

The main interface to Mondrian is its olap4j driver,
mondrian.olap4j.MondrianOlap4jDriver.

This release runs on Java version 1.5 and higher. For
backwards compatibility on Java 1.4, substitute
mondrian-jdk14.jar for mondrian.jar, and add
retroweaver-rt-1.2.4.jar to your classpath.

For further installation instructions, see
http://mondrian.pentaho.com/documentation/installation.php
or the included file doc/installation.html.

Main functionality in this release
----------------------------------

 - Measure groups
 - Attributes
 - Time dimension generator
 - Composite keys
 - Physical schema
 - API to define aggregate tables (not yet complete)
 - Schema format does not require XML elements to be in a
   particular order.
 - Schema validation gives multiple errors and warnings, and
   errors and warnings have a precise XML location.
 - If connection property 'Ignore' is specified, warnings do not
   prevent a connection from being established.

Limitations
-----------

Some functionality is not complete in this beta version.
 - Ragged hierarchies are not supported.
 - No all native SQL optimizations supported in Mondrian-3.x
   still work.
 - There are approximately 250 test failures out of 2700 tests.
 - Pentaho Analyzer does not yet work against Mondrian-4.
 - It's not as easy as it should be to download and load sample
   data sets such as FoodMart.
 - Default value of Attribute.hasHierarchy is currently false.
   Before production, this will change it to true, if attribute
   is not included in any hierarchies.

API and MDX language changes from 3.x to 4.0
--------------------------------------------

 - Mondrian-4 is compatible with olap4j version 1.0.1.
 - In MDX, hierarchy elements must be specified using
   [dimension].[hierarchy], and level elements must be specified
   using [dimension].[hierarchy].[level]. (In Mondrian-3.x, this
   behavior was enabled only if the property
   mondrian.olap.SsasCompatibleNaming was set to true.)
 - The following deprecated APIs have been removed:
  -- mondrian.olap.DimensionType
  -- mondrian.olap.LevelType
- createDimension has been removed from the API mondrian.olap.Schema
 - TODO

Other changes from 3.x to 4.0
-----------------------------

The XML schema format is not compatible between
versions. Mondrian-3.x software would not be able to read a
Mondrian-4 schema, and Mondrian-4 software can read a Mondrian-3
schema only because it recognizes that the schema is a legacy
format and internally converts to Mondrian-4 format.

We recommend using the metamodelVersion attribute of the Schema
element, to make it clear which version the schema is intended
for. This attribute is mandatory from 4.0 onwards.

The schema converter can handle most schemas, but has the
following limitations. It cannot recognize uses of columns in
expressions and convert them to Column elements.

The VirtualCube element is obsolete. The equivalent in Mondrian-4
is a Cube that contains multiple MeasureGroups.

The AggName element is obsolete. In Mondrian-4 you define
aggregate tables using the MeasureGroup element (which is also
used to define fact tables).

The AggPattern element is obsolete. There is no direct way in
Mondrian-4 to define a set of aggregate tables by
pattern-matching the names of tables and columns in the
schema. You can use the new aggregate table API to scan the
schema and define aggregate tables.

The standard demonstration schema is now called
FoodMart.mondrian.xml.  (The longer extension helps tools such as
Pentaho more easily identify the purpose of the file.)  It is in
version 4 format, of course. The previous demonstration schema,
which was called FoodMart.xml in version 3, is now called
FoodMart3.mondrian.xml.

In 3.x releases, the mondrian-<version>.zip file contained the
FoodMart data set in MySQL and Microsoft Access formats, and
there was an additional mondrian-<version>-derby.zip containing
the FoodMart data set in Apache Derby format. The binary
distribution of this release does not contain the data set. It
can be downloaded from
http://repo.pentaho.org/artifactory/repo/pentaho/mondrian-data-foodmart.

JPivot is no longer included with the release. Neither is there a
mondrian.war file in the distribution.

Schema workbench is obsolete and has been removed. Pentaho hopes
to build a replacement, but no firm plans exist at this time.

XMLA server has been removed from the Mondrian code base. It is
now a separate project. See
https://github.com/julianhyde/olap4j-xmlaserver.

The "high-cardinality dimensions" feature has been removed. There
are better ways to support dimensions that have large numbers of
members.

See CHANGES.txt for a list of all source code changes since the
previous release.

Upgrading from 3.x to 4.x
---------------------------------------
Mondrian 3 schemas will be automatically upgraded to Mondrian 4 
schemas internally.  Set the log4j log level for 
mondrian.rolap.RolapSchemaUpgrader to DEBUG to log the XML for the
upgraded schema.

Bugs and feature requests fixed for 4.0
---------------------------------------
TODO

Removed
-------

To be removed
-------------
Property.formatter (XML attribute)
SchemaGrant.access value "all_dimensions"

# End

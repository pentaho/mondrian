/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, May 2, 2003
*/
package mondrian.xmla;

import mondrian.olap.EnumeratedValues;
import mondrian.olap.Util;

/**
 * Defines an XML for Analysis Property.
 *
 * @author jhyde
 * @since May 2, 2003
 * @version $Id$
 */
class PropertyDefinition extends EnumeratedValues.BasicValue {
    private static final String nl = Util.nl;

    final RowsetDefinition.Type type;
    final EnumeratedValues typeValues;
    final Enumeration.Access access;
    final Enumeration.Methods usage;
    final String value;

    /**
     * @pre (enumeration != null) == type.isEnum()
     */
    public PropertyDefinition(String name, int ordinal, RowsetDefinition.Type type, EnumeratedValues enumeration, Enumeration.Access access, String value, Enumeration.Methods usage, String description) {
        super(name, ordinal, description);
        this.type = type;
        this.typeValues = enumeration;
        Util.assertPrecondition((enumeration != null) == type.isEnum(), "(enumeration != null) == type.isEnum()");
        this.access = access;
        this.usage = usage;
        this.value = value;
    }

    public static final int AxisFormat_ORDINAL = 0;
    static final PropertyDefinition AxisFormat = 
        new PropertyDefinition(
            "AxisFormat", 
            AxisFormat_ORDINAL, 
            RowsetDefinition.Type.Enumeration, 
            Enumeration.AxisFormat.enumeration, 
            Enumeration.Access.write, 
            "", 
            Enumeration.Methods.execute,
            "Client asks for the MDDataSet axis to be formatted in one of these ways: TupleFormat, ClusterFormat, CustomFormat.");

    public static final int BeginRange_ORDINAL = 1;
    static final PropertyDefinition BeginRange = 
        new PropertyDefinition(
            "BeginRange", 
            BeginRange_ORDINAL, 
            RowsetDefinition.Type.Integer, 
            null, 
            Enumeration.Access.write, 
            "-1", 
            Enumeration.Methods.execute, 
            "An integer value corresponding to a CellOrdinal used to restrict an MDDataSet returned by a command to a specific range of cells. Used in conjunction with the EndRange property. If unspecified, all cells are returned in the rowset. The value -1 means unspecified.");

    public static final int Catalog_ORDINAL = 2;
    static final PropertyDefinition Catalog = 
        new PropertyDefinition(
            "Catalog", 
            Catalog_ORDINAL, 
            RowsetDefinition.Type.String, 
            null, Enumeration.Access.readWrite, 
            "", 
            Enumeration.Methods.discoverAndExecute, 
            "Specifies the initial catalog or database on which to connect.");

    public static final int Content_ORDINAL = 3;
    static final PropertyDefinition Content = 
        new PropertyDefinition(
            "Content", 
            Content_ORDINAL, 
            RowsetDefinition.Type.EnumString, 
            Enumeration.Content.enumeration, 
            Enumeration.Access.write, 
            XmlaConstants.CONTENT_DEFAULT.getName(),
            Enumeration.Methods.discoverAndExecute,
            "An enumerator that specifies what type of data is returned in the result set. " + nl + 
            "None: Allows the structure of the command to be verified, but not executed. Analogous to using Prepare to check syntax, and so on." + nl + 
            "Schema: Contains the XML schema (which indicates column information, and so on) that relates to the requested query." + nl + 
            "Data: Contains only the data that was requested." + nl + 
            "SchemaData: Returns both the schema information as well as the data.");

    public static final int Cube_ORDINAL = 4;
    static final PropertyDefinition Cube = 
        new PropertyDefinition(
            "Cube", 
            Cube_ORDINAL, 
            RowsetDefinition.Type.String, 
            null, 
            Enumeration.Access.readWrite, 
            "", 
            Enumeration.Methods.execute,
            "The cube context for the Command parameter. If the command contains a cube name (such as an MDX FROM clause) the setting of this property is ignored.");

    public static final int DataSourceInfo_ORDINAL = 5;
    static final PropertyDefinition DataSourceInfo = 
        new PropertyDefinition(
            "DataSourceInfo", 
            DataSourceInfo_ORDINAL, 
            RowsetDefinition.Type.String, 
            null, 
            Enumeration.Access.readWrite, 
            "", 
            Enumeration.Methods.discoverAndExecute,
            "A string containing provider specific information, required to access the data source.");

    public static final int EndRange_ORDINAL = 6;
    static final PropertyDefinition EndRange = 
        new PropertyDefinition(
            "EndRange", 
            EndRange_ORDINAL, 
            RowsetDefinition.Type.Integer, 
            null, 
            Enumeration.Access.write, 
            "-1", 
            Enumeration.Methods.execute,
            "An integer value corresponding to a CellOrdinal used to restrict an MDDataSet returned by a command to a specific range of cells. Used in conjunction with the BeginRange property. If unspecified, all cells are returned in the rowset. The value -1 means unspecified.");

    public static final int Format_ORDINAL = 7;
    static final PropertyDefinition Format = 
        new PropertyDefinition(
            "Format", 
            Format_ORDINAL, 
            RowsetDefinition.Type.EnumString, 
            Enumeration.Format.enumeration, 
            Enumeration.Access.write, 
            "Native", 
            Enumeration.Methods.discoverAndExecute,
            "Enumerator that determines the format of the returned result set. Values include:" + nl + 
            "Tabular: a flat or hierarchical rowset. Similar to the XML RAW format in SQL. The Format property should be set to Tabular for OLE DB for Data Mining commands." + nl + 
            "Multidimensional: Indicates that the result set will use the MDDataSet format (Execute method only)." + nl + 
            "Native: The client does not request a specific format, so the provider may return the format  appropriate to the query. (The actual result type is identified by namespace of the result.)");

    public static final int LocaleIdentifier_ORDINAL = 8;
    static final PropertyDefinition LocaleIdentifier = 
        new PropertyDefinition(
            "LocaleIdentifier", 
            LocaleIdentifier_ORDINAL, 
            RowsetDefinition.Type.UnsignedInteger, 
            null, 
            Enumeration.Access.readWrite, 
            "None", 
            Enumeration.Methods.discoverAndExecute,
            "Use this to read or set the numeric locale identifier for this request. The default is provider-specific." + nl + 
            "For the complete hexadecimal list of language identifiers, search on \"Language Identifiers\" in the MSDN Library at http://www.msdn.microsoft.com.");

    public static final int MDXSupport_ORDINAL = 9;
    static final PropertyDefinition MDXSupport = 
        new PropertyDefinition(
            "MDXSupport", 
            MDXSupport_ORDINAL, 
            RowsetDefinition.Type.EnumString, 
            Enumeration.MDXSupport.enumeration, 
            Enumeration.Access.read, "Core", Enumeration.Methods.discover,
            "Enumeration that describes the degree of MDX support. At initial release Core is the only value in the enumeration. In future releases, other values will be defined for this enumeration.");

    public static final int Password_ORDINAL = 10;
    static final PropertyDefinition Password = 
        new PropertyDefinition(
            "Password", 
            Password_ORDINAL, 
            RowsetDefinition.Type.String, 
            null, 
            Enumeration.Access.read, 
            "", 
            Enumeration.Methods.discoverAndExecute,
            "This property is deprecated in XMLA 1.1. To support legacy applications, the provider accepts but ignores the Password property setting when it is used with the Discover and Execute method");

    public static final int ProviderName_ORDINAL = 11;
    static final PropertyDefinition ProviderName = 
        new PropertyDefinition(
            "ProviderName", 
            ProviderName_ORDINAL, 
            RowsetDefinition.Type.String, 
            null, 
            Enumeration.Access.read, 
            "Mondrian XML for Analysis Provider", 
            Enumeration.Methods.discover,
            "The XML for Analysis Provider name.");

    //TODO: the below version string "2.1.0.0" ought to be read at compile
    // time from some build property rather than being hard-coded.
    public static final int ProviderVersion_ORDINAL = 12;
    static final PropertyDefinition ProviderVersion = 
        new PropertyDefinition(
            "ProviderVersion", 
            ProviderVersion_ORDINAL, 
            RowsetDefinition.Type.String, 
            null, 
            Enumeration.Access.read, 
            "2.1.0.0", 
            Enumeration.Methods.discover,
            "The version of the Mondrian XMLA Provider");

    public static final int StateSupport_ORDINAL = 13;
    static final PropertyDefinition StateSupport = 
        new PropertyDefinition(
            "StateSupport", 
            StateSupport_ORDINAL, 
            RowsetDefinition.Type.EnumString, 
            Enumeration.StateSupport.enumeration, 
            Enumeration.Access.read, 
            "None", 
            Enumeration.Methods.discover,
            "Property that specifies the degree of support in the provider for state. For information about state in XML for Analysis, see \"Support for Statefulness in XML for Analysis.\" Minimum enumeration values are as follows:" + nl +
            "None - No support for sessions or stateful operations." + nl + 
            "Sessions - Provider supports sessions.");

    public static final int Timeout_ORDINAL = 14;
    static final PropertyDefinition Timeout = 
        new PropertyDefinition(
            "Timeout", 
            Timeout_ORDINAL, 
            RowsetDefinition.Type.UnsignedInteger, 
            null, Enumeration.Access.readWrite, 
            "Undefined", 
            Enumeration.Methods.discoverAndExecute,
            "A numeric time-out specifying in seconds the amount of time to wait for a request to be successful.");

    public static final int UserName_ORDINAL = 15;
    static final PropertyDefinition UserName = 
        new PropertyDefinition(
            "UserName", 
            UserName_ORDINAL, 
            RowsetDefinition.Type.String, 
            null, Enumeration.Access.read, 
            "", 
            Enumeration.Methods.discoverAndExecute,
            "Returns the UserName the server associates with the command." + nl + 
            "This property is deprecated as writeable in XMLA 1.1. To support legacy applications, servers accept but ignore the password setting when it is used with the Execute method.");

    static final EnumeratedValues enumeration = new EnumeratedValues(new PropertyDefinition[]{
            AxisFormat,
            BeginRange,
            Catalog,
            Content,
            Cube,
            DataSourceInfo,
            EndRange,
            Format,
            LocaleIdentifier,
            MDXSupport,
            Password,
            ProviderName,
            ProviderVersion,
            StateSupport,
            Timeout,
            UserName,
    });

    public static PropertyDefinition getValue(final String propertyName) {
        return (PropertyDefinition) enumeration.getValue(propertyName, true);
    }
}

// End PropertyDefinition.java

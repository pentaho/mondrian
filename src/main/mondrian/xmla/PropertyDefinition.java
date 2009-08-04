/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2003-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, May 2, 2003
*/
package mondrian.xmla;

import mondrian.olap.Util;
import mondrian.olap.MondrianServer;

import java.util.Set;

/**
 * Defines an XML for Analysis Property.
 *
 * @author jhyde
 * @version $Id$
 * @since May 2, 2003
 */
enum PropertyDefinition {
    AxisFormat(
        RowsetDefinition.Type.Enumeration,
        Util.enumSetAllOf(Enumeration.AxisFormat.class),
        Enumeration.Access.Write,
        "",
        Enumeration.Methods.execute,
        "Determines the format used within an MDDataSet result set to describe the axes of the multidimensional dataset. This property can have the values listed in the following table: TupleFormat (default), ClusterFormat, CustomFormat."),

    BeginRange(
        RowsetDefinition.Type.Integer,
        null,
        Enumeration.Access.Write,
        "-1",
        Enumeration.Methods.execute,
        "Contains a zero-based integer value corresponding to a CellOrdinal attribute value. (The CellOrdinal attribute is part of the Cell element in the CellData section of MDDataSet.)\n"
        + "Used together with the EndRange property, the client application can use this property to restrict an OLAP dataset returned by a command to a specific range of cells. If -1 is specified, all cells up to the cell specified in the EndRange property are returned.\n"
        + "The default value for this property is -1."),

    Catalog(
        RowsetDefinition.Type.String,
        null,
        Enumeration.Access.ReadWrite,
        "",
        Enumeration.Methods.discoverAndExecute,
        "When establishing a session with an Analysis Services instance to send an XMLA command, this property is equivalent to the OLE DB property, DBPROP_INIT_CATALOG.\n"
        + "When you set this property during a session to change the current database for the session, this property is equivalent to the OLE DB property, DBPROP_CURRENTCATALOG.\n"
        + "The default value for this property is an empty string."),

    Content(
        RowsetDefinition.Type.EnumString,
        Util.enumSetAllOf(Enumeration.Content.class),
        Enumeration.Access.Write,
        XmlaConstants.CONTENT_DEFAULT.name(),
        Enumeration.Methods.discoverAndExecute,
        "An enumerator that specifies what type of data is returned in the result set.\n"
        + "None: Allows the structure of the command to be verified, but not executed. Analogous to using Prepare to check syntax, and so on.\n"
        + "Schema: Contains the XML schema (which indicates column information, and so on) that relates to the requested query.\n"
        + "Data: Contains only the data that was requested.\n"
        + "SchemaData: Returns both the schema information as well as the data."),

    Cube(
        RowsetDefinition.Type.String,
        null,
        Enumeration.Access.ReadWrite,
        "",
        Enumeration.Methods.execute,
        "The cube context for the Command parameter. If the command contains a cube name (such as an MDX FROM clause) the setting of this property is ignored."),

    DataSourceInfo(
        RowsetDefinition.Type.String,
        null,
        Enumeration.Access.ReadWrite,
        "",
        Enumeration.Methods.discoverAndExecute,
        "A string containing provider specific information, required to access the data source."),

    EndRange(
        RowsetDefinition.Type.Integer,
        null,
        Enumeration.Access.Write,
        "-1",
        Enumeration.Methods.execute,
        "An integer value corresponding to a CellOrdinal used to restrict an MDDataSet returned by a command to a specific range of cells. Used in conjunction with the BeginRange property. If unspecified, all cells are returned in the rowset. The value -1 means unspecified."),

    Format(
        RowsetDefinition.Type.EnumString,
        Util.enumSetAllOf(Enumeration.Format.class),
        Enumeration.Access.Write,
        "Native",
        Enumeration.Methods.discoverAndExecute,
        "Enumerator that determines the format of the returned result set. Values include:\n"
        + "Tabular: a flat or hierarchical rowset. Similar to the XML RAW format in SQL. The Format property should be set to Tabular for OLE DB for Data Mining commands.\n"
        + "Multidimensional: Indicates that the result set will use the MDDataSet format (Execute method only).\n"
        + "Native: The client does not request a specific format, so the provider may return the format  appropriate to the query. (The actual result type is identified by namespace of the result.)"),

    LocaleIdentifier(
        RowsetDefinition.Type.UnsignedInteger,
        null,
        Enumeration.Access.ReadWrite,
        "None",
        Enumeration.Methods.discoverAndExecute,
        "Use this to read or set the numeric locale identifier for this request. The default is provider-specific.\n"
        + "For the complete hexadecimal list of language identifiers, search on \"Language Identifiers\" in the MSDN Library at http://www.msdn.microsoft.com."),

    MDXSupport(
        RowsetDefinition.Type.EnumString,
        Util.enumSetAllOf(Enumeration.MdxSupport.class),
        Enumeration.Access.Read,
        "Core",
        Enumeration.Methods.discover,
        "Enumeration that describes the degree of MDX support. At initial release Core is the only value in the enumeration. In future releases, other values will be defined for this enumeration."),

    Password(
        RowsetDefinition.Type.String,
        null,
        Enumeration.Access.Read,
        "",
        Enumeration.Methods.discoverAndExecute,
        "This property is deprecated in XMLA 1.1. To support legacy applications, the provider accepts but ignores the Password property setting when it is used with the Discover and Execute method"),

    ProviderName(
        RowsetDefinition.Type.String,
        null,
        Enumeration.Access.Read,
        "Mondrian XML for Analysis Provider",
        Enumeration.Methods.discover,
        "The XML for Analysis Provider name."),

    ProviderVersion(
        RowsetDefinition.Type.String,
        null,
        Enumeration.Access.Read,
        MondrianServer.forConnection(null).getVersion().getVersionString(),
        Enumeration.Methods.discover,
        "The version of the Mondrian XMLA Provider"),

    StateSupport(
        RowsetDefinition.Type.EnumString,
        Util.enumSetAllOf(Enumeration.StateSupport.class),
        Enumeration.Access.Read,
        "None",
        Enumeration.Methods.discover,
        "Property that specifies the degree of support in the provider for state. For information about state in XML for Analysis, see \"Support for Statefulness in XML for Analysis.\" Minimum enumeration values are as follows:\n"
        + "None - No support for sessions or stateful operations.\n"
        + "Sessions - Provider supports sessions."),

    Timeout(
        RowsetDefinition.Type.UnsignedInteger,
        null,
        Enumeration.Access.ReadWrite,
        "Undefined",
        Enumeration.Methods.discoverAndExecute,
        "A numeric time-out specifying in seconds the amount of time to wait for a request to be successful."),

    UserName(
        RowsetDefinition.Type.String,
        null,
        Enumeration.Access.Read,
        "",
        Enumeration.Methods.discoverAndExecute,
        "Returns the UserName the server associates with the command.\n"
        + "This property is deprecated as writeable in XMLA 1.1. To support legacy applications, servers accept but ignore the password setting when it is used with the Execute method."),

    VisualMode(
        RowsetDefinition.Type.Enumeration,
        Util.enumSetAllOf(Enumeration.VisualMode.class),
        Enumeration.Access.Write,
        Integer.toString(Enumeration.VisualMode.Visual.ordinal()),
        Enumeration.Methods.discoverAndExecute,
        "This property is equivalent to the OLE DB property, MDPROP_VISUALMODE.\n"
        + "The default value for this property is zero (0), equivalent to DBPROPVAL_VISUAL_MODE_DEFAULT."),

    // mondrian-specific property for advanced drill-through
    TableFields(
        RowsetDefinition.Type.String,
        null,
        Enumeration.Access.Read,
        "",
        Enumeration.Methods.discoverAndExecute,
        "List of fields to return for drill-through.\n"
        + "The default value of this property is the empty string,"
        + "in which case, all fields are returned."),

    // mondrian-specific property for advanced drill-through
    AdvancedFlag(
        RowsetDefinition.Type.Boolean,
        null,
        Enumeration.Access.Read,
        "false",
        Enumeration.Methods.discoverAndExecute,
        "");

    final RowsetDefinition.Type type;
    final Set<? extends Enum> enumSet;
    final Enumeration.Access access;
    final Enumeration.Methods usage;
    final String value;
    final String description;

    PropertyDefinition(
        RowsetDefinition.Type type,
        Set<? extends Enum> enumSet,
        Enumeration.Access access,
        String value,
        Enumeration.Methods usage,
        String description)
    {
        // Line endings must be UNIX style (LF) not Windows style (LF+CR).
        // Thus the client will receive the same XML, regardless
        // of the server O/S.
        assert description.indexOf('\r') == -1;
        assert value.indexOf('\r') == -1;
        assert (enumSet != null) == type.isEnum();
        this.type = type;
        this.enumSet = enumSet;
        this.access = access;
        this.usage = usage;
        this.value = value;
        this.description = description;
    }

    /**
     * Returns the description of this PropertyDefinition.
     *
     * @return description
     */
    public String getDescription() {
        return description;
    }
}

// End PropertyDefinition.java

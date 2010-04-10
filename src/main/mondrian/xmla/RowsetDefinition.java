/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2003-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;

import mondrian.olap.*;
import mondrian.olap.fun.FunInfo;
import mondrian.rolap.*;

import static mondrian.xmla.XmlaConstants.*;

import org.olap4j.impl.Olap4jUtil;
import org.olap4j.metadata.Member.TreeOp;
import org.olap4j.metadata.XmlaConstant;
import org.olap4j.metadata.XmlaConstants;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.text.SimpleDateFormat;
import java.text.Format;

/**
 * <code>RowsetDefinition</code> defines a rowset, including the columns it
 * should contain.
 *
 * <p>See "XML for Analysis Rowsets", page 38 of the XML for Analysis
 * Specification, version 1.1.
 *
 * @author jhyde
 * @version $Id$
 */
enum RowsetDefinition {
    /**
     * Returns a list of XML for Analysis data sources
     * available on the server or Web Service. (For an
     * example of how these may be published, see
     * "XML for Analysis Implementation Walkthrough"
     * in the XML for Analysis specification.)
     *
     *  http://msdn2.microsoft.com/en-us/library/ms126129(SQL.90).aspx
     *
     *
     * restrictions
     *
     * Not supported
     */
    DISCOVER_DATASOURCES(
        0,
        "Returns a list of XML for Analysis data sources available on the "
        + "server or Web Service.",
        new Column[] {
            DiscoverDatasourcesRowset.DataSourceName,
            DiscoverDatasourcesRowset.DataSourceDescription,
            DiscoverDatasourcesRowset.URL,
            DiscoverDatasourcesRowset.DataSourceInfo,
            DiscoverDatasourcesRowset.ProviderName,
            DiscoverDatasourcesRowset.ProviderType,
            DiscoverDatasourcesRowset.AuthenticationMode,
        },
        null /* not sorted */)
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new DiscoverDatasourcesRowset(request, handler);
        }
    },

    /**
     * Note that SQL Server also returns the data-mining columns.
     *
     *
     * restrictions
     *
     * Not supported
     */
    DISCOVER_SCHEMA_ROWSETS(
        2,
        "Returns the names, values, and other information of all supported "
        + "RequestType enumeration values.",
        new Column[] {
            DiscoverSchemaRowsetsRowset.SchemaName,
            DiscoverSchemaRowsetsRowset.SchemaGuid,
            DiscoverSchemaRowsetsRowset.Restrictions,
            DiscoverSchemaRowsetsRowset.Description,
        },
        null /* not sorted */)
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new DiscoverSchemaRowsetsRowset(request, handler);
        }
        protected void writeRowsetXmlSchemaRowDef(SaxWriter writer) {
            writer.startElement(
                "xsd:complexType",
                "name", "row");
            writer.startElement("xsd:sequence");
            for (Column column : columnDefinitions) {
                final String name = XmlaUtil.encodeElementName(column.name);

                if (column == DiscoverSchemaRowsetsRowset.Restrictions) {
                    writer.startElement(
                        "xsd:element",
                        "sql:field", column.name,
                        "name", name,
                        "minOccurs", 0,
                        "maxOccurs", "unbounded");
                    writer.startElement("xsd:complexType");
                    writer.startElement("xsd:sequence");
                    writer.element(
                        "xsd:element",
                        "name", "Name",
                        "type", "xsd:string",
                        "sql:field", "Name");
                    writer.element(
                        "xsd:element",
                        "name", "Type",
                        "type", "xsd:string",
                        "sql:field", "Type");

                    writer.endElement(); // xsd:sequence
                    writer.endElement(); // xsd:complexType
                    writer.endElement(); // xsd:element

                } else {
                    final String xsdType = column.type.columnType;

                    Object[] attrs;
                    if (column.nullable) {
                        if (column.unbounded) {
                            attrs = new Object[]{
                                "sql:field", column.name,
                                "name", name,
                                "type", xsdType,
                                "minOccurs", 0,
                                "maxOccurs", "unbounded"
                            };
                        } else {
                            attrs = new Object[]{
                                "sql:field", column.name,
                                "name", name,
                                "type", xsdType,
                                "minOccurs", 0
                            };
                        }
                    } else {
                        if (column.unbounded) {
                            attrs = new Object[]{
                                "sql:field", column.name,
                                "name", name,
                                "type", xsdType,
                                "maxOccurs", "unbounded"
                            };
                        } else {
                            attrs = new Object[]{
                                "sql:field", column.name,
                                "name", name,
                                "type", xsdType
                            };
                        }
                    }
                    writer.element("xsd:element", attrs);
                }
            }
            writer.endElement(); // xsd:sequence
            writer.endElement(); // xsd:complexType
        }
    },

    /**
     *
     *
     *
     * restrictions
     *
     * Not supported
     */
    DISCOVER_ENUMERATORS(
        3,
        "Returns a list of names, data types, and enumeration values for "
        + "enumerators supported by the provider of a specific data source.",
        new Column[] {
            DiscoverEnumeratorsRowset.EnumName,
            DiscoverEnumeratorsRowset.EnumDescription,
            DiscoverEnumeratorsRowset.EnumType,
            DiscoverEnumeratorsRowset.ElementName,
            DiscoverEnumeratorsRowset.ElementDescription,
            DiscoverEnumeratorsRowset.ElementValue,
        },
        null /* not sorted */)
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new DiscoverEnumeratorsRowset(request, handler);
        }
    },

    /**
     *
     *
     *
     * restrictions
     *
     * Not supported
     */
    DISCOVER_PROPERTIES(
        1,
        "Returns a list of information and values about the requested "
        + "properties that are supported by the specified data source "
        + "provider.",
        new Column[] {
            DiscoverPropertiesRowset.PropertyName,
            DiscoverPropertiesRowset.PropertyDescription,
            DiscoverPropertiesRowset.PropertyType,
            DiscoverPropertiesRowset.PropertyAccessType,
            DiscoverPropertiesRowset.IsRequired,
            DiscoverPropertiesRowset.Value,
        },
        null /* not sorted */)
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new DiscoverPropertiesRowset(request, handler);
        }
    },

    /**
     *
     *
     *
     * restrictions
     *
     * Not supported
     */
    DISCOVER_KEYWORDS(
        4,
        "Returns an XML list of keywords reserved by the provider.",
        new Column[] {
            DiscoverKeywordsRowset.Keyword,
        },
        null /* not sorted */)
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new DiscoverKeywordsRowset(request, handler);
        }
    },

    /**
     *
     *
     *
     * restrictions
     *
     * Not supported
     */
    DISCOVER_LITERALS(
        5,
        "Returns information about literals supported by the provider.",
        new Column[] {
            DiscoverLiteralsRowset.LiteralName,
            DiscoverLiteralsRowset.LiteralValue,
            DiscoverLiteralsRowset.LiteralInvalidChars,
            DiscoverLiteralsRowset.LiteralInvalidStartingChars,
            DiscoverLiteralsRowset.LiteralMaxLength,
        },
        null /* not sorted */)
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new DiscoverLiteralsRowset(request, handler);
        }
    },

    /**
     *
     *
     *
     * restrictions
     *
     * Not supported
     */
    DBSCHEMA_CATALOGS(
        6,
        "Returns information about literals supported by the provider.",
        new Column[] {
            DbschemaCatalogsRowset.CatalogName,
            DbschemaCatalogsRowset.Description,
            DbschemaCatalogsRowset.Roles,
            DbschemaCatalogsRowset.DateModified,
        },
        new Column[] {
            DbschemaCatalogsRowset.CatalogName,
        })
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new DbschemaCatalogsRowset(request, handler);
        }
    },

    /**
     *
     *
     *
     * restrictions
     *
     * Not supported
     *    COLUMN_OLAP_TYPE
     */
    DBSCHEMA_COLUMNS(
        7, null,
        new Column[] {
            DbschemaColumnsRowset.TableCatalog,
            DbschemaColumnsRowset.TableSchema,
            DbschemaColumnsRowset.TableName,
            DbschemaColumnsRowset.ColumnName,
            DbschemaColumnsRowset.OrdinalPosition,
            DbschemaColumnsRowset.ColumnHasDefault,
            DbschemaColumnsRowset.ColumnFlags,
            DbschemaColumnsRowset.IsNullable,
            DbschemaColumnsRowset.DataType,
            DbschemaColumnsRowset.CharacterMaximumLength,
            DbschemaColumnsRowset.CharacterOctetLength,
            DbschemaColumnsRowset.NumericPrecision,
            DbschemaColumnsRowset.NumericScale,
        },
        new Column[] {
            DbschemaColumnsRowset.TableCatalog,
            DbschemaColumnsRowset.TableSchema,
            DbschemaColumnsRowset.TableName,
        })
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new DbschemaColumnsRowset(request, handler);
        }
    },

    /**
     *
     *
     *
     * restrictions
     *
     * Not supported
     */
    DBSCHEMA_PROVIDER_TYPES(
        8, null,
        new Column[] {
            DbschemaProviderTypesRowset.TypeName,
            DbschemaProviderTypesRowset.DataType,
            DbschemaProviderTypesRowset.ColumnSize,
            DbschemaProviderTypesRowset.LiteralPrefix,
            DbschemaProviderTypesRowset.LiteralSuffix,
            DbschemaProviderTypesRowset.IsNullable,
            DbschemaProviderTypesRowset.CaseSensitive,
            DbschemaProviderTypesRowset.Searchable,
            DbschemaProviderTypesRowset.UnsignedAttribute,
            DbschemaProviderTypesRowset.FixedPrecScale,
            DbschemaProviderTypesRowset.AutoUniqueValue,
            DbschemaProviderTypesRowset.IsLong,
            DbschemaProviderTypesRowset.BestMatch,
        },
        new Column[] {
            DbschemaProviderTypesRowset.DataType,
        })
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new DbschemaProviderTypesRowset(request, handler);
        }
    },

    DBSCHEMA_SCHEMATA(
        8, null,
        new Column[] {
            DbschemaSchemataRowset.CatalogName,
            DbschemaSchemataRowset.SchemaName,
            DbschemaSchemataRowset.SchemaOwner,
        },
        new Column[] {
            DbschemaSchemataRowset.CatalogName,
            DbschemaSchemataRowset.SchemaName,
            DbschemaSchemataRowset.SchemaOwner,
        })
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new DbschemaSchemataRowset(request, handler);
        }
    },

    /**
     * http://msdn2.microsoft.com/en-us/library/ms126299(SQL.90).aspx
     *
     * restrictions:
     *   TABLE_CATALOG Optional
     *   TABLE_SCHEMA Optional
     *   TABLE_NAME Optional
     *   TABLE_TYPE Optional
     *   TABLE_OLAP_TYPE Optional
     *
     * Not supported
     */
    DBSCHEMA_TABLES(
        9, null,
        new Column[] {
            DbschemaTablesRowset.TableCatalog,
            DbschemaTablesRowset.TableSchema,
            DbschemaTablesRowset.TableName,
            DbschemaTablesRowset.TableType,
            DbschemaTablesRowset.TableGuid,
            DbschemaTablesRowset.Description,
            DbschemaTablesRowset.TablePropId,
            DbschemaTablesRowset.DateCreated,
            DbschemaTablesRowset.DateModified,
            //TableOlapType,
        },
        new Column[] {
            DbschemaTablesRowset.TableType,
            DbschemaTablesRowset.TableCatalog,
            DbschemaTablesRowset.TableSchema,
            DbschemaTablesRowset.TableName,
        })
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new DbschemaTablesRowset(request, handler);
        }
    },

    /**
     * http://msdn.microsoft.com/library/en-us/oledb/htm/
     * oledbtables_info_rowset.asp
     *
     *
     * restrictions
     *
     * Not supported
     */
    DBSCHEMA_TABLES_INFO(
        10, null,
        new Column[] {
            DbschemaTablesInfoRowset.TableCatalog,
            DbschemaTablesInfoRowset.TableSchema,
            DbschemaTablesInfoRowset.TableName,
            DbschemaTablesInfoRowset.TableType,
            DbschemaTablesInfoRowset.TableGuid,
            DbschemaTablesInfoRowset.Bookmarks,
            DbschemaTablesInfoRowset.BookmarkType,
            DbschemaTablesInfoRowset.BookmarkDataType,
            DbschemaTablesInfoRowset.BookmarkMaximumLength,
            DbschemaTablesInfoRowset.BookmarkInformation,
            DbschemaTablesInfoRowset.TableVersion,
            DbschemaTablesInfoRowset.Cardinality,
            DbschemaTablesInfoRowset.Description,
            DbschemaTablesInfoRowset.TablePropId,
        },
        null /* cannot find doc -- presume unsorted */)
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new DbschemaTablesInfoRowset(request, handler);
        }
    },

    /**
     * http://msdn2.microsoft.com/en-us/library/ms126032(SQL.90).aspx
     *
     * restrictions
     *   CATALOG_NAME Optional
     *   SCHEMA_NAME Optional
     *   CUBE_NAME Mandatory
     *   ACTION_NAME Optional
     *   ACTION_TYPE Optional
     *   COORDINATE Mandatory
     *   COORDINATE_TYPE Mandatory
     *   INVOCATION
     *      (Optional) The INVOCATION restriction column defaults to the
     *      value of MDACTION_INVOCATION_INTERACTIVE. To retrieve all
     *      actions, use the MDACTION_INVOCATION_ALL value in the
     *      INVOCATION restriction column.
     *   CUBE_SOURCE
     *      (Optional) A bitmap with one of the following valid values:
     *
     *      1 CUBE
     *      2 DIMENSION
     *
     *      Default restriction is a value of 1.
     *
     * Not supported
     */
    MDSCHEMA_ACTIONS(
        11, null, new Column[] {
            MdschemaActionsRowset.CatalogName,
            MdschemaActionsRowset.SchemaName,
            MdschemaActionsRowset.CubeName,
            MdschemaActionsRowset.ActionName,
            MdschemaActionsRowset.Coordinate,
            MdschemaActionsRowset.CoordinateType,
        }, new Column[] {
            // Spec says sort on CATALOG_NAME, SCHEMA_NAME, CUBE_NAME,
            // ACTION_NAME.
            MdschemaActionsRowset.CatalogName,
            MdschemaActionsRowset.SchemaName,
            MdschemaActionsRowset.CubeName,
            MdschemaActionsRowset.ActionName,
        })
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new MdschemaActionsRowset(request, handler);
        }
    },

    /**
     * http://msdn2.microsoft.com/en-us/library/ms126271(SQL.90).aspx
     *
     * restrictions
     *   CATALOG_NAME Optional.
     *   SCHEMA_NAME Optional.
     *   CUBE_NAME Optional.
     *   CUBE_TYPE
     *      (Optional) A bitmap with one of these valid values:
     *      1 CUBE
     *      2 DIMENSION
     *     Default restriction is a value of 1.
     *   BASE_CUBE_NAME Optional.
     *
     * Not supported
     *   CREATED_ON
     *   LAST_SCHEMA_UPDATE
     *   SCHEMA_UPDATED_BY
     *   LAST_DATA_UPDATE
     *   DATA_UPDATED_BY
     *   ANNOTATIONS
     */
    MDSCHEMA_CUBES(
        12, null,
        new Column[] {
            MdschemaCubesRowset.CatalogName,
            MdschemaCubesRowset.SchemaName,
            MdschemaCubesRowset.CubeName,
            MdschemaCubesRowset.CubeType,
            MdschemaCubesRowset.CubeGuid,
            MdschemaCubesRowset.CreatedOn,
            MdschemaCubesRowset.LastSchemaUpdate,
            MdschemaCubesRowset.SchemaUpdatedBy,
            MdschemaCubesRowset.LastDataUpdate,
            MdschemaCubesRowset.DataUpdatedBy,
            MdschemaCubesRowset.IsDrillthroughEnabled,
            MdschemaCubesRowset.IsWriteEnabled,
            MdschemaCubesRowset.IsLinkable,
            MdschemaCubesRowset.IsSqlEnabled,
            MdschemaCubesRowset.Description,
            MdschemaCubesRowset.Dimensions,
            MdschemaCubesRowset.Sets,
            MdschemaCubesRowset.Measures
        },
        new Column[] {
            MdschemaCubesRowset.CatalogName,
            MdschemaCubesRowset.SchemaName,
            MdschemaCubesRowset.CubeName,
        })
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new MdschemaCubesRowset(request, handler);
        }
    },

    /**
     * http://msdn2.microsoft.com/en-us/library/ms126180(SQL.90).aspx
     * http://msdn2.microsoft.com/en-us/library/ms126180.aspx
     *
     * restrictions
     *    CATALOG_NAME Optional.
     *    SCHEMA_NAME Optional.
     *    CUBE_NAME Optional.
     *    DIMENSION_NAME Optional.
     *    DIMENSION_UNIQUE_NAME Optional.
     *    CUBE_SOURCE (Optional) A bitmap with one of the following valid
     *    values:
     *      1 CUBE
     *      2 DIMENSION
     *    Default restriction is a value of 1.
     *
     *    DIMENSION_VISIBILITY (Optional) A bitmap with one of the following
     *    valid values:
     *      1 Visible
     *      2 Not visible
     *    Default restriction is a value of 1.
     */
    MDSCHEMA_DIMENSIONS(
        13, null,
        new Column[] {
            MdschemaDimensionsRowset.CatalogName,
            MdschemaDimensionsRowset.SchemaName,
            MdschemaDimensionsRowset.CubeName,
            MdschemaDimensionsRowset.DimensionName,
            MdschemaDimensionsRowset.DimensionUniqueName,
            MdschemaDimensionsRowset.DimensionGuid,
            MdschemaDimensionsRowset.DimensionCaption,
            MdschemaDimensionsRowset.DimensionOrdinal,
            MdschemaDimensionsRowset.DimensionType,
            MdschemaDimensionsRowset.DimensionCardinality,
            MdschemaDimensionsRowset.DefaultHierarchy,
            MdschemaDimensionsRowset.Description,
            MdschemaDimensionsRowset.IsVirtual,
            MdschemaDimensionsRowset.IsReadWrite,
            MdschemaDimensionsRowset.DimensionUniqueSettings,
            MdschemaDimensionsRowset.DimensionMasterUniqueName,
            MdschemaDimensionsRowset.DimensionIsVisible,
            MdschemaDimensionsRowset.Hierarchies,
        },
        new Column[] {
            MdschemaDimensionsRowset.CatalogName,
            MdschemaDimensionsRowset.SchemaName,
            MdschemaDimensionsRowset.CubeName,
            MdschemaDimensionsRowset.DimensionName,
        })
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new MdschemaDimensionsRowset(request, handler);
        }
    },

    /**
     * http://msdn2.microsoft.com/en-us/library/ms126257(SQL.90).aspx
     *
     * restrictions
     *   LIBRARY_NAME Optional.
     *   INTERFACE_NAME Optional.
     *   FUNCTION_NAME Optional.
     *   ORIGIN Optional.
     *
     * Not supported
     *  DLL_NAME
     *    Optional
     *  HELP_FILE
     *    Optional
     *  HELP_CONTEXT
     *    Optional
     *    - SQL Server xml schema says that this must be present
     *  OBJECT
     *    Optional
     *  CAPTION The display caption for the function.
     */
    MDSCHEMA_FUNCTIONS(
        14, null,
        new Column[] {
            MdschemaFunctionsRowset.FunctionName,
            MdschemaFunctionsRowset.Description,
            MdschemaFunctionsRowset.ParameterList,
            MdschemaFunctionsRowset.ReturnType,
            MdschemaFunctionsRowset.Origin,
            MdschemaFunctionsRowset.InterfaceName,
            MdschemaFunctionsRowset.LibraryName,
            MdschemaFunctionsRowset.Caption,
        },
        new Column[] {
            MdschemaFunctionsRowset.LibraryName,
            MdschemaFunctionsRowset.InterfaceName,
            MdschemaFunctionsRowset.FunctionName,
            MdschemaFunctionsRowset.Origin,
        })
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new MdschemaFunctionsRowset(request, handler);
        }
    },

    /**
     * http://msdn2.microsoft.com/en-us/library/ms126062(SQL.90).aspx
     *
     * restrictions
     *    CATALOG_NAME Optional.
     *    SCHEMA_NAME Optional.
     *    CUBE_NAME Optional.
     *    DIMENSION_UNIQUE_NAME Optional.
     *    HIERARCHY_NAME Optional.
     *    HIERARCHY_UNIQUE_NAME Optional.
     *    HIERARCHY_ORIGIN
     *       (Optional) A default restriction is in effect
     *       on MD_USER_DEFINED and MD_SYSTEM_ENABLED.
     *    CUBE_SOURCE
     *      (Optional) A bitmap with one of the following valid values:
     *      1 CUBE
     *      2 DIMENSION
     *      Default restriction is a value of 1.
     *    HIERARCHY_VISIBILITY
     *      (Optional) A bitmap with one of the following valid values:
     *      1 Visible
     *      2 Not visible
     *      Default restriction is a value of 1.
     *
     * Not supported
     *  HIERARCHY_IS_VISIBLE
     *  HIERARCHY_ORIGIN
     *  HIERARCHY_DISPLAY_FOLDER
     *  INSTANCE_SELECTION
     */
    MDSCHEMA_HIERARCHIES(
        15, null,
        new Column[] {
            MdschemaHierarchiesRowset.CatalogName,
            MdschemaHierarchiesRowset.SchemaName,
            MdschemaHierarchiesRowset.CubeName,
            MdschemaHierarchiesRowset.DimensionUniqueName,
            MdschemaHierarchiesRowset.HierarchyName,
            MdschemaHierarchiesRowset.HierarchyUniqueName,
            MdschemaHierarchiesRowset.HierarchyGuid,
            MdschemaHierarchiesRowset.HierarchyCaption,
            MdschemaHierarchiesRowset.DimensionType,
            MdschemaHierarchiesRowset.HierarchyCardinality,
            MdschemaHierarchiesRowset.DefaultMember,
            MdschemaHierarchiesRowset.AllMember,
            MdschemaHierarchiesRowset.Description,
            MdschemaHierarchiesRowset.Structure,
            MdschemaHierarchiesRowset.IsVirtual,
            MdschemaHierarchiesRowset.IsReadWrite,
            MdschemaHierarchiesRowset.DimensionUniqueSettings,
            MdschemaHierarchiesRowset.DimensionIsVisible,
            MdschemaHierarchiesRowset.HierarchyOrdinal,
            MdschemaHierarchiesRowset.DimensionIsShared,
            MdschemaHierarchiesRowset.ParentChild,
            MdschemaHierarchiesRowset.Levels,
        },
        new Column[] {
            MdschemaHierarchiesRowset.CatalogName,
            MdschemaHierarchiesRowset.SchemaName,
            MdschemaHierarchiesRowset.CubeName,
            MdschemaHierarchiesRowset.DimensionUniqueName,
            MdschemaHierarchiesRowset.HierarchyName,
        })
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new MdschemaHierarchiesRowset(request, handler);
        }
    },

    /**
     * http://msdn2.microsoft.com/en-us/library/ms126038(SQL.90).aspx
     *
     * restriction
     *   CATALOG_NAME Optional.
     *   SCHEMA_NAME Optional.
     *   CUBE_NAME Optional.
     *   DIMENSION_UNIQUE_NAME Optional.
     *   HIERARCHY_UNIQUE_NAME Optional.
     *   LEVEL_NAME Optional.
     *   LEVEL_UNIQUE_NAME Optional.
     *   LEVEL_ORIGIN
     *       (Optional) A default restriction is in effect
     *       on MD_USER_DEFINED and MD_SYSTEM_ENABLED
     *   CUBE_SOURCE
     *       (Optional) A bitmap with one of the following valid values:
     *       1 CUBE
     *       2 DIMENSION
     *       Default restriction is a value of 1.
     *   LEVEL_VISIBILITY
     *       (Optional) A bitmap with one of the following values:
     *       1 Visible
     *       2 Not visible
     *       Default restriction is a value of 1.
     *
     * Not supported
     *  CUSTOM_ROLLUP_SETTINGS
     *  LEVEL_UNIQUE_SETTINGS
     *  LEVEL_ORDERING_PROPERTY
     *  LEVEL_DBTYPE
     *  LEVEL_MASTER_UNIQUE_NAME
     *  LEVEL_NAME_SQL_COLUMN_NAME Customers:(All)!NAME
     *  LEVEL_KEY_SQL_COLUMN_NAME Customers:(All)!KEY
     *  LEVEL_UNIQUE_NAME_SQL_COLUMN_NAME Customers:(All)!UNIQUE_NAME
     *  LEVEL_ATTRIBUTE_HIERARCHY_NAME
     *  LEVEL_KEY_CARDINALITY
     *  LEVEL_ORIGIN
     *
     */
    MDSCHEMA_LEVELS(
        16, null,
        new Column[] {
            MdschemaLevelsRowset.CatalogName,
            MdschemaLevelsRowset.SchemaName,
            MdschemaLevelsRowset.CubeName,
            MdschemaLevelsRowset.DimensionUniqueName,
            MdschemaLevelsRowset.HierarchyUniqueName,
            MdschemaLevelsRowset.LevelName,
            MdschemaLevelsRowset.LevelUniqueName,
            MdschemaLevelsRowset.LevelGuid,
            MdschemaLevelsRowset.LevelCaption,
            MdschemaLevelsRowset.LevelNumber,
            MdschemaLevelsRowset.LevelCardinality,
            MdschemaLevelsRowset.LevelType,
            MdschemaLevelsRowset.CustomRollupSettings,
            MdschemaLevelsRowset.LevelUniqueSettings,
            MdschemaLevelsRowset.LevelIsVisible,
            MdschemaLevelsRowset.Description,
        },
        new Column[] {
            MdschemaLevelsRowset.CatalogName,
            MdschemaLevelsRowset.SchemaName,
            MdschemaLevelsRowset.CubeName,
            MdschemaLevelsRowset.DimensionUniqueName,
            MdschemaLevelsRowset.HierarchyUniqueName,
            MdschemaLevelsRowset.LevelNumber,
        })
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new MdschemaLevelsRowset(request, handler);
        }
    },

    /**
     * http://msdn2.microsoft.com/en-us/library/ms126250(SQL.90).aspx
     *
     * restrictions
     *   CATALOG_NAME Optional.
     *   SCHEMA_NAME Optional.
     *   CUBE_NAME Optional.
     *   MEASURE_NAME Optional.
     *   MEASURE_UNIQUE_NAME Optional.
     *   CUBE_SOURCE
     *     (Optional) A bitmap with one of the following valid values:
     *     1 CUBE
     *     2 DIMENSION
     *     Default restriction is a value of 1.
     *   MEASURE_VISIBILITY
     *     (Optional) A bitmap with one of the following valid values:
     *     1 Visible
     *     2 Not Visible
     *     Default restriction is a value of 1.
     *
     * Not supported
     *  MEASURE_GUID
     *  NUMERIC_PRECISION
     *  NUMERIC_SCALE
     *  MEASURE_UNITS
     *  EXPRESSION
     *  MEASURE_NAME_SQL_COLUMN_NAME
     *  MEASURE_UNQUALIFIED_CAPTION
     *  MEASUREGROUP_NAME
     *  MEASURE_DISPLAY_FOLDER
     *  DEFAULT_FORMAT_STRING
     */
    MDSCHEMA_MEASURES(
        17, null,
        new Column[] {
            MdschemaMeasuresRowset.CatalogName,
            MdschemaMeasuresRowset.SchemaName,
            MdschemaMeasuresRowset.CubeName,
            MdschemaMeasuresRowset.MeasureName,
            MdschemaMeasuresRowset.MeasureUniqueName,
            MdschemaMeasuresRowset.MeasureCaption,
            MdschemaMeasuresRowset.MeasureGuid,
            MdschemaMeasuresRowset.MeasureAggregator,
            MdschemaMeasuresRowset.DataType,
            MdschemaMeasuresRowset.MeasureIsVisible,
            MdschemaMeasuresRowset.LevelsList,
            MdschemaMeasuresRowset.Description,
            MdschemaMeasuresRowset.FormatString,
        },
        new Column[] {
            MdschemaMeasuresRowset.CatalogName,
            MdschemaMeasuresRowset.SchemaName,
            MdschemaMeasuresRowset.CubeName,
            MdschemaMeasuresRowset.MeasureName,
        })
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new MdschemaMeasuresRowset(request, handler);
        }
    },

    /**
     *
     * http://msdn2.microsoft.com/es-es/library/ms126046.aspx
     *
     *
     * restrictions
     *   CATALOG_NAME Optional.
     *   SCHEMA_NAME Optional.
     *   CUBE_NAME Optional.
     *   DIMENSION_UNIQUE_NAME Optional.
     *   HIERARCHY_UNIQUE_NAME Optional.
     *   LEVEL_UNIQUE_NAME Optional.
     *   LEVEL_NUMBER Optional.
     *   MEMBER_NAME Optional.
     *   MEMBER_UNIQUE_NAME Optional.
     *   MEMBER_CAPTION Optional.
     *   MEMBER_TYPE Optional.
     *   TREE_OP (Optional) Only applies to a single member:
     *      MDTREEOP_ANCESTORS (0x20) returns all of the ancestors.
     *      MDTREEOP_CHILDREN (0x01) returns only the immediate children.
     *      MDTREEOP_SIBLINGS (0x02) returns members on the same level.
     *      MDTREEOP_PARENT (0x04) returns only the immediate parent.
     *      MDTREEOP_SELF (0x08) returns itself in the list of
     *                 returned rows.
     *      MDTREEOP_DESCENDANTS (0x10) returns all of the descendants.
     *   CUBE_SOURCE (Optional) A bitmap with one of the
     *      following valid values:
     *        1 CUBE
     *        2 DIMENSION
     *      Default restriction is a value of 1.
     *
     * Not supported
     */
    MDSCHEMA_MEMBERS(
        18, null,
        new Column[] {
            MdschemaMembersRowset.CatalogName,
            MdschemaMembersRowset.SchemaName,
            MdschemaMembersRowset.CubeName,
            MdschemaMembersRowset.DimensionUniqueName,
            MdschemaMembersRowset.HierarchyUniqueName,
            MdschemaMembersRowset.LevelUniqueName,
            MdschemaMembersRowset.LevelNumber,
            MdschemaMembersRowset.MemberOrdinal,
            MdschemaMembersRowset.MemberName,
            MdschemaMembersRowset.MemberUniqueName,
            MdschemaMembersRowset.MemberType,
            MdschemaMembersRowset.MemberGuid,
            MdschemaMembersRowset.MemberCaption,
            MdschemaMembersRowset.ChildrenCardinality,
            MdschemaMembersRowset.ParentLevel,
            MdschemaMembersRowset.ParentUniqueName,
            MdschemaMembersRowset.ParentCount,
            MdschemaMembersRowset.TreeOp_,
            MdschemaMembersRowset.Depth,
        },
        new Column[] {
            MdschemaMembersRowset.CatalogName,
            MdschemaMembersRowset.SchemaName,
            MdschemaMembersRowset.CubeName,
            MdschemaMembersRowset.DimensionUniqueName,
            MdschemaMembersRowset.HierarchyUniqueName,
            MdschemaMembersRowset.LevelUniqueName,
            MdschemaMembersRowset.LevelNumber,
            MdschemaMembersRowset.MemberOrdinal,
        })
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new MdschemaMembersRowset(request, handler);
        }
    },

    /**
     * http://msdn2.microsoft.com/en-us/library/ms126309(SQL.90).aspx
     *
     * restrictions
     *    CATALOG_NAME Mandatory
     *    SCHEMA_NAME Optional
     *    CUBE_NAME Optional
     *    DIMENSION_UNIQUE_NAME Optional
     *    HIERARCHY_UNIQUE_NAME Optional
     *    LEVEL_UNIQUE_NAME Optional
     *
     *    MEMBER_UNIQUE_NAME Optional
     *    PROPERTY_NAME Optional
     *    PROPERTY_TYPE Optional
     *    PROPERTY_CONTENT_TYPE
     *       (Optional) A default restriction is in place on MDPROP_MEMBER
     *       OR MDPROP_CELL.
     *    PROPERTY_ORIGIN
     *       (Optional) A default restriction is in place on MD_USER_DEFINED
     *       OR MD_SYSTEM_ENABLED
     *    CUBE_SOURCE
     *       (Optional) A bitmap with one of the following valid values:
     *       1 CUBE
     *       2 DIMENSION
     *       Default restriction is a value of 1.
     *    PROPERTY_VISIBILITY
     *       (Optional) A bitmap with one of the following valid values:
     *       1 Visible
     *       2 Not visible
     *       Default restriction is a value of 1.
     *
     * Not supported
     *    PROPERTY_ORIGIN
     *    CUBE_SOURCE
     *    PROPERTY_VISIBILITY
     *    CHARACTER_MAXIMUM_LENGTH
     *    CHARACTER_OCTET_LENGTH
     *    NUMERIC_PRECISION
     *    NUMERIC_SCALE
     *    DESCRIPTION
     *    SQL_COLUMN_NAME
     *    LANGUAGE
     *    PROPERTY_ATTRIBUTE_HIERARCHY_NAME
     *    PROPERTY_CARDINALITY
     *    MIME_TYPE
     *    PROPERTY_IS_VISIBLE
     */
    MDSCHEMA_PROPERTIES(
        19, null,
        new Column[] {
            MdschemaPropertiesRowset.CatalogName,
            MdschemaPropertiesRowset.SchemaName,
            MdschemaPropertiesRowset.CubeName,
            MdschemaPropertiesRowset.DimensionUniqueName,
            MdschemaPropertiesRowset.HierarchyUniqueName,
            MdschemaPropertiesRowset.LevelUniqueName,
            MdschemaPropertiesRowset.MemberUniqueName,
            MdschemaPropertiesRowset.PropertyName,
            MdschemaPropertiesRowset.PropertyCaption,
            MdschemaPropertiesRowset.PropertyType,
            MdschemaPropertiesRowset.DataType,
            MdschemaPropertiesRowset.PropertyContentType,
            MdschemaPropertiesRowset.Description
        },
        null /* not sorted */)
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new MdschemaPropertiesRowset(request, handler);
        }
    },

    /**
     * http://msdn2.microsoft.com/en-us/library/ms126290(SQL.90).aspx
     *
     * restrictions
     *    CATALOG_NAME Optional.
     *    SCHEMA_NAME Optional.
     *    CUBE_NAME Optional.
     *    SET_NAME Optional.
     *    SCOPE Optional.
     *    HIERARCHY_UNIQUE_NAME Optional.
     *    CUBE_SOURCE Optional.
     *        Note: Only one hierarchy can be included, and only those named
     *        sets whose hierarchies exactly match the restriction are
     *        returned.
     *
     * Not supported
     *    EXPRESSION
     *    DIMENSIONS
     *    SET_DISPLAY_FOLDER
     */
    MDSCHEMA_SETS(
        20, null,
        new Column[] {
            MdschemaSetsRowset.CatalogName,
            MdschemaSetsRowset.SchemaName,
            MdschemaSetsRowset.CubeName,
            MdschemaSetsRowset.SetName,
            MdschemaSetsRowset.Scope,
        },
        new Column[] {
            MdschemaSetsRowset.CatalogName,
            MdschemaSetsRowset.SchemaName,
            MdschemaSetsRowset.CubeName,
        })
    {
        public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
            return new MdschemaSetsRowset(request, handler);
        }
    };

    private static final boolean EMIT_INVISIBLE_MEMBERS = true;

    transient final Column[] columnDefinitions;
    transient final Column[] sortColumnDefinitions;

    /**
     * Date the schema was last modified.
     *
     * <p>TODO: currently schema grammar does not support modify date
     * so we return just some date for now.
     */
    private static final String dateModified = "2005-01-25T17:35:32";
    private final String description;

    static final String UUID_PATTERN =
        "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
        + "[0-9a-fA-F]{12}";

    /**
     * Creates a rowset definition.
     *
     * @param ordinal Rowset ordinal, per OLE DB for OLAP
     * @param description Description
     * @param columnDefinitions List of column definitions
     * @param sortColumnDefinitions List of column definitions to sort on,
     */
    RowsetDefinition(
        int ordinal,
        String description,
        Column[] columnDefinitions,
        Column[] sortColumnDefinitions)
    {
        Util.discard(ordinal);
        this.description = description;
        this.columnDefinitions = columnDefinitions;
        this.sortColumnDefinitions = sortColumnDefinitions;
    }

    public abstract Rowset getRowset(XmlaRequest request, XmlaHandler handler);

    public Column lookupColumn(String name) {
        for (Column columnDefinition : columnDefinitions) {
            if (columnDefinition.name.equals(name)) {
                return columnDefinition;
            }
        }
        return null;
    }

    /**
     * Returns a comparator with which to sort rows of this rowset definition.
     * The sort order is defined by the {@link #sortColumnDefinitions} field.
     * If the rowset is not sorted, returns null.
     */
    Comparator<Rowset.Row> getComparator() {
        if (sortColumnDefinitions == null) {
            return null;
        }
        return new Comparator<Rowset.Row>() {
            public int compare(Rowset.Row row1, Rowset.Row row2) {
                // A faster implementation is welcome.
                for (Column sortColumn : sortColumnDefinitions) {
                    Comparable val1 = (Comparable) row1.get(sortColumn.name);
                    Comparable val2 = (Comparable) row2.get(sortColumn.name);
                    if ((val1 == null) && (val2 == null)) {
                        // columns can be optional, compare next column
                        continue;
                    } else if (val1 == null) {
                        return -1;
                    } else if (val2 == null) {
                        return 1;
                    } else if (val1 instanceof String
                       && val2 instanceof String)
                    {
                        int v =
                            ((String) val1).compareToIgnoreCase((String) val2);
                        // if equal (= 0), compare next column
                        if (v != 0) {
                            return v;
                        }
                    } else {
                        int v = val1.compareTo(val2);
                        // if equal (= 0), compare next column
                        if (v != 0) {
                            return v;
                        }
                    }
                }
                return 0;
            }
        };
    }

    /**
     * Generates an XML schema description to the writer.
     * This is broken into top, Row definition and bottom so that on a
     * case by case basis a RowsetDefinition can redefine the Row
     * definition output. The default assumes a flat set of elements, but
     * for example, SchemaRowsets has a element with child elements.
     *
     * @param writer SAX writer
     * @see XmlaHandler#writeDatasetXmlSchema(SaxWriter, mondrian.xmla.XmlaHandler.SetType)
     */
    void writeRowsetXmlSchema(SaxWriter writer) {
        writeRowsetXmlSchemaTop(writer);
        writeRowsetXmlSchemaRowDef(writer);
        writeRowsetXmlSchemaBottom(writer);
    }

    protected void writeRowsetXmlSchemaTop(SaxWriter writer) {
        writer.startElement(
            "xsd:schema",
            "xmlns:xsd", NS_XSD,
            "xmlns", NS_XMLA_ROWSET,
            "xmlns:xsi", NS_XSI,
            "xmlns:sql", "urn:schemas-microsoft-com:xml-sql",
            "targetNamespace", NS_XMLA_ROWSET,
            "elementFormDefault", "qualified");

        writer.startElement(
            "xsd:element",
            "name", "root");
        writer.startElement("xsd:complexType");
        writer.startElement("xsd:sequence");
        writer.element(
            "xsd:element",
            "name", "row",
            "type", "row",
            "minOccurs", 0,
            "maxOccurs", "unbounded");
        writer.endElement(); // xsd:sequence
        writer.endElement(); // xsd:complexType
        writer.endElement(); // xsd:element

        // MS SQL includes this in its schema section even thought
        // its not need for most queries.
        writer.startElement(
            "xsd:simpleType",
            "name", "uuid");
        writer.startElement(
            "xsd:restriction",
            "base", "xsd:string");
        writer.element(
            "xsd:pattern",
            "value", UUID_PATTERN);

        writer.endElement(); // xsd:restriction
        writer.endElement(); // xsd:simpleType
    }

    protected void writeRowsetXmlSchemaRowDef(SaxWriter writer) {
        writer.startElement(
            "xsd:complexType",
            "name", "row");
        writer.startElement("xsd:sequence");
        for (Column column : columnDefinitions) {
            final String name = XmlaUtil.encodeElementName(column.name);
            final String xsdType = column.type.columnType;

            Object[] attrs;
            if (column.nullable) {
                if (column.unbounded) {
                    attrs = new Object[]{
                        "sql:field", column.name,
                        "name", name,
                        "type", xsdType,
                        "minOccurs", 0,
                        "maxOccurs", "unbounded"
                    };
                } else {
                    attrs = new Object[]{
                        "sql:field", column.name,
                        "name", name,
                        "type", xsdType,
                        "minOccurs", 0
                    };
                }
            } else {
                if (column.unbounded) {
                    attrs = new Object[]{
                        "sql:field", column.name,
                        "name", name,
                        "type", xsdType,
                        "maxOccurs", "unbounded"
                    };
                } else {
                    attrs = new Object[]{
                        "sql:field", column.name,
                        "name", name,
                        "type", xsdType
                    };
                }
            }
            writer.element("xsd:element", attrs);
        }
        writer.endElement(); // xsd:sequence
        writer.endElement(); // xsd:complexType
    }

    protected void writeRowsetXmlSchemaBottom(SaxWriter writer) {
        writer.endElement(); // xsd:schema
    }

    enum Type {
        String("xsd:string"),
        StringArray("xsd:string"),
        Array("xsd:string"),
        Enumeration("xsd:string"),
        EnumerationArray("xsd:string"),
        EnumString("xsd:string"),
        Boolean("xsd:boolean"),
        StringSometimesArray("xsd:string"),
        Integer("xsd:int"),
        UnsignedInteger("xsd:unsignedInt"),
        DateTime("xsd:dateTime"),
        Rowset(null),
        Short("xsd:short"),
        UUID("uuid"),
        UnsignedShort("xsd:unsignedShort"),
        Long("xsd:long"),
        UnsignedLong("xsd:unsignedLong");

        public final String columnType;

        Type(String columnType) {
            this.columnType = columnType;
        }

        boolean isEnum() {
            return this == Enumeration
               || this == EnumerationArray
               || this == EnumString;
        }

        String getName() {
            return this == String ? "string" : name();
        }
    }

    private static XmlaConstants.DBType getDBTypeFromProperty(Property prop) {
        switch (prop.getType()) {
        case TYPE_STRING:
            return XmlaConstants.DBType.WSTR;
        case TYPE_NUMERIC:
            return XmlaConstants.DBType.R8;
        case TYPE_BOOLEAN:
            return XmlaConstants.DBType.BOOL;
        case TYPE_OTHER:
        default:
            // TODO: what type is it really, its not a string
            return XmlaConstants.DBType.WSTR;
        }
    }

    static class Column {

        /**
         * This is used as the true value for the restriction parameter.
         */
        static final boolean RESTRICTION = true;
        /**
         * This is used as the false value for the restriction parameter.
         */
        static final boolean NOT_RESTRICTION = false;

        /**
         * This is used as the false value for the nullable parameter.
         */
        static final boolean REQUIRED = false;
        /**
         * This is used as the true value for the nullable parameter.
         */
        static final boolean OPTIONAL = true;

        /**
         * This is used as the false value for the unbounded parameter.
         */
        static final boolean ONE_MAX = false;
        /**
         * This is used as the true value for the unbounded parameter.
         */
        static final boolean UNBOUNDED = true;

        final String name;
        final Type type;
        final Enumeration enumeration;
        final String description;
        final boolean restriction;
        final boolean nullable;
        final boolean unbounded;

        /**
         * Creates a column.
         *
         * @param name Name of column
         * @param type A {@link mondrian.xmla.RowsetDefinition.Type} value
         * @param enumeratedType Must be specified for enumeration or array
         *                       of enumerations
         * @param description Description of column
         * @param restriction Whether column can be used as a filter on its
         *     rowset
         * @param nullable Whether column can contain null values
         * @pre type != null
         * @pre (type == Type.Enumeration
         *  || type == Type.EnumerationArray
         *  || type == Type.EnumString)
         *  == (enumeratedType != null)
         * @pre description == null || description.indexOf('\r') == -1
         */
        Column(
            String name,
            Type type,
            Enumeration enumeratedType,
            boolean restriction,
            boolean nullable,
            String description)
        {
            this(
                name, type, enumeratedType,
                restriction, nullable, ONE_MAX, description);
        }

        Column(
            String name,
            Type type,
            Enumeration enumeratedType,
            boolean restriction,
            boolean nullable,
            boolean unbounded,
            String description)
        {
            assert type != null;
            assert (type == Type.Enumeration
                    || type == Type.EnumerationArray
                    || type == Type.EnumString)
                   == (enumeratedType != null);
            // Line endings must be UNIX style (LF) not Windows style (LF+CR).
            // Thus the client will receive the same XML, regardless
            // of the server O/S.
            assert description == null || description.indexOf('\r') == -1;
            this.name = name;
            this.type = type;
            this.enumeration = enumeratedType;
            this.description = description;
            this.restriction = restriction;
            this.nullable = nullable;
            this.unbounded = unbounded;
        }

        /**
         * Retrieves a value of this column from a row. The base implementation
         * uses reflection to call an accessor method; a derived class may
         * provide a different implementation.
         *
         * @param row Row
         */
        protected Object get(Object row) {
            return getFromAccessor(row);
        }

        /**
         * Retrieves the value of this column "MyColumn" from a field called
         * "myColumn".
         *
         * @param row Current row
         * @return Value of given this property of the given row
         */
        protected final Object getFromField(Object row) {
            try {
                String javaFieldName =
                    name.substring(0, 1).toLowerCase()
                    + name.substring(1);
                Field field = row.getClass().getField(javaFieldName);
                return field.get(row);
            } catch (NoSuchFieldException e) {
                throw Util.newInternal(
                    e, "Error while accessing rowset column " + name);
            } catch (SecurityException e) {
                throw Util.newInternal(
                    e, "Error while accessing rowset column " + name);
            } catch (IllegalAccessException e) {
                throw Util.newInternal(
                    e, "Error while accessing rowset column " + name);
            }
        }

        /**
         * Retrieves the value of this column "MyColumn" by calling a method
         * called "getMyColumn()".
         *
         * @param row Current row
         * @return Value of given this property of the given row
         */
        protected final Object getFromAccessor(Object row) {
            try {
                String javaMethodName = "get" + name;
                Method method = row.getClass().getMethod(javaMethodName);
                return method.invoke(row);
            } catch (SecurityException e) {
                throw Util.newInternal(
                    e, "Error while accessing rowset column " + name);
            } catch (IllegalAccessException e) {
                throw Util.newInternal(
                    e, "Error while accessing rowset column " + name);
            } catch (NoSuchMethodException e) {
                throw Util.newInternal(
                    e, "Error while accessing rowset column " + name);
            } catch (InvocationTargetException e) {
                throw Util.newInternal(
                    e, "Error while accessing rowset column " + name);
            }
        }

        public String getColumnType() {
            if (type.isEnum()) {
                return enumeration.type.columnType;
            }
            return type.columnType;
        }
    }

    // -------------------------------------------------------------------------
    // From this point on, just rowset classess.

    static class DiscoverDatasourcesRowset extends Rowset {
        private static final Column DataSourceName =
            new Column(
                "DataSourceName",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the data source, such as FoodMart 2000.");
        private static final Column DataSourceDescription =
            new Column(
                "DataSourceDescription",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A description of the data source, as entered by the "
                + "publisher.");
        private static final Column URL =
            new Column(
                "URL",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The unique path that shows where to invoke the XML for "
                + "Analysis methods for that data source.");
        private static final Column DataSourceInfo =
            new Column(
                "DataSourceInfo",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A string containing any additional information required to "
                + "connect to the data source. This can include the Initial "
                + "Catalog property or other information for the provider.\n"
                + "Example: \"Provider=MSOLAP;Data Source=Local;\"");
        private static final Column ProviderName =
            new Column(
                "ProviderName",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the provider behind the data source.\n"
                + "Example: \"MSDASQL\"");
        private static final Column ProviderType =
            new Column(
                "ProviderType",
                Type.EnumerationArray,
                Enumeration.PROVIDER_TYPE,
                Column.RESTRICTION,
                Column.REQUIRED,
                Column.UNBOUNDED,
                "The types of data supported by the provider. May include one "
                + "or more of the following types. Example follows this "
                + "table.\n"
                + "TDP: tabular data provider.\n"
                + "MDP: multidimensional data provider.\n"
                + "DMP: data mining provider. A DMP provider implements the "
                + "OLE DB for Data Mining specification.");
        private static final Column AuthenticationMode =
            new Column(
                "AuthenticationMode",
                Type.EnumString,
                Enumeration.AUTHENTICATION_MODE,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Specification of what type of security mode the data source "
                + "uses. Values can be one of the following:\n"
                + "Unauthenticated: no user ID or password needs to be sent.\n"
                + "Authenticated: User ID and Password must be included in the "
                + "information required for the connection.\n"
                + "Integrated: the data source uses the underlying security to "
                + "determine authorization, such as Integrated Security "
                + "provided by Microsoft Internet Information Services (IIS).");

        public DiscoverDatasourcesRowset(
            XmlaRequest request, XmlaHandler handler)
        {
            super(DISCOVER_DATASOURCES, request, handler);
        }

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            for (DataSourcesConfig.DataSource ds
                : handler.getDataSourceEntries().values())
            {
                Row row = new Row();
                row.set(DataSourceName.name, ds.getDataSourceName());
                row.set(
                    DataSourceDescription.name,
                    ds.getDataSourceDescription());
                row.set(URL.name, ds.getURL());
                row.set(DataSourceInfo.name, ds.getDataSourceName());
                row.set(ProviderName.name, ds.getProviderName());
                row.set(ProviderType.name, ds.getProviderType());
                row.set(AuthenticationMode.name, ds.getAuthenticationMode());
                addRow(row, rows);
            }
        }

        protected void setProperty(
            PropertyDefinition propertyDef,
            String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DiscoverSchemaRowsetsRowset extends Rowset {
        private static final Column SchemaName =
            new Column(
                "SchemaName",
                Type.StringArray,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the schema/request. This returns the values in "
                + "the RequestTypes enumeration, plus any additional types "
                + "supported by the provider. The provider defines rowset "
                + "structures for the additional types");
        private static final Column SchemaGuid =
            new Column(
                "SchemaGuid",
                Type.UUID,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The GUID of the schema.");
        private static final Column Restrictions =
            new Column(
                "Restrictions",
                Type.Array,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "An array of the restrictions suppoted by provider. An example "
                + "follows this table.");
        private static final Column Description =
            new Column(
                "Description",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A localizable description of the schema");

        public DiscoverSchemaRowsetsRowset(
            XmlaRequest request, XmlaHandler handler)
        {
            super(DISCOVER_SCHEMA_ROWSETS, request, handler);
        }

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            RowsetDefinition[] rowsetDefinitions =
                RowsetDefinition.class.getEnumConstants().clone();
            Arrays.sort(
                rowsetDefinitions,
                new Comparator<RowsetDefinition>() {
                    public int compare(
                        RowsetDefinition o1,
                        RowsetDefinition o2)
                    {
                        return o1.name().compareTo(o2.name());
                    }
                });
            for (RowsetDefinition rowsetDefinition : rowsetDefinitions) {
                Row row = new Row();
                row.set(SchemaName.name, rowsetDefinition.name());

                // TODO: If we have a SchemaGuid output here
                //row.set(SchemaGuid.name, "");

                row.set(Restrictions.name, getRestrictions(rowsetDefinition));

                String desc = rowsetDefinition.getDescription();
                row.set(Description.name, (desc == null) ? "" : desc);
                addRow(row, rows);
            }
        }

        private List<XmlElement> getRestrictions(
            RowsetDefinition rowsetDefinition)
        {
            List<XmlElement> restrictionList = new ArrayList<XmlElement>();
            final Column[] columns = rowsetDefinition.columnDefinitions;
            for (Column column : columns) {
                if (column.restriction) {
                    restrictionList.add(
                        new XmlElement(
                            Restrictions.name,
                            null,
                            new XmlElement[]{
                                new XmlElement("Name", null, column.name),
                                new XmlElement(
                                    "Type",
                                    null,
                                    column.getColumnType())}));
                }
            }
            return restrictionList;
        }

        protected void setProperty(
            PropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    public String getDescription() {
        return description;
    }

    static class DiscoverPropertiesRowset extends Rowset {
        private final RestrictionTest propertyNameRT;

        DiscoverPropertiesRowset(XmlaRequest request, XmlaHandler handler) {
            super(DISCOVER_PROPERTIES, request, handler);
            propertyNameRT = getRestrictionTest(PropertyName);
        }

        private static final Column PropertyName =
            new Column(
                "PropertyName",
                Type.StringSometimesArray,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the property.");
        private static final Column PropertyDescription =
            new Column(
                "PropertyDescription",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A localizable text description of the property.");
        private static final Column PropertyType =
            new Column(
                "PropertyType",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The XML data type of the property.");
        private static final Column PropertyAccessType =
            new Column(
                "PropertyAccessType",
                Type.EnumString,
                Enumeration.ACCESS,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Access for the property. The value can be Read, Write, or "
                + "ReadWrite.");
        private static final Column IsRequired =
            new Column(
                "IsRequired",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "True if a property is required, false if it is not required.");
        private static final Column Value =
            new Column(
                "Value",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The current value of the property.");

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            for (PropertyDefinition propertyDefinition
                : PropertyDefinition.class.getEnumConstants())
            {
                if (!propertyNameRT.passes(propertyDefinition.name())) {
                    continue;
                }
                Row row = new Row();
                row.set(PropertyName.name, propertyDefinition.name());
                row.set(
                    PropertyDescription.name, propertyDefinition.description);
                row.set(PropertyType.name, propertyDefinition.type.getName());
                row.set(PropertyAccessType.name, propertyDefinition.access);
                row.set(IsRequired.name, false);
                row.set(Value.name, propertyDefinition.value);
                addRow(row, rows);
            }
        }

        protected void setProperty(
            PropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DiscoverEnumeratorsRowset extends Rowset {
        DiscoverEnumeratorsRowset(XmlaRequest request, XmlaHandler handler) {
            super(DISCOVER_ENUMERATORS, request, handler);
        }

        private static final Column EnumName =
            new Column(
                "EnumName",
                Type.StringArray,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the enumerator that contains a set of values.");
        private static final Column EnumDescription =
            new Column(
                "EnumDescription",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A localizable description of the enumerator.");
        private static final Column EnumType =
            new Column(
                "EnumType",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The data type of the Enum values.");
        private static final Column ElementName =
            new Column(
                "ElementName",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The name of one of the value elements in the enumerator set.\n"
                + "Example: TDP");
        private static final Column ElementDescription =
            new Column(
                "ElementDescription",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A localizable description of the element (optional).");
        private static final Column ElementValue =
            new Column(
                "ElementValue",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The value of the element.\n" + "Example: 01");

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            List<Enumeration> enumerators = getEnumerators();
            for (Enumeration enumerator : enumerators) {
                final List<? extends Enum> values = enumerator.getValues();
                for (Enum<?> value : values) {
                    Row row = new Row();
                    row.set(EnumName.name, enumerator.name);
                    row.set(EnumDescription.name, enumerator.description);

                    // Note: SQL Server always has EnumType string
                    // Need type of element of array, not the array
                    // it self.
                    row.set(EnumType.name, "string");

                    final String name =
                        (value instanceof XmlaConstant)
                            ? ((XmlaConstant) value).xmlaName()
                            : value.name();
                    row.set(ElementName.name, name);

                    final String description =
                     (value instanceof XmlaConstant)
                        ? ((XmlaConstant) value).getDescription()
                         : (value instanceof XmlaConstants.EnumWithDesc)
                        ? ((XmlaConstants.EnumWithDesc) value).getDescription()
                             : null;
                    if (description != null) {
                        row.set(
                            ElementDescription.name,
                            description);
                    }

                    switch (enumerator.type) {
                    case String:
                    case StringArray:
                        // these don't have ordinals
                        break;
                    default:
                        final int ordinal =
                            (value instanceof XmlaConstant
                             && ((XmlaConstant) value).xmlaOrdinal() != -1)
                                ? ((XmlaConstant) value).xmlaOrdinal()
                                : value.ordinal();
                        row.set(ElementValue.name, ordinal);
                        break;
                    }
                    addRow(row, rows);
                }
            }
        }

        private static List<Enumeration> getEnumerators() {
            SortedSet<Enumeration> enumeratorSet = new TreeSet<Enumeration>(
                new Comparator<Enumeration>() {
                    public int compare(Enumeration o1, Enumeration o2) {
                        return o1.name.compareTo(o2.name);
                    }
                }
            );
            for (RowsetDefinition rowsetDefinition
                : RowsetDefinition.class.getEnumConstants())
            {
                for (Column column : rowsetDefinition.columnDefinitions) {
                    if (column.enumeration != null) {
                        enumeratorSet.add(column.enumeration);
                    }
                }
            }
            return new ArrayList<Enumeration>(enumeratorSet);
        }

        protected void setProperty(
            PropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DiscoverKeywordsRowset extends Rowset {
        DiscoverKeywordsRowset(XmlaRequest request, XmlaHandler handler) {
            super(DISCOVER_KEYWORDS, request, handler);
        }

        private static final Column Keyword =
            new Column(
                "Keyword",
                Type.StringSometimesArray,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "A list of all the keywords reserved by a provider.\n"
                + "Example: AND");

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            MondrianServer mondrianServer = MondrianServer.forConnection(null);
            for (String keyword : mondrianServer.getKeywords()) {
                Row row = new Row();
                row.set(Keyword.name, keyword);
                addRow(row, rows);
            }
        }

        protected void setProperty(
            PropertyDefinition propertyDef,
            String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DiscoverLiteralsRowset extends Rowset {
        DiscoverLiteralsRowset(XmlaRequest request, XmlaHandler handler) {
            super(DISCOVER_LITERALS, request, handler);
        }

        private static final Column LiteralName = new Column(
            "LiteralName",
            Type.StringSometimesArray,
            null,
            Column.RESTRICTION,
            Column.REQUIRED,
            "The name of the literal described in the row.\n"
            + "Example: DBLITERAL_LIKE_PERCENT");

        private static final Column LiteralValue = new Column(
            "LiteralValue",
            Type.String,
            null,
            Column.NOT_RESTRICTION,
            Column.OPTIONAL,
            "Contains the actual literal value.\n"
            + "Example, if LiteralName is DBLITERAL_LIKE_PERCENT and the "
            + "percent character (%) is used to match zero or more characters "
            + "in a LIKE clause, this column's value would be \"%\".");

        private static final Column LiteralInvalidChars = new Column(
            "LiteralInvalidChars",
            Type.String,
            null,
            Column.NOT_RESTRICTION,
            Column.OPTIONAL,
            "The characters, in the literal, that are not valid.\n"
            + "For example, if table names can contain anything other than a "
            + "numeric character, this string would be \"0123456789\".");

        private static final Column LiteralInvalidStartingChars = new Column(
            "LiteralInvalidStartingChars",
            Type.String,
            null,
            Column.NOT_RESTRICTION,
            Column.OPTIONAL,
            "The characters that are not valid as the first character of the "
            + "literal. If the literal can start with any valid character, "
            + "this is null.");

        private static final Column LiteralMaxLength = new Column(
            "LiteralMaxLength",
            Type.Integer,
            null,
            Column.NOT_RESTRICTION,
            Column.OPTIONAL,
            "The maximum number of characters in the literal. If there is no "
            + "maximum or the maximum is unknown, the value is ?1.");

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            populate(
                XmlaConstants.Literal.class,
                rows,
                new Comparator<XmlaConstants.Literal>() {
                public int compare(
                    XmlaConstants.Literal o1,
                    XmlaConstants.Literal o2)
                {
                    return o1.name().compareTo(o2.name());
                }
            });
        }

        protected void setProperty(
            PropertyDefinition propertyDef,
            String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DbschemaCatalogsRowset extends Rowset {
        private final RestrictionTest catalogNameRT;

        DbschemaCatalogsRowset(XmlaRequest request, XmlaHandler handler) {
            super(DBSCHEMA_CATALOGS, request, handler);
            catalogNameRT = getRestrictionTest(CatalogName);
        }

        private static final Column CatalogName =
            new Column(
                "CATALOG_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Catalog name. Cannot be NULL.");
        private static final Column Description =
            new Column(
                "DESCRIPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Human-readable description of the catalog.");
        private static final Column Roles =
            new Column(
                "ROLES",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A comma delimited list of roles to which the current user "
                + "belongs. An asterisk (*) is included as a role if the "
                + "current user is a server or database administrator. "
                + "Username is appended to ROLES if one of the roles uses "
                + "dynamic security.");
        private static final Column DateModified =
            new Column(
                "DATE_MODIFIED",
                Type.DateTime,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The date that the catalog was last modified.");

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            DataSourcesConfig.DataSource ds = handler.getDataSource(request);
            DataSourcesConfig.Catalog[] catalogs = ds.catalogs.catalogs;
            String roleName = request.getRoleName();
            Role role = request.getRole();

            for (DataSourcesConfig.Catalog dsCatalog : catalogs) {
                if (dsCatalog == null || dsCatalog.definition == null) {
                    continue;
                }
                Connection connection =
                    handler.getConnection(dsCatalog, role, roleName);
                if (connection == null) {
                    continue;
                }
                if (!catalogNameRT.passes(dsCatalog.name)) {
                    continue;
                }
                final Schema schema = connection.getSchema();

                Row row = new Row();
                row.set(CatalogName.name, dsCatalog.name);

                // TODO: currently schema grammar does not support a description
                row.set(Description.name, "No description available");

                // get Role names
                // TODO: this returns ALL roles, no the current user's roles
                StringBuilder buf = new StringBuilder(100);
                serialize(buf, ((RolapSchema) schema).roleNames());
                row.set(Roles.name, buf.toString());

                // TODO: currently schema grammar does not support modify date
                // so we return just some date for now.
                if (false) {
                    row.set(DateModified.name, dateModified);
                }
                addRow(row, rows);
            }
        }

        protected void setProperty(
            PropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DbschemaColumnsRowset extends Rowset {
        private final RestrictionTest tableCatalogRT;
        private final RestrictionTest tableNameRT;
        private final RestrictionTest columnNameRT;

        DbschemaColumnsRowset(XmlaRequest request, XmlaHandler handler) {
            super(DBSCHEMA_COLUMNS, request, handler);
            tableCatalogRT = getRestrictionTest(TableCatalog);
            tableNameRT = getRestrictionTest(TableName);
            columnNameRT = getRestrictionTest(ColumnName);
        }

        private static final Column TableCatalog =
            new Column(
                "TABLE_CATALOG",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the Database.");
        private static final Column TableSchema =
            new Column(
                "TABLE_SCHEMA",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                null);
        private static final Column TableName =
            new Column(
                "TABLE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the cube.");
        private static final Column ColumnName =
            new Column(
                "COLUMN_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the attribute hierarchy or measure.");
        private static final Column OrdinalPosition =
            new Column(
                "ORDINAL_POSITION",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The position of the column, beginning with 1.");
        private static final Column ColumnHasDefault =
            new Column(
                "COLUMN_HAS_DEFAULT",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Not supported.");
        /*
         *  A bitmask indicating the information stored in
         *      DBCOLUMNFLAGS in OLE DB.
         *  1 = Bookmark
         *  2 = Fixed length
         *  4 = Nullable
         *  8 = Row versioning
         *  16 = Updateable column
         *
         * And, of course, MS SQL Server sometimes has the value of 80!!
        */
        private static final Column ColumnFlags =
            new Column(
                "COLUMN_FLAGS",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A DBCOLUMNFLAGS bitmask indicating column properties.");
        private static final Column IsNullable =
            new Column(
                "IS_NULLABLE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Always returns false.");
        private static final Column DataType =
            new Column(
                "DATA_TYPE",
                Type.UnsignedShort,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The data type of the column. Returns a string for dimension "
                + "columns and a variant for measures.");
        private static final Column CharacterMaximumLength =
            new Column(
                "CHARACTER_MAXIMUM_LENGTH",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The maximum possible length of a value within the column.");
        private static final Column CharacterOctetLength =
            new Column(
                "CHARACTER_OCTET_LENGTH",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The maximum possible length of a value within the column, in "
                + "bytes, for character or binary columns.");
        private static final Column NumericPrecision =
            new Column(
                "NUMERIC_PRECISION",
                Type.UnsignedShort,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The maximum precision of the column for numeric data types "
                + "other than DBTYPE_VARNUMERIC.");
        private static final Column NumericScale =
            new Column(
                "NUMERIC_SCALE",
                Type.Short,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The number of digits to the right of the decimal point for "
                + "DBTYPE_DECIMAL, DBTYPE_NUMERIC, DBTYPE_VARNUMERIC. "
                + "Otherwise, this is NULL.");

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            DataSourcesConfig.DataSource ds = handler.getDataSource(request);
            DataSourcesConfig.Catalog[] catalogs = ds.catalogs.catalogs;
            String roleName = request.getRoleName();
            Role role = request.getRole();

            for (DataSourcesConfig.Catalog dsCatalog : catalogs) {
                if (dsCatalog == null || dsCatalog.definition == null) {
                    continue;
                }
                Connection connection =
                    handler.getConnection(dsCatalog, role, roleName);
                if (connection == null) {
                    continue;
                }
                final Schema schema = connection.getSchema();
                String catalogName = dsCatalog.name;
                if (!tableCatalogRT.passes(catalogName)) {
                    continue;
                }

                int ordinalPosition = 1;
                Row row;

                for (Cube cube1 : sortedCubes(schema)) {
                    RolapCube cube = (RolapCube) cube1;
                    SchemaReader schemaReader =
                        cube.getSchemaReader(
                            connection.getRole());
                    String cubeName = cube.getName();
                    if (!tableNameRT.passes(cubeName)) {
                        continue;
                    }
                    for (Dimension dimension : cube.getDimensions()) {
                        Hierarchy[] hierarchies = dimension.getHierarchies();
                        for (Hierarchy hierarchy : hierarchies) {
                            ordinalPosition = populateHierarchy(
                                schemaReader, cube, (HierarchyBase) hierarchy,
                                ordinalPosition, rows);
                        }
                    }

                    List<RolapMember> rms = cube.getMeasuresMembers();
                    for (int k = 1; k < rms.size(); k++) {
                        RolapMember member = rms.get(k);

                        // null == true for regular cubes
                        // virtual cubes do not set the visible property
                        // on its measures so it might be null.
                        Boolean visible = (Boolean)
                            member.getPropertyValue(Property.VISIBLE.name);
                        if (visible == null) {
                            visible = true;
                        }
                        if (!EMIT_INVISIBLE_MEMBERS && !visible) {
                            continue;
                        }

                        String memberName = member.getName();
                        if (!columnNameRT.passes("Measures:" + memberName)) {
                            continue;
                        }

                        row = new Row();
                        row.set(TableCatalog.name, catalogName);
                        row.set(TableName.name, cubeName);
                        row.set(ColumnName.name, "Measures:" + memberName);
                        row.set(OrdinalPosition.name, ordinalPosition++);
                        row.set(ColumnHasDefault.name, false);
                        row.set(ColumnFlags.name, 0);
                        row.set(IsNullable.name, false);
                        // TODO: here is where one tries to determine the
                        // type of the column - since these are all
                        // Measures, aggregate Measures??, maybe they
                        // are all numeric? (or currency)
                        row.set(
                            DataType.name,
                            XmlaConstants.DBType.R8.xmlaOrdinal());
                        // TODO: 16/255 seems to be what MS SQL Server
                        // always returns.
                        row.set(NumericPrecision.name, 16);
                        row.set(NumericScale.name, 255);
                        addRow(row, rows);
                    }
                }
            }
        }

        private int populateHierarchy(
            SchemaReader schemaReader,
            RolapCube cube,
            HierarchyBase hierarchy,
            int ordinalPosition,
            List<Row> rows)
        {
            // Access control
            if (!canAccess(schemaReader, hierarchy)) {
                return ordinalPosition;
            }
            String schemaName = cube.getSchema().getName();
            String cubeName = cube.getName();
            String hierarchyName = hierarchy.getName();

            if (hierarchy.hasAll()) {
                Row row = new Row();
                row.set(TableCatalog.name, schemaName);
                row.set(TableName.name, cubeName);
                row.set(ColumnName.name, hierarchyName + ":(All)!NAME");
                row.set(OrdinalPosition.name, ordinalPosition++);
                row.set(ColumnHasDefault.name, false);
                row.set(ColumnFlags.name, 0);
                row.set(IsNullable.name, false);
                // names are always WSTR
                row.set(DataType.name, XmlaConstants.DBType.WSTR.xmlaOrdinal());
                row.set(CharacterMaximumLength.name, 0);
                row.set(CharacterOctetLength.name, 0);
                addRow(row, rows);

                row = new Row();
                row.set(TableCatalog.name, schemaName);
                row.set(TableName.name, cubeName);
                row.set(ColumnName.name, hierarchyName + ":(All)!UNIQUE_NAME");
                row.set(OrdinalPosition.name, ordinalPosition++);
                row.set(ColumnHasDefault.name, false);
                row.set(ColumnFlags.name, 0);
                row.set(IsNullable.name, false);
                // names are always WSTR
                row.set(DataType.name, XmlaConstants.DBType.WSTR.xmlaOrdinal());
                row.set(CharacterMaximumLength.name, 0);
                row.set(CharacterOctetLength.name, 0);
                addRow(row, rows);

                if (false) {
                    // TODO: SQLServer outputs this hasall KEY column name -
                    // don't know what it's for
                    row = new Row();
                    row.set(TableCatalog.name, schemaName);
                    row.set(TableName.name, cubeName);
                    row.set(ColumnName.name, hierarchyName + ":(All)!KEY");
                    row.set(OrdinalPosition.name, ordinalPosition++);
                    row.set(ColumnHasDefault.name, false);
                    row.set(ColumnFlags.name, 0);
                    row.set(IsNullable.name, false);
                    // names are always BOOL
                    row.set(
                        DataType.name, XmlaConstants.DBType.BOOL.xmlaOrdinal());
                    row.set(NumericPrecision.name, 255);
                    row.set(NumericScale.name, 255);
                    addRow(row, rows);
                }
            }

            for (Level level : schemaReader.getHierarchyLevels(hierarchy)) {
                ordinalPosition = populateLevel(
                    cube, hierarchy, level, ordinalPosition, rows);
            }
            return ordinalPosition;
        }

        private int populateLevel(
            Cube cube,
            HierarchyBase hierarchy,
            Level level,
            int ordinalPosition,
            List<Row> rows)
        {
            String schemaName = cube.getSchema().getName();
            String cubeName = cube.getName();
            String hierarchyName = hierarchy.getName();
            String levelName = level.getName();

            Row row = new Row();
            row.set(TableCatalog.name, schemaName);
            row.set(TableName.name, cubeName);
            row.set(
                ColumnName.name,
                hierarchyName + ':' + levelName + "!NAME");
            row.set(OrdinalPosition.name, ordinalPosition++);
            row.set(ColumnHasDefault.name, false);
            row.set(ColumnFlags.name, 0);
            row.set(IsNullable.name, false);
            // names are always WSTR
            row.set(DataType.name, XmlaConstants.DBType.WSTR.xmlaOrdinal());
            row.set(CharacterMaximumLength.name, 0);
            row.set(CharacterOctetLength.name, 0);
            addRow(row, rows);

            row = new Row();
            row.set(TableCatalog.name, schemaName);
            row.set(TableName.name, cubeName);
            row.set(
                ColumnName.name,
                hierarchyName + ':' + levelName + "!UNIQUE_NAME");
            row.set(OrdinalPosition.name, ordinalPosition++);
            row.set(ColumnHasDefault.name, false);
            row.set(ColumnFlags.name, 0);
            row.set(IsNullable.name, false);
            // names are always WSTR
            row.set(DataType.name, XmlaConstants.DBType.WSTR.xmlaOrdinal());
            row.set(CharacterMaximumLength.name, 0);
            row.set(CharacterOctetLength.name, 0);
            addRow(row, rows);

/*
TODO: see above
            row = new Row();
            row.set(TableCatalog.name, schemaName);
            row.set(TableName.name, cubeName);
            row.set(ColumnName.name,
                hierarchyName + ":" + levelName + "!KEY");
            row.set(OrdinalPosition.name, ordinalPosition++);
            row.set(ColumnHasDefault.name, false);
            row.set(ColumnFlags.name, 0);
            row.set(IsNullable.name, false);
            // names are always BOOL
            row.set(DataType.name, DBType.BOOL.ordinal());
            row.set(NumericPrecision.name, 255);
            row.set(NumericScale.name, 255);
            addRow(row, rows);
*/
            Property[] props = level.getProperties();
            for (Property prop : props) {
                String propName = prop.getName();

                row = new Row();
                row.set(TableCatalog.name, schemaName);
                row.set(TableName.name, cubeName);
                row.set(
                    ColumnName.name,
                    hierarchyName + ':' + levelName + '!' + propName);
                row.set(OrdinalPosition.name, ordinalPosition++);
                row.set(ColumnHasDefault.name, false);
                row.set(ColumnFlags.name, 0);
                row.set(IsNullable.name, false);

                XmlaConstants.DBType dbType = getDBTypeFromProperty(prop);
                row.set(DataType.name, dbType.xmlaOrdinal());

                switch (prop.getType()) {
                case TYPE_STRING:
                    row.set(CharacterMaximumLength.name, 0);
                    row.set(CharacterOctetLength.name, 0);
                    break;
                case TYPE_NUMERIC:
                    // TODO: 16/255 seems to be what MS SQL Server
                    // always returns.
                    row.set(NumericPrecision.name, 16);
                    row.set(NumericScale.name, 255);
                    break;
                case TYPE_BOOLEAN:
                    row.set(NumericPrecision.name, 255);
                    row.set(NumericScale.name, 255);
                    break;
                case TYPE_OTHER:
                    // TODO: what type is it really, its
                    // not a string
                    row.set(CharacterMaximumLength.name, 0);
                    row.set(CharacterOctetLength.name, 0);
                    break;
                }
                addRow(row, rows);
            }
            return ordinalPosition;
        }

        protected void setProperty(
            PropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DbschemaProviderTypesRowset extends Rowset {
        private final RestrictionTest dataTypeRT;

        DbschemaProviderTypesRowset(XmlaRequest request, XmlaHandler handler) {
            super(DBSCHEMA_PROVIDER_TYPES, request, handler);
            dataTypeRT = getRestrictionTest(DataType);
        }

        /*
        DATA_TYPE DBTYPE_UI2
        BEST_MATCH DBTYPE_BOOL
        Column(String name, Type type, Enumeration enumeratedType,
        boolean restriction, boolean nullable, String description)
        */
        /*
         * These are the columns returned by SQL Server.
         */
        private static final Column TypeName =
            new Column(
                "TYPE_NAME",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The provider-specific data type name.");
        private static final Column DataType =
            new Column(
                "DATA_TYPE",
                Type.UnsignedShort,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The indicator of the data type.");
        private static final Column ColumnSize =
            new Column(
                "COLUMN_SIZE",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The length of a non-numeric column. If the data type is "
                + "numeric, this is the upper bound on the maximum precision "
                + "of the data type.");
        private static final Column LiteralPrefix =
            new Column(
                "LITERAL_PREFIX",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The character or characters used to prefix a literal of this "
                + "type in a text command.");
        private static final Column LiteralSuffix =
            new Column(
                "LITERAL_SUFFIX",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The character or characters used to suffix a literal of this "
                + "type in a text command.");
        private static final Column IsNullable =
            new Column(
                "IS_NULLABLE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A Boolean that indicates whether the data type is nullable. "
                + "NULL-- indicates that it is not known whether the data type "
                + "is nullable.");
        private static final Column CaseSensitive =
            new Column(
                "CASE_SENSITIVE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A Boolean that indicates whether the data type is a "
                + "characters type and case-sensitive.");
        private static final Column Searchable =
            new Column(
                "SEARCHABLE",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "An integer indicating how the data type can be used in "
                + "searches if the provider supports ICommandText; otherwise, "
                + "NULL.");
        private static final Column UnsignedAttribute =
            new Column(
                "UNSIGNED_ATTRIBUTE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A Boolean that indicates whether the data type is unsigned.");
        private static final Column FixedPrecScale =
            new Column(
                "FIXED_PREC_SCALE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A Boolean that indicates whether the data type has a fixed "
                + "precision and scale.");
        private static final Column AutoUniqueValue =
            new Column(
                "AUTO_UNIQUE_VALUE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A Boolean that indicates whether the data type is "
                + "autoincrementing.");
        private static final Column IsLong =
            new Column(
                "IS_LONG",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A Boolean that indicates whether the data type is a binary "
                + "large object (BLOB) and has very long data.");
        private static final Column BestMatch =
            new Column(
                "BEST_MATCH",
                Type.Boolean,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "A Boolean that indicates whether the data type is a best "
                + "match.");

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            // Identifies the (base) data types supported by the data provider.
            Row row;

            // i4
            Integer dt = XmlaConstants.DBType.I4.xmlaOrdinal();
            if (dataTypeRT.passes(dt)) {
                row = new Row();
                row.set(TypeName.name, XmlaConstants.DBType.I4.userName);
                row.set(DataType.name, dt);
                row.set(ColumnSize.name, 8);
                row.set(IsNullable.name, true);
                row.set(Searchable.name, null);
                row.set(UnsignedAttribute.name, false);
                row.set(FixedPrecScale.name, false);
                row.set(AutoUniqueValue.name, false);
                row.set(IsLong.name, false);
                row.set(BestMatch.name, true);
                addRow(row, rows);
            }

            // R8
            dt = XmlaConstants.DBType.R8.xmlaOrdinal();
            if (dataTypeRT.passes(dt)) {
                row = new Row();
                row.set(TypeName.name, XmlaConstants.DBType.R8.userName);
                row.set(DataType.name, dt);
                row.set(ColumnSize.name, 16);
                row.set(IsNullable.name, true);
                row.set(Searchable.name, null);
                row.set(UnsignedAttribute.name, false);
                row.set(FixedPrecScale.name, false);
                row.set(AutoUniqueValue.name, false);
                row.set(IsLong.name, false);
                row.set(BestMatch.name, true);
                addRow(row, rows);
            }

            // CY
            dt = XmlaConstants.DBType.CY.xmlaOrdinal();
            if (dataTypeRT.passes(dt)) {
                row = new Row();
                row.set(TypeName.name, XmlaConstants.DBType.CY.userName);
                row.set(DataType.name, dt);
                row.set(ColumnSize.name, 8);
                row.set(IsNullable.name, true);
                row.set(Searchable.name, null);
                row.set(UnsignedAttribute.name, false);
                row.set(FixedPrecScale.name, false);
                row.set(AutoUniqueValue.name, false);
                row.set(IsLong.name, false);
                row.set(BestMatch.name, true);
                addRow(row, rows);
            }

            // BOOL
            dt = XmlaConstants.DBType.BOOL.xmlaOrdinal();
            if (dataTypeRT.passes(dt)) {
                row = new Row();
                row.set(TypeName.name, XmlaConstants.DBType.BOOL.userName);
                row.set(DataType.name, dt);
                row.set(ColumnSize.name, 1);
                row.set(IsNullable.name, true);
                row.set(Searchable.name, null);
                row.set(UnsignedAttribute.name, false);
                row.set(FixedPrecScale.name, false);
                row.set(AutoUniqueValue.name, false);
                row.set(IsLong.name, false);
                row.set(BestMatch.name, true);
                addRow(row, rows);
            }

            // I8
            dt = XmlaConstants.DBType.I8.xmlaOrdinal();
            if (dataTypeRT.passes(dt)) {
                row = new Row();
                row.set(TypeName.name, XmlaConstants.DBType.I8.userName);
                row.set(DataType.name, dt);
                row.set(ColumnSize.name, 16);
                row.set(IsNullable.name, true);
                row.set(Searchable.name, null);
                row.set(UnsignedAttribute.name, false);
                row.set(FixedPrecScale.name, false);
                row.set(AutoUniqueValue.name, false);
                row.set(IsLong.name, false);
                row.set(BestMatch.name, true);
                addRow(row, rows);
            }

            // WSTR
            dt = XmlaConstants.DBType.WSTR.xmlaOrdinal();
            if (dataTypeRT.passes(dt)) {
                row = new Row();
                row.set(TypeName.name, XmlaConstants.DBType.WSTR.userName);
                row.set(DataType.name, dt);
                // how big are the string columns in the db
                row.set(ColumnSize.name, 255);
                row.set(LiteralPrefix.name, "\"");
                row.set(LiteralSuffix.name, "\"");
                row.set(IsNullable.name, true);
                row.set(CaseSensitive.name, false);
                row.set(Searchable.name, null);
                row.set(FixedPrecScale.name, false);
                row.set(AutoUniqueValue.name, false);
                row.set(IsLong.name, false);
                row.set(BestMatch.name, true);
                addRow(row, rows);
            }
        }

        protected void setProperty(
            PropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DbschemaSchemataRowset extends Rowset {
        private final RestrictionTest catalogNameRT;

        DbschemaSchemataRowset(XmlaRequest request, XmlaHandler handler) {
            super(DBSCHEMA_SCHEMATA, request, handler);
            catalogNameRT = getRestrictionTest(CatalogName);
        }

        /*
         * These are the columns returned by SQL Server.
         */
        private static final Column CatalogName =
            new Column(
                "CATALOG_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The provider-specific data type name.");
        private static final Column SchemaName =
            new Column(
                "SCHEMA_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The indicator of the data type.");
        private static final Column SchemaOwner =
            new Column(
                "SCHEMA_OWNER",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The length of a non-numeric column. If the data type is "
                + "numeric, this is the upper bound on the maximum precision "
                + "of the data type.");

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            DataSourcesConfig.DataSource ds = handler.getDataSource(request);
            DataSourcesConfig.Catalog[] catalogs = ds.catalogs.catalogs;
            String roleName = request.getRoleName();
            Role role = request.getRole();

            for (DataSourcesConfig.Catalog dsCatalog : catalogs) {
                if (dsCatalog == null || dsCatalog.definition == null) {
                    continue;
                }
                Connection connection =
                    handler.getConnection(dsCatalog, role, roleName);
                if (connection == null) {
                    continue;
                }
                if (!catalogNameRT.passes(dsCatalog.name)) {
                    continue;
                }
                final Schema schema = connection.getSchema();
                Row row = new Row();
                row.set(CatalogName.name, dsCatalog.name);
                row.set(SchemaName.name, schema.getName());
                row.set(SchemaOwner.name, "");
                addRow(row, rows);
            }
        }

        protected void setProperty(
            PropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DbschemaTablesRowset extends Rowset {
        private final RestrictionTest tableCatalogRT;
        private final RestrictionTest tableNameRT;
        private final RestrictionTest tableTypeRT;

        DbschemaTablesRowset(XmlaRequest request, XmlaHandler handler) {
            super(DBSCHEMA_TABLES, request, handler);
            tableCatalogRT = getRestrictionTest(TableCatalog);
            tableNameRT = getRestrictionTest(TableName);
            tableTypeRT = getRestrictionTest(TableType);
        }

        private static final Column TableCatalog =
            new Column(
                "TABLE_CATALOG",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the catalog to which this object belongs.");
        private static final Column TableSchema =
            new Column(
                "TABLE_SCHEMA",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the cube to which this object belongs.");
        private static final Column TableName =
            new Column(
                "TABLE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the object, if TABLE_TYPE is TABLE.");
        private static final Column TableType =
            new Column(
                "TABLE_TYPE",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The type of the table. TABLE indicates the object is a "
                + "measure group. SYSTEM TABLE indicates the object is a "
                + "dimension.");

        private static final Column TableGuid =
            new Column(
                "TABLE_GUID",
                Type.UUID,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Not supported.");
        private static final Column Description =
            new Column(
                "DESCRIPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A human-readable description of the object.");
        private static final Column TablePropId =
            new Column(
                "TABLE_PROPID",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Not supported.");
        private static final Column DateCreated =
            new Column(
                "DATE_CREATED",
                Type.DateTime,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Not supported.");
        private static final Column DateModified =
            new Column(
                "DATE_MODIFIED",
                Type.DateTime,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The date the object was last modified.");

        /*
        private static final Column TableOlapType =
            new Column(
                "TABLE_OLAP_TYPE",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The OLAP type of the object.  MEASURE_GROUP indicates the "
                + "object is a measure group.  CUBE_DIMENSION indicated the "
                + "object is a dimension.");
        */

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            DataSourcesConfig.DataSource ds = handler.getDataSource(request);
            DataSourcesConfig.Catalog[] catalogs =
                handler.getCatalogs(request, ds);
            String roleName = request.getRoleName();
            Role role = request.getRole();

            for (DataSourcesConfig.Catalog dsCatalog : catalogs) {
                if (dsCatalog == null || dsCatalog.definition == null) {
                    continue;
                }
                Connection connection =
                    handler.getConnection(dsCatalog, role, roleName);
                if (connection == null) {
                    continue;
                }
                final Schema schema = connection.getSchema();
                String catalogName = dsCatalog.name;
                if (!tableCatalogRT.passes(catalogName)) {
                    continue;
                }

                //final String schemaName = schema.getName();

                Row row;
                for (Cube cube1 : sortedCubes(schema)) {
                    RolapCube cube = (RolapCube) cube1;
                    String cubeName = cube.getName();

                    if (!tableNameRT.passes(cubeName)) {
                        continue;
                    }
                    SchemaReader schemaReader =
                        cube.getSchemaReader(
                            connection.getRole());

                    String desc = cube.getDescription();
                    if (desc == null) {
                        //TODO: currently this is always null
                        desc = catalogName + " - " + cubeName + " Cube";
                    }


                    if (tableTypeRT.passes("TABLE")) {
                        row = new Row();
                        row.set(TableCatalog.name, catalogName);
                        row.set(TableName.name, cubeName);
                        row.set(TableType.name, "TABLE");
                        row.set(Description.name, desc);
                        if (false) {
                            row.set(DateModified.name, dateModified);
                        }
                        addRow(row, rows);
                    }


                    if (tableTypeRT.passes("SYSTEM TABLE")) {
                        for (Dimension dimension : cube.getDimensions()) {
                            if (dimension.isMeasures()) {
                                continue;
                            }
                            Hierarchy[] hierarchies =
                                dimension.getHierarchies();
                            for (Hierarchy hierarchy1 : hierarchies) {
                                HierarchyBase hierarchy =
                                    (HierarchyBase) hierarchy1;
                                populateHierarchy(
                                    schemaReader, cube,
                                    hierarchy, rows);
                            }
                        }
                    }
                }
            }
        }

        private void populateHierarchy(
            SchemaReader schemaReader,
            RolapCube cube,
            HierarchyBase hierarchy,
            List<Row> rows)
        {
            // Access control
            if (!canAccess(schemaReader, hierarchy)) {
                return;
            }
/*
            String schemaName = cube.getSchema().getName();
            String cubeName = cube.getName();
            String hierarchyName = hierarchy.getName();

            String desc = hierarchy.getDescription();
            if (desc == null) {
                //TODO: currently this is always null
                desc = schemaName +
                    " - " +
                    cubeName +
                    " Cube - " +
                    hierarchyName +
                    " Hierarchy";
            }

            if (hierarchy.hasAll()) {
                String tableName = cubeName +
                    ':' + hierarchyName + ':' + "(All)";

                Row row = new Row();
                row.set(TableCatalog.name, schemaName);
                row.set(TableName.name, tableName);
                row.set(TableType.name, "SYSTEM TABLE");
                row.set(Description.name, desc);
                row.set(DateModified.name, dateModified);
                addRow(row, rows);
            }
*/
            Level[] levels = hierarchy.getLevels();
            for (Level level : levels) {
                populateLevel(cube, hierarchy, level, rows);
            }
        }

        private void populateLevel(
            RolapCube cube,
            HierarchyBase hierarchy,
            Level level,
            List<Row> rows)
        {
            String schemaName = cube.getSchema().getName();
            String cubeName = cube.getName();
            String hierarchyName = getHierarchyName(hierarchy);
            String levelName = level.getName();

            String tableName =
                cubeName + ':' + hierarchyName + ':' + levelName;

            String desc = level.getDescription();
            if (desc == null) {
                //TODO: currently this is always null
                desc =
                    schemaName + " - "
                    + cubeName + " Cube - "
                    + hierarchyName + " Hierarchy - "
                    + levelName + " Level";
            }

            Row row = new Row();
            row.set(TableCatalog.name, schemaName);
            row.set(TableName.name, tableName);
            row.set(TableType.name, "SYSTEM TABLE");
            row.set(Description.name, desc);
            if (false) {
                row.set(DateModified.name, dateModified);
            }
            addRow(row, rows);
        }

        protected void setProperty(
            PropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    // TODO: Is this needed????
    static class DbschemaTablesInfoRowset extends Rowset {
        DbschemaTablesInfoRowset(XmlaRequest request, XmlaHandler handler) {
            super(DBSCHEMA_TABLES_INFO, request, handler);
        }

        private static final Column TableCatalog =
            new Column(
                "TABLE_CATALOG",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "Catalog name. NULL if the provider does not support "
                + "catalogs.");
        private static final Column TableSchema =
            new Column(
                "TABLE_SCHEMA",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "Unqualified schema name. NULL if the provider does not "
                + "support schemas.");
        private static final Column TableName =
            new Column(
                "TABLE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Table name.");
        private static final Column TableType =
            new Column(
                "TABLE_TYPE",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Table type. One of the following or a provider-specific "
                + "value: ALIAS, TABLE, SYNONYM, SYSTEM TABLE, VIEW, GLOBAL "
                + "TEMPORARY, LOCAL TEMPORARY, EXTERNAL TABLE, SYSTEM VIEW");
        private static final Column TableGuid =
            new Column(
                "TABLE_GUID",
                Type.UUID,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "GUID that uniquely identifies the table. Providers that do "
                + "not use GUIDs to identify tables should return NULL in this "
                + "column.");

        private static final Column Bookmarks =
            new Column(
                "BOOKMARKS",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Whether this table supports bookmarks. Allways is false.");
        private static final Column BookmarkType =
            new Column(
                "BOOKMARK_TYPE",
                Type.Integer,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Default bookmark type supported on this table.");
        private static final Column BookmarkDataType =
            new Column(
                "BOOKMARK_DATATYPE",
                Type.UnsignedShort,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The indicator of the bookmark's native data type.");
        private static final Column BookmarkMaximumLength =
            new Column(
                "BOOKMARK_MAXIMUM_LENGTH",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Maximum length of the bookmark in bytes.");
        private static final Column BookmarkInformation =
            new Column(
                "BOOKMARK_INFORMATION",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A bitmask specifying additional information about bookmarks "
                + "over the rowset. ");
        private static final Column TableVersion =
            new Column(
                "TABLE_VERSION",
                Type.Long,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Version number for this table or NULL if the provider does "
                + "not support returning table version information.");
        private static final Column Cardinality =
            new Column(
                "CARDINALITY",
                Type.UnsignedLong,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Cardinality (number of rows) of the table.");
        private static final Column Description =
            new Column(
                "DESCRIPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Human-readable description of the table.");
        private static final Column TablePropId =
            new Column(
                "TABLE_PROPID",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Property ID of the table. Return null.");

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            DataSourcesConfig.DataSource ds = handler.getDataSource(request);
            DataSourcesConfig.Catalog[] catalogs =
                handler.getCatalogs(request, ds);
            String roleName = request.getRoleName();
            Role role = request.getRole();

            for (DataSourcesConfig.Catalog dsCatalog : catalogs) {
                if (dsCatalog == null || dsCatalog.definition == null) {
                    continue;
                }
                Connection connection =
                    handler.getConnection(dsCatalog, role, roleName);
                if (connection == null) {
                    continue;
                }
                final Schema schema = connection.getSchema();
                String catalogName = dsCatalog.name;
                //final String catalogName = schema.getName();

                //TODO: Is this cubes or tables? SQL Server returns what
                // in foodmart are cube names for TABLE_NAME
                for (Cube cube1 : sortedCubes(schema)) {
                    RolapCube cube = (RolapCube) cube1;
                    String cubeName = cube.getName();
                    String desc = cube.getDescription();
                    if (desc == null) {
                        //TODO: currently this is always null
                        desc = catalogName + " - " + cubeName + " Cube";
                    }
                    //TODO: SQL Server returns 1000000 for all tables
                    int cardinality = 1000000;
                    String version = "null";

                    Row row = new Row();
                    row.set(TableCatalog.name, catalogName);
                    row.set(TableName.name, cubeName);
                    row.set(TableType.name, "TABLE");
                    row.set(Bookmarks.name, false);
                    row.set(TableVersion.name, version);
                    row.set(Cardinality.name, cardinality);
                    row.set(Description.name, desc);
                    addRow(row, rows);
                }
            }
        }

        protected void setProperty(
            PropertyDefinition propertyDef,
            String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class MdschemaActionsRowset extends Rowset {
        MdschemaActionsRowset(XmlaRequest request, XmlaHandler handler) {
            super(MDSCHEMA_ACTIONS, request, handler);
        }

        private static final Column CatalogName =
            new Column(
                "CATALOG_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the catalog to which this action belongs.");
        private static final Column SchemaName =
            new Column(
                "SCHEMA_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the schema to which this action belongs.");
        private static final Column CubeName =
            new Column(
                "CUBE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the cube to which this action belongs.");
        private static final Column ActionName =
            new Column(
                "ACTION_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the action.");
        private static final Column Coordinate =
            new Column(
                "COORDINATE",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                null);
        private static final Column CoordinateType =
            new Column(
                "COORDINATE_TYPE",
                Type.Integer,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                null);
        /*
            TODO: optional columns
        ACTION_TYPE
        INVOCATION
        CUBE_SOURCE
    */

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            // mondrian doesn't support actions. It's not an error to ask for
            // them, there just aren't any
        }
    }

    static class MdschemaCubesRowset extends Rowset {
        private final RestrictionTest catalogNameRT;
        private final RestrictionTest schemaNameRT;
        private final RestrictionTest cubeNameRT;

        MdschemaCubesRowset(XmlaRequest request, XmlaHandler handler) {
            super(MDSCHEMA_CUBES, request, handler);
            catalogNameRT = getRestrictionTest(CatalogName);
            schemaNameRT = getRestrictionTest(SchemaName);
            cubeNameRT = getRestrictionTest(CubeName);
        }

        private static final String MD_CUBTYPE_CUBE = "CUBE";
        private static final String MD_CUBTYPE_VIRTUAL_CUBE = "VIRTUAL CUBE";

        private static final Column CatalogName =
            new Column(
                "CATALOG_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the catalog to which this cube belongs.");
        private static final Column SchemaName =
            new Column(
                "SCHEMA_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the schema to which this cube belongs.");
        private static final Column CubeName =
            new Column(
                "CUBE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Name of the cube.");
        private static final Column CubeType =
            new Column(
                "CUBE_TYPE",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Cube type.");
        private static final Column CubeGuid =
            new Column(
                "CUBE_GUID",
                Type.UUID,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Cube type.");
        private static final Column CreatedOn =
            new Column(
                "CREATED_ON",
                Type.DateTime,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Date and time of cube creation.");
        private static final Column LastSchemaUpdate =
            new Column(
                "LAST_SCHEMA_UPDATE",
                Type.DateTime,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Date and time of last schema update.");
        private static final Column SchemaUpdatedBy =
            new Column(
                "SCHEMA_UPDATED_BY",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "User ID of the person who last updated the schema.");
        private static final Column LastDataUpdate =
            new Column(
                "LAST_DATA_UPDATE",
                Type.DateTime,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Date and time of last data update.");
        private static final Column DataUpdatedBy =
            new Column(
                "DATA_UPDATED_BY",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "User ID of the person who last updated the data.");
        private static final Column IsDrillthroughEnabled =
            new Column(
                "IS_DRILLTHROUGH_ENABLED",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Describes whether DRILLTHROUGH can be performed on the "
                + "members of a cube");
        private static final Column IsWriteEnabled =
            new Column(
                "IS_WRITE_ENABLED",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Describes whether a cube is write-enabled");
        private static final Column IsLinkable =
            new Column(
                "IS_LINKABLE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Describes whether a cube can be used in a linked cube");
        private static final Column IsSqlEnabled =
            new Column(
                "IS_SQL_ENABLED",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Describes whether or not SQL can be used on the cube");
        private static final Column Description =
            new Column(
                "DESCRIPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A user-friendly description of the dimension.");
        private static final Column Dimensions =
            new Column(
                "DIMENSIONS",
                Type.Rowset,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Dimensions in this cube.");
        private static final Column Sets =
            new Column(
                "SETS",
                Type.Rowset,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Sets in this cube.");
        private static final Column Measures =
            new Column(
                "MEASURES",
                Type.Rowset,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Measures in this cube.");

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            DataSourcesConfig.DataSource ds = handler.getDataSource(request);
            DataSourcesConfig.Catalog[] catalogs =
                handler.getCatalogs(request, ds);
            String roleName = request.getRoleName();
            Role role = request.getRole();

            for (DataSourcesConfig.Catalog dsCatalog : catalogs) {
                if (dsCatalog == null || dsCatalog.definition == null) {
                    continue;
                }
                Connection connection =
                    handler.getConnection(dsCatalog, role, roleName);
                if (connection == null) {
                    continue;
                }
                String catalogName = dsCatalog.name;
                if (!catalogNameRT.passes(catalogName)) {
                    continue;
                }

                final Schema schema = connection.getSchema();
                if (!schemaNameRT.passes(schema.getName())) {
                    continue;
                }
                for (Cube cube : sortedCubes(schema)) {
                    // Access control
                    if (!canAccess(schema.getSchemaReader(), cube)) {
                        continue;
                    }
                    if (!cubeNameRT.passes(cube.getName())) {
                        continue;
                    }

                    String desc = cube.getDescription();
                    if (desc == null) {
                        desc =
                            catalogName + " Schema - "
                            + cube.getName() + " Cube";
                    }

                    Row row = new Row();
                    row.set(CatalogName.name, catalogName);
                    row.set(SchemaName.name, schema.getName());
                    row.set(CubeName.name, cube.getName());
                    row.set(
                        CubeType.name,
                        ((RolapCube) cube).isVirtual()
                            ? MD_CUBTYPE_VIRTUAL_CUBE : MD_CUBTYPE_CUBE);
                    //row.set(CubeGuid.name, "");
                    //row.set(CreatedOn.name, "");
                    //row.set(LastSchemaUpdate.name, "");
                    //row.set(SchemaUpdatedBy.name, "");
                    //row.set(LastDataUpdate.name, "");
                    //row.set(DataUpdatedBy.name, "");
                    row.set(IsDrillthroughEnabled.name, true);
                    row.set(IsWriteEnabled.name, false);
                    row.set(IsLinkable.name, false);
                    row.set(IsSqlEnabled.name, false);
                    row.set(Description.name, desc);
                    Format formatter =
                        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                    String formattedDate =
                        formatter.format(schema.getSchemaLoadDate());
                    row.set(LastSchemaUpdate.name, formattedDate);
                    if (deep) {
                        row.set(
                            Dimensions.name,
                            new MdschemaDimensionsRowset(
                                wrapRequest(
                                    request,
                                    org.olap4j.impl.Olap4jUtil.mapOf(
                                        MdschemaDimensionsRowset.CatalogName,
                                        catalogName,
                                        MdschemaDimensionsRowset.SchemaName,
                                        schema.getName(),
                                        MdschemaDimensionsRowset.CubeName,
                                        cube.getName())),
                                handler));
                        row.set(
                            Sets.name,
                            new MdschemaSetsRowset(
                                wrapRequest(
                                    request,
                                    org.olap4j.impl.Olap4jUtil.mapOf(
                                        MdschemaSetsRowset.CatalogName,
                                        catalogName,
                                        MdschemaSetsRowset.SchemaName,
                                        schema.getName(),
                                        MdschemaSetsRowset.CubeName,
                                        cube.getName())),
                                handler));
                        row.set(
                            Measures.name,
                            new MdschemaMeasuresRowset(
                                wrapRequest(
                                    request,
                                    org.olap4j.impl.Olap4jUtil.mapOf(
                                        MdschemaMeasuresRowset.CatalogName,
                                        catalogName,
                                        MdschemaMeasuresRowset.SchemaName,
                                        schema.getName(),
                                        MdschemaMeasuresRowset.CubeName,
                                        cube.getName())),
                                handler));
                    }
                    addRow(row, rows);
                }
            }
        }

        protected void setProperty(
            PropertyDefinition propertyDef,
            String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class MdschemaDimensionsRowset extends Rowset {
        private final RestrictionTest catalogRT;
        private final RestrictionTest schemaNameRT;
        private final RestrictionTest cubeNameRT;
        private final RestrictionTest dimensionUniqueNameRT;
        private final RestrictionTest dimensionNameRT;

        MdschemaDimensionsRowset(XmlaRequest request, XmlaHandler handler) {
            super(MDSCHEMA_DIMENSIONS, request, handler);
            catalogRT = getRestrictionTest(CatalogName);
            schemaNameRT = getRestrictionTest(SchemaName);
            cubeNameRT = getRestrictionTest(CubeName);
            dimensionUniqueNameRT = getRestrictionTest(DimensionUniqueName);
            dimensionNameRT = getRestrictionTest(DimensionName);
        }

        public static final int MD_DIMTYPE_OTHER = 3;
        public static final int MD_DIMTYPE_MEASURE = 2;
        public static final int MD_DIMTYPE_TIME = 1;

        private static final Column CatalogName =
            new Column(
                "CATALOG_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the database.");
        private static final Column SchemaName =
            new Column(
                "SCHEMA_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "Not supported.");
        private static final Column CubeName =
            new Column(
                "CUBE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the cube.");
        private static final Column DimensionName =
            new Column(
                "DIMENSION_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the dimension.");
        private static final Column DimensionUniqueName =
            new Column(
                "DIMENSION_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The unique name of the dimension.");
        private static final Column DimensionGuid =
            new Column(
                "DIMENSION_GUID",
                Type.UUID,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Not supported.");
        private static final Column DimensionCaption =
            new Column(
                "DIMENSION_CAPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The caption of the dimension.");
        private static final Column DimensionOrdinal =
            new Column(
                "DIMENSION_ORDINAL",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The position of the dimension within the cube.");
        /*
         * SQL Server returns values:
         *   MD_DIMTYPE_TIME (1)
         *   MD_DIMTYPE_MEASURE (2)
         *   MD_DIMTYPE_OTHER (3)
         */
        private static final Column DimensionType =
            new Column(
                "DIMENSION_TYPE",
                Type.Short,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The type of the dimension.");
        private static final Column DimensionCardinality =
            new Column(
                "DIMENSION_CARDINALITY",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The number of members in the key attribute.");
        private static final Column DefaultHierarchy =
            new Column(
                "DEFAULT_HIERARCHY",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A hierarchy from the dimension. Preserved for backwards "
                + "compatibility.");
        private static final Column Description =
            new Column(
                "DESCRIPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A user-friendly description of the dimension.");
        private static final Column IsVirtual =
            new Column(
                "IS_VIRTUAL",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Always FALSE.");
        private static final Column IsReadWrite =
            new Column(
                "IS_READWRITE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A Boolean that indicates whether the dimension is "
                + "write-enabled.");
        /*
         * SQL Server returns values: 0 or 1
         */
        private static final Column DimensionUniqueSettings =
            new Column(
                "DIMENSION_UNIQUE_SETTINGS",
                Type.Integer,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A bitmap that specifies which columns contain unique values "
                + "if the dimension contains only members with unique names.");
        private static final Column DimensionMasterUniqueName =
            new Column(
                "DIMENSION_MASTER_UNIQUE_NAME",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Always NULL.");
        private static final Column DimensionIsVisible =
            new Column(
                "DIMENSION_IS_VISIBLE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Always TRUE.");
        private static final Column Hierarchies =
            new Column(
                "HIERARCHIES",
                Type.Rowset,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Hierarchies in this dimension.");

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            DataSourcesConfig.DataSource ds =
                handler.getDataSource(request);
            DataSourcesConfig.Catalog[] catalogs =
                handler.getCatalogs(request, ds);
            String roleName = request.getRoleName();
            Role role = request.getRole();

            for (DataSourcesConfig.Catalog dsCatalog : catalogs) {
                if (dsCatalog == null || dsCatalog.definition == null) {
                    continue;
                }
                String catalogName = dsCatalog.name;
                if (!catalogRT.passes(catalogName)) {
                    continue;
                }

                Connection connection =
                    handler.getConnection(dsCatalog, role, roleName);
                if (connection == null) {
                    continue;
                }
                populateCatalog(connection, catalogName, rows);
            }
        }

        protected void populateCatalog(
            Connection connection,
            String catalogName,
            List<Row> rows)
            throws XmlaException
        {
            Schema schema = connection.getSchema();
            if (!schemaNameRT.passes(schema.getName())) {
                return;
            }
            for (Cube cube : sortedCubes(schema)) {
                if (!cubeNameRT.passes(cube.getName())) {
                    continue;
                }
                SchemaReader schemaReader =
                    cube.getSchemaReader(
                        connection.getRole());
                populateCube(schemaReader, catalogName, cube, rows);
            }
        }

        protected void populateCube(
            SchemaReader schemaReader,
            String catalogName,
            Cube cube,
            List<Row> rows)
            throws XmlaException
        {
            for (Dimension dimension : cube.getDimensions()) {
                String name = dimension.getName();
                String unique = dimension.getUniqueName();
                if (dimensionNameRT.passes(name)
                    && dimensionUniqueNameRT.passes(unique))
                {
                    populateDimension(
                        schemaReader, catalogName,
                        cube, dimension, rows);
                }
            }
        }

        protected void populateDimension(
            SchemaReader schemaReader,
            String catalogName,
            Cube cube,
            Dimension dimension,
            List<Row> rows)
            throws XmlaException
        {
            // Access control
            if (!canAccess(schemaReader, dimension)) {
                return;
            }

            String desc = dimension.getDescription();
            if (desc == null) {
                desc =
                    cube.getName() + " Cube - "
                    + dimension.getName() + " Dimension";
            }

            Row row = new Row();
            row.set(CatalogName.name, catalogName);
            row.set(SchemaName.name, cube.getSchema().getName());
            row.set(CubeName.name, cube.getName());
            row.set(DimensionName.name, dimension.getName());
            row.set(DimensionUniqueName.name, dimension.getUniqueName());
            row.set(DimensionCaption.name, dimension.getCaption());
            row.set(
                DimensionOrdinal.name,
                getDimensionOrdinal(cube, dimension));
            row.set(DimensionType.name, getDimensionType(dimension));

            //Is this the number of primaryKey members there are??
            // According to microsoft this is:
            //    "The number of members in the key attribute."
            // There may be a better way of doing this but
            // this is what I came up with. Note that I need to
            // add '1' to the number inorder for it to match
            // match what microsoft SQL Server is producing.
            // The '1' might have to do with whether or not the
            // hierarchy has a 'all' member or not - don't know yet.
            // large data set total for Orders cube 0m42.923s
            Hierarchy firstHierarchy = dimension.getHierarchies()[0];
            Level[] levels = firstHierarchy.getLevels();
            Level lastLevel = levels[levels.length - 1];



            /*
            if override config setting is set
                if approxRowCount has a value
                    use it
            else
                                    do default
            */

            // Added by TWI to returned cached row numbers
            int n = schemaReader.getLevelCardinality(lastLevel, true, true);
            row.set(DimensionCardinality.name, n + 1);

            // TODO: I think that this is just the dimension name
            row.set(DefaultHierarchy.name, dimension.getUniqueName());
            row.set(Description.name, desc);
            row.set(IsVirtual.name, false);
            // SQL Server always returns false
            row.set(IsReadWrite.name, false);
            // TODO: don't know what to do here
            // Are these the levels with uniqueMembers == true?
            // How are they mapped to specific column numbers?
            row.set(DimensionUniqueSettings.name, 0);
            row.set(DimensionIsVisible.name, true);
            if (deep) {
                row.set(
                    Hierarchies.name,
                    new MdschemaHierarchiesRowset(
                        wrapRequest(
                            request,
                            org.olap4j.impl.Olap4jUtil.mapOf(
                                MdschemaHierarchiesRowset.CatalogName,
                                catalogName,
                                MdschemaHierarchiesRowset.SchemaName,
                                cube.getSchema().getName(),
                                MdschemaHierarchiesRowset.CubeName,
                                cube.getName(),
                                MdschemaHierarchiesRowset.DimensionUniqueName,
                                dimension.getUniqueName())),
                        handler));
            }

            addRow(row, rows);
        }

        private static int getDimensionOrdinal(Cube cube, Dimension dimension) {
            return Arrays.asList(cube.getDimensions()).indexOf(dimension);
        }

        protected void setProperty(
            PropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static int getDimensionType(Dimension dim) {
        if (dim.isMeasures()) {
            return MdschemaDimensionsRowset.MD_DIMTYPE_MEASURE;
        } else if (DimensionType.TimeDimension.equals(dim.getDimensionType())) {
            return MdschemaDimensionsRowset.MD_DIMTYPE_TIME;
        } else {
            return MdschemaDimensionsRowset.MD_DIMTYPE_OTHER;
        }
    }

    static class MdschemaFunctionsRowset extends Rowset {
        /**
         * http://www.csidata.com/custserv/onlinehelp/VBSdocs/vbs57.htm
         */
        enum VarType {
            Empty("Uninitialized (default)"),
            Null("Contains no valid data"),
            Integer("Integer subtype"),
            Long("Long subtype"),
            Single("Single subtype"),
            Double("Double subtype"),
            Currency("Currency subtype"),
            Date("Date subtype"),
            String("String subtype"),
            Object("Object subtype"),
            Error("Error subtype"),
            Boolean("Boolean subtype"),
            Variant("Variant subtype"),
            DataObject("DataObject subtype"),
            Decimal("Decimal subtype"),
            Byte("Byte subtype"),
            Array("Array subtype");

            static VarType forCategory(int category) {
                switch (category) {
                case Category.Unknown:
                    // expression == unknown ???
                    // case Category.Expression:
                    return Empty;
                case Category.Array:
                    return Array;
                case Category.Dimension:
                case Category.Hierarchy:
                case Category.Level:
                case Category.Member:
                case Category.Set:
                case Category.Tuple:
                case Category.Cube:
                case Category.Value:
                    return Variant;
                case Category.Logical:
                    return Boolean;
                case Category.Numeric:
                    return Double;
                case Category.String:
                case Category.Symbol:
                case Category.Constant:
                    return String;
                case Category.DateTime:
                    return Date;
                case Category.Integer:
                case Category.Mask:
                    return Integer;
                }
                // NOTE: this should never happen
                return Empty;
            }

            VarType(String description) {
                Util.discard(description);
            }
        }

        private final RestrictionTest functionNameRT;

        MdschemaFunctionsRowset(XmlaRequest request, XmlaHandler handler) {
            super(MDSCHEMA_FUNCTIONS, request, handler);
            functionNameRT = getRestrictionTest(FunctionName);
        }

        private static final Column FunctionName =
            new Column(
                "FUNCTION_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the function.");
        private static final Column Description =
            new Column(
                "DESCRIPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A description of the function.");
        private static final Column ParameterList =
            new Column(
                "PARAMETER_LIST",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A comma delimited list of parameters.");
        private static final Column ReturnType =
            new Column(
                "RETURN_TYPE",
                Type.Integer,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The VARTYPE of the return data type of the function.");
        private static final Column Origin =
            new Column(
                "ORIGIN",
                Type.Integer,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The origin of the function:  1 for MDX functions.  2 for "
                + "user-defined functions.");
        private static final Column InterfaceName =
            new Column(
                "INTERFACE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the interface for user-defined functions");
        private static final Column LibraryName =
            new Column(
                "LIBRARY_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the type library for user-defined functions. "
                + "NULL for MDX functions.");
        private static final Column Caption =
            new Column(
                "CAPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The display caption for the function.");

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            DataSourcesConfig.DataSource ds = handler.getDataSource(request);
            DataSourcesConfig.Catalog[] catalogs =
                handler.getCatalogs(request, ds);
            String roleName = request.getRoleName();
            Role role = request.getRole();

            for (DataSourcesConfig.Catalog dsCatalog : catalogs) {
                if (dsCatalog == null || dsCatalog.definition == null) {
                    continue;
                }
                Connection connection =
                    handler.getConnection(dsCatalog, role, roleName);
                if (connection == null) {
                    continue;
                }
                final Schema schema = connection.getSchema();
                FunTable funTable = schema.getFunTable();

                StringBuilder buf = new StringBuilder(50);
                List<FunInfo> functions = funTable.getFunInfoList();
                for (FunInfo fi : functions) {
                    switch (fi.getSyntax()) {
                    case Empty:
                    case Internal:
                    case Parentheses:
                        continue;
                    }
                    if (!functionNameRT.passes(fi.getName())) {
                        continue;
                    }

                    int[][] paramCategories = fi.getParameterCategories();
                    int[] returnCategories = fi.getReturnCategories();

                    // Convert Windows newlines in 'description' to UNIX format.
                    String description = fi.getDescription();
                    if (description != null) {
                        description = Util.replace(
                            fi.getDescription(),
                            "\r",
                            "");
                    }
                    if ((paramCategories == null)
                        || (paramCategories.length == 0))
                    {
                        Row row = new Row();
                        row.set(FunctionName.name, fi.getName());
                        row.set(Description.name, description);
                        row.set(ParameterList.name, "(none)");
                        row.set(ReturnType.name, 1);
                        row.set(Origin.name, 1);
                        //row.set(LibraryName.name, "");
                        // TODO WHAT VALUE should this have
                        row.set(InterfaceName.name, "");
                        row.set(Caption.name, fi.getName());
                        addRow(row, rows);

                    } else {
                        for (int i = 0; i < paramCategories.length; i++) {
                            int[] pc = paramCategories[i];
                            int returnCategory = returnCategories[i];

                            Row row = new Row();
                            row.set(FunctionName.name, fi.getName());
                            row.set(Description.name, description);

                            buf.setLength(0);
                            for (int j = 0; j < pc.length; j++) {
                                int v = pc[j];
                                if (j > 0) {
                                    buf.append(", ");
                                }
                                buf.append(Category.instance.getDescription(
                                    v & Category.Mask));
                            }
                            row.set(ParameterList.name, buf.toString());

                            VarType varType =
                                VarType.forCategory(returnCategory);
                            row.set(ReturnType.name, varType.ordinal());

                            //TODO: currently FunInfo can not tell us which
                            // functions are MDX and which are UDFs.
                            row.set(Origin.name, 1);

                            // TODO: Name of the type library for UDFs. NULL for
                            // MDX functions.
                            //row.set(LibraryName.name, "");

                            // TODO: Name of the interface for UDF and Group
                            // name for the MDX functions.
                            // TODO WHAT VALUE should this have
                            row.set(InterfaceName.name, "");

                            row.set(Caption.name, fi.getName());
                            addRow(row, rows);
                        }
                    }
                }
            }
        }

        protected void setProperty(
            PropertyDefinition propertyDef,
            String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class MdschemaHierarchiesRowset extends Rowset {
        private final RestrictionTest catalogRT;
        private final RestrictionTest schemaNameRT;
        private final RestrictionTest cubeNameRT;
        private final RestrictionTest dimensionUniqueNameRT;
        private final RestrictionTest hierarchyUniqueNameRT;
        private final RestrictionTest hierarchyNameRT;

        MdschemaHierarchiesRowset(XmlaRequest request, XmlaHandler handler) {
            super(MDSCHEMA_HIERARCHIES, request, handler);
            catalogRT = getRestrictionTest(CatalogName);
            schemaNameRT = getRestrictionTest(SchemaName);
            cubeNameRT = getRestrictionTest(CubeName);
            dimensionUniqueNameRT = getRestrictionTest(DimensionUniqueName);
            hierarchyUniqueNameRT = getRestrictionTest(HierarchyUniqueName);
            hierarchyNameRT = getRestrictionTest(HierarchyName);
        }

        private static final Column CatalogName =
            new Column(
                "CATALOG_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the catalog to which this hierarchy belongs.");
        private static final Column SchemaName =
            new Column(
                "SCHEMA_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "Not supported");
        private static final Column CubeName =
            new Column(
                "CUBE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the cube to which this hierarchy belongs.");
        private static final Column DimensionUniqueName =
            new Column(
                "DIMENSION_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The unique name of the dimension to which this hierarchy "
                + "belongs.");
        private static final Column HierarchyName =
            new Column(
                "HIERARCHY_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the hierarchy. Blank if there is only a single "
                + "hierarchy in the dimension.");
        private static final Column HierarchyUniqueName =
            new Column(
                "HIERARCHY_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The unique name of the hierarchy.");

        private static final Column HierarchyGuid =
            new Column(
                "HIERARCHY_GUID",
                Type.UUID,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Hierarchy GUID.");

        private static final Column HierarchyCaption =
            new Column(
                "HIERARCHY_CAPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A label or a caption associated with the hierarchy.");
        private static final Column DimensionType =
            new Column(
                "DIMENSION_TYPE",
                Type.Short,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The type of the dimension.");
        private static final Column HierarchyCardinality =
            new Column(
                "HIERARCHY_CARDINALITY",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The number of members in the hierarchy.");
        private static final Column DefaultMember =
            new Column(
                "DEFAULT_MEMBER",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The default member for this hierarchy.");
        private static final Column AllMember =
            new Column(
                "ALL_MEMBER",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The member at the highest level of rollup in the hierarchy.");
        private static final Column Description =
            new Column(
                "DESCRIPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A human-readable description of the hierarchy. NULL if no "
                + "description exists.");
        private static final Column Structure =
            new Column(
                "STRUCTURE",
                Type.Short,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The structure of the hierarchy.");
        private static final Column IsVirtual =
            new Column(
                "IS_VIRTUAL",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Always returns False.");
        private static final Column IsReadWrite =
            new Column(
                "IS_READWRITE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A Boolean that indicates whether the Write Back to dimension "
                + "column is enabled.");
        private static final Column DimensionUniqueSettings =
            new Column(
                "DIMENSION_UNIQUE_SETTINGS",
                Type.Integer,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Always returns MDDIMENSIONS_MEMBER_KEY_UNIQUE (1).");
        private static final Column DimensionIsVisible =
            new Column(
                "DIMENSION_IS_VISIBLE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Always returns true.");
        private static final Column HierarchyOrdinal =
            new Column(
                "HIERARCHY_ORDINAL",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The ordinal number of the hierarchy across all hierarchies of "
                + "the cube.");
        private static final Column DimensionIsShared =
            new Column(
                "DIMENSION_IS_SHARED",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Always returns true.");
        private static final Column Levels =
            new Column(
                "LEVELS",
                Type.Rowset,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Levels in this hierarchy.");


        /*
         * NOTE: This is non-standard, where did it come from?
         */
        private static final Column ParentChild =
            new Column(
                "PARENT_CHILD",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Is hierarchy a parent.");

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            DataSourcesConfig.DataSource ds =
                handler.getDataSource(request);
            DataSourcesConfig.Catalog[] catalogs =
                handler.getCatalogs(request, ds);
            String roleName = request.getRoleName();
            Role role = request.getRole();

            for (DataSourcesConfig.Catalog dsCatalog : catalogs) {
                if (dsCatalog == null || dsCatalog.definition == null) {
                    continue;
                }
                String catalogName = dsCatalog.name;
                if (!catalogRT.passes(catalogName)) {
                    continue;
                }

                Connection connection =
                    handler.getConnection(dsCatalog, role, roleName);
                if (connection == null) {
                    continue;
                }
                populateCatalog(connection, catalogName, rows);
            }
        }

        protected void populateCatalog(
            Connection connection,
            String catalogName,
            List<Row> rows)
            throws XmlaException
        {
            Schema schema = connection.getSchema();
            if (!schemaNameRT.passes(schema.getName())) {
                return;
            }
            for (Cube cube : sortedCubes(schema)) {
                if (!cubeNameRT.passes(cube.getName())) {
                    continue;
                }
                // RME
                //SchemaReader schemaReader = connection.getSchemaReader();
                // want to pick up cube's
                SchemaReader schemaReader =
                    cube.getSchemaReader(connection.getRole());
                populateCube(schemaReader, catalogName, cube, rows);
            }
        }

        protected void populateCube(
            SchemaReader schemaReader,
            String catalogName,
            Cube cube,
            List<Row> rows)
            throws XmlaException
        {
            int ordinal = 0;
            for (Dimension dimension : cube.getDimensions()) {
                String unique = dimension.getUniqueName();
                // Must increment ordinal for all dimensions but
                // only output some of them.
                boolean genOutput = dimensionUniqueNameRT.passes(unique);
                ordinal = populateDimension(
                    genOutput,
                    schemaReader, catalogName,
                    cube, dimension, ordinal, rows);
            }
        }

        protected int populateDimension(
            boolean genOutput,
            SchemaReader schemaReader,
            String catalogName,
            Cube cube,
            Dimension dimension,
            int ordinal,
            List<Row> rows)
            throws XmlaException
        {
            Hierarchy[] hierarchies = dimension.getHierarchies();
            for (Hierarchy hierarchy : hierarchies) {
                if (genOutput) {
                    String unique = hierarchy.getUniqueName();
                    if (hierarchyNameRT.passes(hierarchy.getName())
                        && hierarchyUniqueNameRT.passes(unique))
                    {
                        populateHierarchy(
                            schemaReader, catalogName,
                            cube, dimension, (HierarchyBase) hierarchy,
                            ordinal++, rows);
                    } else {
                        ordinal++;
                    }
                } else {
                    ordinal++;
                }
            }
            return ordinal;
        }

        protected void populateHierarchy(
            SchemaReader schemaReader,
            String catalogName,
            Cube cube,
            Dimension dimension,
            HierarchyBase hierarchy,
            int ordinal,
            List<Row> rows)
            throws XmlaException
        {
            // Access control
            if (!canAccess(schemaReader, hierarchy)) {
                return;
            }

            String desc = hierarchy.getDescription();
            if (desc == null) {
                desc =
                    cube.getName() + " Cube - "
                    + getHierarchyName(hierarchy) + " Hierarchy";
            }

            Row row = new Row();
            row.set(CatalogName.name, catalogName);
            row.set(SchemaName.name, cube.getSchema().getName());
            row.set(CubeName.name, cube.getName());
            row.set(DimensionUniqueName.name, dimension.getUniqueName());
            row.set(HierarchyName.name, hierarchy.getName());
            row.set(HierarchyUniqueName.name, hierarchy.getUniqueName());
            //row.set(HierarchyGuid.name, "");

            row.set(HierarchyCaption.name, hierarchy.getCaption());
            row.set(DimensionType.name, getDimensionType(dimension));
            // The number of members in the hierarchy. Because
            // of the presence of multiple hierarchies, this number
            // might not be the same as DIMENSION_CARDINALITY. This
            // value can be an approximation of the real
            // cardinality. Consumers should not assume that this
            // value is accurate.
            int cardinality =
                RolapMemberBase.getHierarchyCardinality(
                    schemaReader, hierarchy);
            row.set(HierarchyCardinality.name, cardinality);

            row.set(DefaultMember.name, hierarchy.getDefaultMember());
            if (hierarchy.hasAll()) {
                row.set(
                    AllMember.name,
                    Util.makeFqName(hierarchy, hierarchy.getAllMemberName()));
            }
            row.set(Description.name, desc);

            //TODO: only support:
            // MD_STRUCTURE_FULLYBALANCED (0)
            // MD_STRUCTURE_RAGGEDBALANCED (1)
            row.set(Structure.name, hierarchy.isRagged() ? 1 : 0);

            row.set(IsVirtual.name, false);
            row.set(IsReadWrite.name, false);

            // NOTE that SQL Server returns '0' not '1'.
            row.set(DimensionUniqueSettings.name, 0);

            // always true
            row.set(DimensionIsVisible.name, true);

            row.set(HierarchyOrdinal.name, ordinal);

            // always true
            row.set(DimensionIsShared.name, true);

            RolapLevel nonAllFirstLevel =
                (RolapLevel) hierarchy.getLevels()[
                    (hierarchy.hasAll() ? 1 : 0)];
            row.set(ParentChild.name, nonAllFirstLevel.isParentChild());
            if (deep) {
                row.set(
                    Levels.name,
                    new MdschemaLevelsRowset(
                        wrapRequest(
                            request,
                            org.olap4j.impl.Olap4jUtil.mapOf(
                                MdschemaLevelsRowset.CatalogName,
                                catalogName,
                                MdschemaLevelsRowset.SchemaName,
                                cube.getSchema().getName(),
                                MdschemaLevelsRowset.CubeName,
                                cube.getName(),
                                MdschemaLevelsRowset.DimensionUniqueName,
                                dimension.getUniqueName(),
                                MdschemaLevelsRowset.HierarchyUniqueName,
                                hierarchy.getUniqueName())),
                        handler));
            }
            addRow(row, rows);
        }

        protected void setProperty(
            PropertyDefinition propertyDef,
            String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class MdschemaLevelsRowset extends Rowset {
        private final RestrictionTest catalogRT;
        private final RestrictionTest schemaNameRT;
        private final RestrictionTest cubeNameRT;
        private final RestrictionTest dimensionUniqueNameRT;
        private final RestrictionTest hierarchyUniqueNameRT;
        private final RestrictionTest levelUniqueNameRT;
        private final RestrictionTest levelNameRT;

        MdschemaLevelsRowset(XmlaRequest request, XmlaHandler handler) {
            super(MDSCHEMA_LEVELS, request, handler);
            catalogRT = getRestrictionTest(CatalogName);
            schemaNameRT = getRestrictionTest(SchemaName);
            cubeNameRT = getRestrictionTest(CubeName);
            dimensionUniqueNameRT = getRestrictionTest(DimensionUniqueName);
            hierarchyUniqueNameRT = getRestrictionTest(HierarchyUniqueName);
            levelUniqueNameRT = getRestrictionTest(LevelUniqueName);
            levelNameRT = getRestrictionTest(LevelName);
        }

        public static final int MDLEVEL_TYPE_UNKNOWN = 0x0000;
        public static final int MDLEVEL_TYPE_REGULAR = 0x0000;
        public static final int MDLEVEL_TYPE_ALL = 0x0001;
        public static final int MDLEVEL_TYPE_CALCULATED = 0x0002;
        public static final int MDLEVEL_TYPE_TIME = 0x0004;
        public static final int MDLEVEL_TYPE_RESERVED1 = 0x0008;
        public static final int MDLEVEL_TYPE_TIME_YEARS = 0x0014;
        public static final int MDLEVEL_TYPE_TIME_HALF_YEAR = 0x0024;
        public static final int MDLEVEL_TYPE_TIME_QUARTERS = 0x0044;
        public static final int MDLEVEL_TYPE_TIME_MONTHS = 0x0084;
        public static final int MDLEVEL_TYPE_TIME_WEEKS = 0x0104;
        public static final int MDLEVEL_TYPE_TIME_DAYS = 0x0204;
        public static final int MDLEVEL_TYPE_TIME_HOURS = 0x0304;
        public static final int MDLEVEL_TYPE_TIME_MINUTES = 0x0404;
        public static final int MDLEVEL_TYPE_TIME_SECONDS = 0x0804;
        public static final int MDLEVEL_TYPE_TIME_UNDEFINED = 0x1004;

        private static final Column CatalogName =
            new Column(
                "CATALOG_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the catalog to which this level belongs.");
        private static final Column SchemaName =
            new Column(
                "SCHEMA_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the schema to which this level belongs.");
        private static final Column CubeName =
            new Column(
                "CUBE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the cube to which this level belongs.");
        private static final Column DimensionUniqueName =
            new Column(
                "DIMENSION_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The unique name of the dimension to which this level "
                + "belongs.");
        private static final Column HierarchyUniqueName =
            new Column(
                "HIERARCHY_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The unique name of the hierarchy.");
        private static final Column LevelName =
            new Column(
                "LEVEL_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the level.");
        private static final Column LevelUniqueName =
            new Column(
                "LEVEL_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The properly escaped unique name of the level.");
        private static final Column LevelGuid =
            new Column(
                "LEVEL_GUID",
                Type.UUID,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Level GUID.");
        private static final Column LevelCaption =
            new Column(
                "LEVEL_CAPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A label or caption associated with the hierarchy.");
        private static final Column LevelNumber =
            new Column(
                "LEVEL_NUMBER",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The distance of the level from the root of the hierarchy. "
                + "Root level is zero (0).");
        private static final Column LevelCardinality =
            new Column(
                "LEVEL_CARDINALITY",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The number of members in the level. This value can be an "
                + "approximation of the real cardinality.");
        private static final Column LevelType =
            new Column(
                "LEVEL_TYPE",
                Type.Integer,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Type of the level");
        private static final Column CustomRollupSettings =
            new Column(
                "CUSTOM_ROLLUP_SETTINGS",
                Type.Integer,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A bitmap that specifies the custom rollup options.");
        private static final Column LevelUniqueSettings =
            new Column(
                "LEVEL_UNIQUE_SETTINGS",
                Type.Integer,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A bitmap that specifies which columns contain unique values, "
                + "if the level only has members with unique names or keys.");
        private static final Column LevelIsVisible =
            new Column(
                "LEVEL_IS_VISIBLE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A Boolean that indicates whether the level is visible.");
        private static final Column Description =
            new Column(
                "DESCRIPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A human-readable description of the level. NULL if no "
                + "description exists.");

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            DataSourcesConfig.DataSource ds = handler.getDataSource(request);
            DataSourcesConfig.Catalog[] catalogs =
                handler.getCatalogs(request, ds);
            String roleName = request.getRoleName();
            Role role = request.getRole();

            for (DataSourcesConfig.Catalog dsCatalog : catalogs) {
                if (dsCatalog == null || dsCatalog.definition == null) {
                    continue;
                }
                String catalogName = dsCatalog.name;
                if (!catalogRT.passes(catalogName)) {
                    continue;
                }

                Connection connection =
                    handler.getConnection(dsCatalog, role, roleName);
                if (connection == null) {
                    continue;
                }
                populateCatalog(connection, catalogName, rows);
            }
        }

        protected void populateCatalog(
            Connection connection,
            String catalogName,
            List<Row> rows)
            throws XmlaException
        {
            Schema schema = connection.getSchema();
            if (!schemaNameRT.passes(schema.getName())) {
                return;
            }
            for (Cube cube : sortedCubes(schema)) {
                if (!cubeNameRT.passes(cube.getName())) {
                    continue;
                }
                SchemaReader schemaReader =
                    cube.getSchemaReader(
                        connection.getRole());
                populateCube(schemaReader, catalogName, cube, rows);
            }
        }

        protected void populateCube(
            SchemaReader schemaReader,
            String catalogName,
            Cube cube,
            List<Row> rows)
            throws XmlaException
        {
            for (Dimension dimension : cube.getDimensions()) {
                String uniqueName = dimension.getUniqueName();
                if (dimensionUniqueNameRT.passes(uniqueName)) {
                    populateDimension(
                        schemaReader, catalogName,
                        cube, dimension, rows);
                }
            }
        }

        protected void populateDimension(
            SchemaReader schemaReader,
            String catalogName,
            Cube cube,
            Dimension dimension,
            List<Row> rows)
            throws XmlaException
        {
            Hierarchy[] hierarchies = dimension.getHierarchies();
            for (Hierarchy hierarchy : hierarchies) {
                String uniqueName = hierarchy.getUniqueName();
                if (hierarchyUniqueNameRT.passes(uniqueName)) {
                    populateHierarchy(
                        schemaReader, catalogName,
                        cube, hierarchy, rows);
                }
            }
        }

        protected void populateHierarchy(
            SchemaReader schemaReader,
            String catalogName,
            Cube cube,
            Hierarchy hierarchy,
            List<Row> rows)
            throws XmlaException
        {
            for (Level level : schemaReader.getHierarchyLevels(hierarchy)) {
                String uniqueName = level.getUniqueName();
                String name = level.getName();
                if (levelUniqueNameRT.passes(uniqueName)
                    && levelNameRT.passes(name))
                {
                    outputLevel(
                        schemaReader,
                        catalogName, cube, hierarchy, level, rows);
                }
            }
        }

        /**
         * Outputs a level.
         *
         * @param schemaReader Schema reader
         * @param catalogName Catalog name
         * @param cube Cube definition
         * @param hierarchy Hierarchy
         * @param level Level
         * @param rows List of rows to output to
         * @return whether the level is visible
         * @throws XmlaException If error occurs
         */
        protected boolean outputLevel(
            SchemaReader schemaReader,
            String catalogName,
            Cube cube,
            Hierarchy hierarchy,
            Level level,
            List<Row> rows)
            throws XmlaException
        {
            // Access control
            if (!canAccess(schemaReader, level)) {
                return false;
            }
            String desc = level.getDescription();
            if (desc == null) {
                desc =
                    cube.getName() + " Cube - "
                    + getHierarchyName(hierarchy) + " Hierarchy - "
                    + level.getName() + " Level";
            }

            int adjustedLevelDepth = level.getDepth();
            Role.HierarchyAccess hierarchyAccess =
                schemaReader.getRole().getAccessDetails(hierarchy);
            if (hierarchyAccess != null) {
                adjustedLevelDepth -= hierarchyAccess.getTopLevelDepth();
            }

            Row row = new Row();
            row.set(CatalogName.name, catalogName);
            row.set(SchemaName.name, cube.getSchema().getName());
            row.set(CubeName.name, cube.getName());
            row.set(
                DimensionUniqueName.name,
                hierarchy.getDimension().getUniqueName());
            row.set(HierarchyUniqueName.name, hierarchy.getUniqueName());
            row.set(LevelName.name, level.getName());
            row.set(LevelUniqueName.name, level.getUniqueName());
            //row.set(LevelGuid.name, "");
            row.set(LevelCaption.name, level.getCaption());
            // see notes on this #getDepth()
            row.set(LevelNumber.name, adjustedLevelDepth);

            // Get level cardinality
            // According to microsoft this is:
            //   "The number of members in the level."
            int n = schemaReader.getLevelCardinality(level, true, true);
            row.set(LevelCardinality.name, n);

            row.set(LevelType.name, getLevelType(level));

            // TODO: most of the time this is correct
            row.set(CustomRollupSettings.name, 0);

            if (level instanceof RolapLevel) {
                RolapLevel rl = (RolapLevel) level;
                row.set(
                    LevelUniqueSettings.name,
                    (rl.isUnique() ? 1 : 0)
                    + (rl.isAll() ? 2 : 0));
            } else {
                // can not access unique member attribute
                // this is the best we can do.
                row.set(
                    LevelUniqueSettings.name,
                    (level.isAll() ? 2 : 0));
            }
            row.set(LevelIsVisible.name, true);
            row.set(Description.name, desc);
            addRow(row, rows);
            return true;
        }

        private int getLevelType(Level lev) {
            int ret = 0;

            if (lev.isAll()) {
                ret |= MDLEVEL_TYPE_ALL;
            }

            mondrian.olap.LevelType type = lev.getLevelType();
            switch (type) {
            case Regular:
                ret |= MDLEVEL_TYPE_REGULAR;
                break;
            case TimeYears:
                ret |= MDLEVEL_TYPE_TIME_YEARS;
                break;
            case TimeHalfYear:
                ret |= MDLEVEL_TYPE_TIME_HALF_YEAR;
                break;
            case TimeQuarters:
                ret |= MDLEVEL_TYPE_TIME_QUARTERS;
                break;
            case TimeMonths:
                ret |= MDLEVEL_TYPE_TIME_MONTHS;
                break;
            case TimeWeeks:
                ret |= MDLEVEL_TYPE_TIME_WEEKS;
                break;
            case TimeDays:
                ret |= MDLEVEL_TYPE_TIME_DAYS;
                break;
            case TimeHours:
                ret |= MDLEVEL_TYPE_TIME_HOURS;
                break;
            case TimeMinutes:
                ret |= MDLEVEL_TYPE_TIME_MINUTES;
                break;
            case TimeSeconds:
                ret |= MDLEVEL_TYPE_TIME_SECONDS;
                break;
            case TimeUndefined:
                ret |= MDLEVEL_TYPE_TIME_UNDEFINED;
                break;
            default:
                ret |= MDLEVEL_TYPE_UNKNOWN;
            }

            return ret;
        }

        protected void setProperty(
            PropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }


    static class MdschemaMeasuresRowset extends Rowset {
        public static final int MDMEASURE_AGGR_UNKNOWN = 0;
        public static final int MDMEASURE_AGGR_SUM = 1;
        public static final int MDMEASURE_AGGR_COUNT = 2;
        public static final int MDMEASURE_AGGR_MIN = 3;
        public static final int MDMEASURE_AGGR_MAX = 4;
        public static final int MDMEASURE_AGGR_AVG = 5;
        public static final int MDMEASURE_AGGR_VAR = 6;
        public static final int MDMEASURE_AGGR_STD = 7;
        public static final int MDMEASURE_AGGR_CALCULATED = 127;

        private final RestrictionTest catalogRT;
        private final RestrictionTest schemaNameRT;
        private final RestrictionTest cubeNameRT;
        private final RestrictionTest measureUniqueNameRT;
        private final RestrictionTest measureNameRT;

        MdschemaMeasuresRowset(XmlaRequest request, XmlaHandler handler) {
            super(MDSCHEMA_MEASURES, request, handler);
            catalogRT = getRestrictionTest(CatalogName);
            schemaNameRT = getRestrictionTest(SchemaName);
            cubeNameRT = getRestrictionTest(CubeName);
            measureNameRT = getRestrictionTest(MeasureName);
            measureUniqueNameRT = getRestrictionTest(MeasureUniqueName);
        }

        private static final Column CatalogName =
            new Column(
                "CATALOG_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the catalog to which this measure belongs.");
        private static final Column SchemaName =
            new Column(
                "SCHEMA_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the schema to which this measure belongs.");
        private static final Column CubeName =
            new Column(
                "CUBE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the cube to which this measure belongs.");
        private static final Column MeasureName =
            new Column(
                "MEASURE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The name of the measure.");
        private static final Column MeasureUniqueName =
            new Column(
                "MEASURE_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The Unique name of the measure.");
        private static final Column MeasureCaption =
            new Column(
                "MEASURE_CAPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A label or caption associated with the measure.");
        private static final Column MeasureGuid =
            new Column(
                "MEASURE_GUID",
                Type.UUID,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Measure GUID.");
        private static final Column MeasureAggregator =
            new Column(
                "MEASURE_AGGREGATOR",
                Type.Integer,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "How a measure was derived.");
        private static final Column DataType =
            new Column(
                "DATA_TYPE",
                Type.UnsignedShort,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Data type of the measure.");
        private static final Column MeasureIsVisible =
            new Column(
                "MEASURE_IS_VISIBLE",
                Type.Boolean,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A Boolean that always returns True. If the measure is not "
                + "visible, it will not be included in the schema rowset.");
        private static final Column LevelsList =
            new Column(
                "LEVELS_LIST",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A string that always returns NULL. EXCEPT that SQL Server "
                + "returns non-null values!!!");
        private static final Column Description =
            new Column(
                "DESCRIPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A human-readable description of the measure.");
        private static final Column FormatString =
            new Column(
                "DEFAULT_FORMAT_STRING",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "The default format string for the measure.");

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            DataSourcesConfig.DataSource ds = handler.getDataSource(request);
            DataSourcesConfig.Catalog[] catalogs =
                handler.getCatalogs(request, ds);
            String roleName = request.getRoleName();
            Role role = request.getRole();

            for (DataSourcesConfig.Catalog dsCatalog : catalogs) {
                if (dsCatalog == null || dsCatalog.definition == null) {
                    continue;
                }
                Connection connection =
                    handler.getConnection(dsCatalog, role, roleName);
                if (connection == null) {
                    continue;
                }

                String catalogName = dsCatalog.name;
                if (catalogRT.passes(catalogName)) {
                    populateCatalog(connection, catalogName, rows);
                }
            }
        }

        protected void populateCatalog(
            Connection connection,
            String catalogName,
            List<Row> rows)
            throws XmlaException
        {
            // SQL Server actually includes the LEVELS_LIST row
            StringBuilder buf = new StringBuilder(100);

            Schema schema = connection.getSchema();
            if (!schemaNameRT.passes(schema.getName())) {
                return;
            }
            for (Cube cube : sortedCubes(schema)) {
                if (cubeNameRT.passes(cube.getName())) {
                    SchemaReader schemaReader =
                        cube.getSchemaReader(
                            connection.getRole());
                    Dimension measuresDimension = cube.getDimensions()[0];
                    Hierarchy measuresHierarchy =
                        measuresDimension.getHierarchies()[0];
                    Level measuresLevel =
                        measuresHierarchy.getLevels()[0];

                    buf.setLength(0);

                    int j = 0;
                    for (Dimension dimension : cube.getDimensions()) {
                        if (dimension.isMeasures()) {
                            continue;
                        }
                        for (Hierarchy hierarchy : dimension.getHierarchies()) {
                            Level[] levels = hierarchy.getLevels();
                            Level lastLevel = levels[levels.length - 1];
                            if (j++ > 0) {
                                buf.append(',');
                            }
                            buf.append(lastLevel.getUniqueName());
                        }
                    }
                    String levelListStr = buf.toString();

                    List<Member> storedMembers =
                        schemaReader.getLevelMembers(measuresLevel, false);
                    for (Member member : storedMembers) {
                        String name = member.getName();
                        String unique = member.getUniqueName();
                        if (measureNameRT.passes(name)
                            && measureUniqueNameRT.passes(unique))
                        {
                            populateMember(
                                schemaReader, catalogName,
                                member, cube, levelListStr, rows);
                        }
                    }

                    for (Member member
                        : schemaReader.getCalculatedMembers(measuresHierarchy))
                    {
                        String name = member.getName();
                        String unique = member.getUniqueName();
                        if (measureNameRT.passes(name)
                            && measureUniqueNameRT.passes(unique))
                        {
                            populateMember(
                                schemaReader, catalogName,
                                member, cube, null, rows);
                        }
                    }
                }
            }
        }

        private void populateMember(
            SchemaReader schemaReader,
            String catalogName,
            Member member,
            Cube cube,
            String levelListStr,
            List<Row> rows)
        {
            // Access control
            if (!canAccess(schemaReader, member)) {
                return;
            }

            Boolean visible =
                (Boolean) member.getPropertyValue(Property.VISIBLE.name);
            if (visible == null) {
                visible = true;
            }
            if (!EMIT_INVISIBLE_MEMBERS && !visible) {
                return;
            }

            //TODO: currently this is always null
            String desc = member.getDescription();
            if (desc == null) {
                desc =
                    cube.getName() + " Cube - "
                    + member.getName() + " Member";
            }
            final String formatString =
                (String) member.getPropertyValue(Property.FORMAT_STRING.name);

            Row row = new Row();
            row.set(CatalogName.name, catalogName);
            row.set(SchemaName.name, cube.getSchema().getName());
            row.set(CubeName.name, cube.getName());
            row.set(MeasureName.name, member.getName());
            row.set(MeasureUniqueName.name, member.getUniqueName());
            row.set(MeasureCaption.name, member.getCaption());
            //row.set(MeasureGuid.name, "");

            Object aggProp =
                member.getPropertyValue(Property.AGGREGATION_TYPE.getName());
            int aggNumber = MDMEASURE_AGGR_UNKNOWN;
            if (aggProp != null) {
                RolapAggregator agg = (RolapAggregator) aggProp;
                if (agg == RolapAggregator.Sum) {
                    aggNumber = MDMEASURE_AGGR_SUM;
                } else if (agg == RolapAggregator.Count) {
                    aggNumber = MDMEASURE_AGGR_COUNT;
                } else if (agg == RolapAggregator.Min) {
                    aggNumber = MDMEASURE_AGGR_MIN;
                } else if (agg == RolapAggregator.Max) {
                    aggNumber = MDMEASURE_AGGR_MAX;
                } else if (agg == RolapAggregator.Avg) {
                    aggNumber = MDMEASURE_AGGR_AVG;
                }
                //TODO: what are VAR and STD
            } else {
                aggNumber = MDMEASURE_AGGR_CALCULATED;
            }
            row.set(MeasureAggregator.name, aggNumber);

            // DATA_TYPE DBType best guess is string
            XmlaConstants.DBType dbType = XmlaConstants.DBType.WSTR;
            String datatype = (String)
                member.getPropertyValue(Property.DATATYPE.getName());
            if (datatype != null) {
                if (datatype.equals("Integer")) {
                    dbType = XmlaConstants.DBType.I4;
                } else if (datatype.equals("Numeric")) {
                    dbType = XmlaConstants.DBType.R8;
                } else {
                    dbType = XmlaConstants.DBType.WSTR;
                }
            }
            row.set(DataType.name, dbType.xmlaOrdinal());
            row.set(MeasureIsVisible.name, visible);

            if (levelListStr != null) {
                row.set(LevelsList.name, levelListStr);
            }

            row.set(Description.name, desc);
            row.set(FormatString.name, formatString);
            addRow(row, rows);
        }

        protected void setProperty(
            PropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class MdschemaMembersRowset extends Rowset {
        private final RestrictionTest catalogRT;
        private final RestrictionTest schemaNameRT;
        private final RestrictionTest cubeNameRT;
        private final RestrictionTest dimensionUniqueNameRT;
        private final RestrictionTest hierarchyUniqueNameRT;
        private final RestrictionTest memberNameRT;
        private final RestrictionTest memberUniqueNameRT;
        private final RestrictionTest memberTypeRT;

        MdschemaMembersRowset(XmlaRequest request, XmlaHandler handler) {
            super(MDSCHEMA_MEMBERS, request, handler);
            catalogRT = getRestrictionTest(CatalogName);
            schemaNameRT = getRestrictionTest(SchemaName);
            cubeNameRT = getRestrictionTest(CubeName);
            dimensionUniqueNameRT = getRestrictionTest(DimensionUniqueName);
            hierarchyUniqueNameRT = getRestrictionTest(HierarchyUniqueName);
            memberNameRT = getRestrictionTest(MemberName);
            memberUniqueNameRT = getRestrictionTest(MemberUniqueName);
            memberTypeRT = getRestrictionTest(MemberType);
        }

        private static final Column CatalogName =
            new Column(
                "CATALOG_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the catalog to which this member belongs.");
        private static final Column SchemaName =
            new Column(
                "SCHEMA_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the schema to which this member belongs.");
        private static final Column CubeName =
            new Column(
                "CUBE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Name of the cube to which this member belongs.");
        private static final Column DimensionUniqueName =
            new Column(
                "DIMENSION_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Unique name of the dimension to which this member belongs.");
        private static final Column HierarchyUniqueName =
            new Column(
                "HIERARCHY_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Unique name of the hierarchy. If the member belongs to more "
                + "than one hierarchy, there is one row for each hierarchy to "
                + "which it belongs.");
        private static final Column LevelUniqueName =
            new Column(
                "LEVEL_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                " Unique name of the level to which the member belongs.");
        private static final Column LevelNumber =
            new Column(
                "LEVEL_NUMBER",
                Type.UnsignedInteger,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "The distance of the member from the root of the hierarchy.");
        private static final Column MemberOrdinal =
            new Column(
                "MEMBER_ORDINAL",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Ordinal number of the member. Sort rank of the member when "
                + "members of this dimension are sorted in their natural sort "
                + "order. If providers do not have the concept of natural "
                + "ordering, this should be the rank when sorted by "
                + "MEMBER_NAME.");
        private static final Column MemberName =
            new Column(
                "MEMBER_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Name of the member.");
        private static final Column MemberUniqueName =
            new Column(
                "MEMBER_UNIQUE_NAME",
                Type.StringSometimesArray,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                " Unique name of the member.");
        private static final Column MemberType =
            new Column(
                "MEMBER_TYPE",
                Type.Integer,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Type of the member.");
        private static final Column MemberGuid =
            new Column(
                "MEMBER_GUID",
                Type.UUID,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Memeber GUID.");
        private static final Column MemberCaption =
            new Column(
                "MEMBER_CAPTION",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "A label or caption associated with the member.");
        private static final Column ChildrenCardinality =
            new Column(
                "CHILDREN_CARDINALITY",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Number of children that the member has.");
        private static final Column ParentLevel =
            new Column(
                "PARENT_LEVEL",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "The distance of the member's parent from the root level of "
                + "the hierarchy.");
        private static final Column ParentUniqueName =
            new Column(
                "PARENT_UNIQUE_NAME",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "Unique name of the member's parent.");
        private static final Column ParentCount =
            new Column(
                "PARENT_COUNT",
                Type.UnsignedInteger,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Number of parents that this member has.");
        private static final Column TreeOp_ =
            new Column(
                "TREE_OP",
                Type.Enumeration,
                Enumeration.TREE_OP,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "Tree Operation");
        /* Mondrian specified member properties. */
        private static final Column Depth =
            new Column(
                "DEPTH",
                Type.Integer,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "depth");

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            DataSourcesConfig.DataSource ds =
                handler.getDataSource(request);
            DataSourcesConfig.Catalog[] catalogs =
                handler.getCatalogs(request, ds);
            String roleName = request.getRoleName();
            Role role = request.getRole();

            for (DataSourcesConfig.Catalog dsCatalog : catalogs) {
                if (dsCatalog == null || dsCatalog.definition == null) {
                    continue;
                }
                Connection connection =
                    handler.getConnection(dsCatalog, role, roleName);
                if (connection == null) {
                    continue;
                }

                String catalogName = dsCatalog.name;
                if (catalogRT.passes(catalogName)) {
                    populateCatalog(connection, catalogName, rows);
                }
            }
        }

        protected void populateCatalog(
            Connection connection,
            String catalogName,
            List<Row> rows)
            throws XmlaException
        {
            Schema schema = connection.getSchema();
            if (!schemaNameRT.passes(schema.getName())) {
                return;
            }
            for (Cube cube : sortedCubes(schema)) {
                if (cubeNameRT.passes(cube.getName())) {
                    if (isRestricted(MemberUniqueName)) {
                        // NOTE: it is believed that if MEMBER_UNIQUE_NAME is
                        // a restriction, then none of the remaining possible
                        // restrictions other than TREE_OP are relevant
                        // (or allowed??).
                        SchemaReader schemaReader = cube.getSchemaReader(null);
                        outputUniqueMemberName(
                            schemaReader, catalogName, cube, rows);
                    } else {
                        SchemaReader schemaReader =
                            cube.getSchemaReader(
                                connection.getRole());
                        populateCube(schemaReader, catalogName, cube, rows);
                    }
                }
            }
        }

        protected void populateCube(
            SchemaReader schemaReader,
            String catalogName,
            Cube cube,
            List<Row> rows)
            throws XmlaException
        {
            if (isRestricted(LevelUniqueName)) {
                // Note: If the LEVEL_UNIQUE_NAME has been specified, then
                // the dimension and hierarchy are specified implicitly.
                String levelUniqueName =
                    getRestrictionValueAsString(LevelUniqueName);
                if (levelUniqueName == null) {
                    // The query specified two or more unique names
                    // which means that nothing will match.
                    return;
                }

                final List<Id.Segment> nameParts =
                    Util.parseIdentifier(levelUniqueName);
                Hierarchy hier = cube.lookupHierarchy(nameParts.get(0), false);
                if (hier == null) {
                    return;
                }
                Level[] levels = hier.getLevels();
                for (Level level : levels) {
                    if (!level.getUniqueName().equals(levelUniqueName)) {
                        continue;
                    }
                    // Get members of this level, without access control, but
                    // including calculated members.
                    List<Member> members =
                        cube.getSchemaReader(null).getLevelMembers(level, true);
                    outputMembers(
                        schemaReader, members, catalogName, cube, rows);
                }
            } else {
                for (Dimension dimension : cube.getDimensions()) {
                    String uniqueName = dimension.getUniqueName();
                    if (dimensionUniqueNameRT.passes(uniqueName)) {
                        populateDimension(
                            schemaReader, catalogName,
                            cube, dimension, rows);
                    }
                }
            }
        }

        protected void populateDimension(
            SchemaReader schemaReader,
            String catalogName,
            Cube cube,
            Dimension dimension,
            List<Row> rows)
            throws XmlaException
        {
            Hierarchy[] hierarchies = dimension.getHierarchies();
            for (Hierarchy hierarchy : hierarchies) {
                String uniqueName = hierarchy.getUniqueName();
                if (hierarchyUniqueNameRT.passes(uniqueName)) {
                    populateHierarchy(
                        schemaReader, catalogName,
                        cube, hierarchy, rows);
                }
            }
        }

        protected void populateHierarchy(
            SchemaReader schemaReader,
            String catalogName,
            Cube cube,
            Hierarchy hierarchy,
            List<Row> rows)
            throws XmlaException
        {
            if (isRestricted(LevelNumber)) {
                int levelNumber = getRestrictionValueAsInt(LevelNumber);
                if (levelNumber == -1) {
                    LOGGER.warn(
                        "RowsetDefinition.populateHierarchy: "
                        + "LevelNumber invalid");
                    return;
                }
                Level[] levels = hierarchy.getLevels();
                if (levelNumber >= levels.length) {
                    LOGGER.warn(
                        "RowsetDefinition.populateHierarchy: "
                        + "LevelNumber ("
                        + levelNumber
                        + ") is greater than number of levels ("
                        + levels.length
                        + ") for hierarchy \""
                        + hierarchy.getUniqueName()
                        + "\"");
                    return;
                }

                Level level = levels[levelNumber];
                List<Member> members =
                    schemaReader.getLevelMembers(level, false);
                outputMembers(schemaReader, members, catalogName, cube, rows);
            } else {
                // At this point we get ALL of the members associated with
                // the Hierarchy (rather than getting them one at a time).
                // The value returned is not used at this point but they are
                // now cached in the SchemaReader.
                List<List<Member>> membersArray =
                    RolapMemberBase.getAllMembers(schemaReader, hierarchy);
                for (List<Member> members : membersArray) {
                    outputMembers(
                        schemaReader, members,
                        catalogName, cube, rows);
                }
            }
        }

        /**
         * Returns whether a value contains all of the bits in a mask.
         */
        private static boolean mask(int value, int mask) {
            return (value & mask) == mask;
        }

        /**
         * Adds a member to a result list and, depending upon the
         * <code>treeOp</code> parameter, other relatives of the member. This
         * method recursively invokes itself to walk up, down, or across the
         * hierarchy.
         */
        private void populateMember(
            final SchemaReader schemaReader,
            String catalogName,
            Cube cube,
            Member member,
            int treeOp,
            List<Row> rows)
        {
            // Visit node itself.
            if (mask(treeOp, TreeOp.SELF.xmlaOrdinal())) {
                outputMember(schemaReader, member, catalogName, cube, rows);
            }
            // Visit node's siblings (not including itself).
            if (mask(treeOp, TreeOp.SIBLINGS.xmlaOrdinal())) {
                final Member parent =
                    schemaReader.getMemberParent(member);
                final List<Member> siblings =
                    (parent == null)
                        ? schemaReader.getHierarchyRootMembers(
                        member.getHierarchy())
                        : schemaReader.getMemberChildren(parent);

                for (Member sibling : siblings) {
                    if (sibling.equals(member)) {
                        continue;
                    }
                    populateMember(
                        schemaReader, catalogName,
                        cube, sibling,
                        TreeOp.SELF.xmlaOrdinal(), rows);
                }
            }
            // Visit node's descendants or its immediate children, but not both.
            if (mask(treeOp, TreeOp.DESCENDANTS.xmlaOrdinal())) {
                final List<Member> children =
                    schemaReader.getMemberChildren(member);
                for (Member child : children) {
                    populateMember(
                        schemaReader, catalogName,
                        cube, child,
                        TreeOp.SELF.xmlaOrdinal() |
                        TreeOp.DESCENDANTS.xmlaOrdinal(),
                        rows);
                }
            } else if (mask(
                treeOp, TreeOp.CHILDREN.xmlaOrdinal()))
            {
                final List<Member> children =
                    schemaReader.getMemberChildren(member);
                for (Member child : children) {
                    populateMember(
                        schemaReader, catalogName,
                        cube, child,
                        TreeOp.SELF.xmlaOrdinal(), rows);
                }
            }
            // Visit node's ancestors or its immediate parent, but not both.
            if (mask(treeOp, TreeOp.ANCESTORS.xmlaOrdinal())) {
                final Member parent = schemaReader.getMemberParent(member);
                if (parent != null) {
                    populateMember(
                        schemaReader, catalogName,
                        cube, parent,
                        TreeOp.SELF.xmlaOrdinal() |
                        TreeOp.ANCESTORS.xmlaOrdinal(), rows);
                }
            } else if (mask(treeOp, TreeOp.PARENT.xmlaOrdinal())) {
                final Member parent = schemaReader.getMemberParent(member);
                if (parent != null) {
                    populateMember(
                        schemaReader, catalogName,
                        cube, parent,
                        TreeOp.SELF.xmlaOrdinal(), rows);
                }
            }
        }

        protected ArrayList<Column> pruneRestrictions(ArrayList<Column> list) {
            // If they've restricted TreeOp, we don't want to literally filter
            // the result on TreeOp (because it's not an output column) or
            // on MemberUniqueName (because TreeOp will have caused us to
            // generate other members than the one asked for).
            if (list.contains(TreeOp_)) {
                list.remove(TreeOp_);
                list.remove(MemberUniqueName);
            }
            return list;
        }

        private void outputMembers(
            final SchemaReader schemaReader,
            List<Member> members,
            final String catalogName,
            Cube cube, List<Row> rows)
        {
            for (Member member : members) {
                outputMember(schemaReader, member, catalogName, cube, rows);
            }
        }

        private void outputUniqueMemberName(
            final SchemaReader schemaReader,
            final String catalogName,
            Cube cube,
            List<Row> rows)
        {
            final Object unameRestrictions =
                restrictions.get(MemberUniqueName.name);
            List<String> list;
            if (unameRestrictions instanceof String) {
                list = Collections.singletonList((String) unameRestrictions);
            } else {
                list = (List<String>) unameRestrictions;
            }
            for (String memberUniqueName : list) {
                final List<Id.Segment> nameParts =
                    Util.parseIdentifier(memberUniqueName);

                Member member = schemaReader.getMemberByUniqueName(
                    nameParts, false);

                if (member == null) {
                    return;
                }
                if (isRestricted(TreeOp_)) {
                    int treeOp = getRestrictionValueAsInt(TreeOp_);
                    if (treeOp == -1) {
                        return;
                    }
                    populateMember(
                        schemaReader, catalogName,
                        cube, member, treeOp, rows);
                } else {
                    outputMember(
                        schemaReader, member, catalogName, cube, rows);
                }
            }
        }

        private void outputMember(
            final SchemaReader schemaReader,
            Member member,
            final String catalogName,
            Cube cube,
            List<Row> rows)
        {
            // Access control
            if (!canAccess(schemaReader, member)) {
                return;
            }
            if (! memberNameRT.passes(member.getName())) {
                return;
            }
            if (! memberTypeRT.passes(member.getMemberType())) {
                return;
            }

            if (member.getOrdinal() == -1) {
                RolapMemberBase.setOrdinals(schemaReader, member);
            }

            // Check whether the member is visible, otherwise do not dump.
            Boolean visible =
                (Boolean) member.getPropertyValue(Property.VISIBLE.name);
            if (visible == null) {
                visible = true;
            }
            if (!EMIT_INVISIBLE_MEMBERS && !visible) {
                return;
            }

            final Level level = member.getLevel();
            final Hierarchy hierarchy = level.getHierarchy();
            final Dimension dimension = hierarchy.getDimension();

            int adjustedLevelDepth = level.getDepth();
            Role.HierarchyAccess hierarchyAccess =
                schemaReader.getRole().getAccessDetails(hierarchy);
            if (hierarchyAccess != null) {
                adjustedLevelDepth -= hierarchyAccess.getTopLevelDepth();
            }

            Row row = new Row();
            row.set(CatalogName.name, catalogName);
            row.set(SchemaName.name, cube.getSchema().getName());
            row.set(CubeName.name, cube.getName());
            row.set(DimensionUniqueName.name, dimension.getUniqueName());
            row.set(HierarchyUniqueName.name, hierarchy.getUniqueName());
            row.set(LevelUniqueName.name, level.getUniqueName());
            row.set(LevelNumber.name, adjustedLevelDepth);
            row.set(MemberOrdinal.name, false ? 0 : member.getOrdinal());
            row.set(MemberName.name, member.getName());
            row.set(MemberUniqueName.name, member.getUniqueName());
            row.set(MemberType.name, member.getMemberType().ordinal());
            //row.set(MemberGuid.name, "");
            row.set(MemberCaption.name, member.getCaption());
            row.set(
                ChildrenCardinality.name,
                member.getPropertyValue(Property.CHILDREN_CARDINALITY.name));
            row.set(ChildrenCardinality.name, 100);

            if (adjustedLevelDepth == 0) {
                row.set(ParentLevel.name, 0);
            } else {
                row.set(ParentLevel.name, adjustedLevelDepth - 1);
                final Member parentMember = member.getParentMember();
                if (parentMember != null) {
                    row.set(
                        ParentUniqueName.name, parentMember.getUniqueName());
                }
            }

            row.set(ParentCount.name, member.getParentMember() == null ? 0 : 1);

            row.set(Depth.name, member.getDepth());
            addRow(row, rows);
        }

        protected void setProperty(
            PropertyDefinition propertyDef,
            String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class MdschemaSetsRowset extends Rowset {
        private final RestrictionTest catalogRT;
        private final RestrictionTest schemaNameRT;
        private final RestrictionTest cubeNameRT;
        private final RestrictionTest setNameRT;
        private static final String GLOBAL_SCOPE = "1";

        MdschemaSetsRowset(XmlaRequest request, XmlaHandler handler) {
            super(MDSCHEMA_SETS, request, handler);
            catalogRT = getRestrictionTest(CatalogName);
            schemaNameRT = getRestrictionTest(SchemaName);
            cubeNameRT = getRestrictionTest(CubeName);
            setNameRT = getRestrictionTest(SetName);
        }

        private static final Column CatalogName =
            new Column(
                "CATALOG_NAME",
                Type.String,
                null,
                true,
                true,
                null);
        private static final Column SchemaName =
            new Column(
                "SCHEMA_NAME",
                Type.String,
                null,
                true,
                true,
                null);
        private static final Column CubeName =
            new Column(
                "CUBE_NAME",
                Type.String,
                null,
                true,
                false,
                null);
        private static final Column SetName =
            new Column(
                "SET_NAME",
                Type.String,
                null,
                true,
                false,
                null);
        private static final Column SetCaption =
            new Column(
                "SET_CAPTION",
                Type.String,
                null,
                true,
                true,
                null);
        private static final Column Scope =
            new Column(
                "SCOPE",
                Type.Integer,
                null,
                true,
                false,
                null);
        private static final Column Description =
            new Column(
                "DESCRIPTION",
                Type.String,
                null,
                false,
                true,
                "A human-readable description of the measure.");

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            DataSourcesConfig.DataSource ds =
                handler.getDataSource(request);
            DataSourcesConfig.Catalog[] catalogs =
                handler.getCatalogs(request, ds);
            String roleName = request.getRoleName();
            Role role = request.getRole();

            for (DataSourcesConfig.Catalog dsCatalog : catalogs) {
                if (dsCatalog == null || dsCatalog.definition == null) {
                    continue;
                }
                Connection connection =
                    handler.getConnection(dsCatalog, role, roleName);
                if (connection == null) {
                    continue;
                }

                String catalogName = dsCatalog.name;
                if (!catalogRT.passes(catalogName)) {
                    continue;
                }
                processCatalog(connection, catalogName, rows);
            }
        }

        private void processCatalog(
            Connection connection,
            String catalogName,
            List<Row> rows)
        {
            Schema schema = connection.getSchema();
            if (!schemaNameRT.passes(schema.getName())) {
                return;
            }
            Cube[] cubes = connection.getSchemaReader().getCubes();
            for (Cube cube : cubes) {
                if (!cubeNameRT.passes(cube.getName())) {
                    continue;
                }
                populateNamedSets(cube, catalogName, rows);
            }
        }

        private void populateNamedSets(
            Cube cube,
            String catalogName,
            List<Row> rows)
        {
            for (NamedSet namedSet : cube.getNamedSets()) {
                if (!setNameRT.passes(namedSet.getUniqueName())) {
                    continue;
                }
                Row row = new Row();
                row.set(CatalogName.name, catalogName);
                row.set(SchemaName.name, cube.getSchema().getName());
                row.set(CubeName.name, cube.getName());
                row.set(SetName.name, namedSet.getUniqueName());
                row.set(Scope.name, GLOBAL_SCOPE);
                row.set(Description.name, namedSet.getDescription());
                addRow(row, rows);
            }
        }
    }

    static class MdschemaPropertiesRowset extends Rowset {
        private final RestrictionTest catalogRT;
        private final RestrictionTest schemaNameRT;
        private final RestrictionTest cubeNameRT;
        private final RestrictionTest dimensionUniqueNameRT;
        private final RestrictionTest hierarchyUniqueNameRT;
        private final RestrictionTest propertyNameRT;

        MdschemaPropertiesRowset(XmlaRequest request, XmlaHandler handler) {
            super(MDSCHEMA_PROPERTIES, request, handler);
            catalogRT = getRestrictionTest(CatalogName);
            schemaNameRT = getRestrictionTest(SchemaName);
            cubeNameRT = getRestrictionTest(CubeName);
            dimensionUniqueNameRT = getRestrictionTest(DimensionUniqueName);
            hierarchyUniqueNameRT = getRestrictionTest(HierarchyUniqueName);
            propertyNameRT = getRestrictionTest(PropertyName);
        }

        private static final Column CatalogName =
            new Column(
                "CATALOG_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the database.");
        private static final Column SchemaName =
            new Column(
                "SCHEMA_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the schema to which this property belongs.");
        private static final Column CubeName =
            new Column(
                "CUBE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The name of the cube.");
        private static final Column DimensionUniqueName =
            new Column(
                "DIMENSION_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The unique name of the dimension.");
        private static final Column HierarchyUniqueName =
            new Column(
                "HIERARCHY_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The unique name of the hierarchy.");
        private static final Column LevelUniqueName =
            new Column(
                "LEVEL_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The unique name of the level to which this property belongs.");
        // According to MS this should not be nullable
        private static final Column MemberUniqueName =
            new Column(
                "MEMBER_UNIQUE_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The unique name of the member to which the property belongs.");
        private static final Column PropertyName =
            new Column(
                "PROPERTY_NAME",
                Type.String,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "Name of the property.");
        private static final Column PropertyType =
            new Column(
                "PROPERTY_TYPE",
                Type.Short,
                null,
                Column.RESTRICTION,
                Column.REQUIRED,
                "A bitmap that specifies the type of the property");
        private static final Column PropertyCaption =
            new Column(
                "PROPERTY_CAPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "A label or caption associated with the property, used "
                + "primarily for display purposes.");
        private static final Column DataType =
            new Column(
                "DATA_TYPE",
                Type.UnsignedShort,
                null,
                Column.NOT_RESTRICTION,
                Column.REQUIRED,
                "Data type of the property.");
        private static final Column PropertyContentType =
            new Column(
                "PROPERTY_CONTENT_TYPE",
                Type.Short,
                null,
                Column.RESTRICTION,
                Column.OPTIONAL,
                "The type of the property.");
        private static final Column Description =
            new Column(
                "DESCRIPTION",
                Type.String,
                null,
                Column.NOT_RESTRICTION,
                Column.OPTIONAL,
                "A human-readable description of the measure.");

        private void populate2(List<Row> rows)
        {
            DataSourcesConfig.DataSource ds =
                handler.getDataSource(request);
            DataSourcesConfig.Catalog[] catalogs =
                handler.getCatalogs(request, ds);
            String roleName = request.getRoleName();
            Role role = request.getRole();

            for (DataSourcesConfig.Catalog dsCatalog : catalogs) {
                if (dsCatalog == null || dsCatalog.definition == null) {
                    continue;
                }
                Connection connection =
                    handler.getConnection(dsCatalog, role, roleName);
                if (connection == null) {
                    continue;
                }

                String catalogName = dsCatalog.name;
                if (catalogRT.passes(catalogName)) {
                    populateCatalog(connection, catalogName, rows);
                }
            }
        }

        public void populate(
            XmlaResponse response,
            List<Row> rows)
            throws XmlaException
        {
            // Default PROPERTY_TYPE is MDPROP_MEMBER.
            @SuppressWarnings({"unchecked"})
            final List<String> list =
                (List<String>) restrictions.get(PropertyType.name);
            Set<org.olap4j.metadata.Property.TypeFlag> typeFlags;
            if (list == null) {
                typeFlags =
                    Olap4jUtil.enumSetOf(
                        org.olap4j.metadata.Property.TypeFlag.MEMBER);
            } else {
                typeFlags =
                    org.olap4j.metadata.Property.TypeFlag.getDictionary()
                        .forMask(
                        Integer.valueOf(list.get(0)));
            }

            for (org.olap4j.metadata.Property.TypeFlag typeFlag : typeFlags) {
                switch (typeFlag) {
                case MEMBER:
                    populate2(rows);
                    break;
                case CELL:
                    populateCell(rows);
                    break;
                case SYSTEM:
                case BLOB:
                default:
                    break;
                }
            }
        }

        private void populateCell(List<Row> rows) {
            for (org.olap4j.metadata.Property.StandardCellProperty property
                : org.olap4j.metadata.Property.StandardCellProperty.values())
            {
                Row row = new Row();
                row.set(
                    PropertyType.name,
                    org.olap4j.metadata.Property.TypeFlag.getDictionary()
                        .toMask(
                            property.getType()));
                row.set(PropertyName.name, property.name());
                row.set(PropertyCaption.name, property.getCaption(null));
                row.set(DataType.name, property.getDatatype().xmlaOrdinal());
                addRow(row, rows);
            }
        }

        protected void populateCatalog(
            Connection connection,
            String catalogName,
            List<Row> rows)
            throws XmlaException
        {
            Schema schema = connection.getSchema();
            if (!schemaNameRT.passes(schema.getName())) {
                return;
            }
            for (Cube cube : sortedCubes(schema)) {
                if (cubeNameRT.passes(cube.getName())) {
                    SchemaReader schemaReader =
                        cube.getSchemaReader(
                            connection.getRole());
                    populateCube(schemaReader, catalogName, cube, rows);
                }
            }
        }

        protected void populateCube(
            SchemaReader schemaReader,
            String catalogName,
            Cube cube,
            List<Row> rows)
            throws XmlaException
        {
            if (isRestricted(LevelUniqueName)) {
                // Note: If the LEVEL_UNIQUE_NAME has been specified, then
                // the dimension and hierarchy are specified implicitly.
                String levelUniqueName =
                    getRestrictionValueAsString(LevelUniqueName);
                if (levelUniqueName == null) {
                    // The query specified two or more unique names
                    // which means that nothing will match.
                    return;
                }
                final List<Id.Segment> nameParts =
                    Util.parseIdentifier(levelUniqueName);
                Hierarchy hier = cube.lookupHierarchy(nameParts.get(0), false);
                if (hier == null) {
                    return;
                }
                for (Level level : schemaReader.getHierarchyLevels(hier)) {
                    if (level.getUniqueName().equals(levelUniqueName)) {
                        populateLevel(
                            schemaReader, catalogName,
                            cube, level, rows);
                        break;
                    }
                }

            } else {
                for (Dimension dimension : cube.getDimensions()) {
                    String uniqueName = dimension.getUniqueName();
                    if (dimensionUniqueNameRT.passes(uniqueName)) {
                        populateDimension(
                            schemaReader, catalogName,
                            cube, dimension, rows);
                    }
                }
            }
        }

        private void populateDimension(
            final SchemaReader schemaReader,
            final String catalogName,
            Cube cube,
            Dimension dimension,
            List<Row> rows)
        {
            Hierarchy[] hierarchies = dimension.getHierarchies();
            for (Hierarchy hierarchy : hierarchies) {
                String unique = hierarchy.getUniqueName();
                if (hierarchyUniqueNameRT.passes(unique)) {
                    populateHierarchy(
                        schemaReader, catalogName,
                        cube, hierarchy, rows);
                }
            }
        }

        private void populateHierarchy(
            final SchemaReader schemaReader,
            final String catalogName,
            Cube cube,
            Hierarchy hierarchy,
            List<Row> rows)
        {
            for (Level level : schemaReader.getHierarchyLevels(hierarchy)) {
                populateLevel(
                    schemaReader, catalogName,
                    cube, level, rows);
            }
        }

        private void populateLevel(
            final SchemaReader schemaReader,
            final String catalogName,
            Cube cube,
            Level level,
            List<Row> rows)
        {
            Property[] properties = level.getProperties();
            for (Property property : properties) {
                if (property.isInternal()) {
                    continue;
                }
                if (propertyNameRT.passes(property.getName())) {
                    outputProperty(
                        schemaReader, property,
                        catalogName, cube, level, rows);
                }
            }
        }

        private void outputProperty(
            final SchemaReader schemaReader,
            Property property,
            final String catalogName,
            Cube cube,
            Level level,
            List<Row> rows)
        {
            Hierarchy hierarchy = level.getHierarchy();
            Dimension dimension = hierarchy.getDimension();

            String propertyName = property.getName();

            Row row = new Row();
            row.set(CatalogName.name, catalogName);
            row.set(SchemaName.name, cube.getSchema().getName());
            row.set(CubeName.name, cube.getName());
            row.set(DimensionUniqueName.name, dimension.getUniqueName());
            row.set(HierarchyUniqueName.name, hierarchy.getUniqueName());
            row.set(LevelUniqueName.name, level.getUniqueName());
            //TODO: what is the correct value here
            //row.set(MemberUniqueName.name, "");

            row.set(PropertyName.name, propertyName);
            // Only member properties now
            row.set(
                PropertyType.name,
                org.olap4j.metadata.Property.TypeFlag.MEMBER.xmlaOrdinal());
            row.set(
                PropertyContentType.name,
                org.olap4j.metadata.Property.ContentType.REGULAR.xmlaOrdinal());
            row.set(PropertyCaption.name, property.getCaption());
            XmlaConstants.DBType dbType = getDBTypeFromProperty(property);
            row.set(DataType.name, dbType.xmlaOrdinal());

            String desc =
                cube.getName() + " Cube - "
                + getHierarchyName(hierarchy) + " Hierarchy - "
                + level.getName() + " Level - "
                + property.getName() + " Property";
            row.set(Description.name, desc);

            addRow(row, rows);
        }

        protected void setProperty(
            PropertyDefinition propertyDef,
            String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    private static boolean canAccess(
        SchemaReader schemaReader,
        OlapElement elem)
    {
        Role role = schemaReader.getRole();
        return role.canAccess(elem);
    }

    private static <T extends Comparable> List<T> sort(
        Collection<T> collection)
    {
        Object[] a = collection.toArray(new Object[collection.size()]);
        Arrays.sort(a);
        return Util.cast(Arrays.asList(a));
    }

    private static <T> List<T> sortArray(
        T[] a,
        Comparator<T> comparator)
    {
        T[] a2 = a.clone();
        Arrays.sort(a2, comparator);
        return Arrays.asList(a2);
    }

    static void serialize(StringBuilder buf, Collection<String> strings) {
        int k = 0;
        for (String name : sort(strings)) {
            if (k++ > 0) {
                buf.append(',');
            }
            buf.append(name);
        }
    }

    static List<Cube> sortedCubes(Schema schema) {
        final Cube[] cubes = schema.getCubes();
        return sortArray(
            cubes,
            new Comparator<Cube>() {
                public int compare(Cube o1, Cube o2) {
                    return o1.getName().compareTo(o2.getName());
                }
            }
        );
    }

    private static String getHierarchyName(Hierarchy hierarchy) {
        String hierarchyName = hierarchy.getName();
        if (MondrianProperties.instance().SsasCompatibleNaming.get()
            && !hierarchyName.equals(hierarchy.getDimension().getName()))
        {
            hierarchyName =
                hierarchy.getDimension().getName() + "." + hierarchyName;
        }
        return hierarchyName;
    }

    private static XmlaRequest wrapRequest(
        XmlaRequest request, Map<Column, String> map)
    {
        final Map<String, Object> restrictionsMap =
            new HashMap<String, Object>(request.getRestrictions());
        for (Map.Entry<Column, String> entry : map.entrySet()) {
            restrictionsMap.put(
                entry.getKey().name,
                Collections.singletonList(entry.getValue()));
        }

        return new DelegatingXmlaRequest(request) {
            @Override
            public Map<String, Object> getRestrictions() {
                return restrictionsMap;
            }
        };
    }

    private static class DelegatingXmlaRequest implements XmlaRequest {
        protected final XmlaRequest request;

        public DelegatingXmlaRequest(XmlaRequest request) {
            this.request = request;
        }

        public XmlaConstants.Method getMethod() {
            return request.getMethod();
        }

        public Map<String, String> getProperties() {
            return request.getProperties();
        }

        public Map<String, Object> getRestrictions() {
            return request.getRestrictions();
        }

        public String getStatement() {
            return request.getStatement();
        }

        public String getRoleName() {
            return request.getRoleName();
        }

        public Role getRole() {
            return request.getRole();
        }

        public String getRequestType() {
            return request.getRequestType();
        }

        public boolean isDrillThrough() {
            return request.isDrillThrough();
        }

        public int drillThroughMaxRows() {
            return request.drillThroughMaxRows();
        }

        public int drillThroughFirstRowset() {
            return request.drillThroughFirstRowset();
        }
    }
}

// End RowsetDefinition.java

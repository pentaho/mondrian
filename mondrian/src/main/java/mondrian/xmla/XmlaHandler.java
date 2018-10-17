/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2018 Hitachi Vantara
// All Rights Reserved.
*/
package mondrian.xmla;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.olap4j.IMondrianOlap4jProperty;
import mondrian.util.CompositeList;
import mondrian.xmla.impl.DefaultSaxWriter;

import org.apache.log4j.Logger;

import org.olap4j.*;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.metadata.*;
import org.olap4j.metadata.Property.StandardCellProperty;
import org.olap4j.metadata.Property.StandardMemberProperty;

import org.xml.sax.SAXException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.*;
import java.util.Date;

import static mondrian.xmla.XmlaConstants.*;
import static org.olap4j.metadata.XmlaConstants.*;

/**
 * An <code>XmlaHandler</code> responds to XML for Analysis (XML/A) requests.
 *
 * @author jhyde, Gang Chen
 * @since 27 April, 2003
 */
public class XmlaHandler {
    private static final Logger LOGGER = Logger.getLogger(XmlaHandler.class);

    /**
     * Name of property used by JDBC to hold user name.
     */
    private static final String JDBC_USER = "user";

    /**
     * Name of property used by JDBC to hold password.
     */
    private static final String JDBC_PASSWORD = "password";

    /**
     * Name of property used by JDBC to hold locale. It is not hard-wired into
     * DriverManager like "user" and "password", but we do expect any olap4j
     * driver that supports i18n to use this property name.
     */
    public static final String JDBC_LOCALE = "locale";

    final ConnectionFactory connectionFactory;
    private final String prefix;

    public static XmlaExtra getExtra(OlapConnection connection) {
        try {
            final XmlaExtra extra = connection.unwrap(XmlaExtra.class);
            if (extra != null) {
                return extra;
            }
        } catch (SQLException e) {
            // Connection cannot provide an XmlaExtra. Fall back and give a
            // default implementation.
        } catch (UndeclaredThrowableException ute) {
            //
            // Note: this is necessary because we use a dynamic proxy for the
            // connection.
            // I could not catch and un-wrap the Undeclared Throwable within
            // the proxy.
            // The exception comes out here and I couldn't find any better
            // ways to deal with it.
            //
            // The undeclared throwable contains an Invocation Target Exception
            // which in turns contains the real exception thrown by the "unwrap"
            // method, for example OlapException.
            //

            Throwable cause = ute.getCause();
            if (cause instanceof InvocationTargetException) {
                cause = cause.getCause();
            }

            // this maintains the original behaviour: don't catch exceptions
            // that are not subclasses of SQLException

            if (! (cause instanceof SQLException)) {
                throw ute;
            }
        }
        return new XmlaExtraImpl();
    }

    /**
     * Returns a new OlapConnection opened with the credentials specified in the
     * XMLA request or an existing connection if one can be found associated
     * with the request session id.
     *
     * @param request Request
     * @param propMap Extra properties
     */
    public OlapConnection getConnection(
        XmlaRequest request,
        Map<String, String> propMap)
    {
        String sessionId = request.getSessionId();
        if (sessionId == null) {
            // With a Simba O2X Client session ID is only null when
            // serving "discover datasources".
            //
            // Let's have a magic ID for the non-authenticated session.
            //
            // REVIEW: Security hole?
            sessionId = "<no_session>";
        }
        LOGGER.debug(
            "Creating new connection for user [" + request.getUsername()
            + "] and session [" + sessionId + "]");

        Properties props = new Properties();
        for (Map.Entry<String, String> entry : propMap.entrySet()) {
            props.put(entry.getKey(), entry.getValue());
        }
        if (request.getUsername() != null) {
            props.put(JDBC_USER, request.getUsername());
        }
        if (request.getPassword() != null) {
            props.put(JDBC_PASSWORD, request.getPassword());
        }

        final String databaseName =
           request
               .getProperties()
                   .get(PropertyDefinition.DataSourceInfo.name());

        String catalogName =
            request
                .getProperties()
                    .get(PropertyDefinition.Catalog.name());

        if (catalogName == null
            && request.getMethod() == Method.DISCOVER
            && request.getRestrictions().containsKey(
                Property.StandardMemberProperty
                    .CATALOG_NAME.name()))
        {
            Object restriction =
                request.getRestrictions().get(
                    Property.StandardMemberProperty
                        .CATALOG_NAME.name());
            if (restriction instanceof List) {
                final List requiredValues = (List) restriction;
                catalogName =
                    String.valueOf(
                        requiredValues.get(0));
            } else {
                throw Util.newInternal(
                    "unexpected restriction type: " + restriction.getClass());
            }
        }

        OlapConnection connection = getConnection(
                databaseName,
                catalogName,
                request.getRoleName(),
                props );
        String localeIdentifier = request.getProperties().get("LocaleIdentifier");
        Locale locale = XmlaUtil.convertToLocale( localeIdentifier );
        if (locale != null) {
            connection.setLocale(locale);
        }
        return connection;
    }

    private enum SetType {
        ROW_SET,
        MD_DATA_SET
    }

    private static final String EMPTY_ROW_SET_XML_SCHEMA =
        computeEmptyXsd(SetType.ROW_SET);

    private static final String MD_DATA_SET_XML_SCHEMA =
        computeXsd(SetType.MD_DATA_SET);

    private static final String EMPTY_MD_DATA_SET_XML_SCHEMA =
        computeEmptyXsd(SetType.MD_DATA_SET);

    private static final String NS_XML_SQL =
        "urn:schemas-microsoft-com:xml-sql";

    //
    // Some xml schema data types.
    //
    public static final String XSD_BOOLEAN = "xsd:boolean";
    public static final String XSD_STRING = "xsd:string";
    public static final String XSD_UNSIGNED_INT = "xsd:unsignedInt";

    public static final String XSD_BYTE = "xsd:byte";
    public static final byte XSD_BYTE_MAX_INCLUSIVE = 127;
    public static final byte XSD_BYTE_MIN_INCLUSIVE = -128;

    public static final String XSD_SHORT = "xsd:short";
    public static final short XSD_SHORT_MAX_INCLUSIVE = 32767;
    public static final short XSD_SHORT_MIN_INCLUSIVE = -32768;

    public static final String XSD_INT = "xsd:int";
    public static final int XSD_INT_MAX_INCLUSIVE = 2147483647;
    public static final int XSD_INT_MIN_INCLUSIVE = -2147483648;

    public static final String XSD_LONG = "xsd:long";
    public static final long XSD_LONG_MAX_INCLUSIVE = 9223372036854775807L;
    public static final long XSD_LONG_MIN_INCLUSIVE = -9223372036854775808L;

    // xsd:double: IEEE 64-bit floating-point
    public static final String XSD_DOUBLE = "xsd:double";

    public static final String XSD_FLOAT = "xsd:float";

    // xsd:decimal: Decimal numbers (BigDecimal)
    public static final String XSD_DECIMAL = "xsd:decimal";

    // xsd:integer: Signed integers of arbitrary length (BigInteger)
    public static final String XSD_INTEGER = "xsd:integer";

    public static boolean isValidXsdInt(long l) {
        return (l <= XSD_INT_MAX_INCLUSIVE) && (l >= XSD_INT_MIN_INCLUSIVE);
    }

    /**
     * Takes a DataType String (null, Integer, Numeric or non-null)
     * and Value Object (Integer, Double, String, other) and
     * canonicalizes them to XSD data type and corresponding object.
     * <p>
     * If the input DataType is Integer, then it attempts to return
     * an XSD_INT with value java.lang.Integer (and failing that an
     * XSD_LONG (java.lang.Long) or XSD_INTEGER (java.math.BigInteger)).
     * Worst case is the value loses precision with any integral
     * representation and must be returned as a decimal type (Double
     * or java.math.BigDecimal).
     * <p>
     * If the input DataType is Decimal, then it attempts to return
     * an XSD_DOUBLE with value java.lang.Double (and failing that an
     * XSD_DECIMAL (java.math.BigDecimal)).
     */
    static class ValueInfo {

        /**
         * Returns XSD_INT, XSD_DOUBLE, XSD_STRING or null.
         *
         * @param dataType null, Integer, Numeric or non-null.
         * @return Returns the suggested XSD type for a given datatype
         */
        static String getValueTypeHint(final String dataType) {
            if (dataType != null) {
                return (dataType.equals("Integer"))
                    ? XSD_INT
                    : ((dataType.equals("Numeric"))
                        ? XSD_DOUBLE
                        : XSD_STRING);
            } else {
                return null;
            }
        }

        String valueType;
        Object value;
        boolean isDecimal;

        ValueInfo(final String dataType, final Object inputValue) {
            final String valueTypeHint = getValueTypeHint(dataType);

            // This is a hint: should it be a string, integer or decimal type.
            // In the following, if the hint is integer, then there is
            // an attempt that the value types
            // be XSD_INT, XST_LONG, or XSD_INTEGER (but they could turn
            // out to be XSD_DOUBLE or XSD_DECIMAL if precision is loss
            // with the integral formats). It the hint is a decimal type
            // (double, float, decimal), then a XSD_DOUBLE or XSD_DECIMAL
            // is returned.
            if (valueTypeHint != null) {
                // The value type is a hint. If the value can be
                // converted to the data type without precision loss, ok;
                // otherwise value data type must be adjusted.

                if (valueTypeHint.equals(XSD_STRING)) {
                    // For String types, nothing to do.
                    this.valueType = valueTypeHint;
                    this.value = inputValue;
                    this.isDecimal = false;

                } else if (valueTypeHint.equals(XSD_INT)) {
                    // If valueTypeHint is XSD_INT, then see if value can be
                    // converted to (first choice) integer, (second choice),
                    // long and (last choice) BigInteger - otherwise must
                    // use double/decimal.

                    // Most of the time value ought to be an Integer so
                    // try it first
                    if (inputValue instanceof Integer) {
                        // For integer, its already the right type
                        this.valueType = valueTypeHint;
                        this.value = inputValue;
                        this.isDecimal = false;

                    } else if (inputValue instanceof Byte) {
                        this.valueType = XSD_BYTE;
                        this.value = inputValue;
                        this.isDecimal = false;

                    } else if (inputValue instanceof Short) {
                        this.valueType = XSD_SHORT;
                        this.value = inputValue;
                        this.isDecimal = false;

                    } else if (inputValue instanceof Long) {
                        // See if it can be an integer or long
                        long lval = (Long) inputValue;
                        setValueAndType(lval);

                    } else if (inputValue instanceof BigInteger) {
                        BigInteger bi = (BigInteger) inputValue;
                        // See if it can be an integer or long
                        long lval = bi.longValue();
                        if (bi.equals(BigInteger.valueOf(lval))) {
                            // It can be converted from BigInteger to long
                            // without loss of precision.
                            setValueAndType(lval);
                        } else {
                            // It can not be converted to a long.
                            this.valueType = XSD_INTEGER;
                            this.value = inputValue;
                            this.isDecimal = false;
                        }

                    } else if (inputValue instanceof Float) {
                        Float f = (Float) inputValue;
                        // See if it can be an integer or long
                        long lval = f.longValue();
                        if (f.equals(new Float(lval))) {
                            // It can be converted from double to long
                            // without loss of precision.
                            setValueAndType(lval);

                        } else {
                            // It can not be converted to a long.
                            this.valueType = XSD_FLOAT;
                            this.value = inputValue;
                            this.isDecimal = true;
                        }

                    } else if (inputValue instanceof Double) {
                        Double d = (Double) inputValue;
                        // See if it can be an integer or long
                        long lval = d.longValue();
                        if (d.equals(new Double(lval))) {
                            // It can be converted from double to long
                            // without loss of precision.
                            setValueAndType(lval);

                        } else {
                            // It can not be converted to a long.
                            this.valueType = XSD_DOUBLE;
                            this.value = inputValue;
                            this.isDecimal = true;
                        }

                    } else if (inputValue instanceof BigDecimal) {
                        // See if it can be an integer or long
                        BigDecimal bd = (BigDecimal) inputValue;
                        try {
                            // Can it be converted to a long
                            // Throws ArithmeticException on conversion failure.
                            // The following line is only available in
                            // Java5 and above:
                            //long lval = bd.longValueExact();
                            long lval = bd.longValue();

                            setValueAndType(lval);
                        } catch (ArithmeticException ex) {
                            // No, it can not be converted to long

                            try {
                                // Can it be an integer
                                BigInteger bi = bd.toBigIntegerExact();
                                this.valueType = XSD_INTEGER;
                                this.value = bi;
                                this.isDecimal = false;
                            } catch (ArithmeticException ex1) {
                                // OK, its a decimal
                                this.valueType = XSD_DECIMAL;
                                this.value = inputValue;
                                this.isDecimal = true;
                            }
                        }

                    } else if (inputValue instanceof Number) {
                        // Don't know what Number type we have here.
                        // Note: this could result in precision loss.
                        this.value = ((Number) inputValue).longValue();
                        this.valueType = valueTypeHint;
                        this.isDecimal = false;

                    } else {
                        // Who knows what we are dealing with,
                        // hope for the best?!?
                        this.valueType = valueTypeHint;
                        this.value = inputValue;
                        this.isDecimal = false;
                    }

                } else if (valueTypeHint.equals(XSD_DOUBLE)) {
                    // The desired type is double.

                    // Most of the time value ought to be an Double so
                    // try it first
                    if (inputValue instanceof Double) {
                        // For Double, its already the right type
                        this.valueType = valueTypeHint;
                        this.value = inputValue;
                        this.isDecimal = true;

                    } else if (inputValue instanceof Byte
                        || inputValue instanceof Short
                        || inputValue instanceof Integer
                        || inputValue instanceof Long)
                    {
                        // Convert from byte/short/integer/long to double
                        this.value = ((Number) inputValue).doubleValue();
                        this.valueType = valueTypeHint;
                        this.isDecimal = true;

                    } else if (inputValue instanceof Float) {
                        this.value = inputValue;
                        this.valueType = XSD_FLOAT;
                        this.isDecimal = true;

                    } else if (inputValue instanceof BigDecimal) {
                        BigDecimal bd = (BigDecimal) inputValue;
                        double dval = bd.doubleValue();
                        // make with same scale as Double
                        try {
                            BigDecimal bd2 =
                                Util.makeBigDecimalFromDouble(dval);
                            // Can it be a double
                            // Must use compareTo - see BigDecimal.equals
                            if (bd.compareTo(bd2) == 0) {
                                this.valueType = XSD_DOUBLE;
                                this.value = dval;
                            } else {
                                this.valueType = XSD_DECIMAL;
                                this.value = inputValue;
                            }
                        } catch (NumberFormatException ex) {
                            this.valueType = XSD_DECIMAL;
                            this.value = inputValue;
                        }
                        this.isDecimal = true;

                    } else if (inputValue instanceof BigInteger) {
                        // What should be done here? Convert ot BigDecimal
                        // and see if it can be a double or not?
                        // See if there is loss of precision in the convertion?
                        // Don't know. For now, just keep it a integral
                        // value.
                        BigInteger bi = (BigInteger) inputValue;
                        // See if it can be an integer or long
                        long lval = bi.longValue();
                        if (bi.equals(BigInteger.valueOf(lval))) {
                            // It can be converted from BigInteger to long
                            // without loss of precision.
                            setValueAndType(lval);
                        } else {
                            // It can not be converted to a long.
                            this.valueType = XSD_INTEGER;
                            this.value = inputValue;
                            this.isDecimal = true;
                        }

                    } else if (inputValue instanceof Number) {
                        // Don't know what Number type we have here.
                        // Note: this could result in precision loss.
                        this.value = ((Number) inputValue).doubleValue();
                        this.valueType = valueTypeHint;
                        this.isDecimal = true;

                    } else {
                        // Who knows what we are dealing with,
                        // hope for the best?!?
                        this.valueType = valueTypeHint;
                        this.value = inputValue;
                        this.isDecimal = true;
                    }
                }
            } else {
                // There is no valueType "hint", so just get it from the value.
                if (inputValue instanceof String) {
                    this.valueType = XSD_STRING;
                    this.value = inputValue;
                    this.isDecimal = false;

                } else if (inputValue instanceof Integer) {
                    this.valueType = XSD_INT;
                    this.value = inputValue;
                    this.isDecimal = false;

                } else if (inputValue instanceof Byte) {
                    Byte b = (Byte) inputValue;
                    this.valueType = XSD_BYTE;
                    this.value = b.intValue();
                    this.isDecimal = false;

                } else if (inputValue instanceof Short) {
                    Short s = (Short) inputValue;
                    this.valueType = XSD_SHORT;
                    this.value = s.intValue();
                    this.isDecimal = false;

                } else if (inputValue instanceof Long) {
                    // See if it can be an integer or long
                    setValueAndType((Long) inputValue);

                } else if (inputValue instanceof BigInteger) {
                    BigInteger bi = (BigInteger) inputValue;
                    // See if it can be an integer or long
                    long lval = bi.longValue();
                    if (bi.equals(BigInteger.valueOf(lval))) {
                        // It can be converted from BigInteger to long
                        // without loss of precision.
                        setValueAndType(lval);
                    } else {
                        // It can not be converted to a long.
                        this.valueType = XSD_INTEGER;
                        this.value = inputValue;
                        this.isDecimal = false;
                    }

                } else if (inputValue instanceof Float) {
                    this.valueType = XSD_FLOAT;
                    this.value = inputValue;
                    this.isDecimal = true;

                } else if (inputValue instanceof Double) {
                    this.valueType = XSD_DOUBLE;
                    this.value = inputValue;
                    this.isDecimal = true;

                } else if (inputValue instanceof BigDecimal) {
                    // See if it can be a double
                    BigDecimal bd = (BigDecimal) inputValue;
                    double dval = bd.doubleValue();
                    // make with same scale as Double
                    try {
                        BigDecimal bd2 =
                                Util.makeBigDecimalFromDouble(dval);
                        // Can it be a double
                        // Must use compareTo - see BigDecimal.equals
                        if (bd.compareTo(bd2) == 0) {
                            this.valueType = XSD_DOUBLE;
                            this.value = dval;
                        } else {
                            this.valueType = XSD_DECIMAL;
                            this.value = inputValue;
                        }
                    } catch (NumberFormatException ex) {
                        this.valueType = XSD_DECIMAL;
                        this.value = inputValue;
                    }
                    this.isDecimal = true;

                } else if (inputValue instanceof Number) {
                    // Don't know what Number type we have here.
                    // Note: this could result in precision loss.
                    this.value = ((Number) inputValue).longValue();
                    this.valueType = XSD_LONG;
                    this.isDecimal = false;

                } else if (inputValue instanceof Boolean) {
                    this.value = inputValue;
                    this.valueType = XSD_BOOLEAN;
                    this.isDecimal = false;
                } else {
                    // Who knows what we are dealing with,
                    // hope for the best?!?
                    this.valueType = XSD_STRING;
                    this.value = inputValue;
                    this.isDecimal = false;
                }
            }
        }
        private void setValueAndType(long lval) {
            if (! isValidXsdInt(lval)) {
                // No, it can not be a integer, must be a long
                this.valueType = XSD_LONG;
                this.value = lval;
            } else {
                // Its an integer.
                this.valueType = XSD_INT;
                this.value = (int) lval;
            }
            this.isDecimal = false;
        }
    }



    private static String computeXsd(SetType setType) {
        final StringWriter sw = new StringWriter();
        SaxWriter writer = new DefaultSaxWriter(new PrintWriter(sw), 3);
        writeDatasetXmlSchema(writer, setType);
        writer.flush();
        return sw.toString();
    }

    private static String computeEmptyXsd(SetType setType) {
        final StringWriter sw = new StringWriter();
        SaxWriter writer = new DefaultSaxWriter(new PrintWriter(sw), 3);
        writeEmptyDatasetXmlSchema(writer, setType);
        writer.flush();
        return sw.toString();
    }

    private static interface QueryResult {
        void unparse(SaxWriter res) throws SAXException, OlapException;
        void close() throws SQLException;
        void metadata(SaxWriter writer);
    }

    /**
     * Creates an <code>XmlaHandler</code>.
     *
     * <p>The connection factory may be null, as long as you override
     * {@link #getConnection(String, String, String, Properties)}.
     *
     * @param connectionFactory Connection factory
     * @param prefix XML Namespace. Typical value is "xmla", but a value of
     *   "cxmla" works around an Internet Explorer 7 bug
     */
    public XmlaHandler(ConnectionFactory connectionFactory, String prefix)
    {
        assert prefix != null;
        this.connectionFactory = connectionFactory;
        this.prefix = prefix;
    }

    /**
     * Processes a request.
     *
     * @param request  XML request, for example, "<SOAP-ENV:Envelope ...>".
     * @param response Destination for response
     * @throws XmlaException on error
     */
    public void process(XmlaRequest request, XmlaResponse response)
        throws XmlaException
    {
        Method method = request.getMethod();
        long start = System.currentTimeMillis();

        switch (method) {
        case DISCOVER:
            discover(request, response);
            break;
        case EXECUTE:
            execute(request, response);
            break;
        default:
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_BAD_METHOD_CODE,
                HSB_BAD_METHOD_FAULT_FS,
                new IllegalArgumentException(
                    "Unsupported XML/A method: " + method));
        }
        if (LOGGER.isDebugEnabled()) {
            long end = System.currentTimeMillis();
            LOGGER.debug("XmlaHandler.process: time = " + (end - start));
            LOGGER.debug("XmlaHandler.process: " + Util.printMemory());
        }
    }

    private void checkFormat(XmlaRequest request) throws XmlaException {
        // Check response's rowset format in request
        final Map<String, String> properties = request.getProperties();
        if (request.isDrillThrough()) {
            Format format = getFormat(request, null);
            if (format != Format.Tabular) {
                throw new XmlaException(
                    CLIENT_FAULT_FC,
                    HSB_DRILL_THROUGH_FORMAT_CODE,
                    HSB_DRILL_THROUGH_FORMAT_FAULT_FS,
                    new UnsupportedOperationException(
                        "<Format>: only 'Tabular' allowed when drilling "
                        + "through"));
            }
        } else {
            final String formatName =
                properties.get(PropertyDefinition.Format.name());
            if (formatName != null) {
                Format format = getFormat(request, null);
                if (format != Format.Multidimensional
                    && format != Format.Tabular)
                {
                    throw new UnsupportedOperationException(
                        "<Format>: only 'Multidimensional', 'Tabular' "
                        + "currently supported");
                }
            }
            final String axisFormatName =
                properties.get(PropertyDefinition.AxisFormat.name());
            if (axisFormatName != null) {
                AxisFormat axisFormat = Util.lookup(
                    AxisFormat.class, axisFormatName, null);

                if (axisFormat != AxisFormat.TupleFormat) {
                    throw new UnsupportedOperationException(
                        "<AxisFormat>: only 'TupleFormat' currently supported");
                }
            }
        }
    }

    private void execute(
        XmlaRequest request,
        XmlaResponse response)
        throws XmlaException
    {
        final Map<String, String> properties = request.getProperties();

        // Default responseMimeType is SOAP.
        Enumeration.ResponseMimeType responseMimeType =
            getResponseMimeType(request);

        // Default value is SchemaData, or Data for JSON responses.
        final String contentName =
            properties.get(PropertyDefinition.Content.name());
        Content content = Util.lookup(
            Content.class,
            contentName,
            responseMimeType == Enumeration.ResponseMimeType.JSON
                ? Content.Data
                : Content.DEFAULT);

        // Handle execute
        QueryResult result = null;
        try {
            if (request.isDrillThrough()) {
                result = executeDrillThroughQuery(request);
            } else {
                result = executeQuery(request);
            }

            SaxWriter writer = response.getWriter();
            writer.startDocument();

            writer.startElement(
                prefix + ":ExecuteResponse",
                "xmlns:" + prefix, NS_XMLA);
            writer.startElement(prefix + ":return");
            boolean rowset =
                request.isDrillThrough()
                || Format.Tabular.name().equals(
                    request.getProperties().get(
                        PropertyDefinition.Format.name()));
            writer.startElement(
                "root",
                "xmlns",
                result == null
                    ? NS_XMLA_EMPTY
                    : rowset
                        ? NS_XMLA_ROWSET
                        : NS_XMLA_MDDATASET,
                "xmlns:xsi", NS_XSI,
                "xmlns:xsd", NS_XSD,
                "xmlns:EX", NS_XMLA_EX);

            switch (content) {
            case Schema:
            case SchemaData:
                if (result != null) {
                    result.metadata(writer);
                } else {
                    if (rowset) {
                        writer.verbatim(EMPTY_ROW_SET_XML_SCHEMA);
                    } else {
                        writer.verbatim(EMPTY_MD_DATA_SET_XML_SCHEMA);
                    }
                }
                break;
            }

            try {
                switch (content) {
                case Data:
                case SchemaData:
                case DataOmitDefaultSlicer:
                case DataIncludeDefaultSlicer:
                    if (result != null) {
                        result.unparse(writer);
                    }
                    break;
                }
            } catch (XmlaException xex) {
                throw xex;
            } catch (Throwable t) {
                throw new XmlaException(
                    SERVER_FAULT_FC,
                    HSB_EXECUTE_UNPARSE_CODE,
                    HSB_EXECUTE_UNPARSE_FAULT_FS,
                    t);
            } finally {
                writer.endElement(); // root
                writer.endElement(); // return
                writer.endElement(); // ExecuteResponse
            }
            writer.endDocument();
        } finally {
            if (result != null) {
                try {
                    result.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Computes the XML Schema for a dataset.
     *
     * @param writer SAX writer
     * @param settype rowset or dataset?
     * @see RowsetDefinition#writeRowsetXmlSchema(SaxWriter)
     */
    static void writeDatasetXmlSchema(SaxWriter writer, SetType settype) {
        String setNsXmla =
            (settype == SetType.ROW_SET)
            ? NS_XMLA_ROWSET
            : NS_XMLA_MDDATASET;

        writer.startElement(
            "xsd:schema",
            "xmlns:xsd", NS_XSD,
            "targetNamespace", setNsXmla,
            "xmlns", setNsXmla,
            "xmlns:xsi", NS_XSI,
            "xmlns:sql", NS_XML_SQL,
            "elementFormDefault", "qualified");

        // MemberType

        writer.startElement(
            "xsd:complexType",
            "name", "MemberType");
        writer.startElement("xsd:sequence");
        writer.element(
            "xsd:element",
            "name", "UName",
            "type", XSD_STRING);
        writer.element(
            "xsd:element",
            "name", "Caption",
            "type", XSD_STRING);
        writer.element(
            "xsd:element",
            "name", "LName",
            "type", XSD_STRING);
        writer.element(
            "xsd:element",
            "name", "LNum",
            "type", XSD_UNSIGNED_INT);
        writer.element(
            "xsd:element",
            "name", "DisplayInfo",
            "type", XSD_UNSIGNED_INT);
        writer.startElement(
            "xsd:sequence",
            "maxOccurs", "unbounded",
            "minOccurs", 0);
        writer.element(
            "xsd:any",
            "processContents", "lax",
            "maxOccurs", "unbounded");
        writer.endElement(); // xsd:sequence
        writer.endElement(); // xsd:sequence
        writer.element(
            "xsd:attribute",
                "name", "Hierarchy",
                "type", XSD_STRING);
        writer.endElement(); // xsd:complexType name="MemberType"

        // PropType

        writer.startElement(
            "xsd:complexType",
            "name", "PropType");
        writer.element(
            "xsd:attribute",
            "name", "name",
            "type", XSD_STRING);
        writer.endElement(); // xsd:complexType name="PropType"

        // TupleType

        writer.startElement(
            "xsd:complexType",
            "name", "TupleType");
        writer.startElement(
            "xsd:sequence",
            "maxOccurs", "unbounded");
        writer.element(
            "xsd:element",
            "name", "Member",
            "type", "MemberType");
        writer.endElement(); // xsd:sequence
        writer.endElement(); // xsd:complexType name="TupleType"

        // MembersType

        writer.startElement(
            "xsd:complexType",
            "name", "MembersType");
        writer.startElement(
            "xsd:sequence",
            "maxOccurs", "unbounded");
        writer.element(
            "xsd:element",
            "name", "Member",
            "type", "MemberType");
        writer.endElement(); // xsd:sequence
        writer.element(
            "xsd:attribute",
            "name", "Hierarchy",
            "type", XSD_STRING);
        writer.endElement(); // xsd:complexType

        // TuplesType

        writer.startElement(
            "xsd:complexType",
            "name", "TuplesType");
        writer.startElement(
            "xsd:sequence",
            "maxOccurs", "unbounded");
        writer.element(
            "xsd:element",
            "name", "Tuple",
            "type", "TupleType");
        writer.endElement(); // xsd:sequence
        writer.endElement(); // xsd:complexType

        // CrossProductType

        writer.startElement(
            "xsd:complexType",
            "name", "CrossProductType");
        writer.startElement("xsd:sequence");
        writer.startElement(
            "xsd:choice",
            "minOccurs", 0,
            "maxOccurs", "unbounded");
        writer.element(
            "xsd:element",
            "name", "Members",
            "type", "MembersType");
        writer.element(
            "xsd:element",
            "name", "Tuples",
            "type", "TuplesType");
        writer.endElement(); // xsd:choice
        writer.endElement(); // xsd:sequence
        writer.element(
            "xsd:attribute",
            "name", "Size",
            "type", XSD_UNSIGNED_INT);
        writer.endElement(); // xsd:complexType

        // OlapInfo

        writer.startElement(
            "xsd:complexType",
            "name", "OlapInfo");
        writer.startElement("xsd:sequence");

        { // <CubeInfo>
            writer.startElement(
                "xsd:element",
                "name", "CubeInfo");
            writer.startElement("xsd:complexType");
            writer.startElement("xsd:sequence");

            { // <Cube>
                writer.startElement(
                    "xsd:element",
                    "name", "Cube",
                    "maxOccurs", "unbounded");
                writer.startElement("xsd:complexType");
                writer.startElement("xsd:sequence");

                writer.element(
                    "xsd:element",
                    "name", "CubeName",
                    "type", XSD_STRING);

                writer.endElement(); // xsd:sequence
                writer.endElement(); // xsd:complexType
                writer.endElement(); // xsd:element name=Cube
            }

            writer.endElement(); // xsd:sequence
            writer.endElement(); // xsd:complexType
            writer.endElement(); // xsd:element name=CubeInfo
        }

        { // <AxesInfo>
            writer.startElement(
                "xsd:element",
                "name", "AxesInfo");
            writer.startElement("xsd:complexType");
            writer.startElement("xsd:sequence");
            { // <AxisInfo>
                writer.startElement(
                    "xsd:element",
                    "name", "AxisInfo",
                    "maxOccurs", "unbounded");
                writer.startElement("xsd:complexType");
                writer.startElement("xsd:sequence");

                { // <HierarchyInfo>
                    writer.startElement(
                        "xsd:element",
                        "name", "HierarchyInfo",
                        "minOccurs", 0,
                        "maxOccurs", "unbounded");
                    writer.startElement("xsd:complexType");
                    writer.startElement("xsd:sequence");
                    writer.startElement(
                        "xsd:sequence",
                        "maxOccurs", "unbounded");
                    writer.element(
                        "xsd:element",
                        "name", "UName",
                        "type", "PropType");
                    writer.element(
                        "xsd:element",
                        "name", "Caption",
                        "type", "PropType");
                    writer.element(
                        "xsd:element",
                        "name", "LName",
                        "type", "PropType");
                    writer.element(
                        "xsd:element",
                        "name", "LNum",
                        "type", "PropType");
                    writer.element(
                        "xsd:element",
                        "name", "DisplayInfo",
                        "type", "PropType",
                        "minOccurs", 0,
                        "maxOccurs", "unbounded");
                    if (false)
                    writer.element(
                        "xsd:element",
                        "name", "PARENT_MEMBER_NAME",
                        "type", "PropType",
                        "minOccurs", 0,
                        "maxOccurs", "unbounded");
                    writer.endElement(); // xsd:sequence

                    // This is the Depth element for JPivot??
                    writer.startElement("xsd:sequence");
                    writer.element(
                        "xsd:any",
                        "processContents", "lax",
                        "minOccurs", 0,
                        "maxOccurs", "unbounded");
                    writer.endElement(); // xsd:sequence

                    writer.endElement(); // xsd:sequence
                    writer.element(
                        "xsd:attribute",
                        "name", "name",
                        "type", XSD_STRING,
                        "use", "required");
                    writer.endElement(); // xsd:complexType
                    writer.endElement(); // xsd:element name=HierarchyInfo
                }
                writer.endElement(); // xsd:sequence
                writer.element(
                    "xsd:attribute",
                    "name", "name",
                    "type", XSD_STRING);
                writer.endElement(); // xsd:complexType
                writer.endElement(); // xsd:element name=AxisInfo
            }
            writer.endElement(); // xsd:sequence
            writer.endElement(); // xsd:complexType
            writer.endElement(); // xsd:element name=AxesInfo
        }

        // CellInfo

        { // <CellInfo>
            writer.startElement(
                "xsd:element",
                "name", "CellInfo");
            writer.startElement("xsd:complexType");
            writer.startElement("xsd:sequence");
            writer.startElement(
                "xsd:sequence",
                "minOccurs", 0,
                "maxOccurs", "unbounded");
            writer.startElement("xsd:choice");
            writer.element(
                "xsd:element",
                "name", "Value",
                "type", "PropType");
            writer.element(
                "xsd:element",
                "name", "FmtValue",
                "type", "PropType");
            writer.element(
                "xsd:element",
                "name", "BackColor",
                "type", "PropType");
            writer.element(
                "xsd:element",
                "name", "ForeColor",
                "type", "PropType");
            writer.element(
                "xsd:element",
                "name", "FontName",
                "type", "PropType");
            writer.element(
                "xsd:element",
                "name", "FontSize",
                "type", "PropType");
            writer.element(
                "xsd:element",
                "name", "FontFlags",
                "type", "PropType");
            writer.element(
                "xsd:element",
                "name", "FormatString",
                "type", "PropType");
            writer.element(
                "xsd:element",
                "name", "NonEmptyBehavior",
                "type", "PropType");
            writer.element(
                "xsd:element",
                "name", "SolveOrder",
                "type", "PropType");
            writer.element(
                "xsd:element",
                "name", "Updateable",
                "type", "PropType");
            writer.element(
                "xsd:element",
                "name", "Visible",
                "type", "PropType");
            writer.element(
                "xsd:element",
                "name", "Expression",
                "type", "PropType");
            writer.endElement(); // xsd:choice
            writer.endElement(); // xsd:sequence
            writer.startElement(
                "xsd:sequence",
                "maxOccurs", "unbounded",
                "minOccurs", 0);
            writer.element(
                "xsd:any",
                "processContents", "lax",
                "maxOccurs", "unbounded");
            writer.endElement(); // xsd:sequence
            writer.endElement(); // xsd:sequence
            writer.endElement(); // xsd:complexType
            writer.endElement(); // xsd:element name=CellInfo
        }

        writer.endElement(); // xsd:sequence
        writer.endElement(); // xsd:complexType

        // Axes

        writer.startElement(
            "xsd:complexType",
            "name", "Axes");
        writer.startElement(
            "xsd:sequence",
            "maxOccurs", "unbounded");
        { // <Axis>
            writer.startElement(
                "xsd:element",
                "name", "Axis");
            writer.startElement("xsd:complexType");
            writer.startElement(
                "xsd:choice",
                "minOccurs", 0,
                "maxOccurs", "unbounded");
            writer.element(
                "xsd:element",
                "name", "CrossProduct",
                "type", "CrossProductType");
            writer.element(
                "xsd:element",
                "name", "Tuples",
                "type", "TuplesType");
            writer.element(
                "xsd:element",
                "name", "Members",
                "type", "MembersType");
            writer.endElement(); // xsd:choice
            writer.element(
                "xsd:attribute",
                "name", "name",
                "type", XSD_STRING);
            writer.endElement(); // xsd:complexType
        }
        writer.endElement(); // xsd:element
        writer.endElement(); // xsd:sequence
        writer.endElement(); // xsd:complexType

        // CellData

        writer.startElement(
            "xsd:complexType",
            "name", "CellData");
        writer.startElement("xsd:sequence");
        { // <Cell>
            writer.startElement(
                "xsd:element",
                "name", "Cell",
                "minOccurs", 0,
                "maxOccurs", "unbounded");
            writer.startElement("xsd:complexType");
            writer.startElement(
                "xsd:sequence",
                "maxOccurs", "unbounded");
            writer.startElement("xsd:choice");
            writer.element(
                "xsd:element",
                "name", "Value");
            writer.element(
                "xsd:element",
                "name", "FmtValue",
                "type", XSD_STRING);
            writer.element(
                "xsd:element",
                "name", "BackColor",
                "type", XSD_UNSIGNED_INT);
            writer.element(
                "xsd:element",
                "name", "ForeColor",
                "type", XSD_UNSIGNED_INT);
            writer.element(
                "xsd:element",
                "name", "FontName",
                "type", XSD_STRING);
            writer.element(
                "xsd:element",
                "name", "FontSize",
                "type", "xsd:unsignedShort");
            writer.element(
                "xsd:element",
                "name", "FontFlags",
                "type", XSD_UNSIGNED_INT);
            writer.element(
                "xsd:element",
                "name", "FormatString",
                "type", XSD_STRING);
            writer.element(
                "xsd:element",
                "name", "NonEmptyBehavior",
                "type", "xsd:unsignedShort");
            writer.element(
                "xsd:element",
                "name", "SolveOrder",
                "type", XSD_UNSIGNED_INT);
            writer.element(
                "xsd:element",
                "name", "Updateable",
                "type", XSD_UNSIGNED_INT);
            writer.element(
                "xsd:element",
                "name", "Visible",
                "type", XSD_UNSIGNED_INT);
            writer.element(
                "xsd:element",
                "name", "Expression",
                "type", XSD_STRING);
            writer.endElement(); // xsd:choice
            writer.endElement(); // xsd:sequence
            writer.element(
                "xsd:attribute",
                "name", "CellOrdinal",
                "type", XSD_UNSIGNED_INT,
                "use", "required");
            writer.endElement(); // xsd:complexType
            writer.endElement(); // xsd:element name=Cell
        }
        writer.endElement(); // xsd:sequence
        writer.endElement(); // xsd:complexType

        { // <root>
            writer.startElement(
                "xsd:element",
                "name", "root");
            writer.startElement("xsd:complexType");
            writer.startElement(
                "xsd:sequence",
                "maxOccurs", "unbounded");
            writer.element(
                "xsd:element",
                "name", "OlapInfo",
                "type", "OlapInfo");
            writer.element(
                "xsd:element",
                "name", "Axes",
                "type", "Axes");
            writer.element(
                "xsd:element",
                "name", "CellData",
                "type", "CellData");
            writer.endElement(); // xsd:sequence
            writer.endElement(); // xsd:complexType
            writer.endElement(); // xsd:element name=root
        }

        writer.endElement(); // xsd:schema
    }

    static void writeEmptyDatasetXmlSchema(SaxWriter writer, SetType setType) {
        String setNsXmla = NS_XMLA_ROWSET;
        writer.startElement(
            "xsd:schema",
            "xmlns:xsd", NS_XSD,
            "targetNamespace", setNsXmla,
            "xmlns", setNsXmla,
            "xmlns:xsi", NS_XSI,
            "xmlns:sql", NS_XML_SQL,
            "elementFormDefault", "qualified");

        writer.element(
            "xsd:element",
            "name", "root");

        writer.endElement(); // xsd:schema
    }

    private QueryResult executeDrillThroughQuery(XmlaRequest request)
        throws XmlaException
    {
        checkFormat(request);

        final Map<String, String> properties = request.getProperties();
        String tabFields =
            properties.get(PropertyDefinition.TableFields.name());
        if (tabFields != null && tabFields.length() == 0) {
            tabFields = null;
        }
        final String advancedFlag =
            properties.get(PropertyDefinition.AdvancedFlag.name());
        final boolean advanced = Boolean.parseBoolean(advancedFlag);
        final boolean enableRowCount =
            MondrianProperties.instance().EnableTotalCount.booleanValue();
        final int[] rowCountSlot = enableRowCount ? new int[]{0} : null;
        OlapConnection connection = null;
        OlapStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection =
                getConnection(request, Collections.<String, String>emptyMap());
            statement = connection.createStatement();
            resultSet =
                getExtra(connection).executeDrillthrough(
                    statement,
                    request.getStatement(),
                    advanced,
                    tabFields,
                    rowCountSlot);
            int rowCount = enableRowCount ? rowCountSlot[0] : -1;
            return new TabularRowSet(resultSet, rowCount);
        } catch (XmlaException xex) {
            throw xex;
        } catch (SQLException sqle) {
            throw new XmlaException(
                SERVER_FAULT_FC,
                HSB_DRILL_THROUGH_SQL_CODE,
                HSB_DRILL_THROUGH_SQL_FAULT_FS,
                Util.newError(sqle, "Error in drill through"));
        } catch (RuntimeException e) {
            // NOTE: One important error is "cannot drill through on the cell"
            throw new XmlaException(
                SERVER_FAULT_FC,
                HSB_DRILL_THROUGH_SQL_CODE,
                HSB_DRILL_THROUGH_SQL_FAULT_FS,
                e);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
    }

    static class Column {
        private final String name;
        private final String encodedName;
        private final String xsdType;

        Column(String name, int type, int scale) {
            this.name = name;

            // replace invalid XML element name, like " ", with "_x0020_" in
            // column headers, otherwise will generate a badly-formatted xml
            // doc.
            this.encodedName =
                XmlaUtil.ElementNameEncoder.INSTANCE.encode(name);
            this.xsdType = sqlToXsdType(type, scale);
        }
    }

    static class TabularRowSet implements QueryResult {
        private final List<Column> columns = new ArrayList<Column>();
        private final List<Object[]> rows;
        private int totalCount;

        /**
         * Creates a TabularRowSet based upon a SQL statement result.
         *
         * <p>Does not close the ResultSet, on success or failure. Client
         * must do it.
         *
         * @param rs Result set
         * @param totalCount Total number of rows. If >= 0, writes the
         *   "totalCount" attribute into the XMLA response.
         *
         * @throws SQLException on error
         */
        public TabularRowSet(
            ResultSet rs,
            int totalCount)
            throws SQLException
        {
            this.totalCount = totalCount;
            ResultSetMetaData md = rs.getMetaData();
            int columnCount = md.getColumnCount();

            // populate column defs
            for (int i = 0; i < columnCount; i++) {
                columns.add(
                    new Column(
                        md.getColumnLabel(i + 1),
                        md.getColumnType(i + 1),
                        md.getScale(i + 1)));
            }

            // Populate data; assume that SqlStatement is already positioned
            // on first row (or isDone() is true), and assume that the
            // number of rows returned is limited.
            rows = new ArrayList<Object[]>();
            while (rs.next()) {
                Object[] row = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rs.getObject(i + 1);
                }
                rows.add(row);
            }
        }

        /**
         * Alternate constructor for advanced drill-through.
         *
         * @param tableFieldMap Map from table name to a list of the names of
         *      the fields in the table
         * @param tableList List of table names
         */
        public TabularRowSet(
            Map<String, List<String>> tableFieldMap, List<String> tableList)
        {
            for (String tableName : tableList) {
                List<String> fieldNames = tableFieldMap.get(tableName);
                for (String fieldName : fieldNames) {
                    columns.add(
                        new Column(
                            tableName + "." + fieldName,
                            Types.VARCHAR, // don't know the real type
                            0));
                }
            }

            rows = new ArrayList<Object[]>();
            Object[] row = new Object[columns.size()];
            for (int k = 0; k < row.length; k++) {
                row[k] = k;
            }
            rows.add(row);
        }

        public void close() {
            // no resources to close
        }

        public void unparse(SaxWriter writer) throws SAXException {
            // write total count row if enabled
            if (totalCount >= 0) {
                String countStr = Integer.toString(totalCount);
                writer.startElement("row");
                for (Column column : columns) {
                    writer.startElement(column.encodedName);
                    writer.characters(countStr);
                    writer.endElement();
                }
                writer.endElement(); // row
            }

            for (Object[] row : rows) {
                writer.startElement("row");
                for (int i = 0; i < row.length; i++) {
                    writer.startElement(
                        columns.get(i).encodedName,
                        new Object[] {
                            "xsi:type",
                            columns.get(i).xsdType});
                    Object value = row[i];
                    if (value == null) {
                        writer.characters("null");
                    } else {
                        String valueString = value.toString();
                        if (value instanceof Number) {
                            valueString =
                                XmlaUtil.normalizeNumericString(valueString);
                        }
                        writer.characters(valueString);
                    }
                    writer.endElement();
                }
                writer.endElement(); // row
            }
        }

        /**
         * Writes the tabular drillthrough schema
         *
         * @param writer Writer
         */
        public void metadata(SaxWriter writer) {
            writer.startElement(
                "xsd:schema",
                "xmlns:xsd", NS_XSD,
                "targetNamespace", NS_XMLA_ROWSET,
                "xmlns", NS_XMLA_ROWSET,
                "xmlns:xsi", NS_XSI,
                "xmlns:sql", NS_XML_SQL,
                "elementFormDefault", "qualified");

            { // <root>
                writer.startElement(
                    "xsd:element",
                    "name", "root");
                writer.startElement("xsd:complexType");
                writer.startElement("xsd:sequence");
                writer.element(
                    "xsd:element",
                    "maxOccurs", "unbounded",
                    "minOccurs", 0,
                    "name", "row",
                    "type", "row");
                writer.endElement(); // xsd:sequence
                writer.endElement(); // xsd:complexType
                writer.endElement(); // xsd:element name=root
            }

            { // xsd:simpleType name="uuid"
                writer.startElement(
                    "xsd:simpleType",
                        "name", "uuid");
                writer.startElement(
                    "xsd:restriction",
                    "base", XSD_STRING);
                writer.element(
                    "xsd:pattern",
                    "value", RowsetDefinition.UUID_PATTERN);
                writer.endElement(); // xsd:restriction
                writer.endElement(); // xsd:simpleType
            }

            { // xsd:complexType name="row"
                writer.startElement(
                    "xsd:complexType",
                    "name", "row");
                writer.startElement("xsd:sequence");
                for (Column column : columns) {
                    writer.element(
                        "xsd:element",
                        "minOccurs", 0,
                        "name", column.encodedName,
                        "sql:field", column.name,
                        "type", column.xsdType);
                }

                writer.endElement(); // xsd:sequence
                writer.endElement(); // xsd:complexType
            }
            writer.endElement(); // xsd:schema
        }
    }

    /**
     * Converts a SQL type to XSD type.
     *
     * @param sqlType SQL type
     * @return XSD type
     */
    private static String sqlToXsdType(final int sqlType, final int scale) {
        switch (sqlType) {
        // Integer
        case Types.INTEGER:
        case Types.SMALLINT:
        case Types.TINYINT:
            return XSD_INT;
        case Types.NUMERIC:
        case Types.DECIMAL:
             // Oracle reports all numbers as NUMERIC. We check
             // the scale of the column and pick the right XSD type.
            if (scale == 0) {
                return XSD_INT;
            } else {
                return XSD_DECIMAL;
            }
        case Types.BIGINT:
            return XSD_INTEGER;
        // Real
        case Types.DOUBLE:
        case Types.FLOAT:
            return XSD_DOUBLE;
            // Date and time
        case Types.TIME:
        case Types.TIMESTAMP:
        case Types.DATE:
            return XSD_STRING;
            // Other
        default:
            return XSD_STRING;
        }
    }

    private QueryResult executeQuery(XmlaRequest request)
        throws XmlaException
    {
        final String mdx = request.getStatement();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("mdx: \"" + mdx + "\"");
        }

        if ((mdx == null) || (mdx.length() == 0)) {
            return null;
        }
        checkFormat(request);

        OlapConnection connection = null;
        PreparedOlapStatement statement = null;
        CellSet cellSet = null;
        boolean success = false;
        try {
            connection =
                getConnection(request, Collections.<String, String>emptyMap());
            getExtra(connection).setPreferList(connection);
            try {
                statement = connection.prepareOlapStatement(mdx);
            } catch (XmlaException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new XmlaException(
                    CLIENT_FAULT_FC,
                    HSB_PARSE_QUERY_CODE,
                    HSB_PARSE_QUERY_FAULT_FS,
                    ex);
            }
            try {
                cellSet = statement.executeQuery();

                final Format format = getFormat(request, null);
                final Content content = getContent(request);
                final Enumeration.ResponseMimeType responseMimeType =
                    getResponseMimeType(request);
                final MDDataSet dataSet;
                if (format == Format.Multidimensional) {
                    dataSet =
                        new MDDataSet_Multidimensional(
                            cellSet,
                            content != Content.DataIncludeDefaultSlicer,
                            responseMimeType
                            == Enumeration.ResponseMimeType.JSON);
                } else {
                    dataSet =
                        new MDDataSet_Tabular(cellSet);
                }
                success = true;
                return dataSet;
            } catch (XmlaException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new XmlaException(
                    SERVER_FAULT_FC,
                    HSB_EXECUTE_QUERY_CODE,
                    HSB_EXECUTE_QUERY_FAULT_FS,
                    ex);
            }
        } finally {
            if (!success) {
                if (cellSet != null) {
                    try {
                        cellSet.close();
                    } catch (SQLException e) {
                        // ignore
                    }
                }
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        // ignore
                    }
                }
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        // ignore
                    }
                }
            }
        }
    }

    private static Format getFormat(
        XmlaRequest request,
        Format defaultValue)
    {
        final String formatName =
            request.getProperties().get(
                PropertyDefinition.Format.name());
        return Util.lookup(
            Format.class,
            formatName, defaultValue);
    }

    private static Content getContent(XmlaRequest request) {
        final String contentName =
            request.getProperties().get(
                PropertyDefinition.Content.name());
        return Util.lookup(
            Content.class,
            contentName,
            Content.DEFAULT);
    }

    private static Enumeration.ResponseMimeType getResponseMimeType(
        XmlaRequest request)
    {
        Enumeration.ResponseMimeType mimeType =
            Enumeration.ResponseMimeType.MAP.get(
                request.getProperties().get(
                    PropertyDefinition.ResponseMimeType.name()));
        if (mimeType == null) {
            mimeType = Enumeration.ResponseMimeType.SOAP;
        }
        return mimeType;
    }

    static abstract class MDDataSet implements QueryResult {
        protected final CellSet cellSet;

        protected static final List<Property> cellProps =
            Arrays.asList(
                rename(StandardCellProperty.VALUE, "Value"),
                rename(StandardCellProperty.FORMATTED_VALUE, "FmtValue"),
                rename(StandardCellProperty.FORMAT_STRING, "FormatString"));

        protected static final List<StandardCellProperty> cellPropLongs =
            Arrays.asList(
                StandardCellProperty.VALUE,
                StandardCellProperty.FORMATTED_VALUE,
                StandardCellProperty.FORMAT_STRING);

        protected static final List<Property> defaultProps =
            Arrays.asList(
                rename(StandardMemberProperty.MEMBER_UNIQUE_NAME, "UName"),
                rename(StandardMemberProperty.MEMBER_CAPTION, "Caption"),
                rename(StandardMemberProperty.LEVEL_UNIQUE_NAME, "LName"),
                rename(StandardMemberProperty.LEVEL_NUMBER, "LNum"),
                rename(StandardMemberProperty.DISPLAY_INFO, "DisplayInfo"));

        protected static final Map<String, StandardMemberProperty> longProps =
            new HashMap<String, StandardMemberProperty>();

        static {
            longProps.put("UName", StandardMemberProperty.MEMBER_UNIQUE_NAME);
            longProps.put("Caption", StandardMemberProperty.MEMBER_CAPTION);
            longProps.put("LName", StandardMemberProperty.LEVEL_UNIQUE_NAME);
            longProps.put("LNum", StandardMemberProperty.LEVEL_NUMBER);
            longProps.put("DisplayInfo", StandardMemberProperty.DISPLAY_INFO);
        }

        protected MDDataSet(CellSet cellSet) {
            this.cellSet = cellSet;
        }

        public void close() throws SQLException {
            cellSet.getStatement().getConnection().close();
        }

        private static Property rename(
            final Property property,
            final String name)
        {
            return new Property() {
                public Datatype getDatatype() {
                    return property.getDatatype();
                }

                public Set<TypeFlag> getType() {
                    return property.getType();
                }

                public ContentType getContentType() {
                    return property.getContentType();
                }

                public String getName() {
                    return name;
                }

                public String getUniqueName() {
                    return property.getUniqueName();
                }

                public String getCaption() {
                    return property.getCaption();
                }

                public String getDescription() {
                    return property.getDescription();
                }

                public boolean isVisible() {
                    return property.isVisible();
                }
            };
        }
    }

    static class MDDataSet_Multidimensional extends MDDataSet {
        private List<Hierarchy> slicerAxisHierarchies;
        private final boolean omitDefaultSlicerInfo;
        private final boolean json;
        private XmlaUtil.ElementNameEncoder encoder =
            XmlaUtil.ElementNameEncoder.INSTANCE;
        private XmlaExtra extra;

        protected MDDataSet_Multidimensional(
            CellSet cellSet,
            boolean omitDefaultSlicerInfo,
            boolean json)
            throws SQLException
        {
            super(cellSet);
            this.omitDefaultSlicerInfo = omitDefaultSlicerInfo;
            this.json = json;
            this.extra = getExtra(cellSet.getStatement().getConnection());
        }

        public void unparse(SaxWriter writer)
            throws SAXException, OlapException
        {
            olapInfo(writer);
            axes(writer);
            cellData(writer);
        }

        public void metadata(SaxWriter writer) {
            writer.verbatim(MD_DATA_SET_XML_SCHEMA);
        }

        private void olapInfo(SaxWriter writer) throws OlapException {
            // What are all of the cube's hierachies
            Cube cube = cellSet.getMetaData().getCube();

            writer.startElement("OlapInfo");
            writer.startElement("CubeInfo");
            writer.startElement("Cube");
            writer.textElement("CubeName", cube.getName());
            writer.endElement();
            writer.endElement(); // CubeInfo

            // create AxesInfo for axes
            // -----------
            writer.startSequence("AxesInfo", "AxisInfo");
            final List<CellSetAxis> axes = cellSet.getAxes();
            List<Hierarchy> axisHierarchyList = new ArrayList<Hierarchy>();
            for (int i = 0; i < axes.size(); i++) {
                List<Hierarchy> hiers =
                    axisInfo(writer, axes.get(i), "Axis" + i);
                axisHierarchyList.addAll(hiers);
            }
            ///////////////////////////////////////////////
            // create AxesInfo for slicer axes
            //
            List<Hierarchy> hierarchies;
            CellSetAxis slicerAxis = cellSet.getFilterAxis();
            if (omitDefaultSlicerInfo) {
                hierarchies =
                    axisInfo(
                        writer, slicerAxis, "SlicerAxis");
            } else {
                // The slicer axes contains the default hierarchy
                // of each dimension not seen on another axis.
                List<Dimension> unseenDimensionList =
                    new ArrayList<Dimension>(cube.getDimensions());
                for (Hierarchy hier1 : axisHierarchyList) {
                    unseenDimensionList.remove(hier1.getDimension());
                }
                hierarchies = new ArrayList<Hierarchy>();
                for (Dimension dimension : unseenDimensionList) {
                    for (Hierarchy hierarchy : dimension.getHierarchies()) {
                        hierarchies.add(hierarchy);
                    }
                }
                writer.startElement(
                    "AxisInfo",
                    "name", "SlicerAxis");
                writeHierarchyInfo(
                    writer, hierarchies,
                    getProps(slicerAxis.getAxisMetaData()));
                writer.endElement(); // AxisInfo
            }
            slicerAxisHierarchies = hierarchies;
            //
            ///////////////////////////////////////////////

            writer.endSequence(); // AxesInfo

            // -----------
            writer.startElement("CellInfo");
            cellProperty(writer, StandardCellProperty.VALUE, true, "Value");
            cellProperty(
                writer, StandardCellProperty.FORMATTED_VALUE, true, "FmtValue");
            cellProperty(
                writer, StandardCellProperty.FORMAT_STRING, true,
                "FormatString");
            cellProperty(
                writer, StandardCellProperty.LANGUAGE, false, "Language");
            cellProperty(
                writer, StandardCellProperty.BACK_COLOR, false, "BackColor");
            cellProperty(
                writer, StandardCellProperty.FORE_COLOR, false, "ForeColor");
            cellProperty(
                writer, StandardCellProperty.FONT_FLAGS, false, "FontFlags");
            writer.endElement(); // CellInfo
            // -----------
            writer.endElement(); // OlapInfo
        }

        private void cellProperty(
            SaxWriter writer,
            StandardCellProperty cellProperty,
            boolean evenEmpty,
            String elementName)
        {
            if (extra.shouldReturnCellProperty(
                    cellSet, cellProperty, evenEmpty))
            {
                writer.element(
                    elementName,
                    "name", cellProperty.getName());
            }
        }

        private List<Hierarchy> axisInfo(
            SaxWriter writer,
            CellSetAxis axis,
            String axisName)
        {
            writer.startElement(
                "AxisInfo",
                "name", axisName);

            List<Hierarchy> hierarchies;
            List<Property> props = new ArrayList<>(getProps(axis.getAxisMetaData()));
            Iterator<org.olap4j.Position> it = axis.getPositions().iterator();
            if (it.hasNext()) {
                final org.olap4j.Position position = it.next();
                hierarchies = new ArrayList<Hierarchy>();
                for (Member member : position.getMembers()) {
                    hierarchies.add(member.getHierarchy());
                }
            } else {
                hierarchies = axis.getAxisMetaData().getHierarchies();
            }

            // remove a property without a valid associated hierarchy
            props.removeIf(prop -> !isValidProp(axis.getPositions(), prop));

            writeHierarchyInfo(writer, hierarchies, props);

            writer.endElement(); // AxisInfo

            return hierarchies;
        }

        private boolean isValidProp(List<Position> positions, Property prop) {
            if(!(prop instanceof IMondrianOlap4jProperty)){
                return true;
            }

            for (Position pos : positions){
                if(pos.getMembers().stream()
                        .anyMatch(member -> Objects.nonNull(getHierarchyProperty(member, prop)))){
                    return true;
                }
            }
            return false;
        }

        private void writeHierarchyInfo(
            SaxWriter writer,
            List<Hierarchy> hierarchies,
            List<Property> props)
        {
            writer.startSequence(null, "HierarchyInfo");
            for (Hierarchy hierarchy : hierarchies) {
                writer.startElement(
                    "HierarchyInfo", "name", hierarchy.getName());
                for (final Property prop : props) {
                    if (prop instanceof IMondrianOlap4jProperty) {
                        writeProperty(writer, hierarchy, prop);
                    } else {
                        writeElement(writer, hierarchy, prop);
                    }
                }
                writer.endElement();
            }
            writer.endSequence(); // "HierarchyInfo"
        }

        private void writeProperty(
            SaxWriter writer, Hierarchy hierarchy,
            final Property prop)
        {
            IMondrianOlap4jProperty currentProperty =
                (IMondrianOlap4jProperty) prop;
            String thisHierarchyName = hierarchy.getName();
            String thatHierarchiName = currentProperty.getLevel()
                .getHierarchy().getName();
            if (thisHierarchyName.equals(thatHierarchiName)) {
                writeElement(writer, hierarchy, prop);
            }
        }

        private void writeElement(
            SaxWriter writer, Hierarchy hierarchy,
            final Property prop)
        {
            final String encodedProp = encoder
                .encode(prop.getName());
            final Object[] attributes = getAttributes(
                prop, hierarchy);
            writer.element(encodedProp, attributes);
        }

        private Object[] getAttributes(Property prop, Hierarchy hierarchy) {
            Property longProp = longProps.get(prop.getName());
            if (longProp == null) {
                longProp = prop;
            }
            List<Object> values = new ArrayList<Object>();
            values.add("name");
            values.add(
                hierarchy.getUniqueName()
                + "."
                + Util.quoteMdxIdentifier(longProp.getName()));
            if (longProp == prop) {
                // Adding type attribute to the optional properties
                values.add("type");
                values.add(getXsdType(longProp));
            }
            return values.toArray();
        }

        private String getXsdType(Property property) {
            Datatype datatype = property.getDatatype();
            switch (datatype) {
            case UNSIGNED_INTEGER:
                return RowsetDefinition.Type.UnsignedInteger.columnType;
            case DOUBLE:
                return RowsetDefinition.Type.Double.columnType;
            case LARGE_INTEGER:
                return RowsetDefinition.Type.Long.columnType;
            case INTEGER:
                return RowsetDefinition.Type.Integer.columnType;
            case BOOLEAN:
                return RowsetDefinition.Type.Boolean.columnType;
            default:
                return RowsetDefinition.Type.String.columnType;
            }
        }

        private void axes(SaxWriter writer) throws OlapException {
            writer.startSequence("Axes", "Axis");
            //axis(writer, result.getSlicerAxis(), "SlicerAxis");
            final List<CellSetAxis> axes = cellSet.getAxes();
            for (int i = 0; i < axes.size(); i++) {
                final CellSetAxis axis = axes.get(i);
                final List<Property> props = getProps(axis.getAxisMetaData());
                axis(writer, axis, props, "Axis" + i);
            }

            ////////////////////////////////////////////
            // now generate SlicerAxis information
            //
            if (omitDefaultSlicerInfo) {
                CellSetAxis slicerAxis = cellSet.getFilterAxis();
                // We always write a slicer axis. There are two 'empty' cases:
                // zero positions (which happens when the WHERE clause evalutes
                // to an empty set) or one position containing a tuple of zero
                // members (which happens when there is no WHERE clause) and we
                // need to be able to distinguish between the two.
                axis(
                    writer,
                    slicerAxis,
                    getProps(slicerAxis.getAxisMetaData()),
                    "SlicerAxis");
            } else {
                List<Hierarchy> hierarchies = slicerAxisHierarchies;
                writer.startElement(
                    "Axis",
                    "name", "SlicerAxis");
                writer.startSequence("Tuples", "Tuple");
                writer.startSequence("Tuple", "Member");

                Map<String, Integer> memberMap = new HashMap<String, Integer>();
                Member positionMember;
                CellSetAxis slicerAxis = cellSet.getFilterAxis();
                final List<Position> slicerPositions =
                    slicerAxis.getPositions();
                if (slicerPositions != null
                    && slicerPositions.size() > 0)
                {
                    final Position pos0 = slicerPositions.get(0);
                    int i = 0;
                    for (Member member : pos0.getMembers()) {
                        memberMap.put(member.getHierarchy().getName(), i++);
                    }
                }

                final List<Member> slicerMembers =
                    slicerPositions.isEmpty()
                        ? Collections.<Member>emptyList()
                        : slicerPositions.get(0).getMembers();
                for (Hierarchy hierarchy : hierarchies) {
                    // Find which member is on the slicer.
                    // If it's not explicitly there, use the default member.
                    Member member = hierarchy.getDefaultMember();
                    final Integer indexPosition =
                        memberMap.get(hierarchy.getName());
                    if (indexPosition != null) {
                        positionMember = slicerMembers.get(indexPosition);
                    } else {
                        positionMember = null;
                    }
                    for (Member slicerMember : slicerMembers) {
                        if (slicerMember.getHierarchy().equals(hierarchy)) {
                            member = slicerMember;
                            break;
                        }
                    }

                    if (member != null) {
                        if (positionMember != null) {
                            writeMember(
                                writer, positionMember, null,
                                slicerPositions.get(0), indexPosition,
                                getProps(slicerAxis.getAxisMetaData()));
                        } else {
                            slicerAxis(
                                writer, member,
                                getProps(slicerAxis.getAxisMetaData()));
                        }
                    } else {
                        LOGGER.warn(
                            "Can not create SlicerAxis: "
                            + "null default member for Hierarchy "
                            + hierarchy.getUniqueName());
                    }
                }
                writer.endSequence(); // Tuple
                writer.endSequence(); // Tuples
                writer.endElement(); // Axis
            }

            //
            ////////////////////////////////////////////

            writer.endSequence(); // Axes
        }

        private List<Property> getProps(CellSetAxisMetaData queryAxis) {
            if (queryAxis == null) {
                return defaultProps;
            }
            return CompositeList.of(
                defaultProps,
                queryAxis.getProperties());
        }

        private void axis(
            SaxWriter writer,
            CellSetAxis axis,
            List<Property> props,
            String axisName) throws OlapException
        {
            writer.startElement(
                "Axis",
                "name", axisName);
            writer.startSequence("Tuples", "Tuple");

            List<Position> positions = axis.getPositions();
            Iterator<Position> pit = positions.iterator();
            Position prevPosition = null;
            Position position = pit.hasNext() ? pit.next() : null;
            Position nextPosition = pit.hasNext() ? pit.next() : null;
            while (position != null) {
                writer.startSequence("Tuple", "Member");
                int k = 0;
                for (Member member : position.getMembers()) {
                    writeMember(
                        writer, member, prevPosition, nextPosition, k++, props);
                }
                writer.endSequence(); // Tuple
                prevPosition = position;
                position = nextPosition;
                nextPosition = pit.hasNext() ? pit.next() : null;
            }
            writer.endSequence(); // Tuples
            writer.endElement(); // Axis
        }

        private void writeMember(
            SaxWriter writer, Member member, Position prevPosition,
            Position nextPosition, int k, List<Property> props)
            throws OlapException
        {
            writer.startElement(
                "Member", "Hierarchy", member.getHierarchy().getName());
            for (final Property prop : props) {
                Object value = null;
                Property longProp = (longProps.get(prop.getName()) != null)
                    ? longProps.get(prop.getName()) : prop;
                if (longProp == StandardMemberProperty.DISPLAY_INFO) {
                    Integer childrenCard = (Integer) member
                        .getPropertyValue(
                            StandardMemberProperty.CHILDREN_CARDINALITY);
                    value = calculateDisplayInfo(
                        prevPosition, nextPosition, member, k, childrenCard);
                } else if (longProp == StandardMemberProperty.DEPTH) {
                    value = member.getDepth();
                } else {
                    value = (longProp instanceof IMondrianOlap4jProperty)
                        ? getHierarchyProperty(member, longProp)
                        : member.getPropertyValue(longProp);
                }
                if (value != null) {
                    writer.textElement(encoder.encode(prop.getName()), value);
                }
            }
            writer.endElement(); // Member
        }

        private Object getHierarchyProperty(
            Member member, Property longProp)
        {
            IMondrianOlap4jProperty currentProperty =
                (IMondrianOlap4jProperty) longProp;
            String thisHierarchyName = member.getHierarchy().getName();
            String thatHierarchyName = currentProperty.getLevel()
                .getHierarchy().getName();
            if (thisHierarchyName.equals(thatHierarchyName)) {
                try {
                    return member.getPropertyValue(currentProperty);
                } catch (OlapException e) {
                    throw new XmlaException(
                            SERVER_FAULT_FC,
                            HSB_BAD_PROPERTIES_LIST_CODE,
                            HSB_BAD_PROPERTIES_LIST_FAULT_FS,
                            e);
                }
            }
            // if property doesn't belong to current hierarchy return null
            return null;
        }

        private void slicerAxis(
            SaxWriter writer, Member member, List<Property> props)
            throws OlapException
        {
            writer.startElement(
                "Member",
                "Hierarchy", member.getHierarchy().getName());
            for (Property prop : props) {
                Object value;
                Property longProp = longProps.get(prop.getName());
                if (longProp == null) {
                    longProp = prop;
                }
                if (longProp == StandardMemberProperty.DISPLAY_INFO) {
                    Integer childrenCard =
                        (Integer) member.getPropertyValue(
                            StandardMemberProperty.CHILDREN_CARDINALITY);
                    // NOTE: don't know if this is correct for
                    // SlicerAxis
                    int displayInfo = 0xffff & childrenCard;
/*
                    int displayInfo =
                        calculateDisplayInfo((j == 0 ? null : positions[j - 1]),
                          (j + 1 == positions.length ? null : positions[j + 1]),
                          member, k, childrenCard.intValue());
*/
                    value = displayInfo;
                } else if (longProp == StandardMemberProperty.DEPTH) {
                    value = member.getDepth();
                } else {
                    value = member.getPropertyValue(longProp);
                }
                if (value != null) {
                    writer.textElement(
                        encoder.encode(prop.getName()), value);
                }
            }
            writer.endElement(); // Member
        }

        private int calculateDisplayInfo(
            Position prevPosition,
            Position nextPosition,
            Member currentMember,
            int memberOrdinal,
            int childrenCount)
        {
            int displayInfo = 0xffff & childrenCount;

            if (nextPosition != null) {
                String currentUName = currentMember.getUniqueName();
                Member nextMember =
                    nextPosition.getMembers().get(memberOrdinal);
                String nextParentUName = parentUniqueName(nextMember);
                if (currentUName.equals(nextParentUName)) {
                    displayInfo |= 0x10000;
                }
            }
            if (prevPosition != null) {
                String currentParentUName = parentUniqueName(currentMember);
                Member prevMember =
                    prevPosition.getMembers().get(memberOrdinal);
                String prevParentUName = parentUniqueName(prevMember);
                if (currentParentUName != null
                    && currentParentUName.equals(prevParentUName))
                {
                    displayInfo |= 0x20000;
                }
            }
            return displayInfo;
        }

        private String parentUniqueName(Member member) {
            final Member parent = member.getParentMember();
            if (parent == null) {
                return null;
            }
            return parent.getUniqueName();
        }

        private void cellData(SaxWriter writer) {
            writer.startSequence("CellData", "Cell");
            final int axisCount = cellSet.getAxes().size();
            List<Integer> pos = new ArrayList<Integer>();
            for (int i = 0; i < axisCount; i++) {
                pos.add(-1);
            }
            int[] cellOrdinal = new int[] {0};

            int axisOrdinal = axisCount - 1;
            recurse(writer, pos, axisOrdinal, cellOrdinal);

            writer.endSequence(); // CellData
        }

        private void recurse(
            SaxWriter writer,
            List<Integer> pos,
            int axisOrdinal,
            int[] cellOrdinal)
        {
            if (axisOrdinal < 0) {
                emitCell(writer, pos, cellOrdinal[0]++);
            } else {
                CellSetAxis axis = cellSet.getAxes().get(axisOrdinal);
                List<Position> positions = axis.getPositions();
                for (int i = 0, n = positions.size(); i < n; i++) {
                    pos.set(axisOrdinal, i);
                    recurse(writer, pos, axisOrdinal - 1, cellOrdinal);
                }
            }
        }

        private void emitCell(
            SaxWriter writer,
            List<Integer> pos,
            int ordinal)
        {
            Cell cell = cellSet.getCell(pos);
            if (cell.isNull() && ordinal != 0) {
                // Ignore null cell like MS AS, except for Oth ordinal
                return;
            }

            writer.startElement(
                "Cell",
                "CellOrdinal", ordinal);
            for (int i = 0; i < cellProps.size(); i++) {
                Property cellPropLong = cellPropLongs.get(i);
                Object value = cell.getPropertyValue(cellPropLong);
                if (value == null) {
                    continue;
                }
                if (!extra.shouldReturnCellProperty(
                        cellSet, cellPropLong, true))
                {
                    continue;
                }

                if (!json && cellPropLong == StandardCellProperty.VALUE) {
                    if (cell.isNull()) {
                        // Return cell without value as in case of AS2005
                        continue;
                    }
                    final String dataType =
                        (String) cell.getPropertyValue(
                            StandardCellProperty.DATATYPE);
                    final ValueInfo vi = new ValueInfo(dataType, value);
                    final String valueType = vi.valueType;
                    final String valueString;
                    if (vi.isDecimal) {
                        valueString =
                            XmlaUtil.normalizeNumericString(
                                vi.value.toString());
                    } else {
                        valueString = vi.value.toString();
                    }

                    writer.startElement(
                        cellProps.get(i).getName(),
                        "xsi:type", valueType);
                    writer.characters(valueString);
                    writer.endElement();
                } else {
                    writer.textElement(cellProps.get(i).getName(), value);
                }
            }
            writer.endElement(); // Cell
        }
    }

    static abstract class ColumnHandler {
        protected final String name;
        protected final String encodedName;

        protected ColumnHandler(String name) {
            this.name = name;
            this.encodedName =
                XmlaUtil.ElementNameEncoder.INSTANCE.encode(name);
        }

        abstract void write(SaxWriter writer, Cell cell, Member[] members)
            throws OlapException;
        abstract void metadata(SaxWriter writer);
    }


    /**
     * Callback to handle one column, representing the combination of a
     * level and a property (e.g. [Store].[Store State].[MEMBER_UNIQUE_NAME])
     * in a flattened dataset.
     */
    static class CellColumnHandler extends ColumnHandler {

        CellColumnHandler(String name) {
            super(name);
        }

        public void metadata(SaxWriter writer) {
            writer.element(
                "xsd:element",
                "minOccurs", 0,
                "name", encodedName,
                "sql:field", name);
        }

        public void write(
            SaxWriter writer, Cell cell, Member[] members)
        {
            if (cell.isNull()) {
                return;
            }
            Object value = cell.getValue();
            final String dataType = (String)
                cell.getPropertyValue(StandardCellProperty.DATATYPE);

            final ValueInfo vi = new ValueInfo(dataType, value);
            final String valueType = vi.valueType;
            value = vi.value;
            boolean isDecimal = vi.isDecimal;

            String valueString = value.toString();

            writer.startElement(
                encodedName,
                "xsi:type", valueType);
            if (isDecimal) {
                valueString = XmlaUtil.normalizeNumericString(valueString);
            }
            writer.characters(valueString);
            writer.endElement();
        }
    }

    /**
     * Callback to handle one column, representing the combination of a
     * level and a property (e.g. [Store].[Store State].[MEMBER_UNIQUE_NAME])
     * in a flattened dataset.
     */
    static class MemberColumnHandler extends ColumnHandler {
        private final Property property;
        private final Level level;
        private final int memberOrdinal;

        public MemberColumnHandler(
            Property property, Level level, int memberOrdinal)
        {
            super(
                level.getUniqueName() + "."
                + Util.quoteMdxIdentifier(property.getName()));
            this.property = property;
            this.level = level;
            this.memberOrdinal = memberOrdinal;
        }

        public void metadata(SaxWriter writer) {
            writer.element(
                "xsd:element",
                "minOccurs", 0,
                "name", encodedName,
                "sql:field", name,
                "type", XSD_STRING);
        }

        public void write(
            SaxWriter writer, Cell cell, Member[] members) throws OlapException
        {
            Member member = members[memberOrdinal];
            final int depth = level.getDepth();
            if (member.getDepth() < depth) {
                // This column deals with a level below the current member.
                // There is no value to write.
                return;
            }
            while (member.getDepth() > depth) {
                member = member.getParentMember();
            }
            final Object propertyValue = member.getPropertyValue(property);
            if (propertyValue == null) {
                return;
            }

            writer.startElement(encodedName);
            writer.characters(propertyValue.toString());
            writer.endElement();
        }
    }

    static class MDDataSet_Tabular extends MDDataSet {
        private final boolean empty;
        private final int[] pos;
        private final List<Integer> posList;
        private final int axisCount;
        private int cellOrdinal;

        private static final List<Property> MemberCaptionIdArray =
            Collections.<Property>singletonList(
                StandardMemberProperty.MEMBER_CAPTION);

        private final Member[] members;
        private final ColumnHandler[] columnHandlers;

        public MDDataSet_Tabular(CellSet cellSet) {
            super(cellSet);
            final List<CellSetAxis> axes = cellSet.getAxes();
            axisCount = axes.size();
            pos = new int[axisCount];
            posList = new IntList(pos);

            // Count dimensions, and deduce list of levels which appear on
            // non-COLUMNS axes.
            boolean empty = false;
            int dimensionCount = 0;
            for (int i = axes.size() - 1; i > 0; i--) {
                CellSetAxis axis = axes.get(i);
                if (axis.getPositions().size() == 0) {
                    // If any axis is empty, the whole data set is empty.
                    empty = true;
                    continue;
                }
                dimensionCount +=
                    axis.getPositions().get(0).getMembers().size();
            }
            this.empty = empty;

            // Build a list of the lowest level used on each non-COLUMNS axis.
            Level[] levels = new Level[dimensionCount];
            List<ColumnHandler> columnHandlerList =
                new ArrayList<ColumnHandler>();
            int memberOrdinal = 0;
            if (!empty) {
                for (int i = axes.size() - 1; i > 0; i--) {
                    final CellSetAxis axis = axes.get(i);
                    final int z0 = memberOrdinal; // save ordinal so can rewind
                    final List<Position> positions = axis.getPositions();
                    int jj = 0;
                    for (Position position : positions) {
                        memberOrdinal = z0; // rewind to start
                        for (Member member : position.getMembers()) {
                            if (jj == 0
                                || member.getLevel().getDepth()
                                > levels[memberOrdinal].getDepth())
                            {
                                levels[memberOrdinal] = member.getLevel();
                            }
                            memberOrdinal++;
                        }
                        jj++;
                    }

                    // Now we know the lowest levels on this axis, add
                    // properties.
                    List<Property> dimProps =
                        axis.getAxisMetaData().getProperties();
                    if (dimProps.size() == 0) {
                        dimProps = MemberCaptionIdArray;
                    }
                    for (int j = z0; j < memberOrdinal; j++) {
                        Level level = levels[j];
                        for (int k = 0; k <= level.getDepth(); k++) {
                            final Level level2 =
                                level.getHierarchy().getLevels().get(k);
                            if (level2.getLevelType() == Level.Type.ALL) {
                                continue;
                            }
                            for (Property dimProp : dimProps) {
                                columnHandlerList.add(
                                    new MemberColumnHandler(
                                        dimProp, level2, j));
                            }
                        }
                    }
                }
            }
            this.members = new Member[memberOrdinal + 1];

            // Deduce the list of column headings.
            if (axes.size() > 0) {
                CellSetAxis columnsAxis = axes.get(0);
                for (Position position : columnsAxis.getPositions()) {
                    String name = null;
                    int j = 0;
                    for (Member member : position.getMembers()) {
                        if (j == 0) {
                            name = member.getUniqueName();
                        } else {
                            name = name + "." + member.getUniqueName();
                        }
                        j++;
                    }
                    columnHandlerList.add(
                        new CellColumnHandler(name));
                }
            }

            this.columnHandlers =
                columnHandlerList.toArray(
                    new ColumnHandler[columnHandlerList.size()]);
        }

        public void metadata(SaxWriter writer) {
            writer.startElement(
                "xsd:schema",
                "xmlns:xsd", NS_XSD,
                "targetNamespace", NS_XMLA_ROWSET,
                "xmlns", NS_XMLA_ROWSET,
                "xmlns:xsi", NS_XSI,
                "xmlns:sql", NS_XML_SQL,
                "elementFormDefault", "qualified");

            { // <root>
                writer.startElement(
                    "xsd:element",
                    "name", "root");
                writer.startElement("xsd:complexType");
                writer.startElement("xsd:sequence");
                writer.element(
                    "xsd:element",
                    "maxOccurs", "unbounded",
                    "minOccurs", 0,
                    "name", "row",
                    "type", "row");
                writer.endElement(); // xsd:sequence
                writer.endElement(); // xsd:complexType
                writer.endElement(); // xsd:element name=root
            }

            { // xsd:simpleType name="uuid"
                writer.startElement(
                    "xsd:simpleType",
                    "name", "uuid");
                writer.startElement(
                    "xsd:restriction",
                    "base", XSD_STRING);
                writer.element(
                    "xsd:pattern",
                    "value", RowsetDefinition.UUID_PATTERN);
                writer.endElement(); // xsd:restriction
                writer.endElement(); // xsd:simpleType
            }

            { // xsd:complexType name="row"
                writer.startElement(
                    "xsd:complexType",
                    "name", "row");
                writer.startElement("xsd:sequence");
                for (ColumnHandler columnHandler : columnHandlers) {
                    columnHandler.metadata(writer);
                }
                writer.endElement(); // xsd:sequence
                writer.endElement(); // xsd:complexType
            }
            writer.endElement(); // xsd:schema
        }

        public void unparse(SaxWriter writer)
            throws SAXException, OlapException
        {
            if (empty) {
                return;
            }
            cellData(writer);
        }

        private void cellData(SaxWriter writer)
            throws SAXException, OlapException
        {
            cellOrdinal = 0;
            iterate(writer);
        }

        /**
         * Iterates over the resust writing tabular rows.
         *
         * @param writer Writer
         * @throws org.xml.sax.SAXException on error
         */
        private void iterate(SaxWriter writer)
            throws SAXException, OlapException
        {
            switch (axisCount) {
            case 0:
                // For MDX like: SELECT FROM Sales
                emitCell(writer, cellSet.getCell(posList));
                return;
            default:
//                throw new SAXException("Too many axes: " + axisCount);
                iterate(writer, axisCount - 1, 0);
                break;
            }
        }

        private void iterate(SaxWriter writer, int axis, final int xxx)
            throws OlapException
        {
            final List<Position> positions =
                cellSet.getAxes().get(axis).getPositions();
            int axisLength = axis == 0 ? 1 : positions.size();

            for (int i = 0; i < axisLength; i++) {
                final Position position = positions.get(i);
                int ho = xxx;
                final List<Member> members = position.getMembers();
                for (int j = 0;
                     j < members.size() && ho < this.members.length;
                     j++, ho++)
                {
                    this.members[ho] = position.getMembers().get(j);
                }

                ++cellOrdinal;
                Util.discard(cellOrdinal);

                if (axis >= 2) {
                    iterate(writer, axis - 1, ho);
                } else {
                    writer.startElement("row");// abrimos la fila
                    pos[axis] = i; // coordenadas: fila i
                    pos[0] = 0; // coordenadas (0,i): columna 0
                    for (ColumnHandler columnHandler : columnHandlers) {
                        if (columnHandler instanceof MemberColumnHandler) {
                            columnHandler.write(writer, null, this.members);
                        } else if (columnHandler instanceof CellColumnHandler) {
                            columnHandler.write(
                                writer, cellSet.getCell(posList), null);
                            pos[0]++;// next col.
                        }
                    }
                    writer.endElement(); // cerramos la fila
                }
            }
        }

        private void emitCell(SaxWriter writer, Cell cell)
            throws OlapException
        {
            ++cellOrdinal;
            Util.discard(cellOrdinal);

            // Ignore empty cells.
            final Object cellValue = cell.getValue();
            if (cellValue == null) {
                return;
            }

            writer.startElement("row");
            for (ColumnHandler columnHandler : columnHandlers) {
                columnHandler.write(writer, cell, members);
            }
            writer.endElement();
        }
    }

    private void discover(XmlaRequest request, XmlaResponse response)
        throws XmlaException
    {
        final RowsetDefinition rowsetDefinition =
            RowsetDefinition.valueOf(request.getRequestType());
        Rowset rowset = rowsetDefinition.getRowset(request, this);

        Format format = getFormat(request, Format.Tabular);
        if (format != Format.Tabular) {
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_DISCOVER_FORMAT_CODE,
                HSB_DISCOVER_FORMAT_FAULT_FS,
                new UnsupportedOperationException(
                    "<Format>: only 'Tabular' allowed in Discover method "
                    + "type"));
        }
        final Content content = getContent(request);

        SaxWriter writer = response.getWriter();
        writer.startDocument();

        writer.startElement(
            prefix + ":DiscoverResponse",
            "xmlns:" + prefix, NS_XMLA);
        writer.startElement(prefix + ":return");
        writer.startElement(
            "root",
            "xmlns", NS_XMLA_ROWSET,
            "xmlns:xsi", NS_XSI,
            "xmlns:xsd", NS_XSD,
            "xmlns:EX", NS_XMLA_EX);

        switch (content) {
        case Schema:
        case SchemaData:
            rowset.rowsetDefinition.writeRowsetXmlSchema(writer);
            break;
        }

        try {
            switch (content) {
            case Data:
            case SchemaData:
                rowset.unparse(response);
                break;
            }
        } catch (XmlaException xex) {
            throw xex;
        } catch (Throwable t) {
            throw new XmlaException(
                SERVER_FAULT_FC,
                HSB_DISCOVER_UNPARSE_CODE,
                HSB_DISCOVER_UNPARSE_FAULT_FS,
                t);
        } finally {
            // keep the tags balanced, even if there's an error
            try {
                writer.endElement();
                writer.endElement();
                writer.endElement();
            } catch (Throwable e) {
                // Ignore any errors balancing the tags. The original exception
                // is more important.
            }
        }

        writer.endDocument();
    }

    /**
     * Gets a Connection given a catalog (and implicitly the catalog's data
     * source) and the name of a user role.
     *
     * <p>If you want to pass in a role object, and you are making the call
     * within the same JVM (i.e. not RPC), register the role using
     * {@link mondrian.olap.MondrianServer#getLockBox()} and pass in the moniker
     * for the generated lock box entry. The server will retrieve the role from
     * the moniker.
     *
     * @param catalog Catalog name
     * @param schema Schema name
     * @param role User role name
     * @return Connection
     * @throws XmlaException If error occurs
     */
    protected OlapConnection getConnection(
        String catalog,
        String schema,
        final String role)
        throws XmlaException
    {
        return this.getConnection(
            catalog, schema, role,
            new Properties());
    }

    /**
     * Gets a Connection given a catalog (and implicitly the catalog's data
     * source) and the name of a user role.
     *
     * <p>If you want to pass in a role object, and you are making the call
     * within the same JVM (i.e. not RPC), register the role using
     * {@link mondrian.olap.MondrianServer#getLockBox()} and pass in the moniker
     * for the generated lock box entry. The server will retrieve the role from
     * the moniker.
     *
     * @param catalog Catalog name
     * @param schema Schema name
     * @param role User role name
     * @param props Properties to pass down to the native driver.
     * @return Connection
     * @throws XmlaException If error occurs
     */
    protected OlapConnection getConnection(
        String catalog,
        String schema,
        final String role,
        Properties props)
        throws XmlaException
    {
        try {
            return
                connectionFactory.getConnection(
                    catalog, schema, role, props);
        } catch (SecurityException e) {
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_ACCESS_DENIED_CODE,
                HSB_ACCESS_DENIED_FAULT_FS,
                e);
        } catch (SQLException e) {
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_CONNECTION_DATA_SOURCE_CODE,
                HSB_CONNECTION_DATA_SOURCE_FAULT_FS,
                e);
        }
    }

    private static class IntList extends AbstractList<Integer> {
        private final int[] ints;

        IntList(int[] ints) {
            this.ints = ints;
        }

        public Integer get(int index) {
            return ints[index];
        }

        public int size() {
            return ints.length;
        }
    }

    /**
     * Extra support for XMLA server. If a connection provides this interface,
     * the XMLA server will call methods in this interface instead of relying
     * on the core olap4j interface.
     *
     * <p>The {@link mondrian.xmla.XmlaHandler.XmlaExtraImpl} class provides
     * a default implementation that uses the olap4j interface exclusively.
     */
    public interface XmlaExtra {

        ResultSet executeDrillthrough(
            OlapStatement olapStatement,
            String mdx,
            boolean advanced,
            String tabFields,
            int[] rowCountSlot) throws SQLException;

        void setPreferList(OlapConnection connection);

        Date getSchemaLoadDate(Schema schema);

        int getLevelCardinality(Level level) throws OlapException;

        void getSchemaFunctionList(
            List<FunctionDefinition> funDefs,
            Schema schema,
            Util.Functor1<Boolean, String> functionFilter);

        int getHierarchyCardinality(Hierarchy hierarchy) throws OlapException;

        int getHierarchyStructure(Hierarchy hierarchy);

        boolean isHierarchyParentChild(Hierarchy hierarchy);

        int getMeasureAggregator(Member member);

        void checkMemberOrdinal(Member member) throws OlapException;

        /**
         * Returns whether we should return a cell property in the XMLA result.
         *
         * @param cellSet Cell set
         * @param cellProperty Cell property definition
         * @param evenEmpty Whether to return even if cell has no properties
         * @return Whether to return cell property in XMLA result
         */
        boolean shouldReturnCellProperty(
            CellSet cellSet,
            Property cellProperty,
            boolean evenEmpty);

        /**
         * Returns a list of names of roles in the given schema to which the
         * current user belongs.
         *
         * @param schema Schema
         * @return List of roles
         */
        List<String> getSchemaRoleNames(Schema schema);

        /**
         * Returns the unique ID of a schema.
         */
        String getSchemaId(Schema schema);

        String getCubeType(Cube cube);

        boolean isLevelUnique(Level level);

        /**
         * Returns the defined properties of a level. (Not including system
         * properties that every level has.)
         *
         * @param level Level
         * @return Defined properties
         */
        List<Property> getLevelProperties(Level level);

        boolean isPropertyInternal(Property property);

        /**
         * Returns a list of the data sources in this server. One element
         * per data source, each element a map whose keys are the XMLA fields
         * describing a data source: "DataSourceName", "DataSourceDescription",
         * "URL", etc. Unrecognized fields are ignored.
         *
         * @param connection Connection
         * @return List of data source definitions
         * @throws OlapException on error
         */
        List<Map<String, Object>> getDataSources(OlapConnection connection)
            throws OlapException;

        /**
         * Returns a map containing annotations on this element.
         *
         * @param element Element
         * @return Annotation map, never null
         */
        Map<String, Object> getAnnotationMap(MetadataElement element)
            throws SQLException;

        /**
         * Returns a boolean indicating if the specified
         * cell can be drilled on.
         */
        boolean canDrillThrough(Cell cell);

        /**
         * Returns the number of rows returned by a
         * drillthrough on the specified cell. Will also
         * return -1 if it cannot determine the cardinality.
         */
        int getDrillThroughCount(Cell cell);

        /**
         * Makes the connection send a command to the server
         * to flush all caches.
         */
        void flushSchemaCache(OlapConnection conn) throws OlapException;

        /**
         * Returns the key for a given member.
         */
        Object getMemberKey(Member m) throws OlapException;

        /**
         * Returns the ordering key for a given member.
         */
        Object getOrderKey(Member m) throws OlapException;

        class FunctionDefinition {
            public final String functionName;
            public final String description;
            public final String parameterList;
            public final int returnType;
            public final int origin;
            public final String interfaceName;
            public final String caption;

            public FunctionDefinition(
                String functionName,
                String description,
                String parameterList,
                int returnType,
                int origin,
                String interfaceName,
                String caption)
            {
                this.functionName = functionName;
                this.description = description;
                this.parameterList = parameterList;
                this.returnType = returnType;
                this.origin = origin;
                this.interfaceName = interfaceName;
                this.caption = caption;
            }
        }
    }

    /**
     * Default implementation of {@link mondrian.xmla.XmlaHandler.XmlaExtra}.
     * Connections based on mondrian's olap4j driver can do better.
     */
    private static class XmlaExtraImpl implements XmlaExtra {
        public ResultSet executeDrillthrough(
            OlapStatement olapStatement,
            String mdx,
            boolean advanced,
            String tabFields,
            int[] rowCountSlot) throws SQLException
        {
            return olapStatement.executeQuery(mdx);
        }

        public void setPreferList(OlapConnection connection) {
            // ignore
        }

        public Date getSchemaLoadDate(Schema schema) {
            return new Date();
        }

        public int getLevelCardinality(Level level) {
            return level.getCardinality();
        }

        public void getSchemaFunctionList(
            List<FunctionDefinition> funDefs,
            Schema schema,
            Util.Functor1<Boolean, String> functionFilter)
        {
            // no function definitions
        }

        public int getHierarchyCardinality(Hierarchy hierarchy) {
            int cardinality = 0;
            for (Level level : hierarchy.getLevels()) {
                cardinality += level.getCardinality();
            }
            return cardinality;
        }

        public int getHierarchyStructure(Hierarchy hierarchy) {
            return 0;
        }

        public boolean isHierarchyParentChild(Hierarchy hierarchy) {
            return false;
        }

        public int getMeasureAggregator(Member member) {
            return RowsetDefinition.MdschemaMeasuresRowset
                .MDMEASURE_AGGR_UNKNOWN;
        }

        public void checkMemberOrdinal(Member member) {
            // nothing to do
        }

        public boolean shouldReturnCellProperty(
            CellSet cellSet, Property cellProperty, boolean evenEmpty)
        {
            return true;
        }

        public List<String> getSchemaRoleNames(Schema schema) {
            return Collections.emptyList();
        }

        public String getSchemaId(Schema schema) {
            return schema.getName();
        }

        public String getCubeType(Cube cube) {
            return RowsetDefinition.MdschemaCubesRowset.MD_CUBTYPE_CUBE;
        }

        public boolean isLevelUnique(Level level) {
            return false;
        }

        public List<Property> getLevelProperties(Level level) {
            return level.getProperties();
        }

        public boolean isPropertyInternal(Property property) {
            return
                property instanceof Property.StandardMemberProperty
                && ((Property.StandardMemberProperty) property).isInternal()
                || property instanceof Property.StandardCellProperty
                && ((Property.StandardCellProperty) property).isInternal();
        }

        public List<Map<String, Object>> getDataSources(
            OlapConnection connection) throws OlapException
        {
            Database olapDb = connection.getOlapDatabase();
            final String modes = createCsv(olapDb.getAuthenticationModes());
            final String providerTypes = createCsv(olapDb.getProviderTypes());
            return Collections.singletonList(
                Olap4jUtil.mapOf(
                    "DataSourceName", (Object) olapDb.getName(),
                    "DataSourceDescription", olapDb.getDescription(),
                    "URL", olapDb.getURL(),
                    "DataSourceInfo", olapDb.getDataSourceInfo(),
                    "ProviderName", olapDb.getProviderName(),
                    "ProviderType", providerTypes,
                    "AuthenticationMode", modes));
        }

        public Map<String, Object> getAnnotationMap(MetadataElement element) {
            return Collections.emptyMap();
        }

        public boolean canDrillThrough(Cell cell) {
            return false;
        }

        public int getDrillThroughCount(Cell cell) {
            return -1;
        }

        public void flushSchemaCache(OlapConnection conn) {
            // no op.
        }

        public Object getMemberKey(Member m) throws OlapException {
            return
                m.getPropertyValue(
                    Property.StandardMemberProperty.MEMBER_KEY);
        }

        public Object getOrderKey(Member m) throws OlapException {
            return m.getOrdinal();
        }
    }

    private static String createCsv(Iterable<? extends Object> iterable) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Object o : iterable) {
            if (!first) {
                sb.append(',');
            }
            sb.append(o);
            first = false;
        }
        return sb.toString();
    }

    /**
     * Creates an olap4j connection for responding to XMLA requests.
     *
     * <p>A typical implementation will probably just use a
     * {@link javax.sql.DataSource} or a connect string, but it is important
     * that the connection is assigned to the correct catalog, schema and role
     * consistent with the client's XMLA context.
     */
    public interface ConnectionFactory {
        /**
         * Creates a connection.
         *
         * <p>The implementation passes the properties to the underlying driver.
         *
         * @param catalog The name of the catalog to use.
         * @param schema The name of the schema to use.
         * @param roleName The name of the role to use, or NULL.
         * @param props Properties to be passed to the underlying native driver.
         * @return An OlapConnection object.
         * @throws SQLException on error
         */
        OlapConnection getConnection(
            String catalog,
            String schema,
            String roleName,
            Properties props)
            throws SQLException;

        /**
         * Returns a map of property name-value pairs with which to populate
         * the response to the DISCOVER_DATASOURCES request.
         *
         * <p>Properties correspond to the columns of that request:
         * ""DataSourceName", et cetera.</p>
         *
         * <p>Returns null if there is no pre-configured response; in
         * which case, the driver will have to connect to get a response.</p>
         *
         * @return Column names and values for the DISCOVER_DATASOURCES
         * response
         */
        Map<String, Object> getPreConfiguredDiscoverDatasourcesResponse();
    }
}

// End XmlaHandler.java

/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2009 Pentaho and others
// All Rights Reserved.
*/
package mondrian.xmla;

import mondrian.olap.Util;
import mondrian.test.FoodMartTestCase;

import junit.framework.Assert;

import java.math.BigDecimal;
import java.math.BigInteger;
// Only in Java5 and above
//import java.math.MathContext;


/**
 * Extends FoodMartTestCase, adding support for testing XMLA Utility
 * functionality.
 *
 * @author Richard M. Emberson
 * @since Jul 12 2007
 */

public class XmlaTests extends FoodMartTestCase {
    public XmlaTests() {
    }

    public XmlaTests(String name) {
        super(name);
    }
    public void testXmlaUtilNormalizeNumericString() throws Exception {
        String vin = "1.0E10";
        String expected = "1.0E10";
        doXmlaUtilNormalizeNumericString(vin, expected);

        vin = "1.0E1";
        expected = "1.0E1";
        doXmlaUtilNormalizeNumericString(vin, expected);

        vin = "1.0E11";
        expected = "1.0E11";
        doXmlaUtilNormalizeNumericString(vin, expected);

        vin = "1.40";
        expected = "1.4";
        doXmlaUtilNormalizeNumericString(vin, expected);

        vin = "1.400";
        expected = "1.4";
        doXmlaUtilNormalizeNumericString(vin, expected);

        vin = "1.4040";
        expected = "1.404";
        doXmlaUtilNormalizeNumericString(vin, expected);

        vin = "1.0";
        expected = "1";
        doXmlaUtilNormalizeNumericString(vin, expected);

        vin = "1";
        expected = "1";
        doXmlaUtilNormalizeNumericString(vin, expected);

        vin = "10";
        expected = "10";
        doXmlaUtilNormalizeNumericString(vin, expected);
    }

    public void testXmlaHandlerGetValueTypeHint() throws Exception {
        String dataType = "Integer";
        doXmlaHandlerGetValueTypeHint(dataType, XmlaHandler.XSD_INT);

        dataType = "Numeric";
        doXmlaHandlerGetValueTypeHint(dataType, XmlaHandler.XSD_DOUBLE);

        dataType = "FOO";
        doXmlaHandlerGetValueTypeHint(dataType, XmlaHandler.XSD_STRING);

        dataType = null;
        doXmlaHandlerGetValueTypeHint(dataType, null);
    }

    public void testXmlaHandlerValueInfo() throws Exception {
        // Integer or null
        String dataType = "Integer";
        Object inputValue = new Integer(4);
        String valueType = XmlaHandler.XSD_INT;
        Object value = inputValue;
        boolean isDecimal = false;
        doXmlaHandlerValueInfo(
            dataType, inputValue, valueType, value, isDecimal);
        doXmlaHandlerValueInfo(
            null, inputValue, valueType, value, isDecimal);

        dataType = "Integer";
        inputValue = new Long((long)XmlaHandler.XSD_INT_MAX_INCLUSIVE + 1);
        valueType = XmlaHandler.XSD_LONG;
        value = inputValue;
        isDecimal = false;
        doXmlaHandlerValueInfo(
            dataType, inputValue, valueType, value, isDecimal);
        doXmlaHandlerValueInfo(
            null, inputValue, valueType, value, isDecimal);

        dataType = "Integer";
        inputValue = new Long((long)XmlaHandler.XSD_INT_MIN_INCLUSIVE - 1);
        valueType = XmlaHandler.XSD_LONG;
        value = inputValue;
        isDecimal = false;
        doXmlaHandlerValueInfo(
            dataType, inputValue, valueType, value, isDecimal);
        doXmlaHandlerValueInfo(
            null, inputValue, valueType, value, isDecimal);

        dataType = "Integer";
        inputValue = new BigInteger("9223372036854775807");
        valueType = XmlaHandler.XSD_LONG;
        value = new Long(XmlaHandler.XSD_LONG_MAX_INCLUSIVE);
        isDecimal = false;
        doXmlaHandlerValueInfo(
            dataType, inputValue, valueType, value, isDecimal);
        doXmlaHandlerValueInfo(
            null, inputValue, valueType, value, isDecimal);

        dataType = "Integer";
        inputValue = new BigInteger("-9223372036854775808");
        valueType = XmlaHandler.XSD_LONG;
        value = new Long(XmlaHandler.XSD_LONG_MIN_INCLUSIVE);
        isDecimal = false;
        doXmlaHandlerValueInfo(
            dataType, inputValue, valueType, value, isDecimal);
        doXmlaHandlerValueInfo(
            null, inputValue, valueType, value, isDecimal);

        // one more than XSD_LONG_MAX_INCLUSIVE
        dataType = "Integer";
        inputValue = new BigInteger("9223372036854775808");
        valueType = XmlaHandler.XSD_INTEGER;
        value = inputValue;
        isDecimal = false;
        doXmlaHandlerValueInfo(
            dataType, inputValue, valueType, value, isDecimal);
        doXmlaHandlerValueInfo(
            null, inputValue, valueType, value, isDecimal);

        // one less than XSD_LONG_MIN_INCLUSIVE
        dataType = "Integer";
        inputValue = new BigInteger("-9223372036854775809");
        valueType = XmlaHandler.XSD_INTEGER;
        value = inputValue;
        isDecimal = false;
        doXmlaHandlerValueInfo(
            dataType, inputValue, valueType, value, isDecimal);
        doXmlaHandlerValueInfo(
            null, inputValue, valueType, value, isDecimal);

        dataType = "Integer";
        inputValue = new BigDecimal("9223372036854775807.0");
        valueType = XmlaHandler.XSD_LONG;
        value = new Long(XmlaHandler.XSD_LONG_MAX_INCLUSIVE);
        isDecimal = false;
        doXmlaHandlerValueInfo(
            dataType, inputValue, valueType, value, isDecimal);
        valueType = XmlaHandler.XSD_DECIMAL;
        value = inputValue;
        isDecimal = true;
        doXmlaHandlerValueInfo(
            null, inputValue, valueType, value, isDecimal);

        dataType = "Integer";
        inputValue = new BigDecimal("-9223372036854775808.0");
        valueType = XmlaHandler.XSD_LONG;
        value = new Long(XmlaHandler.XSD_LONG_MIN_INCLUSIVE);
        isDecimal = false;
        doXmlaHandlerValueInfo(
            dataType, inputValue, valueType, value, isDecimal);
        valueType = (Util.Retrowoven)
            ? XmlaHandler.XSD_DOUBLE : XmlaHandler.XSD_DECIMAL;
        value = (Util.Retrowoven)
            ? Double.valueOf("-9.223372036854776E18")
            : inputValue;
        isDecimal = true;
        doXmlaHandlerValueInfo(
            null, inputValue, valueType, value, isDecimal);

        // Numeric or null
        dataType = "Numeric";
        inputValue = new Double(4.0);
        valueType = XmlaHandler.XSD_DOUBLE;
        value = inputValue;
        isDecimal = true;
        doXmlaHandlerValueInfo(
            dataType, inputValue, valueType, value, isDecimal);
        doXmlaHandlerValueInfo(
            null, inputValue, valueType, value, isDecimal);

        dataType = "Numeric";
        inputValue = new Integer(4);
        valueType = XmlaHandler.XSD_DOUBLE;
        value = new Double(4.0);
        isDecimal = true;
        doXmlaHandlerValueInfo(
            dataType, inputValue, valueType, value, isDecimal);

        dataType = "Numeric";
        inputValue = new Long(4);
        valueType = XmlaHandler.XSD_DOUBLE;
        value = new Double(4.0);
        isDecimal = true;
        doXmlaHandlerValueInfo(
            dataType, inputValue, valueType, value, isDecimal);

        // MAX_VALUE = 1.7976931348623157e+308
        // one less decimal point than max value
        if (! Util.Retrowoven) {
            dataType = "Numeric";
            inputValue = new BigDecimal("1.797693134862315e+308");
            valueType = XmlaHandler.XSD_DOUBLE;
            value = Double.valueOf("1.797693134862315e+308");
            isDecimal = true;
            doXmlaHandlerValueInfo(
                dataType, inputValue, valueType, value, isDecimal);
        }

/*
 does not work - BigDecimal converts double 4.9E-323
                        to 4.940656458412465E-323
        // MIN_VALUE = 4.9e-324
        // slightly larger
        dataType = "Numeric";
        inputValue = new BigDecimal("4.9e-323");
                            //MathContext.DECIMAL32);
                            //MathContext.DECIMAL64);
        valueType = XmlaHandler.XSD_DOUBLE;
        value = Double.valueOf("4.9e-323");
System.out.println("    value=" +value);
        isDecimal = true;
        doXmlaHandlerValueInfo(dataType, inputValue,
                                valueType, value, isDecimal);
*/

        dataType = "Numeric";
        inputValue = new BigDecimal("1.9e+500");
        valueType = XmlaHandler.XSD_DECIMAL;
        value = inputValue;
        isDecimal = true;
        doXmlaHandlerValueInfo(
            dataType, inputValue, valueType, value, isDecimal);


        dataType = "Numeric";
        inputValue = new BigInteger("4");
        valueType = XmlaHandler.XSD_INT;
        value = new Integer(4);
        isDecimal = false;
        doXmlaHandlerValueInfo(
            dataType, inputValue, valueType, value, isDecimal);
    }

    // Helper methods
    protected void doXmlaUtilNormalizeNumericString(
        final String vin,
        final String expected)
        throws Exception
    {
        String actual = XmlaUtil.normalizeNumericString(vin);
        Assert.assertEquals(expected, actual);
    }

    protected void doXmlaHandlerGetValueTypeHint(
        final String dataType,
        final String expected)
        throws Exception
    {
        String actual = XmlaHandler.ValueInfo.getValueTypeHint(dataType);
        Assert.assertEquals(expected, actual);
    }

    protected void doXmlaHandlerValueInfo(
        final String dataType,
        final Object inputValue,
        final String valueType,
        final Object value,
        final boolean isDecimal)
        throws Exception
    {
        XmlaHandler.ValueInfo vi =
            new XmlaHandler.ValueInfo(dataType, inputValue);
        Assert.assertEquals("valueType:", valueType, vi.valueType);
        Assert.assertEquals("value:", value, vi.value);
        Assert.assertEquals("isDecimal:", isDecimal, vi.isDecimal);
    }

}

// End XmlaTests.java


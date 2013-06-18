/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.xmla;

import junit.framework.TestCase;

import java.math.BigInteger;

/**
 * Unit test to validate expected marshalling of Java objects
 * to their respective XML Schema types
 * {@link mondrian.xmla}).
 *
 * @author mcampbell
 */
public class XmlaHandlerTypeTest extends TestCase {

    TestVal[] typeTests = {
        TestVal.having("StringValue", "xsd:string", "String"),
        TestVal.having(new Double(0), "xsd:double", "Numeric"),
        TestVal.having(new Integer(0), "xsd:int", "Integer"),
        TestVal.having(Long.MAX_VALUE, "xsd:long", "Integer"),
        TestVal.having(new Float(0), "xsd:float", "Numeric"),
        TestVal.having(Byte.MAX_VALUE, "xsd:byte", "Integer"),
        TestVal.having(Short.MAX_VALUE, "xsd:short", "Integer"),
        TestVal.having(new Boolean(true), "xsd:boolean",  null),
        TestVal.having(
            BigInteger.valueOf(Long.MAX_VALUE)
            .add(BigInteger.valueOf(1)), "xsd:integer", "Integer")
    };


    public void testMarshalledValueType() {
        // run through the tests once with no hint, then again with
        // the hint value.
        for (TestVal val : typeTests) {
            assertEquals(
                val.expectedXsdType,
                new XmlaHandler.ValueInfo(null, val.value).valueType);
        }

        for (TestVal val : typeTests) {
            assertEquals(
                val.expectedXsdType,
                new XmlaHandler.ValueInfo(val.hint, val.value).valueType);
        }
    }

    static class TestVal {
        Object value;
        String expectedXsdType;
        String hint;

        static TestVal having(Object val, String xsd, String hint) {
            TestVal typeTest = new TestVal();
            typeTest.value = val;
            typeTest.expectedXsdType = xsd;
            typeTest.hint = hint;
            return typeTest;
        }
    }

}

// End XmlaHandlerTypeTest.java


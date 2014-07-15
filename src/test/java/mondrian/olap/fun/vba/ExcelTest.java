/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2010 Pentaho
// All Rights Reserved.
*/
package mondrian.olap.fun.vba;

import junit.framework.TestCase;

/**
 * Unit tests for implementations of Excel worksheet functions.
 *
 * <p>Every function defined in {@link Excel} must have a test here. In addition,
 * there should be MDX tests (usually in
 * {@link mondrian.olap.fun.FunctionTest}) if handling of argument types,
 * result types, operator overloading, exception handling or null handling
 * are non-trivial.
 *
 * @author jhyde
 * @since Jan 16, 2008
 */
public class ExcelTest extends TestCase {
    private static final double SMALL = 1e-10d;

    public void testAcos() {
        // Cos(0) = 1
        // Cos(60 degrees) = .5
        // Cos(90 degrees) = 0
        // Cos(180 degrees) = -1
        assertEquals(0.0, Excel.acos(1.0));
        assertEquals(Math.PI / 3.0, Excel.acos(.5), SMALL);
        assertEquals(Math.PI / 2.0, Excel.acos(0.0));
        assertEquals(Math.PI, Excel.acos(-1.0));
    }

    public void testAcosh() {
        // acosh(1) = 0
        // acosh(2) ~= 1
        // acosh(4) ~= 2
        assertEquals(0.0, Excel.acosh(1.0));
        assertEquals(1.3169578969248166, Excel.acosh(2.0), SMALL);
        assertEquals(2.0634370688955608, Excel.acosh(4.0), SMALL);
    }

    public void testAsinh() {
        // asinh(0) = 0
        // asinh(1) ~= 1
        // asinh(10) ~= 3
        // asinh(-x) = -asinh(x)
        assertEquals(0.0, Excel.asinh(0.0));
        assertEquals(0.8813735870195429, Excel.asinh(1.0), SMALL);
        assertEquals(2.99822295029797, Excel.asinh(10.0), SMALL);
        assertEquals(-2.99822295029797, Excel.asinh(-10.0), SMALL);
    }

    public void testAtan2() {
        assertEquals(Math.atan2(0, 10), Excel.atan2(0, 10));
        assertEquals(Math.atan2(1, .8), Excel.atan2(1, .8));
        assertEquals(Math.atan2(-5, 0), Excel.atan2(-5, 0));
    }

    public void testAtanh() {
        // atanh(0) = 0
        // atanh(1) = +inf
        // atanh(-x) = -atanh(x)
        assertEquals(0.0, Excel.atanh(0));
        assertEquals(0.0100003333533347, Excel.atanh(0.01), SMALL);
        assertEquals(0.549306144334054, Excel.atanh(0.5), SMALL);
        assertEquals(1.4722194895832, Excel.atanh(0.9), SMALL);
        assertEquals(2.64665241236224, Excel.atanh(0.99), SMALL);
        assertEquals(6.1030338227611125, Excel.atanh(0.99999), SMALL);
        assertEquals(-6.1030338227611125, Excel.atanh(-0.99999), SMALL);
    }

    public void testCosh() {
        assertEquals(Math.cosh(0), Excel.cosh(0));
    }

    public void testDegrees() {
        assertEquals(90.0, Excel.degrees(Math.PI / 2));
    }

    public void testLog10() {
        assertEquals(1.0, Excel.log10(10));
        assertEquals(-2.0, Excel.log10(.01), 0.00000000000001);
    }

    public void testPi() {
        assertEquals(Math.PI, Excel.pi());
    }

    public void testPower() {
        assertEquals(0.0, Excel.power(0, 5));
        assertEquals(1.0, Excel.power(5, 0));
        assertEquals(2.0, Excel.power(4, 0.5));
        assertEquals(0.125, Excel.power(2, -3));
    }

    public void testRadians() {
        assertEquals(Math.PI, Excel.radians(180.0));
        assertEquals(-Math.PI * 3.0, Excel.radians(-540.0));
    }

    public void testSinh() {
        assertEquals(Math.sinh(0), Excel.sinh(0));
    }

    public void testSqrtPi() {
        // sqrt(2 pi) = sqrt(6.28) ~ 2.5
        assertEquals(2.506628274631, Excel.sqrtPi(2.0), SMALL);
    }

    public void testTanh() {
        assertEquals(Math.tanh(0), Excel.tanh(0));
        assertEquals(Math.tanh(0.44), Excel.tanh(0.44));
    }

    public void testMod() {
        assertEquals(2.0, Excel.mod(28, 13));
        assertEquals(-11.0, Excel.mod(28, -13));
    }

    public void testIntNative() {
        assertEquals(5, Vba.intNative(5.1));
        assertEquals(5, Vba.intNative(5.9));
        assertEquals(-6, Vba.intNative(-5.9));
        assertEquals(0, Vba.intNative(0.1));
        assertEquals(0, Vba.intNative(0));
    }
}

// End ExcelTest.java

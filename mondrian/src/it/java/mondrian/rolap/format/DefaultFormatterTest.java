/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/
package mondrian.rolap.format;

import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit Test for {@link DefaultFormatter}.
 */
public class DefaultFormatterTest extends TestCase {

    // property values and their expected formatted values
    private static final Map<Object, String> VALUES =
            new HashMap<Object, String>() {{
                // string
                put("String", "String");

                // integer
                put(1234567, "1234567");
                put(0, "0");
                put(-0, "0");
                put(-1234567890, "-1234567890");

                // long
                put(1234567L, "1234567");
                put(0L, "0");
                put(-1234567890123456L, "-1234567890123456");
                put(1200000000000000000L, "1200000000000000000");

                // float
                put(1234567f, "1234567");
                put(1234567.0f, "1234567");
                put(123.4567f, "123.4567");
                put(0f, "0");
                put(0.0f, "0");
                put(1.234567e-1f, "0.1234567");
                put(1.234567e-23f, "0.00000000000000000000001234567");
                put(1.234567e20f, "123456700000000000000");

                // double
                put(123.4567, "123.4567");
                put(1.234567e2, "123.4567");
                put(1.234567e25, "12345670000000000000000000");
                put(0.1234567, "0.1234567");
                put(1.234567e-1, "0.1234567");
                put(1200000000000000000.0, "1200000000000000000");
                put(
                    0.00000000000000000001234567,
                    "0.00000000000000000001234567");
                put(1.234567e-20, "0.00000000000000000001234567");
                put(12E2, "1200");
                put(12E20, "1200000000000000000000");
                put(1.2E21, "1200000000000000000000");
                put(1.2E-20, "0.000000000000000000012");
                put(-1.2E-20, "-0.000000000000000000012");
            }};

    private DefaultFormatter formatter = new DefaultFormatter();

    /**
     * <p>
     * Given that the value to format is a number.
     * </p>
     * When the formatted value is requested, then the output should not contain
     * any unwanted decimal digits due to floating point representation,
     * as well as scientific notations.
     */
    public void testNumberFormatting() {
        for (Map.Entry<Object, String> entry : VALUES.entrySet()) {
            String formatted = formatter.format(entry.getKey());

            assertEquals(
                "Value type: " + entry.getKey().getClass().toString(),
                entry.getValue(), formatted);
        }
    }
}
// End DefaultFormatterTest.java
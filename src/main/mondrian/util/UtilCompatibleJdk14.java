/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.util;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Random;

/**
 * Implementation of {@link UtilCompatible} which runs in
 * JDK 1.4.
 *
 * <p>The code uses JDK 1.5 constructs such as generics and for-each loops,
 * but retroweaver can convert these. It does not use
 * <code>java.util.EnumSet</code>, which is important, because retroweaver has
 * trouble with this.
 *
 * @author jhyde
 * @version $Id$
 * @since Feb 5, 2007
 */
public class UtilCompatibleJdk14 implements UtilCompatible {
    private static String previousUuid = "";
    private static final String UUID_BASE =
        Long.toHexString(new Random().nextLong());

    /**
     * This generates a BigDecimal that can have a precision that does
     * not reflect the precision of the input double.
     *
     * @param d input double
     * @return BigDecimal
     */
    public BigDecimal makeBigDecimalFromDouble(double d) {
        return new BigDecimal(d);
    }

    public String quotePattern(String s) {
        int slashEIndex = s.indexOf("\\E");
        if (slashEIndex == -1) {
            return "\\Q" + s + "\\E";
        }
        StringBuilder sb = new StringBuilder(s.length() * 2);
        sb.append("\\Q");
        int current = 0;
        while ((slashEIndex = s.indexOf("\\E", current)) != -1) {
            sb.append(s.substring(current, slashEIndex));
            current = slashEIndex + 2;
            sb.append("\\E\\\\E\\Q");
        }
        sb.append(s.substring(current, s.length()));
        sb.append("\\E");
        return sb.toString();
    }

    public <T> T getAnnotation(
        Method method, String annotationClassName, T defaultValue)
    {
        return defaultValue;
    }

    public String generateUuidString() {
        return generateUuidStringStatic();
    }

    public static synchronized String generateUuidStringStatic() {
        while (true) {
            String uuid =
                UUID_BASE
                + Long.toHexString(System.currentTimeMillis());
            if (!uuid.equals(previousUuid)) {
                previousUuid = uuid;
                return uuid;
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public <T> T compileScript(
        Class<T> iface,
        String script,
        String engineName)
    {
        throw new UnsupportedOperationException(
            "Scripting not supported until Java 1.6");
    }

    public <T> void threadLocalRemove(ThreadLocal<T> threadLocal) {
        // nothing: ThreadLocal.remove() does not exist until JDK 1.5
    }
}

// End UtilCompatibleJdk14.java

/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import java.util.Locale;

/**
 * Implementation of {@link mondrian.util.UtilCompatible} that runs in
 * JDK 1.7.
 *
 * <p>Prior to JDK 1.7, this class should never be loaded. Applications should
 * instantiate this class via {@link Class#forName(String)} or better, use
 * methods in {@link mondrian.olap.Util}, and not instantiate it at all.
 */
@SuppressWarnings("UnusedDeclaration")
public class UtilCompatibleJdk17 extends UtilCompatibleJdk16 {
    @Override
    public Locale localeForLanguageTag(String localeString) {
        return Locale.forLanguageTag(localeString);
    }
}

// End UtilCompatibleJdk17.java

/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 December, 2001
*/

package mondrian.resource;
import java.util.Locale;

/**
 * A <code>Resource</code> is a collection of messages for a particular
 * software component and locale. It is loaded from an XML file whose root
 * element is <code>&lt;BaflResourceList&gt;</code>.
 *
 * <p>Given such an XML file, {@link ResourceGen} can generate Java a wrapper
 * class which implements this interface, and also has a method to create an
 * error for each message.</p>
 *
 * @author jhyde
 * @since 3 December, 2001
 * @version $Id$
 **/
public interface Resource {
    /**
     * Populates this <code>Resource</code> from a URL.
     *
     * @param url The URL of the XML file containing the error messages
     * @param locale The ISO locale code (e.g. <code>"en"</code>, or
     *    <code>"en_US"</code>, or <code>"en_US_WIN"</code>) of the messages
     * @throws IOException if <code>url</code> cannot be opened, or if the
     *    format of its contents are invalid
     **/
    void init(java.net.URL url, Locale locale) throws java.io.IOException;

    /**
     * Populates this <code>Resource</code> from an XML document.
     *
     * @param resourceList The URL of the XML file containing the error messages
     * @param locale The ISO locale code (e.g. <code>"en"</code>, or
     *    <code>"en_US"</code>, or <code>"en_US_WIN"</code>) of the messages
     **/
    void init(ResourceDef.ResourceBundle resourceList, Locale locale);

    /**
     * Returns the locale of the messages.
     **/
    Locale getLocale();

    /**
     * Formats the message corresponding to <code>code</code> with the given
     * arguments. If an argument is not supplied, the tokens remain in the
     * returned message string.
     **/
    String formatError(int code, Object[] args);

    /**
     * Returns the severity of this message.
     **/
    int getSeverity(int code);
    int SEVERITY_INFO = 0;
    int SEVERITY_ERR  = 1;
    int SEVERITY_WARN = 2;
    int SEVERITY_NON_FATAL_ERR = 3;
}

// End Resource.java

/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 2 August, 2001
*/

package mondrian.xom;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;

/**
 * The <code>Parser</code> interface abstracts the behavior which the
 * <code>mondrian.xom</code> package needs from an XML parser.
 *
 * <p>If you don't care which implementation you get, call {@link
 * XOMUtil#createDefaultParser} to create a parser.</p>
 *
 * @author jhyde
 * @since 2 August, 2001
 * @version $Id$
 **/
public interface Parser {
    /** Parses a string and returns a wrapped element. */
    DOMWrapper parse(String sXml) throws XOMException;
    /** Parses an input stream and returns a wrapped element. **/
    DOMWrapper parse(InputStream is) throws XOMException;
    /** Parses the contents of a URL and returns a wrapped element .**/
    DOMWrapper parse(URL url) throws XOMException;
    /** Parses the contents of a reader and returns a wrapped element. **/
    DOMWrapper parse(Reader reader) throws XOMException;
    /** Creates a wrapper representing an XML element. **/
    DOMWrapper create(String tagName);
}

// End Parser.java

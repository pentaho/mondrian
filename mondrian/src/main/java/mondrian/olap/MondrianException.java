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

package mondrian.olap;

/**
 * Instances of this class are thrown for all exceptions that Mondrian
 * generates through as a result of known error conditions. It is used in the
 * resource classes generated from mondrian.resource.MondrianResource.xml.
 *
 * @author Galt Johnson (gjabx)
 * @see org.eigenbase.xom
 */
public class MondrianException extends RuntimeException {
    public MondrianException() {
        super();
    }

    public MondrianException(Throwable cause) {
        super(cause);
    }

    public MondrianException(String message) {
        super(message);
    }

    public MondrianException(String message, Throwable cause) {
        super(message, cause);
    }

    public String getLocalizedMessage() {
        return getMessage();
    }

    public String getMessage() {
        return "Mondrian Error:" + super.getMessage();
    }
}

// End MondrianException.java

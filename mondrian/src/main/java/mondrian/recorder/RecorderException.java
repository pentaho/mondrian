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

package mondrian.recorder;

import mondrian.olap.MondrianException;

/**
 * Exception thrown by {@link MessageRecorder} when too many errors
 * have been reported.
 *
 * @author Richard M. Emberson
 */
public final class RecorderException extends MondrianException {
     protected RecorderException(String msg) {
        super(msg);
    }
}

// End RecorderException.java

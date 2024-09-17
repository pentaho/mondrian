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
package mondrian.olap4j;

import mondrian.olap.Level;

import org.olap4j.metadata.Property;

/**
 * Wraps {@org.olap4j.metadata.Propery} to provide
 * information about level that contains current property
 *
 * @author Yury_Bakhmutski
 * @since Apr 17, 2015
 *
 */
public interface IMondrianOlap4jProperty extends Property {

    /**
     * @return {@mondrian.olap.Level}
     */
    Level getLevel();

}

// End IMondrianOlap4jProperty.java
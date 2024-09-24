/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2017 Hitachi Vantara and others
// All Rights Reserved.
*/
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
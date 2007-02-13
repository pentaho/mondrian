/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi;


import mondrian.rolap.RolapHierarchy;
import mondrian.rolap.agg.Aggregation;


/**
 * Definition of a data source change listener.
 *
 * A change listener can be specified in the connection string.  It is used
 * to ask what is changed in the datasource (e.g. database).
 *
 * Everytime mondrian has to decide whether it will use data from cache, it
 * will call the change listener.  When the change listener tells mondrian
 * the datasource has changed for a dimension, cube, ... then mondrian will
 * flush the cache and read from database again.
 *
 * It is specified in the connection string, like this :
 *
 * <blockquote><code>
 * Jdbc=jdbc:odbc:MondrianFoodMart; JdbcUser=ziggy; JdbcPassword=stardust; DataSourceChangeListener=com.acme.MyChangeListener;
 * </code></blockquote>
 *
 * This class should be called in mondrian before any data is read, so
 * even before cache is build.  This way, the plugin is able to register
 * the first timestamp mondrian tries to read the datasource.
 *
 * @author Bart Pappyn
 * @version $Id$
 * @since Dec 12, 2006
 */

public interface DataSourceChangeListener {
	
    /**
     * Checks if the given hierarchy has changed since the previous
     * time this function was called.
     *
     * The first time, this function will be called when the cache
     * is still empty.  This is because the plugin is able to register
     * the first timestamp the function was accessed.
     *
     * It is highly recommended to optimize the plugin and minimize
     * the time needed to evaluate this function, because this plugin
     * is called many times for each mondrian query.
     */
    public boolean isHierarchyChanged(RolapHierarchy hierarchy);

    /**
     * Checks if the given aggregation has changed since the previous
     * time this function was called.
     *
     * The first time, this function will be called when the cache
     * is still empty.  This is because the plugin is able to register
     * the first timestamp the function was accessed.
     */
    public boolean isAggregationChanged(Aggregation aggregation);
}

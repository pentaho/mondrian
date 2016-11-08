/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.spi.impl;

import mondrian.olap.MondrianDef;
import mondrian.rolap.RolapHierarchy;
import mondrian.rolap.agg.AggregationKey;
import mondrian.spi.DataSourceChangeListener;

import java.util.Random;


/**
 * Default implementation of a data source change listener
 * that always returns that the datasource is changed.
 *
 * A change listener can be specified in the connection string.  It is used
 * to ask what is changed in the datasource (e.g. database).
 *
 * Everytime mondrian has to decide whether it will use data from cache, it
 * will call the change listener.  When the change listener tells mondrian
 * the datasource has changed for a dimension, cube, ... then mondrian will
 * flush the cache and read from database again.
 *
 * It is specified in the connection string, like this:
 *
 * <blockquote><code>
 * Jdbc=jdbc:odbc:MondrianFoodMart; JdbcUser=ziggy; JdbcPassword=stardust;
 * DataSourceChangeListener=com.acme.MyChangeListener;
 * </code></blockquote>
 *
 * This class should be called in mondrian before any data is read, so
 * even before cache is build.  This way, the plugin is able to register
 * the first timestamp mondrian tries to read the datasource.
 *
 * @author Bart Pappyn
 * @since Dec 12, 2006
 */

public class DataSourceChangeListenerImpl4 implements DataSourceChangeListener {
    private int flushInverseFrequencyHierarchy;
    private int flushInverseFrequencyAggregation;
    final Random random = new Random(123456);

    /** Creates a new instance of DataSourceChangeListenerImpl2 */
    public DataSourceChangeListenerImpl4() {
        this(0, 0);
    }

    public DataSourceChangeListenerImpl4(
        int flushInverseFrequencyHierarchy,
        int flushInverseFrequencyAggregation)
    {
        this.flushInverseFrequencyHierarchy = flushInverseFrequencyHierarchy;
        this.flushInverseFrequencyAggregation =
            flushInverseFrequencyAggregation;
    }

    public synchronized boolean isHierarchyChanged(RolapHierarchy hierarchy) {
        if (flushInverseFrequencyHierarchy != 0) {
            if (random.nextInt(flushInverseFrequencyHierarchy) == 0) {
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    public synchronized boolean isAggregationChanged(
        AggregationKey aggregation)
    {
        if (flushInverseFrequencyAggregation != 0) {
            if (random.nextInt(flushInverseFrequencyAggregation) == 0) {
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    public String getTableName(RolapHierarchy hierarchy) {
        MondrianDef.RelationOrJoin relation = hierarchy.getRelation();
        if (relation instanceof MondrianDef.Table) {
            MondrianDef.Table tableRelation = (MondrianDef.Table)relation;
            return tableRelation.name;
        } else {
            return null;
        }
    }
}

// End DataSourceChangeListenerImpl4.java

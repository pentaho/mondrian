/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import java.util.List;

/**
 * Describes where to find level information in the datasource results
 */
public class LevelColumnLayout<T> {
    private final List<T> keys;
    private final T name;
    private final T caption;
    private final OrderKeySource orderBySource;
    private final List<T> orderBys;
    private final List<T> properties;
    private final List<T> parents;

    public LevelColumnLayout(
        List<T> keys,
        T name,
        T caption,
        OrderKeySource orderBySource,
        List<T> orderBys,
        List<T> properties,
        List<T> parents)
    {
        this.keys = keys;
        this.name = name;
        this.caption = caption;
        this.orderBySource = orderBySource;
        this.orderBys = orderBys;
        this.properties = properties;
        this.parents = parents;
    }

    /**
     * Returns the List of keys for accessing database results
     */
    public List<T> getKeys() {
        return keys;
    }

    /**
     * Returns the key for accessing the name field in database results
     */
    public T getNameKey() {
        return name;
    }

    /**
     * Returns the key for accessing the caption field in database results
     */
    public T getCaptionKey() {
        return caption;
    }

    /**
     * Returns the Order Key Source for database results
     */
    public OrderKeySource getOrderBySource() {
        return orderBySource;
    }

    /**
     * Returns List of keys for orderBy fields in database results
     */
    public List<T> getOrderByKeys() {
        return orderBys;
    }

    /**
     * Returns the List of keys for accessing property fields in
     * database results
     */
    public List<T> getPropertyKeys() {
        return properties;
    }

    /**
     * Returns the List of keys for accessing parent fields in
     * database results
     */
    public List<T> getParentKeys() {
        return parents;
    }

    public enum OrderKeySource {
        NONE,
        KEY,
        NAME,
        MAPPED
    }
}
// End LevelColumnLayout.java

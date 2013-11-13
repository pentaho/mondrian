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

    public List<T> getKeys() {
        return keys;
    }

    public T getName() {
        return name;
    }

    public T getCaption() {
        return caption;
    }

    public OrderKeySource getOrderBySource() {
        return orderBySource;
    }

    public List<T> getOrderBys() {
        return orderBys;
    }

    public List<T> getProperties() {
        return properties;
    }

    public List<T> getParents() {
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

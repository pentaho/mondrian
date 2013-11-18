/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.spi;

import mondrian.olap.Util;
import mondrian.rolap.DefaultDataServicesProvider;
import mondrian.util.ServiceDiscovery;

import org.apache.commons.collections.*;

import java.util.List;

import static org.apache.commons.collections.CollectionUtils.*;

/**
 * Locates available implementations of {@link DataServicesProvider}
 */
public class DataServicesLocator {
    public static DataServicesProvider getDataServicesProvider(
        final String className)
    {
        if (Util.isEmpty(className)) {
            return new DefaultDataServicesProvider();
        } else {
            ServiceDiscovery<DataServicesProvider> discovery =
                ServiceDiscovery.forClass(DataServicesProvider.class);
            List<Class<DataServicesProvider>> implementors =
                discovery.getImplementor();
            Predicate providerNamePredicate = new Predicate() {
                public boolean evaluate(Object o) {
                    Class<DataServicesProvider> providerClass =
                        (Class<DataServicesProvider>) o;
                    return providerClass.getName().equals(className);
                }
            };
            Class<DataServicesProvider> provider =
                (Class<DataServicesProvider>) find(
                    implementors, providerNamePredicate);
            if (provider != null) {
                try {
                    return provider.newInstance();
                } catch (InstantiationException e) {
                    throw new RuntimeException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
            throw new RuntimeException(
                "Unrecognized Service Provider: " + className);
        }
    }
}
// End DataServicesLocator.java

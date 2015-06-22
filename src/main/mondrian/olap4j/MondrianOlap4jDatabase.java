/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap4j;

import mondrian.olap.OlapElement;

import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.impl.Named;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.metadata.*;

import java.util.Collections;
import java.util.List;

/**
 * Implementation of {@link org.olap4j.metadata.Database}
 * for the Mondrian OLAP engine.
 *
 * @author LBoudreau
 */
class MondrianOlap4jDatabase
    extends MondrianOlap4jMetadataElement
    implements Database, Named
{
    private NamedList<MondrianOlap4jCatalog> catalogs;
    private final MondrianOlap4jConnection olap4jConnection;
    private final String name;
    private final String description;
    private final String providerName;
    private final String url;
    private final String dataSourceInfo;
    private final List<ProviderType> providerType;
    private final List<AuthenticationMode> authenticationMode;

    /**
     * Creates a MondrianOlap4jDatabase.
     *
     * @param olap4jConnection Connection
     * @param name Name of database
     * @param description Description of database
     * @param providerName Provider name
     * @param url URL of provider
     * @param dataSourceInfo Data source info
     * @param providerType List of provider types supported by this database
     * @param authenticationMode Authentication modes
     */
    MondrianOlap4jDatabase(
        MondrianOlap4jConnection olap4jConnection,
        String name,
        String description,
        String providerName,
        String url,
        String dataSourceInfo,
        List<ProviderType> providerType,
        List<AuthenticationMode> authenticationMode)
    {
        this.olap4jConnection = olap4jConnection;
        this.name = name;
        this.description = description;
        this.providerName = providerName;
        this.url = url;
        this.dataSourceInfo = dataSourceInfo;
        this.providerType =
            Collections.unmodifiableList(providerType);
        this.authenticationMode =
            Collections.unmodifiableList(authenticationMode);
    }

    public List<AuthenticationMode> getAuthenticationModes()
        throws OlapException
    {
        return authenticationMode;
    }

    public NamedList<Catalog> getCatalogs() throws OlapException {
        return Olap4jUtil.cast(getOlap4jCatalogs());
    }

    protected NamedList<MondrianOlap4jCatalog> getOlap4jCatalogs() {
        if (this.catalogs == null) {
            this.catalogs = this.olap4jConnection.getCatalogs(this.name);
        }
        return this.catalogs;
    }

    public String getDescription() throws OlapException {
        return this.description;
    }

    public String getName() {
        return this.name;
    }

    public OlapConnection getOlapConnection() {
        return this.olap4jConnection;
    }

    public String getProviderName() throws OlapException {
        return this.providerName;
    }

    public List<ProviderType> getProviderTypes() throws OlapException {
        return this.providerType;
    }

    public String getURL() throws OlapException {
        return this.url;
    }

    public String getDataSourceInfo() throws OlapException {
        return this.dataSourceInfo;
    }

    protected OlapElement getOlapElement() {
        return null;
    }
}

// End MondrianOlap4jDatabase.java

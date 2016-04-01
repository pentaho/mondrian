/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2015-2015 Pentaho Corporation..  All rights reserved.
*/
package mondrian.rolap.agg;

import mondrian.olap.MondrianServer;
import mondrian.rolap.BitKey;
import mondrian.rolap.RolapSchema;
import mondrian.rolap.RolapStar;
import mondrian.rolap.cache.SegmentCacheIndex;
import mondrian.server.Locus;
import mondrian.server.monitor.CellCacheSegmentCreateEvent;
import mondrian.server.monitor.Monitor;
import mondrian.spi.SegmentColumn;
import mondrian.spi.SegmentHeader;
import mondrian.test.PropertyRestoringTestCase;
import mondrian.util.ByteString;

import java.util.Collections;

import static org.mockito.Mockito.*;

/**
 * @author Andrey Khayrutdinov
 */
public class SegmentCacheManager_ExternalCache_Test
    extends PropertyRestoringTestCase
{

    private static final String TABLE = "factTable";

    private MondrianServer server;
    private SegmentHeader cachedHeader;
    private RolapSchema schema;

    private SegmentCacheManager manager;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        initDefaultSettings();

        schema = mockSchema();
        cachedHeader = createSegmentHeader();
        prepareMockCache();
        server = mockServer();

        manager = new SegmentCacheManager(server);
    }

    private void initDefaultSettings() {
        // ensure caching is enabled
        propSaver.set(propSaver.properties.DisableCaching, false);
        // switch off in-memory cache in these tests
        propSaver.set(propSaver.properties.DisableLocalSegmentCache, true);
        // define test instance of cache
        propSaver.set(
            propSaver.properties.SegmentCache,
            MockSegmentCache.class.getName());
    }

    private RolapSchema mockSchema() {
        RolapSchema schema = mock(RolapSchema.class);
        when(schema.getName()).thenReturn("schema");
        when(schema.getChecksum()).thenReturn(
            new ByteString("schema".getBytes()));
        return schema;
    }

    private SegmentHeader createSegmentHeader() {
        return new SegmentHeader(
            schema.getName(), schema.getChecksum(), "cube", "measure",
            Collections.<SegmentColumn>emptyList(),
            Collections.<String>emptyList(), TABLE,
            BitKey.EMPTY, Collections.<SegmentColumn>emptyList());
    }

    private void prepareMockCache() {
        MockSegmentCache cache = new MockSegmentCache();
        // clear static map and initialise with values for test
        cache.tearDown();
        cache.put(cachedHeader, MockSegmentCache.mockSegmentBody("body"));
    }

    private MondrianServer mockServer() {
        MondrianServer server = mock(MondrianServer.class);
        Monitor monitor = mock(Monitor.class);
        when(server.getMonitor()).thenReturn(monitor);
        return server;
    }


    @Override
    public void tearDown() throws Exception {
        manager.shutdown();
        manager = null;

        server = null;
        cachedHeader = null;
        schema = null;

        super.tearDown();
    }


    private RolapStar mockStar() {
        return mockStar(TABLE);
    }

    private RolapStar mockStar(String tableName) {
        RolapStar.Table table = mock(RolapStar.Table.class);
        when(table.getAlias()).thenReturn(tableName);

        RolapStar star = mock(RolapStar.class);
        when(star.getFactTable()).thenReturn(table);
        when(star.getSchema()).thenReturn(schema);
        return star;
    }


    public void testAddsPreloadedCacheForKnownTable() {
        final RolapStar star = mockStar();

        assertTrue(manager.loadCacheForStar(star));
        verify(server.getMonitor(), times(1))
            .sendEvent(any(CellCacheSegmentCreateEvent.class));

        SegmentCacheIndex index = manager.execute(
            new SegmentCacheManager.Command<SegmentCacheIndex>()
            {
                @Override
                public Locus getLocus() {
                    return null;
                }

                @Override
                public SegmentCacheIndex call() throws Exception {
                    return manager.getIndexRegistry().getIndex(star);
                }
        });
        assertTrue(index.contains(cachedHeader));
    }

    public void testDoesNothing_ForUnknownTable() {
        RolapStar star = mockStar("someTable");

        assertFalse(manager.loadCacheForStar(star));
        verify(server.getMonitor(), never())
            .sendEvent(any(CellCacheSegmentCreateEvent.class));
    }

    @SuppressWarnings("deprecation")
    public void testDoesNothing_WhenCacheIsDisabled() {
        propSaver.set(propSaver.properties.DisableCaching, true);
        RolapStar star = mockStar();

        assertFalse(manager.loadCacheForStar(star));
        assertTrue(manager.getStarFactTablesToSync().contains(TABLE));
    }
}

// End SegmentCacheManager_ExternalCache_Test.java

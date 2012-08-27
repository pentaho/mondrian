package mondrian.spi.impl;

import javax.sql.DataSource;

import mondrian.olap.Util;
import mondrian.spi.DataSourceResolver;
import mondrian.spi.SchemaKeyProcessor;

public class DataSourceResolverSchemaKeyProcessor implements SchemaKeyProcessor {

	@Override
	public String generateKey(
            final String catalogUrl,
            final String connectionKey,
            final String jdbcUser,
            final String dataSourceStr,
            final DataSource dataSource,
            final Util.PropertyList connectInfo) throws Exception {

		DataSource keyDs = dataSource;
		
		if (dataSourceStr != null) {
			DataSourceResolver dataSourceResolver = Util.getDataSourceResolver();
			try {
				keyDs = dataSourceResolver.lookup(dataSourceStr);
			} catch (Exception e) {
				throw Util.newInternal(
						e,
						"Error while looking up data source ("
						+ dataSourceStr + ")");
			}
		}
    	 
        String key =
            (keyDs == null)
            ? makeKey(catalogUrl, connectionKey, jdbcUser, dataSourceStr)
            : makeKey(catalogUrl, keyDs);

		
		return key;
	}

	
    /**
     * Creates a key with which to identify a schema in the cache.
     */
    private static String makeKey(
        final String catalogUrl,
        final String connectionKey,
        final String jdbcUser,
        final String dataSourceStr)
    {
        final StringBuilder buf = new StringBuilder(100);

        appendIfNotNull(buf, catalogUrl);
        appendIfNotNull(buf, connectionKey);
        appendIfNotNull(buf, jdbcUser);
        appendIfNotNull(buf, dataSourceStr);

        return buf.toString();
    }

    /**
     * Creates a key with which to identify a schema in the cache.
     */
    private static String makeKey(
        final String catalogUrl,
        final DataSource dataSource)
    {
        final StringBuilder buf = new StringBuilder(100);

        appendIfNotNull(buf, catalogUrl);
        buf.append('.');
        buf.append("external#");
        buf.append(System.identityHashCode(dataSource));

        return buf.toString();
    }

    private static void appendIfNotNull(StringBuilder buf, String s) {
        if (s != null) {
            if (buf.length() > 0) {
                buf.append('.');
            }
            buf.append(s);
        }
    }

}

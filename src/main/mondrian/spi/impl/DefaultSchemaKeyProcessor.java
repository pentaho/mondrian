package mondrian.spi.impl;

import javax.sql.DataSource;

import mondrian.olap.Util;
import mondrian.spi.SchemaKeyProcessor;

public class DefaultSchemaKeyProcessor implements SchemaKeyProcessor {

	@Override
	public String generateKey(
            final String catalogUrl,
            final String connectionKey,
            final String jdbcUser,
            final String dataSourceStr,
            final DataSource dataSource,
            final Util.PropertyList connectInfo) throws Exception {

		String key =
            (dataSource == null)
            ? makeKey(catalogUrl, connectionKey, jdbcUser, dataSourceStr)
            : makeKey(catalogUrl, dataSource);
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

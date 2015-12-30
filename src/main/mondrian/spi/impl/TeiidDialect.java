package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.SQLException;

public class TeiidDialect extends JdbcDialectImpl {

	public static final JdbcDialectFactory FACTORY = new JdbcDialectFactory(TeiidDialect.class, DatabaseProduct.TEIID);

	/**
	 * @param connection
	 * @throws SQLException
	 */
	public TeiidDialect(Connection connection) throws SQLException {
		super(connection);
	}

	@Override
	public String generateOrderByNulls(String expr, boolean ascending, boolean collateNullsLast) {

		return super.generateOrderByNullsAnsi(expr, ascending, collateNullsLast);

	}

	@Override
	public String generateOrderItem(String expr, boolean nullable, boolean ascending, boolean collateNullsLast) {

		if (nullable) {
			return generateOrderByNulls(expr, ascending, collateNullsLast);
		} else {
			if (ascending) {
				return expr + " ASC";
			} else {
				return expr + " DESC";
			}
		}
	}

}

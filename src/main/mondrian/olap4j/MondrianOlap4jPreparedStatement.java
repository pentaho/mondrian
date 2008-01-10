/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap4j;

import org.olap4j.*;
import org.olap4j.type.*;
import org.olap4j.metadata.*;

import java.sql.*;
import java.math.BigDecimal;
import java.io.InputStream;
import java.io.Reader;
import java.util.Calendar;
import java.net.URL;

import mondrian.olap.Query;
import mondrian.olap.Parameter;

/**
 * Implementation of {@link PreparedOlapStatement}
 * for the Mondrian OLAP engine.
 *
 * <p>This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs;
 * it is instantiated using {@link Factory#newPreparedStatement}.</p>
 *
 * @author jhyde
 * @version $Id$
 * @since Jun 12, 2007
 */
abstract class MondrianOlap4jPreparedStatement
    extends MondrianOlap4jStatement
    implements PreparedOlapStatement, OlapParameterMetaData
{
    private final String mdx;
    private Query query;
    MondrianOlap4jCellSetMetaData cellSetMetaData;

    public MondrianOlap4jPreparedStatement(
        MondrianOlap4jConnection olap4jConnection,
        String mdx)
    {
        super(olap4jConnection);
        this.mdx = mdx;
        this.query = olap4jConnection.connection.parseQuery(mdx);
        this.cellSetMetaData = new MondrianOlap4jCellSetMetaData(this, query);
    }

    // override OlapStatement

    public CellSet executeOlapQuery(String mdx) throws OlapException {
        this.query = olap4jConnection.connection.parseQuery(mdx);
        this.cellSetMetaData = new MondrianOlap4jCellSetMetaData(this, query);
        return executeOlapQueryInternal(query);
    }

    // implement PreparedOlapStatement

    public CellSet executeQuery() throws OlapException {
        return executeOlapQueryInternal(query);
    }

    public OlapParameterMetaData getParameterMetaData() throws OlapException {
        return this;
    }

    public Cube getCube() {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement

    public int executeUpdate() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        getParameter(parameterIndex).setValue(null);
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setBigDecimal(
        int parameterIndex, BigDecimal x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setBytes(int parameterIndex, byte x[]) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setTime(int parameterIndex, Time x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setTimestamp(
        int parameterIndex, Timestamp x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setAsciiStream(
        int parameterIndex, InputStream x, int length) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setUnicodeStream(
        int parameterIndex, InputStream x, int length) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setBinaryStream(
        int parameterIndex, InputStream x, int length) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void clearParameters() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setObject(
        int parameterIndex, Object x, int targetSqlType) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setObject(int parameterIndex, Object x) throws SQLException {
        final Parameter parameter = getParameter(parameterIndex);
        if (x instanceof MondrianOlap4jMember) {
            MondrianOlap4jMember mondrianOlap4jMember =
                (MondrianOlap4jMember) x;
            x = mondrianOlap4jMember.member;
        }
        parameter.setValue(x);
    }

    public boolean execute() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void addBatch() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setCharacterStream(
        int parameterIndex, Reader reader, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public CellSetMetaData getMetaData() {
        return cellSetMetaData;
    }

    public void setDate(
        int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setTime(
        int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setTimestamp(
        int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setNull(
        int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setObject(
        int parameterIndex,
        Object x,
        int targetSqlType,
        int scaleOrLength) throws SQLException {
        throw new UnsupportedOperationException();
    }

    // implement OlapParameterMetaData

    public String getParameterName(int param) throws OlapException {
        Parameter paramDef = getParameter(param);
        return paramDef.getName();
    }

    private Parameter getParameter(int param) throws OlapException {
        final Parameter[] parameters = query.getParameters();
        if (param < 1 || param > parameters.length) {
            throw this.olap4jConnection.helper.toOlapException(
                this.olap4jConnection.helper.createException(
                    "parameter ordinal " + param + " out of range"));
        }
        return parameters[param - 1];
    }

    public Type getParameterOlapType(int param) throws OlapException {
        Parameter paramDef = getParameter(param);
        return olap4jConnection.toOlap4j(paramDef.getType());
    }

    public int getParameterCount() {
        return query.getParameters().length;
    }

    public int isNullable(int param) throws SQLException {
        return ParameterMetaData.parameterNullableUnknown;
    }

    public boolean isSigned(int param) throws SQLException {
        final Type type = getParameterOlapType(param);
        return type instanceof NumericType;
    }

    public int getPrecision(int param) throws SQLException {
        final Type type = getParameterOlapType(param);
        if (type instanceof NumericType) {
            return 0; // precision not applicable
        }
        if (type instanceof StringType) {
            return Integer.MAX_VALUE;
        }
        return 0;
    }

    public int getScale(int param) throws SQLException {
        return 0; // scale not applicable
    }

    public int getParameterType(int param) throws SQLException {
        final Type type = getParameterOlapType(param);
        if (type instanceof NumericType) {
            return Types.NUMERIC;
        } else if (type instanceof StringType) {
            return Types.VARCHAR;
        } else if (type instanceof NullType) {
            return Types.NULL;
        } else {
            return Types.OTHER;
        }
    }

    public String getParameterTypeName(int param) throws SQLException {
        final Type type = getParameterOlapType(param);
        return type.toString();
    }

    public String getParameterClassName(int param) throws SQLException {
        final Type type = getParameterOlapType(param);
        return foo(
            new TypeHelper<Class>() {
                public Class booleanType(BooleanType type) {
                    return Boolean.class;
                }

                public Class<Cube> cubeType(CubeType cubeType) {
                    return Cube.class;
                }

                public Class<Number> decimalType(DecimalType decimalType) {
                    return Number.class;
                }

                public Class<Dimension> dimensionType(DimensionType dimensionType) {
                    return Dimension.class;
                }

                public Class<Hierarchy> hierarchyType(HierarchyType hierarchyType) {
                    return Hierarchy.class;
                }

                public Class<Level> levelType(LevelType levelType) {
                    return Level.class;
                }

                public Class<Member> memberType(MemberType memberType) {
                    return Member.class;
                }

                public Class<Void> nullType(NullType nullType) {
                    return Void.class;
                }

                public Class<Number> numericType(NumericType numericType) {
                    return Number.class;
                }

                public Class<Iterable> setType(SetType setType) {
                    return Iterable.class;
                }

                public Class<String> stringType(StringType stringType) {
                    return String.class;
                }

                public Class<Member[]> tupleType(TupleType tupleType) {
                    return Member[].class;
                }

                public Class symbolType(SymbolType symbolType) {
                    // parameters cannot be of this type
                    throw new UnsupportedOperationException();
                }
            },
            type).getName();
    }

    public int getParameterMode(int param) throws SQLException {
        Parameter paramDef = getParameter(param); // forces param range check
        return ParameterMetaData.parameterModeIn;
    }

    // Helper classes

    private interface TypeHelper<T> {
        T booleanType(BooleanType type);
        T cubeType(CubeType cubeType);
        T decimalType(DecimalType decimalType);
        T dimensionType(DimensionType dimensionType);
        T hierarchyType(HierarchyType hierarchyType);
        T levelType(LevelType levelType);
        T memberType(MemberType memberType);
        T nullType(NullType nullType);
        T numericType(NumericType numericType);
        T setType(SetType setType);
        T stringType(StringType stringType);
        T tupleType(TupleType tupleType);
        T symbolType(SymbolType symbolType);
    }

    <T> T foo(TypeHelper<T> helper, Type type) {
        if (type instanceof BooleanType) {
            return helper.booleanType((BooleanType) type);
        } else if (type instanceof CubeType) {
            return helper.cubeType((CubeType) type);
        } else if (type instanceof DecimalType) {
            return helper.decimalType((DecimalType) type);
        } else if (type instanceof DimensionType) {
            return helper.dimensionType((DimensionType) type);
        } else if (type instanceof HierarchyType) {
            return helper.hierarchyType((HierarchyType) type);
        } else if (type instanceof LevelType) {
            return helper.levelType((LevelType) type);
        } else if (type instanceof MemberType) {
            return helper.memberType((MemberType) type);
        } else if (type instanceof NullType) {
            return helper.nullType((NullType) type);
        } else if (type instanceof NumericType) {
            return helper.numericType((NumericType) type);
        } else if (type instanceof SetType) {
            return helper.setType((SetType) type);
        } else if (type instanceof StringType) {
            return helper.stringType((StringType) type);
        } else if (type instanceof TupleType) {
            return helper.tupleType((TupleType) type);
        } else if (type instanceof SymbolType) {
            return helper.symbolType((SymbolType) type);
        } else {
            throw new UnsupportedOperationException();
        }
    }
}

// End MondrianOlap4jPreparedStatement.java

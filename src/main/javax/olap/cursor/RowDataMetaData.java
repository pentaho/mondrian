package javax. olap. cursor;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import java. io.*;
import javax. olap. query. querycoremodel.*; import java. math.*;
import java. util.*; import javax. olap. query. querytransaction.*;


public interface RowDataMetaData extends NamedObject {
// class scalar attributes
// class references // class operations
public int getColumnCount() throws OLAPException; public boolean isCaseSensitive( int arg0) throws OLAPException;
public boolean isCurrency( int arg0) throws OLAPException; public int isNullable( int arg0) throws OLAPException;
public boolean isSigned( int arg0) throws OLAPException; public int getColumnDisplaySize( int arg0) throws OLAPException;
public String getColumnLabel( int arg0) throws OLAPException; public String getColumnName( int arg0) throws OLAPException;
public int getPrecision( int arg0) throws OLAPException; public int getScale( int arg0) throws OLAPException;
public int getColumnType( int arg0) throws OLAPException; public String getColumnTypeName( int arg0) throws OLAPException;
public String getColumnClassName( int arg0) throws OLAPException;
}



package javax. olap. resource;
import java. util. List;
import java. util. Collection;
import javax. olap. OLAPException;
import javax. olap. metadata.*;
import javax. olap. query. querycoremodel.*;
import javax. jmi. reflect.*;


public interface ConnectionFactory extends RefObject {
// class scalar attributes
// class references // class operations
public Connection getConnection() throws OLAPException;
public Connection getConnection( ConnectionSpec properties) throws OLAPException;
public ConnectionSpec createConnectionSpec() throws OLAPException;
public ResourceAdapterMetaData getMetaData() throws OLAPException;
}


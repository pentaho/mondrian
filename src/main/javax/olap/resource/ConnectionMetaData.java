package javax. olap. resource;
import java. util. List;
import java. util. Collection;
import javax. olap. OLAPException;
import javax. olap. metadata.*;
import javax. olap. query. querycoremodel.*;
import javax. jmi. reflect.*;


public interface ConnectionMetaData {
// class scalar attributes
// class references // class operations
public String getEISProductName() throws OLAPException;
public String getEISProductVersion() throws OLAPException;
public String getUserName() throws OLAPException;
}



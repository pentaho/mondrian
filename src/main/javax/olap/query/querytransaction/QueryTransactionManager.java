package javax. olap. query. querytransaction;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import javax. olap. resource.*;
import javax. olap. query. querycoremodel.*; import javax. olap. query. enumerations.*;
import javax. olap. query. calculatedmembers.*; import javax. olap. query. dimensionfilters.*;
import javax. olap. query. edgefilters.*; import javax. olap. query. sorting.*;


public interface QueryTransactionManager extends NamedObject {
// class scalar attributes
// class references
public void setConnection( Connection input) throws OLAPException;


public Connection getConnection() throws OLAPException;
public void setQueryTransaction( Collection input) throws OLAPException;
public Collection getQueryTransaction() throws OLAPException;
public void removeQueryTransaction( QueryTransaction input) throws OLAPException;


public void setCurrentTransaction( QueryTransaction input) throws OLAPException;
public QueryTransaction getCurrentTransaction() throws OLAPException;
// class operations
public QueryTransaction beginRootTransaction() throws OLAPException;
public QueryTransaction beginChildSubTransaction() throws OLAPException;
public void prepareCurrentTransaction() throws OLAPException; public void commitCurrentTransaction() throws OLAPException;
public void rollbackCurrentTransaction() throws OLAPException;
}



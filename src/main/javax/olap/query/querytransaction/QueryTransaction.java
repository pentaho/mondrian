package javax. olap. query. querytransaction;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import javax. olap. resource.*;
import javax. olap. query. querycoremodel.*; import javax. olap. query. enumerations.*;
import javax. olap. query. calculatedmembers.*; import javax. olap. query. dimensionfilters.*;
import javax. olap. query. edgefilters.*; import javax. olap. query. sorting.*;
public interface QueryTransaction extends NamedObject {
// class scalar attributes
// class references
public void setTransactionElements( Collection input) throws
OLAPException;
public Collection getTransactionElements() throws OLAPException;
public void addTransactionElements( TransactionalObject input) throws OLAPException;


public void removeTransactionElements( TransactionalObject input) throws OLAPException;
public void setChild( QueryTransaction input) throws OLAPException;
public QueryTransaction getChild() throws OLAPException;
public void setParent( QueryTransaction input) throws OLAPException;
public QueryTransaction getParent() throws OLAPException;
public QueryTransactionManager getTransactionManager() throws OLAPException;


// class operations
}



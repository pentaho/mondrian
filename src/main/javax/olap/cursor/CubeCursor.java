package javax. olap. cursor;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import java. io.*;
import javax. olap. query. querycoremodel.*; import java. math.*;
import java. util.*; import javax. olap. query. querytransaction.*;


public interface CubeCursor extends TransactionalObject, RowDataAccessor, Cursor
{
// class scalar attributes
// class references
public void setOrdinateEdge( Collection input) throws OLAPException;


public List getOrdinateEdge() throws OLAPException;
public void addOrdinateEdge( EdgeCursor input) throws OLAPException;
public void removeOrdinateEdge( EdgeCursor input) throws OLAPException;
public void addOrdinateEdgeBefore( EdgeCursor before, EdgeCursor input) throws OLAPException;
public void addOrdinateEdgeAfter( EdgeCursor before, EdgeCursor input) throws OLAPException;
public void moveOrdinateEdgeBefore( EdgeCursor before, EdgeCursor input) throws OLAPException;
public void moveOrdinateEdgeAfter( EdgeCursor before, EdgeCursor input) throws OLAPException;
public void setPageEdge( Collection input) throws OLAPException;
public Collection getPageEdge() throws OLAPException;
public void addPageEdge( EdgeCursor input) throws OLAPException;
public void removePageEdge( EdgeCursor input) throws OLAPException;
// class operations
public void synchronizePages() throws OLAPException;
}


package javax. olap. cursor;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import java. io.*;
import javax. olap. query. querycoremodel.*; import java. math.*;
import java. util.*; import javax. olap. query. querytransaction.*;


public interface DimensionCursor extends RowDataAccessor, RowDataNavigation, Cursor
{
// class scalar attributes
public long getEdgeStart() throws OLAPException;


public void setEdgeStart( long input) throws OLAPException;
public long getEdgeEnd() throws OLAPException;
public void setEdgeEnd( long input) throws OLAPException;


// class references
public void setEdgeCursor( EdgeCursor input) throws OLAPException;
public EdgeCursor getEdgeCursor() throws OLAPException;
public void setCurrentDimensionStepManager( DimensionStepManager input) throws OLAPException;


public DimensionStepManager getCurrentDimensionStepManager() throws OLAPException;
// class operations
}

package javax. olap. cursor;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import java. io.*;
import javax. olap. query. querycoremodel.*; import java. math.*;
import java. util.*; import javax. olap. query. querytransaction.*;


public interface EdgeCursor extends RowDataNavigation, Cursor {
// class scalar attributes
// class references
public void setDimensionCursor( Collection input) throws
OLAPException;
public List getDimensionCursor() throws OLAPException;
public void addDimensionCursor( DimensionCursor input) throws OLAPException;


public void removeDimensionCursor( DimensionCursor input) throws OLAPException;
public void addDimensionCursorBefore( DimensionCursor before, DimensionCursor input) throws OLAPException;
public void addDimensionCursorAfter( DimensionCursor before, DimensionCursor input) throws OLAPException;
public void moveDimensionCursorBefore( DimensionCursor before, DimensionCursor input) throws OLAPException;
public void moveDimensionCursorAfter( DimensionCursor before, DimensionCursor input) throws OLAPException;
public void setPageOwner( CubeCursor input) throws OLAPException;
public CubeCursor getPageOwner() throws OLAPException;
public void setOrdinateOwner( CubeCursor input) throws OLAPException;
public CubeCursor getOrdinateOwner() throws OLAPException;
public void setCurrentSegment( Segment input) throws OLAPException;
public Segment getCurrentSegment() throws OLAPException;
// class operations
}

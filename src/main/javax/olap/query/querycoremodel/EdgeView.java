package javax. olap. query. querycoremodel;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. objectmodel. core.*;
import javax. jmi. reflect.*; import javax. olap. query. calculatedmembers.*;
import javax. olap. query. dimensionfilters.*; import javax. olap. query. derivedattribute.*;
import javax. olap. query. enumerations.*; import javax. olap. cursor.*;
import javax. olap. metadata.*; import javax. olap. query. edgefilters.*;
import javax. olap. query. querytransaction.*; import javax. olap. query. sorting.*;


public interface EdgeView extends Ordinate {
// class scalar attributes
// class references
public CubeView getPageOwner() throws OLAPException;


public CubeView getOrdinateOwner() throws OLAPException;
public void setDimensionView( Collection input) throws OLAPException;
public List getDimensionView() throws OLAPException;
public void addDimensionView( DimensionView input) throws OLAPException;


public void removeDimensionView( DimensionView input) throws OLAPException;
public void addDimensionViewBefore( DimensionView before, DimensionView input) throws OLAPException;
public void addDimensionViewAfter( DimensionView before, DimensionView input) throws OLAPException;
public void moveDimensionViewBefore( DimensionView before, DimensionView input) throws OLAPException;
public void moveDimensionViewAfter( DimensionView before, DimensionView input) throws OLAPException;
public void setEdgeCursor( Collection input) throws OLAPException;
public Collection getEdgeCursor() throws OLAPException;
public void removeEdgeCursor( EdgeCursor input) throws OLAPException;
public void setSegment( Collection input) throws OLAPException;
public Collection getSegment() throws OLAPException;
public void removeSegment( Segment input) throws OLAPException;
public void setEdgeFilter( Collection input) throws OLAPException;
public List getEdgeFilter() throws OLAPException;
public void removeEdgeFilter( EdgeFilter input) throws OLAPException;
public void moveEdgeFilterBefore( EdgeFilter before, EdgeFilter input) throws OLAPException;
public void moveEdgeFilterAfter( EdgeFilter before, EdgeFilter input) throws OLAPException;
public void setTuple( Collection input) throws OLAPException;
public Collection getTuple() throws OLAPException;
public void removeTuple( Tuple input) throws OLAPException;
// class operations
public EdgeCursor createCursor() throws OLAPException;
public Segment createSegment() throws OLAPException; public Segment createSegmentBefore( Segment member) throws
OLAPException; public Segment createSegmentAfter( Segment member) throws
OLAPException; public EdgeFilter createEdgeFilter( EdgeFilterType type) throws
OLAPException; public EdgeFilter createEdgeFilterBefore( EdgeFilterType type,
EdgeFilter member) throws OLAPException; public EdgeFilter createEdgeFilterAfter( EdgeFilterType type,
EdgeFilter member) throws OLAPException; public Tuple createTuple() throws OLAPException;
public CurrentEdgeMember createCurrentEdgeMember() throws OLAPException;
}


package javax. olap. query. querycoremodel;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. objectmodel. core.*;
import javax. jmi. reflect.*; import javax. olap. query. calculatedmembers.*;
import javax. olap. query. dimensionfilters.*; import javax. olap. query. derivedattribute.*;
import javax. olap. query. enumerations.*; import javax. olap. cursor.*;
import javax. olap. metadata.*; import javax. olap. query. edgefilters.*;
import javax. olap. query. querytransaction.*; import javax. olap. query. sorting.*;


public interface CubeView extends QueryObject {
// class scalar attributes
// class references
public void setOrdinateEdge( Collection input) throws OLAPException;


public List getOrdinateEdge() throws OLAPException;
public void removeOrdinateEdge( EdgeView input) throws OLAPException;
public void moveOrdinateEdgeBefore( EdgeView before, EdgeView input) throws OLAPException;


public void moveOrdinateEdgeAfter( EdgeView before, EdgeView input) throws OLAPException;
public void setPageEdge( Collection input) throws OLAPException;
public Collection getPageEdge() throws OLAPException;
public void removePageEdge( EdgeView input) throws OLAPException;
public void setDefaultOrdinatePrecedence( Collection input) throws OLAPException;
public List getDefaultOrdinatePrecedence() throws OLAPException;
public void addDefaultOrdinatePrecedence( Ordinate input) throws OLAPException;


public void removeDefaultOrdinatePrecedence( Ordinate input) throws OLAPException;
public void addDefaultOrdinatePrecedenceBefore( Ordinate before, Ordinate input) throws OLAPException;
public void addDefaultOrdinatePrecedenceAfter( Ordinate before, Ordinate input) throws OLAPException;
public void moveDefaultOrdinatePrecedenceBefore( Ordinate before, Ordinate input) throws OLAPException;
public void moveDefaultOrdinatePrecedenceAfter( Ordinate before, Ordinate input) throws OLAPException;
public void setCubeCursor( Collection input) throws OLAPException;
public Collection getCubeCursor() throws OLAPException;
public void removeCubeCursor( CubeCursor input) throws OLAPException;
// class operations
public CubeCursor createCursor() throws OLAPException;
public EdgeView createOrdinateEdge() throws OLAPException; public EdgeView createPageEdge() throws OLAPException;
public EdgeView createPageEdgeBefore( EdgeView member) throws OLAPException;
public EdgeView createPageEdgeAfter( EdgeView member) throws OLAPException;
public CalculationRelationship createCalculationRelationship() throws OLAPException;
public void pivot( DimensionView dv, EdgeView source, EdgeView target) throws OLAPException;
public void pivot( DimensionView dv, EdgeView source, EdgeView target, int position) throws OLAPException;
public void rotate( EdgeView edv1, EdgeView edv2) throws OLAPException;
}



package javax. olap. query. querycoremodel;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. objectmodel. core.*;
import javax. jmi. reflect.*; import javax. olap. query. calculatedmembers.*;
import javax. olap. query. dimensionfilters.*; import javax. olap. query. derivedattribute.*;
import javax. olap. query. enumerations.*; import javax. olap. cursor.*;
import javax. olap. metadata.*; import javax. olap. query. edgefilters.*;
import javax. olap. query. querytransaction.*; import javax. olap. query. sorting.*;


public interface Segment extends NamedObject {
// class scalar attributes
// class references
public EdgeView getEdgeView() throws OLAPException;


public void setDimensionStepManager( Collection input) throws OLAPException;
public Collection getDimensionStepManager() throws OLAPException;
public void addDimensionStepManager( DimensionStepManager input) throws OLAPException;
public void removeDimensionStepManager( DimensionStepManager input) throws OLAPException;
// class operations
}


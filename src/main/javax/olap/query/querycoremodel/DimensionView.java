package javax. olap. query. querycoremodel;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. objectmodel. core.*;
import javax. jmi. reflect.*; import javax. olap. query. calculatedmembers.*;
import javax. olap. query. dimensionfilters.*; import javax. olap. query. derivedattribute.*;
import javax. olap. query. enumerations.*; import javax. olap. cursor.*;
import javax. olap. metadata.*; import javax. olap. query. edgefilters.*;
import javax. olap. query. querytransaction.*; import javax. olap. query. sorting.*;


public interface DimensionView extends Ordinate {
// class scalar attributes
public Boolean getIsDistinct() throws OLAPException;
public void setIsDistinct( Boolean input) throws OLAPException;
// class references public
void setEdgeView( EdgeView input) throws OLAPException;
public EdgeView getEdgeView() throws OLAPException;
public void setDimension( Dimension input) throws OLAPException;
public Dimension getDimension() throws OLAPException;
public void setDimensionStepManager( Collection input) throws OLAPException;


public Collection getDimensionStepManager() throws OLAPException;
public void removeDimensionStepManager( DimensionStepManager input) throws OLAPException;


public void setDimensionCursor( Collection input) throws OLAPException;
public Collection getDimensionCursor() throws OLAPException;
public void removeDimensionCursor( DimensionCursor input) throws OLAPException;


public void setSelectedObject( Collection input) throws OLAPException;
public List getSelectedObject() throws OLAPException;
public void removeSelectedObject( SelectedObject input) throws OLAPException;


public void moveSelectedObjectBefore( SelectedObject before, SelectedObject input) throws OLAPException;
public void moveSelectedObjectAfter( SelectedObject before, SelectedObject input) throws OLAPException;
public void setDerivedAttribute( Collection input) throws OLAPException;
public Collection getDerivedAttribute() throws OLAPException;
public void removeDerivedAttribute( DerivedAttribute input) throws OLAPException;


// class operations
public DimensionCursor createCursor() throws OLAPException;
public SelectedObject createSelectedObject( SelectedObjectType type) throws OLAPException;
public SelectedObject createSelectedObjectBefore( SelectedObjectType type, SelectedObject member) throws OLAPException;
public SelectedObject createSelectedObjectAfter( SelectedObjectType type, SelectedObject member) throws OLAPException;
public DimensionStepManager createDimensionStepManager() throws OLAPException;
public DerivedAttribute createDerivedAttribute() throws OLAPException;
}


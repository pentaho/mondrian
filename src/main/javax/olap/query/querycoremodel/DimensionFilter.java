package javax. olap. query. querycoremodel;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. objectmodel. core.*;
import javax. jmi. reflect.*; import javax. olap. query. calculatedmembers.*;
import javax. olap. query. dimensionfilters.*; import javax. olap. query. derivedattribute.*;
import javax. olap. query. enumerations.*; import javax. olap. cursor.*;
import javax. olap. metadata.*; import javax. olap. query. edgefilters.*;
import javax. olap. query. querytransaction.*; import javax. olap. query. sorting.*;


public abstract interface DimensionFilter extends DimensionStep {
// class scalar attributes
public SetActionType getSetAction() throws OLAPException;
public void setSetAction( SetActionType input) throws OLAPException;
// class references
public void setDimensionInsertOffset( DimensionInsertOffset input)
throws OLAPException;
public DimensionInsertOffset getDimensionInsertOffset() throws OLAPException;


// class operations
public DimensionInsertOffset createDimensionInsertOffset( DimensionInsertOffsetType type) throws OLAPException;
}



package javax. olap. query. querycoremodel;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. objectmodel. core.*;
import javax. jmi. reflect.*;
import javax. olap. query. calculatedmembers.*; import javax. olap. query. dimensionfilters.*;
import javax. olap. query. derivedattribute.*; import javax. olap. query. enumerations.*;
import javax. olap. cursor.*; import javax. olap. metadata.*;
import javax. olap. query. edgefilters.*; import javax. olap. query. querytransaction.*;
import javax. olap. query. sorting.*;
public interface CompoundDimensionStep extends DimensionStep {


// class scalar attributes
// class references
public void setDimensionStep( Collection input) throws OLAPException;


public List getDimensionStep() throws OLAPException;
public void removeDimensionStep( DimensionStep input) throws OLAPException;


public void moveDimensionStepBefore( DimensionStep before, DimensionStep input) throws OLAPException;
public void moveDimensionStepAfter( DimensionStep before, DimensionStep input) throws OLAPException;
// class operations
public DimensionStep createDimensionStep( DimensionStepType stepType) throws OLAPException;
public DimensionStep createDimensionStepBefore( DimensionStepType stepType, DimensionStep member) throws OLAPException;
public DimensionStep createDimensionStepAfter( DimensionStepType stepType, DimensionStep member) throws OLAPException;
}



package javax. olap. query. dimensionfilters;
import java. util. List; import java. util. Collection;

import javax. olap. OLAPException; import org. omg. cwm. objectmodel. core.*;
import javax. olap. metadata.*; import javax. olap. query. querycoremodel.*;
import javax. olap. query. enumerations.*; import javax. olap. query. calculatedmembers.*;
import javax. olap. query. edgefilters.*; import javax. olap. query. querytransaction.*;
import javax. olap. query. sorting.*;
public interface Drill extends DimensionFilter {


// class scalar attributes
public DrillType getDrillType() throws OLAPException;
public void setDrillType( DrillType input) throws OLAPException;
// class references
public void setHierarchy( Hierarchy input) throws OLAPException;
public Hierarchy getHierarchy() throws OLAPException;
public void setLevel( Level input) throws OLAPException;
public Level getLevel() throws OLAPException;
public void setDrillMember( Member input) throws OLAPException;
public Member getDrillMember() throws OLAPException;
// class operations
}



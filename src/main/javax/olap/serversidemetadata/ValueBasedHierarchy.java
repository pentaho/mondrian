package javax. olap. serversidemetadata;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. objectmodel. core.*; import javax. olap. metadata.*;
import org. omg. cwm. analysis. transformation.*;
public interface ValueBasedHierarchy extends javax. olap. metadata. ValueBasedHierarchy
{
// class scalar attributes
// class references
public void setDimensionDeployment( Collection input) throws OLAPException;
public List getDimensionDeployment() throws OLAPException;
public void removeDimensionDeployment( DimensionDeployment input) throws OLAPException;


public void moveDimensionDeploymentBefore( DimensionDeployment before, DimensionDeployment input) throws OLAPException;
public void moveDimensionDeploymentAfter( DimensionDeployment before, DimensionDeployment input) throws OLAPException;
// class operations
}



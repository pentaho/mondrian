package javax. olap. serversidemetadata;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. objectmodel. core.*; import javax. olap. metadata.*;
import org. omg. cwm. analysis. transformation.*;
public interface Schema extends javax. olap. metadata. Schema {


// class scalar attributes
// class references
public void setDeploymentGroup( Collection input) throws OLAPException;
public Collection getDeploymentGroup() throws OLAPException;
public void removeDeploymentGroup( DeploymentGroup input) throws OLAPException;


// class operations
}



package javax. olap. sourcemodel;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import javax. jmi. reflect.*;


public interface Template extends RefObject {
// class scalar attributes
// class references
public void setSource( Source input) throws OLAPException;


public Source getSource() throws OLAPException;
public void setCurrentState( MetadataState input) throws OLAPException;
public MetadataState getCurrentState() throws OLAPException;
public void setSourceGenerator( SourceGenerator input) throws OLAPException;


public SourceGenerator getSourceGenerator() throws OLAPException;
// class operations
}



package javax. olap. sourcemodel; 
import java. util. List; import java. util. Collection; 
import javax. olap. OLAPException; import javax. jmi. reflect.*; 


public interface SourceGenerator extends RefObject { 
// class scalar attributes 
// class references // class operations 
public Source generateSource( MetadataState state) throws OLAPException; 
} 



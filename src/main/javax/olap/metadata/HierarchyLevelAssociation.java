package javax.olap.metadata;
import javax.olap.OLAPException;
public interface HierarchyLevelAssociation extends org.omg.cwm.objectmodel.core.Class {
// class scalar attributes
// class references
	public LevelBasedHierarchy getLevelBasedHierarchy() throws OLAPException;
	public void setCurrentLevel(Level input) throws OLAPException;
	public Level getCurrentLevel() throws OLAPException;
// class operations
}

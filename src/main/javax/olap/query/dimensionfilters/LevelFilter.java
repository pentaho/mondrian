package javax.olap.query.dimensionfilters;
import javax.olap.OLAPException;
import javax.olap.metadata.Level;
import javax.olap.query.querycoremodel.DimensionFilter;
public interface LevelFilter extends DimensionFilter {
// class scalar attributes
// class references
	public void setLevel( Level input) throws OLAPException;
	public Level getLevel() throws OLAPException;
// class operations
}



package javax.olap.metadata;

public interface Dimension {
	/**
	*  if true, isMeasure must be false
	*/
	public boolean isTime();
	/**
	*  if true, isTime must be false
	*/
	public boolean isMeasure();
}
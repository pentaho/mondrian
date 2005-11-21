package mondrian.olap;


/**
 * allows expressions to be evaluated native, e.g. in SQL.
 * 
 * @author av
 * @since Nov 11, 2005
 */

public interface NativeEvaluator {
    Object execute();
}

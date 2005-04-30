package mondrian.spi;

import mondrian.olap.*;
import mondrian.olap.type.Type;

/**
 * Created by IntelliJ IDEA.
 * User: jhyde
 * Date: Mar 31, 2005
 * Time: 12:02:24 AM
 * To change this template use File | Settings | File Templates.
 */
public interface UserDefinedFunction {
    public String getName();
    public String getDescription();
    public Syntax getSyntax();
    public Type getReturnType();
    public Type[] getParameterTypes();
    public Object execute(Evaluator evaluator, Exp[] arguments);
    /**
     * Returns a list of reserved words used by this function.
     */ 
    public String[] getReservedWords();
}

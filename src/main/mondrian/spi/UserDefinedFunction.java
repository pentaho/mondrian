package mondrian.spi;

import mondrian.olap.*;
import mondrian.olap.type.Type;

/**
 * Definition of a user-defined function.
 *
 * <p>The class must have a public, zero-arguments constructor, be on
 * Mondrian's runtime class-path, and be referenced from the schema file:
 *
 * <blockquote><code>
 * &lt;Schema&gt;
 * &nbsp;&nbsp;&nbsp;&nbsp;....
 * &nbsp;&nbsp;&nbsp;&nbsp;&lt;UserDefinedFunction name="MyFun" class="com.acme.MyFun"&gt;
 * &lt;/Schema&gt;
 */
public interface UserDefinedFunction {
    /**
     * Returns the name with which the user-defined function will be used
     * from within MDX expressions.
     */
    public String getName();

    /**
     * Returns a description of the user-defined function.
     */
    public String getDescription();

    /**
     * Returns the syntactic type of the user-defined function.
     * Usually {@link Syntax#Function}.
     */
    public Syntax getSyntax();

    /**
     * Returns the return-type of this function.
     */
    public Type getReturnType();

    /**
     * Returns an array of the types of the parameters of this function.
     */
    public Type[] getParameterTypes();

    /**
     * Applies this function to a set of arguments, and returns a result.
     *
     * @param evaluator Evaluator containts the runtime context, in particular
     *   the current member of each dimension.
     * @param arguments Expressions which yield the arguments of this function.
     *   Most user-defined functions will evaluate all arguments before using
     *   them. Functions such as <code>IIf</code> do not evaluate all
     *   arguments; this technique is called <dfn>lazy evaluation</dfn>.
     * @return The result value.
     */
    public Object execute(Evaluator evaluator, Exp[] arguments);

    /**
     * Returns a list of reserved words used by this function.
     * May return an empty array or null if this function does not require
     * any reserved words.
     */
    public String[] getReservedWords();
}

// UserDefinedFunction.java

package mondrian.olap.fun;

import mondrian.olap.*;

/**
 * Defines the <code>PROPERTIES</code> MDX function.
 */
class PropertiesFunDef extends FunDefBase {
    public PropertiesFunDef(
            String name, String signature, String description,
            Syntax syntax, int returnType, int[] parameterTypes) {
        super(name, signature, description, syntax, returnType, parameterTypes);
    }

    public Object evaluate(Evaluator evaluator, Exp[] args) {
        Member member = getMemberArg(evaluator, args, 0, true);
        String s = getStringArg(evaluator, args, 1, null);
        Object o = member.getPropertyValue(s);
        if (o == null) {
            if (isValidProperty(member, s)) {
                o = member.getHierarchy().getNullMember();
            } else {
                throw new MondrianEvaluationException(
                        "Property '" + s +
                        "' is not valid for member '" + member + "'");
            }
        }
        return o;
    }

    private static boolean isValidProperty(
            Member member, String propertyName) {
        return lookupProperty(member.getLevel(), propertyName) != null;
    }

    /**
     * Finds a member property called <code>propertyName</code> at, or above,
     * <code>level</code>.
     */
    private static Property lookupProperty(
            Level level, String propertyName) {
        do {
            Property[] properties = level.getProperties();
            for (int i = 0; i < properties.length; i++) {
                Property property = properties[i];
                if (property.getName().equals(propertyName)) {
                    return property;
                }
            }
            level = level.getParentLevel();
        } while (level != null);
        return null;
    }

    /**
     * Resolves calls to the <code>PROPERTIES</code> MDX function.
     */
    static class Resolver extends ResolverBase
    {
        Resolver()
        {
            super("Properties", "<Member>.Properties(<String Expression>)",
                "Returns the value of a member property.", Syntax.Method);
        }

        public FunDef resolve(Exp[] args, int[] conversionCount) {
            final int[] argTypes = new int[]{Category.Member, Category.String};
            if (args.length != 2 ||
                    args[0].getType() != Category.Member ||
                    args[1].getType() != Category.String) {
                return null;
            }
            int returnType;
            if (args[1] instanceof Literal) {
                String propertyName = (String) ((Literal) args[1]).getValue();
                Hierarchy hierarchy = args[0].getHierarchy();
                Level[] levels = hierarchy.getLevels();
                Property property = lookupProperty(
                        levels[levels.length - 1], propertyName);
                if (property == null) {
                    // we'll likely get a runtime error
                    returnType = Category.Value;
                } else {
                    switch (property.getType()) {
                    case Property.TYPE_BOOLEAN:
                        returnType = Category.Logical;
                        break;
                    case Property.TYPE_NUMERIC:
                        returnType = Category.Numeric;
                        break;
                    case Property.TYPE_STRING:
                        returnType = Category.String;
                        break;
                    default:
                        throw Util.newInternal("Unknown property type " + property.getType());
                    }
                }
            } else {
                returnType = Category.Value;
            }
            return new PropertiesFunDef(name, signature, description, syntax, returnType, argTypes);
        }

        public boolean requiresExpression(int k) {
            return true;
        }
    }
}

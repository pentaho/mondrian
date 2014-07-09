/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho
// All Rights Reserved.
*/
package mondrian.olap;

import java.io.PrintWriter;

/**
 * Enumerated values describing the syntax of an expression.
 *
 * @author jhyde
 * @since 21 July, 2003
 */
public enum Syntax {
    /**
     * Defines syntax for expression invoked <code>FUNCTION()</code> or
     * <code>FUNCTION(args)</code>.
     */
    Function {
        public void unparse(String fun, Exp[] args, PrintWriter pw) {
            ExpBase.unparseList(pw, args, fun + "(", ", ", ")");
        }
    },

    /**
     * Defines syntax for expression invoked as <code>object.PROPERTY</code>.
     */
    Property {
        public void unparse(String fun, Exp[] args, PrintWriter pw) {
            Util.assertTrue(args.length >= 1);
            args[0].unparse(pw); // 'this'
            pw.print(".");
            pw.print(fun);
        }

        public String getSignature(
            String name, int returnType, int[] argTypes)
        {
            // e.g. "<Set>.Current"
            return getTypeDescription(argTypes[0]) + "." + name;
        }
    },

    /**
     * Defines syntax for expression invoked invoked as
     * <code>object.METHOD()</code> or
     * <code>object.METHOD(args)</code>.
     */
    Method {
        public void unparse(String fun, Exp[] args, PrintWriter pw) {
            Util.assertTrue(args.length >= 1);
            args[0].unparse(pw); // 'this'
            pw.print(".");
            pw.print(fun);
            pw.print("(");
            for (int i = 1; i < args.length; i++) {
                if (i > 1) {
                    pw.print(", ");
                }
                args[i].unparse(pw);
            }
            pw.print(")");
        }

        public String getSignature(String name, int returnType, int[] argTypes)
        {
            // e.g. "<Member>.Lead(<Numeric Expression>)"
            return (returnType == Category.Unknown
                    ? ""
                    : getTypeDescription(returnType) + " ")
                + getTypeDescription(argTypes[0]) + "."
                + name + "(" + getTypeDescriptionCommaList(argTypes, 1)
                + ")";
        }
    },

    /**
     * Defines syntax for expression invoked as <code>arg OPERATOR arg</code>
     * (like '+' or 'AND').
     */
    Infix {
        public void unparse(String fun, Exp[] args, PrintWriter pw) {
            if (needParen(args)) {
                ExpBase.unparseList(pw, args, "(", " " + fun + " ", ")");
            } else {
                ExpBase.unparseList(pw, args, "", " " + fun + " ", "");
            }
        }

        public String getSignature(String name, int returnType, int[] argTypes)
        {
            // e.g. "<Numeric Expression> / <Numeric Expression>"
            return getTypeDescription(argTypes[0]) + " " + name + " "
                + getTypeDescription(argTypes[1]);
        }
    },

    /**
     * Defines syntax for expression invoked as <code>OPERATOR arg</code>
     * (like unary '-').
     */
    Prefix {
        public void unparse(String fun, Exp[] args, PrintWriter pw) {
            if (needParen(args)) {
                ExpBase.unparseList(pw, args, "(" + fun + " ", null, ")");
            } else {
                ExpBase.unparseList(pw, args, fun + " ", null, "");
            }
        }

        public String getSignature(String name, int returnType, int[] argTypes)
        {
            // e.g. "- <Numeric Expression>"
            return name + " " + getTypeDescription(argTypes[0]);
        }
    },

    /**
     * Defines syntax for expression invoked as <code>arg OPERATOR</code>
     * (like <code>IS EMPTY</code>).
     */
    Postfix {
        public void unparse(String fun, Exp[] args, PrintWriter pw) {
            if (needParen(args)) {
                ExpBase.unparseList(pw, args, "(", null, " " + fun + ")");
            } else {
                ExpBase.unparseList(pw, args, "", null, " " + fun);
            }
        }

        public String getSignature(String name, int returnType, int[] argTypes)
        {
            // e.g. "<Expression> IS NULL"
            return getTypeDescription(argTypes[0]) + " " + name;
        }
    },

    /**
     * Defines syntax for expression invoked as
     * <code>{ARG, &#46;&#46;&#46;}</code>; that
     * is, the set construction operator.
     */
    Braces {
        public String getSignature(String name, int returnType, int[] argTypes)
        {
            return "{" + getTypeDescriptionCommaList(argTypes, 0) + "}";
        }

        public void unparse(String fun, Exp[] args, PrintWriter pw) {
            ExpBase.unparseList(pw, args, "{", ", ", "}");
        }
    },

    /**
     * Defines syntax for expression invoked as <code>(ARG)</code> or
     * <code>(ARG, &#46;&#46;&#46;)</code>; that is, parentheses for grouping
     * expressions, and the tuple construction operator.
     */
    Parentheses {
        public String getSignature(String name, int returnType, int[] argTypes)
        {
            return "(" + getTypeDescriptionCommaList(argTypes, 0) + ")";
        }

        public void unparse(String fun, Exp[] args, PrintWriter pw) {
            ExpBase.unparseList(pw, args, "(", ", ", ")");
        }
    },

    /**
     * Defines syntax for expression invoked as <code>CASE ... END</code>.
     */
    Case {
        public void unparse(String fun, Exp[] args, PrintWriter pw) {
            if (fun.equals("_CaseTest")) {
                pw.print("CASE");
                int j = 0;
                int clauseCount = (args.length - j) / 2;
                for (int i = 0; i < clauseCount; i++) {
                    pw.print(" WHEN ");
                    args[j++].unparse(pw);
                    pw.print(" THEN ");
                    args[j++].unparse(pw);
                }
                if (j < args.length) {
                    pw.print(" ELSE ");
                    args[j++].unparse(pw);
                }
                Util.assertTrue(j == args.length);
                pw.print(" END");
            } else {
                Util.assertTrue(fun.equals("_CaseMatch"));

                pw.print("CASE ");
                int j = 0;
                args[j++].unparse(pw);
                int clauseCount = (args.length - j) / 2;
                for (int i = 0; i < clauseCount; i++) {
                    pw.print(" WHEN ");
                    args[j++].unparse(pw);
                    pw.print(" THEN ");
                    args[j++].unparse(pw);
                }
                if (j < args.length) {
                    pw.print(" ELSE ");
                    args[j++].unparse(pw);
                }
                Util.assertTrue(j == args.length);
                pw.print(" END");
            }
        }

        public String getSignature(String name, int returnType, int[] argTypes)
        {
            String s = getTypeDescription(argTypes[0]);
            if (argTypes[0] == Category.Logical) {
                return "CASE WHEN " + s + " THEN <Expression> ... END";
            } else {
                return "CASE " + s + " WHEN " + s
                    + " THEN <Expression> ... END";
            }
        }
    },

    /**
     * Defines syntax for expression generated by the Mondrian system which
     * cannot be specified syntactically.
     */
    Internal,

    /**
     * Defines syntax for a CAST expression
     * <code>CAST(expression AS type)</code>.
     */
    Cast {
        public void unparse(String fun, Exp[] args, PrintWriter pw) {
            pw.print("CAST(");
            args[0].unparse(pw);
            pw.print(" AS ");
            args[1].unparse(pw);
            pw.print(")");
        }

        public String getSignature(String name, int returnType, int[] argTypes)
        {
            return "CAST(<Expression> AS <Type>)";
        }
    },

    /**
     * Defines syntax for expression invoked <code>object&#46;&PROPERTY</code>
     * (a variant of {@link #Property}).
     */
    QuotedProperty,

    /**
     * Defines syntax for expression invoked <code>object&#46;[&PROPERTY]</code>
     * (a variant of {@link #Property}).
     */
    AmpersandQuotedProperty,

    /**
     * Defines the syntax for an empty expression. Empty expressions can occur
     * within function calls, and are denoted by a pair of commas with only
     * whitespace between them, for example
     *
     * <blockquote>
     * <code>DrillDownLevelTop({[Product].[All Products]}, 3, ,
     *  [Measures].[Unit Sales])</code>
     * </blockquote>
     */
    Empty {
        public void unparse(String fun, Exp[] args, PrintWriter pw) {
            assert args.length == 0;
        }
        public String getSignature(String name, int returnType, int[] argTypes)
        {
            return "";
        }};

    /**
     * Converts a call to a function of this syntax into source code.
     *
     * @param fun Function name
     * @param args Arguments to the function
     * @param pw Writer
     */
    public void unparse(String fun, Exp[] args, PrintWriter pw) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a description of the signature of a function call, for
     * example, "CoalesceEmpty(<Numeric Expression>, <String Expression>)".
     *
     * @param name Function name
     * @param returnType Function's return category
     * @param argTypes Categories of the function's arguments
     * @return Function signature
     */
    public String getSignature(String name, int returnType, int[] argTypes) {
        // e.g. "StripCalculatedMembers(<Set>)"
        return (returnType == Category.Unknown
                ? ""
                : getTypeDescription(returnType) + " ")
            + name + "(" + getTypeDescriptionCommaList(argTypes, 0)
            + ")";
    }

    private static boolean needParen(Exp[] args) {
        return !(args.length == 1
                 && args[0] instanceof FunCall
                 && ((FunCall) args[0]).getSyntax() == Syntax.Parentheses);
    }

    private static String getTypeDescription(int type) {
        return "<" + Category.instance.getDescription(type & Category.Mask)
            + ">";
    }

    private static String getTypeDescriptionCommaList(int[] types, int start) {
        int initialSize = (types.length - start) * 16;
        StringBuilder sb =
            new StringBuilder(initialSize > 0 ? initialSize : 16);
        for (int i = start; i < types.length; i++) {
            if (i > start) {
                sb.append(", ");
            }
            sb.append("<")
                .append(
                    Category.instance.getDescription(types[i] & Category.Mask))
                .append(">");
        }
        return sb.toString();
    }
}

// End Syntax.java

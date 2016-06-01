/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 1998-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/

package mondrian.olap;

import org.apache.log4j.Logger;

import java_cup.runtime.Symbol;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

/**
 * Lexical analyzer for MDX.
 *
 * @author jhyde, 20 January, 1999
 */
public class Scanner {
    private static final Logger LOGGER = Logger.getLogger(Scanner.class);

    /** single lookahead character */
    protected int nextChar;
    /** next lookahead character */
    private int lookaheadChars[] = new int[16];
    private int firstLookaheadChar = 0;
    private int lastLookaheadChar = 0;
    private Hashtable<String, Integer> m_resWordsTable;
    private int iMaxResword;
    private String m_aResWords[];
    protected boolean debug;

    /** lines[x] is the start of the x'th line */
    private List<Integer> lines;

    /** number of times advance() has been called */
    private int iChar;

    /** end of previous token */
    private int iPrevChar;

    /** previous symbol returned */
    private int previousSymbol;
    private boolean inFormula;

    /**
     * Comment delimiters. Modify this list to support other comment styles.
     */
    private static final String[][] commentDelim = {
        new String[] {"//", null},
        new String[] {"--", null},
        new String[] {"/*", "*/"}
    };

    /**
     * Whether to allow nested comments.
     */
    private static final boolean allowNestedComments = true;

    /**
     * The {@link java.math.BigDecimal} value 0.
     * Note that BigDecimal.ZERO does not exist until JDK 1.5.
     */
    private static final BigDecimal BigDecimalZero = BigDecimal.valueOf(0);

    /**
     * Creates a Scanner.
     *
     * @param debug Whether to emit debug messages.
     */
    Scanner(boolean debug) {
        this.debug = debug;
    }

    /**
     * Returns the current nested comments state.
     */
    public static boolean getNestedCommentsState() {
        return allowNestedComments;
    }

    /**
     * Returns the list of comment delimiters.
     */
    public static String[][] getCommentDelimiters() {
        return commentDelim;
    }

    /**
     * Advance input by one character, setting {@link #nextChar}.
     */
    private void advance() throws IOException {
        if (firstLookaheadChar == lastLookaheadChar) {
            // We have nothing in the lookahead buffer.
            nextChar = getChar();
        } else {
            // We have called lookahead(); advance to the next character it got.
            nextChar = lookaheadChars[firstLookaheadChar++];
            if (firstLookaheadChar == lastLookaheadChar) {
                firstLookaheadChar = 0;
                lastLookaheadChar = 0;
            }
        }
        if (nextChar == '\012') {
            lines.add(iChar);
        }
        iChar++;
    }

    /** Peek at the character after {@link #nextChar} without advancing. */
    private int lookahead() throws IOException {
        return lookahead(1);
    }

    /**
     * Peeks at the character n after {@link #nextChar} without advancing.
     *
     * <p>lookahead(0) returns the current char (nextChar).
     * lookahead(1) returns the next char (was lookaheadChar, same as
     * lookahead());
     */
    private int lookahead(int n) throws IOException {
        if (n == 0) {
            return nextChar;
        } else {
            // if the desired character not in lookahead buffer, read it in
            if (n > lastLookaheadChar - firstLookaheadChar) {
                int len = lastLookaheadChar - firstLookaheadChar;
                int t[];

                // make sure we do not go off the end of the buffer
                if (n + firstLookaheadChar > lookaheadChars.length) {
                    if (n > lookaheadChars.length) {
                        // the array is too small; make it bigger and shift
                        // everything to the beginning.
                        t = new int[n * 2];
                    } else {
                        // the array is big enough, so just shift everything
                        // to the beginning of it.
                        t = lookaheadChars;
                    }

                    System.arraycopy(
                        lookaheadChars, firstLookaheadChar, t, 0, len);
                    lookaheadChars = t;
                    firstLookaheadChar = 0;
                    lastLookaheadChar = len;
                }

                // read ahead enough
                while (n > lastLookaheadChar - firstLookaheadChar) {
                    lookaheadChars[lastLookaheadChar++] = getChar();
                }
            }

            return lookaheadChars[n - 1 + firstLookaheadChar];
        }
    }

    /** Read a character from input, returning -1 if end of input. */
    protected int getChar() throws IOException {
        return System.in.read();
    }

    /** Initialize the scanner */
    public void init() throws IOException {
        initReswords();
        lines = new ArrayList<Integer>();
        iChar = iPrevChar = 0;
        advance();
    }

    /**
     * Deduces the line and column (0-based) of a symbol.
     * Called by {@link Parser#syntax_error}.
     */
    void getLocation(Symbol symbol, int[] loc) {
        int iTarget = symbol.left;
        int iLine = -1;
        int iLineEnd = 0;
        int iLineStart;
        do {
            iLine++;
            iLineStart = iLineEnd;
            iLineEnd = Integer.MAX_VALUE;
            if (iLine < lines.size()) {
                iLineEnd = lines.get(iLine);
            }
        } while (iLineEnd < iTarget);

        loc[0] = iLine; // line
        loc[1] = iTarget - iLineStart; // column
    }

    private Symbol trace(Symbol s) {
        if (debug) {
            String name = null;
            if (s.sym < m_aResWords.length) {
                name = m_aResWords[s.sym];
            }

            LOGGER.error(
                "Scanner returns #" + s.sym
                + (name == null ? "" : ":" + name)
                + (s.value == null ? "" : "(" + s.value.toString() + ")"));
        }
        return s;
    }

    private void initResword(int id, String s) {
        m_resWordsTable.put(s, id);
        if (id > iMaxResword) {
            iMaxResword = id;
        }
    }

    /**
     * Initializes the table of reserved words.
     */
    private void initReswords() {
        // This list generated by piping the 'terminal' declaration in mdx.cup
        // through:
        //   grep -list // |
        //   sed -e 's/,//' |
        //   awk '{printf "initResword(%20s,%c%s%c);",$1,34,$1,34}'
        m_resWordsTable = new Hashtable<String, Integer>();
        iMaxResword = 0;
//      initResword(ParserSym.ALL,                  "ALL");
        initResword(ParserSym.AND,                  "AND");
        initResword(ParserSym.AS,                   "AS");
//      initResword(ParserSym.ASC,                  "ASC");
        initResword(ParserSym.AXIS,                 "AXIS");
//      initResword(ParserSym.BACK_COLOR,           "BACK_COLOR");
//      initResword(ParserSym.BASC,                 "BASC");
//      initResword(ParserSym.BDESC,                "BDESC");
        // CAST is a mondrian extension
        initResword(ParserSym.CAST,                 "CAST");
        initResword(ParserSym.CASE,                 "CASE");
        initResword(ParserSym.CELL,                 "CELL");
//      initResword(ParserSym.CELL_ORDINAL,         "CELL_ORDINAL");
        initResword(ParserSym.CHAPTERS,             "CHAPTERS");
//      initResword(ParserSym.CHILDREN,             "CHILDREN");
        initResword(ParserSym.COLUMNS,              "COLUMNS");
//      initResword(ParserSym.DESC,                 "DESC");
        initResword(ParserSym.DIMENSION,            "DIMENSION");
        initResword(ParserSym.DRILLTHROUGH,         "DRILLTHROUGH");
        initResword(ParserSym.ELSE,                 "ELSE");
        initResword(ParserSym.EMPTY,                "EMPTY");
        initResword(ParserSym.END,                  "END");
        initResword(ParserSym.EXPLAIN,              "EXPLAIN");
//      initResword(ParserSym.FIRSTCHILD,           "FIRSTCHILD");
        initResword(ParserSym.FIRSTROWSET,          "FIRSTROWSET");
//      initResword(ParserSym.FIRSTSIBLING,         "FIRSTSIBLING");
//      initResword(ParserSym.FONT_FLAGS,           "FONT_FLAGS");
//      initResword(ParserSym.FONT_NAME,            "FONT_NAME");
//      initResword(ParserSym.FONT_SIZE,            "FONT_SIZE");
//      initResword(ParserSym.FORE_COLOR,           "FORE_COLOR");
//      initResword(ParserSym.FORMATTED_VALUE,      "FORMATTED_VALUE");
//      initResword(ParserSym.FORMAT_STRING,        "FORMAT_STRING");
        initResword(ParserSym.FOR,                  "FOR");
        initResword(ParserSym.FROM,                 "FROM");
        initResword(ParserSym.IS,                   "IS");
        initResword(ParserSym.IN,                   "IN");
//      initResword(ParserSym.LAG,                  "LAG");
//      initResword(ParserSym.LASTCHILD,            "LASTCHILD");
//      initResword(ParserSym.LASTSIBLING,          "LASTSIBLING");
//      initResword(ParserSym.LEAD,                 "LEAD");
        initResword(ParserSym.MATCHES,              "MATCHES");
        initResword(ParserSym.MAXROWS,              "MAXROWS");
        initResword(ParserSym.MEMBER,               "MEMBER");
//      initResword(ParserSym.MEMBERS,              "MEMBERS");
//      initResword(ParserSym.NEXTMEMBER,           "NEXTMEMBER");
        initResword(ParserSym.NON,                  "NON");
        initResword(ParserSym.NOT,                  "NOT");
        initResword(ParserSym.NULL,                 "NULL");
        initResword(ParserSym.ON,                   "ON");
        initResword(ParserSym.OR,                   "OR");
        initResword(ParserSym.PAGES,                "PAGES");
//      initResword(ParserSym.PARENT,               "PARENT");
        initResword(ParserSym.PLAN,                 "PLAN");
//      initResword(ParserSym.PREVMEMBER,           "PREVMEMBER");
        initResword(ParserSym.PROPERTIES,           "PROPERTIES");
//      initResword(ParserSym.RECURSIVE,            "RECURSIVE");
        initResword(ParserSym.RETURN,               "RETURN");
        initResword(ParserSym.ROWS,                 "ROWS");
        initResword(ParserSym.SECTIONS,             "SECTIONS");
        initResword(ParserSym.SELECT,               "SELECT");
        initResword(ParserSym.SET,                  "SET");
//      initResword(ParserSym.SOLVE_ORDER,          "SOLVE_ORDER");
        initResword(ParserSym.THEN,                 "THEN");
//      initResword(ParserSym.VALUE,                "VALUE");
        initResword(ParserSym.WHEN,                 "WHEN");
        initResword(ParserSym.WHERE,                "WHERE");
        initResword(ParserSym.WITH,                 "WITH");
        initResword(ParserSym.XOR,                  "XOR");

        m_aResWords = new String[iMaxResword + 1];
        Enumeration<String> e = m_resWordsTable.keys();
        while (e.hasMoreElements()) {
            Object o = e.nextElement();
            String s = (String) o;
            int i = (m_resWordsTable.get(s)).intValue();
            m_aResWords[i] = s;
        }
    }

    /** return the name of the reserved word whose token code is "i" */
    public String lookupReserved(int i) {
        return m_aResWords[i];
    }

    private Symbol makeSymbol(int id, Object o) {
        int iPrevPrevChar = iPrevChar;
        this.iPrevChar = iChar;
        this.previousSymbol = id;
        return trace(new Symbol(id, iPrevPrevChar, iChar, o));
    }

    /**
     * Creates a token representing a numeric literal.
     *
     * @param mantissa The digits of the number
     * @param exponent The base-10 exponent of the number
     * @return number literal token
     */
    private Symbol makeNumber(BigDecimal mantissa, int exponent) {
        BigDecimal d = mantissa.movePointRight(exponent);
        return makeSymbol(ParserSym.NUMBER, d);
    }

    private Symbol makeId(String s, boolean quoted, boolean ampersand) {
        return makeSymbol(
            quoted && ampersand
            ? ParserSym.AMP_QUOTED_ID
            : quoted
            ? ParserSym.QUOTED_ID
            : ParserSym.ID,
            s);
    }

    /**
     * Creates a token representing a reserved word.
     *
     * @param i Token code
     * @return Token
     */
    private Symbol makeRes(int i) {
        return makeSymbol(i, m_aResWords[i]);
    }

    /**
     * Creates a token.
     *
     * @param i Token code
     * @param s Text of the token
     * @return Token
     */
    private Symbol makeToken(int i, String s) {
        return makeSymbol(i, s);
    }

    /**
     * Creates a token representing a string literal.
     *
     * @param s String
     * @return String token
     */
    private Symbol makeString(String s) {
        if (inFormula) {
            inFormula = false;
            return makeSymbol(ParserSym.FORMULA_STRING, s);
        } else {
            return makeSymbol(ParserSym.STRING, s);
        }
    }

    /**
     * Discards all characters until the end of the current line.
     */
    private void skipToEOL() throws IOException {
        while (nextChar != -1 && nextChar != '\012') {
            advance();
        }
    }

    /**
     * Eats a delimited comment.
     * The type of delimiters are kept in commentDelim.  The current
     * comment type is indicated by commentType.
     * end of file terminates a comment without error.
     */
    private void skipComment(
        final String startDelim,
        final String endDelim) throws IOException
    {
        int depth = 1;

        // skip the starting delimiter
        for (int x = 0; x < startDelim.length(); x++) {
            advance();
        }

        for (;;) {
            if (nextChar == -1) {
                return;
            } else if (checkForSymbol(endDelim)) {
                // eat the end delimiter
                for (int x = 0; x < endDelim.length(); x++) {
                    advance();
                }
                if (--depth == 0) {
                    return;
                }
            } else if (allowNestedComments && checkForSymbol(startDelim)) {
               // eat the nested start delimiter
                for (int x = 0; x < startDelim.length(); x++) {
                    advance();
                }
                depth++;
            } else {
                advance();
            }
        }
    }

    /**
     * If the next tokens are comments, skip over them.
     */
    private void searchForComments() throws IOException {
        // eat all following comments
        boolean foundComment;
        do {
            foundComment = false;
            for (String[] aCommentDelim : commentDelim) {
                if (checkForSymbol(aCommentDelim[0])) {
                    if (aCommentDelim[1] == null) {
                        foundComment = true;
                        skipToEOL();
                    } else {
                        foundComment = true;
                        skipComment(aCommentDelim[0], aCommentDelim[1]);
                    }
                }
            }
        } while (foundComment);
    }

    /**
     * Checks if the next symbol is the supplied string
     */
    private boolean checkForSymbol(final String symb) throws IOException {
            for (int x = 0; x < symb.length(); x++) {
                if (symb.charAt(x) != lookahead(x)) {
                    return false;
                }
            }
            return true;
    }

    /**
     * Recognizes and returns the next complete token.
     */
    public Symbol next_token() throws IOException {
        StringBuilder id;
        boolean ampersandId = false;
        for (;;) {
            searchForComments();
            switch (nextChar) {
            case '.':
                switch (lookahead()) {
                case '0': case '1': case '2': case '3': case '4':
                case '5': case '6': case '7': case '8': case '9':
                    // We're looking at the '.' on the start of a number,
                    // e.g. .1; fall through to parse a number.
                    break;
                default:
                    advance();
                    return makeToken(ParserSym.DOT, ".");
                }
                // fall through

            case '0': case '1': case '2': case '3': case '4':
            case '5': case '6': case '7': case '8': case '9':

                // Parse a number.  Valid examples include 1, 1.2, 0.1, .1,
                // 1e2, 1E2, 1e-2, 1e+2.  Invalid examples include e2, 1.2.3,
                // 1e2e3, 1e2.3.
                //
                // Signs preceding numbers (e.g. -1,  + 1E-5) are valid, but are
                // handled by the parser.
                //
                BigDecimal n = BigDecimalZero;
                int digitCount = 0, exponent = 0;
                boolean positive = true;
                BigDecimal mantissa = BigDecimalZero;
                State state = State.leftOfPoint;

                for (;;) {
                    switch (nextChar) {
                    case '.':
                        switch (state) {
                        case leftOfPoint:
                            state = State.rightOfPoint;
                            mantissa = n;
                            n = BigDecimalZero;
                            digitCount = 0;
                            positive = true;
                            advance();
                            break;
                            // Error: we are seeing a point in the exponent
                            // (e.g. 1E2.3 or 1.2E3.4) or a second point in the
                            // mantissa (e.g. 1.2.3).  Return what we've got
                            // and let the parser raise the error.
                        case rightOfPoint:
                            mantissa =
                                mantissa.add(
                                    n.movePointRight(-digitCount));
                            return makeNumber(mantissa, exponent);
                        case inExponent:
                            if (!positive) {
                                n = n.negate();
                            }
                            exponent = n.intValue();
                            return makeNumber(mantissa, exponent);
                        }
                        break;

                    case 'E':
                    case 'e':
                        switch (state) {
                        case inExponent:
                            // Error: we are seeing an 'e' in the exponent
                            // (e.g. 1.2e3e4).  Return what we've got and let
                            // the parser raise the error.
                            if (!positive) {
                                n = n.negate();
                            }
                            exponent = n.intValue();
                            return makeNumber(mantissa, exponent);
                        case leftOfPoint:
                            mantissa = n;
                            break;
                        default:
                            mantissa =
                                mantissa.add(
                                    n.movePointRight(-digitCount));
                            break;
                        }

                        digitCount = 0;
                        n = BigDecimalZero;
                        positive = true;
                        advance();
                        state = State.inExponent;
                        break;

                    case'0': case'1': case'2': case'3': case'4':
                    case'5': case'6': case'7': case'8': case'9':
                        n = n.movePointRight(1);
                        n = n.add(BigDecimal.valueOf(nextChar - '0'));
                        digitCount++;
                        advance();
                        break;

                    case '+':
                    case '-':
                        if (state == State.inExponent
                            && digitCount == 0)
                        {
                            // We're looking at the sign after the 'e'.
                            positive = !positive;
                            advance();
                            break;
                        }
                        // fall through - end of number

                    default:
                        // Reached end of number.
                        switch (state) {
                        case leftOfPoint:
                            mantissa = n;
                            break;
                        case rightOfPoint:
                            mantissa =
                                mantissa.add(
                                    n.movePointRight(-digitCount));
                            break;
                        default:
                            if (!positive) {
                                n = n.negate();
                            }
                            exponent = n.intValue();
                            break;
                        }
                        return makeNumber(mantissa, exponent);
                    }
                }

            case 'a': case 'b': case 'c': case 'd': case 'e': case 'f':
            case 'g': case 'h': case 'i': case 'j': case 'k': case 'l':
            case 'm': case 'n': case 'o': case 'p': case 'q': case 'r':
            case 's': case 't': case 'u': case 'v': case 'w': case 'x':
            case 'y': case 'z':
            case 'A': case 'B': case 'C': case 'D': case 'E': case 'F':
            case 'G': case 'H': case 'I': case 'J': case 'K': case 'L':
            case 'M': case 'N': case 'O': case 'P': case 'Q': case 'R':
            case 'S': case 'T': case 'U': case 'V': case 'W': case 'X':
            case 'Y': case 'Z':
            case '_': case '$':
                /* parse an identifier */
                id = new StringBuilder();
                for (;;) {
                    id.append((char)nextChar);
                    advance();
                    switch (nextChar) {
                    case 'a': case 'b': case 'c': case 'd': case 'e': case 'f':
                    case 'g': case 'h': case 'i': case 'j': case 'k': case 'l':
                    case 'm': case 'n': case 'o': case 'p': case 'q': case 'r':
                    case 's': case 't': case 'u': case 'v': case 'w': case 'x':
                    case 'y': case 'z':
                    case 'A': case 'B': case 'C': case 'D': case 'E': case 'F':
                    case 'G': case 'H': case 'I': case 'J': case 'K': case 'L':
                    case 'M': case 'N': case 'O': case 'P': case 'Q': case 'R':
                    case 'S': case 'T': case 'U': case 'V': case 'W': case 'X':
                    case 'Y': case 'Z':
                    case '0': case '1': case '2': case '3': case '4':
                    case '5': case '6': case '7': case '8': case '9':
                    case '_': case '$':
                        break;
                    default:
                        String strId = id.toString();
                        Integer i = m_resWordsTable.get(
                            strId.toUpperCase());
                        if (i == null) {
                            // identifier
                            return makeId(strId, false, false);
                        } else {
                            // reserved word
                            return makeRes(i);
                        }
                    }
                }

            case '&':
                advance();
                if (nextChar == '[') {
                    ampersandId = true;
                    // fall through
                } else {
                    return makeToken(ParserSym.UNKNOWN, "&");
                }

            case '[':
                /* parse a delimited identifier */
                id = new StringBuilder();
                for (;;) {
                    advance();
                    switch (nextChar) {
                    case ']':
                        advance();
                        if (nextChar == ']') {
                            // ] escaped with ] - just take one
                            id.append(']');
                            break;
                        } else {
                            // end of identifier
                            if (ampersandId) {
                                ampersandId = false;
                                return makeId(id.toString(), true, true);
                            } else {
                                return makeId(id.toString(), true, false);
                            }
                        }
                    case -1:
                        if (ampersandId) {
                            ampersandId = false;
                            return makeId(id.toString(), true, true);
                        } else {
                            return makeId(id.toString(), true, false);
                        }
                    default:
                        id.append((char)nextChar);
                    }
                }

            case ':':
                advance();
                return makeToken(ParserSym.COLON, ":");
            case ',':
                advance();
                return makeToken(ParserSym.COMMA, ",");
            case '=':
                advance();
                return makeToken(ParserSym.EQ, "=");
            case '<':
                advance();
                switch (nextChar) {
                case '>':
                    advance();
                    return makeToken(ParserSym.NE, "<>");
                case '=':
                    advance();
                    return makeToken(ParserSym.LE, "<=");
                default:
                    return makeToken(ParserSym.LT, "<");
                }
            case '>':
                advance();
                switch (nextChar) {
                case '=':
                    advance();
                    return makeToken(ParserSym.GE, ">=");
                default:
                    return makeToken(ParserSym.GT, ">");
                }
            case '{':
                advance();
                return makeToken(ParserSym.LBRACE, "{");
            case '(':
                advance();
                return makeToken(ParserSym.LPAREN, "(");
            case '}':
                advance();
                return makeToken(ParserSym.RBRACE, "}");
            case ')':
                advance();
                return makeToken(ParserSym.RPAREN, ")");
            case '+':
                advance();
                return makeToken(ParserSym.PLUS, "+");
            case '-':
                advance();
                return makeToken(ParserSym.MINUS, "-");
            case '*':
                advance();
                return makeToken(ParserSym.ASTERISK, "*");
            case '/':
                advance();
                return makeToken(ParserSym.SOLIDUS, "/");
            case '!':
                advance();
                return makeToken(ParserSym.BANG, "!");
            case '|':
                advance();
                switch (nextChar) {
                case '|':
                    advance();
                    return makeToken(ParserSym.CONCAT, "||");
                default:
                    return makeToken(ParserSym.UNKNOWN, "|");
                }

            case '"':
                /* parse a double-quoted string */
                id = new StringBuilder();
                for (;;) {
                    advance();
                    switch (nextChar) {
                    case '"':
                        advance();
                        if (nextChar == '"') {
                            // " escaped with "
                            id.append('"');
                            break;
                        } else {
                            // end of string
                            return makeString(id.toString());
                        }
                    case -1:
                        return makeString(id.toString());
                    default:
                        id.append((char)nextChar);
                    }
                }

            case '\'':
                if (previousSymbol == ParserSym.AS) {
                    inFormula = true;
                }

                /* parse a single-quoted string */
                id = new StringBuilder();
                for (;;) {
                    advance();
                    switch (nextChar) {
                    case '\'':
                        advance();
                        if (nextChar == '\'') {
                            // " escaped with "
                            id.append('\'');
                            break;
                        } else {
                            // end of string
                            return makeString(id.toString());
                        }
                    case -1:
                        return makeString(id.toString());
                    default:
                        id.append((char)nextChar);
                    }
                }

            case -1:
                // we're done
                return makeToken(ParserSym.EOF, "EOF");

            default:
                // If it's whitespace, skip over it.
                // (When we switch to JDK 1.5, use Character.isWhitespace(int);
                // til then, there's just Character.isWhitespace(char).)
                if (nextChar <= Character.MAX_VALUE
                    && Character.isWhitespace((char) nextChar))
                {
                    // fall through
                } else {
                    // everything else is an error
                    throw new RuntimeException(
                        "Unexpected character '" + (char) nextChar + "'");
                }

            case ' ':
            case '\t':
            case '\n':
            case '\r':
                // whitespace can be ignored
                iPrevChar = iChar;
                advance();
                break;
            }
        }
    }

    private enum State {
        leftOfPoint,
        rightOfPoint,
        inExponent,
    }
}

// End Scanner.java

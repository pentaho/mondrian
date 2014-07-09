/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test.loader;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * This is a basic Comma-separated-value (CSV, Csv) reader. As input it
 * ultimately takes a <code>java.io.Reader</code> but has helper support for
 * <code>java.io.InputStream, file</code> names and
 * <code>java.io.File</code>.
 * One can also specify a separator character other than the default
 * comma, ',', character and, also, that the input's first line contains the
 * names of the columns (by default this is not assumed). Lastly, this supports
 * only the comment character '#' and only at the start of a line.  This comment
 * support could be generalized but that task is left to others.
 * <p>
 * To use this class one gives it a <code>java.io.Reader</code> and then calls
 * the <code>hasNextLine</code> and <code>nextLine</code> methods much like a
 * <code>java.io.Iterator</code> but in this case the <code>nextLine</code>
 * method returns a <code>String[]</code> holding the, possibly null, values of
 * the parsed next line. The size of the <code>String[]</code> is the size of
 * the first line parsed that contains the separator character (comment lines
 * are not used). If the number of separator characters in subsequent lines is
 * less than the initial numbers, the trailing entries in the
 * <code>String[]</code> returned by the <code>nextLine</code> method are null.
 * On the other hand, if there are more separator characters in a subsequent
 * line, the world ends with an <code>IndexOutOfBoundsException</code> (sorry,
 * making this more graceful is also a task for others). When one is through
 * using a <code>CsvLoader</code> instance one should call the close method
 * (which closes the <code>Reader</code>).
 * <p>
 * All well and good, but there are two additional methods that can be used to
 * extend the capabilities of this CSV parser, the <code>nextSet</code> and
 * <code>putBack</code> methods. With these methods one can, basically, reset
 * the <code>CsvLoader</code> to a state where it does not yet know how many
 * separator characters to expect per line (while stay at the current line in
 * the <code>Reader</code>).  The <code>nextSet</code> (next set of CSV lines)
 * resets the loader while the <code>putBack</code> method can be used to place
 * the last line returned back into loader. These methods are used in
 * <code>CsvDBLoader</code> allowing one to have multiple sets of CSV rows with
 * differing number of values per sets.
 * <p>
 * There are six special start/end characters when seen prevent the
 * recognition of both the separator character and new lines:
 *
 * <blockquote><pre>
 *    double quotes: "" ""
 *    single quotes: '  '
 *    bracket: i     [ ]
 *    parenthesis:   ()
 *    braces:        { }
 *    chevrons:      < >
 * </pre></blockquote>
 *
 * <p>
 * Its certainly not the penultimate such parser but its hoped that its
 * adequate.
 *
 * @author <a>Richard M. Emberson</a>
 */
public class CsvLoader {
    protected static final Logger LOGGER = Logger.getLogger(CsvLoader.class);
    public static final char DEFAULT_SEPARATOR = ',';
    public static final char DOUBLE_QUOTE = '"';
    public static final char SINGLE_QUOTE = '\'';
    // bracket []
    public static final char BRACKET_START = '[';
    public static final char BRACKET_END = '[';
    // parenthesis ()
    public static final char PAREN_START = '(';
    public static final char PAREN_END = ')';
    // braces {}
    public static final char BRACES_START = '{';
    public static final char BRACES_END = '}';
    // chevrons <>
    public static final char CHEVRON_START = '<';
    public static final char CHEVRON_END = '>';

    private BufferedReader bufReader;
    private final char separator;
    private final boolean includesHeader;
    private int nextSet;
    private boolean inComment;
    private String[] columnNames;
    private String[] columns;

    public CsvLoader(InputStream in, String charset)
        throws UnsupportedEncodingException
    {
        this(new InputStreamReader(in, charset));
    }

    public CsvLoader(
        InputStream in, char separator,
        boolean includesHeader, String charset)
        throws UnsupportedEncodingException
    {
        this(new InputStreamReader(in, charset), separator, includesHeader);
    }

    public CsvLoader(InputStream in) {
        this(new InputStreamReader(in));
    }

    public CsvLoader(InputStream in, char separator, boolean includesHeader) {
        this(new InputStreamReader(in), separator, includesHeader);
    }

    public CsvLoader(String filename) throws FileNotFoundException {
        this(new FileReader(filename));
    }

    public CsvLoader(String filename, char separator, boolean includesHeader)
        throws FileNotFoundException
    {
        this(new FileReader(filename), separator, includesHeader);
    }

    public CsvLoader(File file) throws FileNotFoundException {
        this(new FileReader(file));
    }

    public CsvLoader(File file, char separator, boolean includesHeader)
        throws FileNotFoundException
    {
        this(new FileReader(file), separator, includesHeader);
    }

    public CsvLoader(Reader reader) {
        this(reader, DEFAULT_SEPARATOR, false);
    }

    public CsvLoader(Reader reader, char separator, boolean includesHeader) {
        this.bufReader = (reader instanceof BufferedReader)
            ? (BufferedReader) reader
            : new BufferedReader(reader);

        this.separator = separator;
        this.includesHeader = includesHeader;
    }

    protected void initialize() throws IOException {
        if (this.columnNames == null) {
            if (this.columns == null) {
                this.columns = nextColumns();
            }
            if (this.columns == null) {
                if (this.nextSet > 0) {
                    return;
                }
                throw new IOException("No columns can be read");
            }
            if (! this.inComment) {
                if (this.includesHeader) {
                    this.columnNames = this.columns;
                    this.columns = null;
                } else {
                    this.columnNames = new String[this.columns.length];
                    for (int i = 0; i < this.columns.length; i++) {
                        this.columnNames[i] = "Column" + i;
                    }
                }
            }
        }
    }

    public String[] getColumnNames() throws IOException {
        initialize();
        return this.columnNames;
    }
    public boolean inComment() {
        return this.inComment;
    }

    public boolean hasNextLine() throws IOException {
        initialize();
        if (this.bufReader == null) {
            return false;
        } else if (this.columns != null) {
            return true;
        } else {
            this.columns = nextColumns();
            return (this.columns != null);
        }
    }
    public String[] nextLine() {
        if (this.bufReader == null) {
            return null;
        } else {
            try {
                return this.columns;
            } finally {
                this.columns = null;
            }
        }
    }


    public int getNextSetCount() {
        return this.nextSet;
    }
    public void nextSet() {
        this.nextSet++;
        this.columns = null;
        this.columnNames = null;
    }
    public void putBack(String[] columns) {
        this.columns = columns;
    }
    public void close() throws IOException {
        if (this.bufReader != null) {
            this.bufReader.close();
            this.bufReader = null;
        }
    }


  protected String[] nextColumns() throws IOException {
        StringBuilder buf = new StringBuilder();
        // the separator char seen in single or double quotes is not treated
        // as a separator
        boolean inDoubleQuote = false;
        boolean inSingleQuote = false;
        // bracket []
        boolean inBracket = false;
        // parenthesis ()
        boolean inParen = false;
        // braces {}
        boolean inBraces = false;
        // chevrons <>
        boolean inChevrons = false;
        String[] columns = null;
        // if we do not know how many columns there are
        List list = null;
        if (this.columnNames == null) {
            list = new ArrayList();
        } else {
            columns = new String[columnNames.length];
        }
        int columnCount = 0;
        char c;

        do {
            String line = this.bufReader.readLine();
            if (line == null) {
                return null;
            }
            recordInCommentLine(line);
            int pos = 0;
            while (pos < line.length()) {
                c = line.charAt(pos++);

                if (inDoubleQuote) {
                    if (c == DOUBLE_QUOTE) {
                        inDoubleQuote = false;
                    }
                } else if (inSingleQuote) {
                    if (c == SINGLE_QUOTE) {
                        inSingleQuote = false;
                    }
                } else if (inParen) {
                    if (c == PAREN_END) {
                        inParen = false;
                    }
                } else if (inBracket) {
                    if (c == BRACKET_END) {
                        inBracket = false;
                    }
                } else if (inBraces) {
                    if (c == BRACES_END) {
                        inBraces = false;
                    }
                } else if (inChevrons) {
                    if (c == CHEVRON_END) {
                        inChevrons = false;
                    }
                } else if (c == DOUBLE_QUOTE) {
                    inDoubleQuote = true;
                } else if (c == SINGLE_QUOTE) {
                    inSingleQuote = true;
                } else if (c == BRACKET_START) {
                    inBracket = true;
                } else if (c == PAREN_START) {
                    inParen = true;
                } else if (c == BRACES_START) {
                    inBraces = true;
                } else if (c == CHEVRON_START) {
                    inChevrons = true;
                }
                if (inDoubleQuote || inSingleQuote
                    || inParen || inBracket || inBraces || inChevrons)
                {
                    buf.append(c);
                } else if (c == this.separator) {
                    String data  = buf.toString();
                    if (list != null) {
                        list.add(data);
                    } else {
                        columns[columnCount++] = data;
                    }
                    buf.setLength(0);
                } else {
                    buf.append(c);
                }
            }
        } while (inDoubleQuote || inSingleQuote);

        String data  = buf.toString();
        if (list != null) {
            list.add(data);
        } else {
            columns[columnCount++] = data;
        }

        if (list != null) {
            int size = list.size();
            columns = (String[]) list.toArray(new String[size]);
        }
        if (LOGGER.isDebugEnabled()) {
            buf.setLength(0);
            buf.append("Columns: ");
            if (this.inComment) {
                buf.append("comment=true: ");
            }
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                if (i > 0) {
                    buf.append(", ");
                }
                buf.append(column);
            }
            LOGGER.debug(buf.toString());
        }
        return columns;
    }

    protected void recordInCommentLine(String line) {
        this.inComment = (line.charAt(0) == '#');
    }


    public static void main(String[] args) throws Exception {
        char separator = DEFAULT_SEPARATOR;
        for (int cnt = 0; cnt < args.length; cnt++) {
            String filename = args[cnt];
            System.out.println("FileName:" + filename);

            CsvLoader csvLoader = new CsvLoader(filename);

            String[] columnNames = csvLoader.getColumnNames();
            System.out.println("Column Names:");
            System.out.print("  ");
            for (int i = 0; i < columnNames.length; i++) {
                System.out.print(columnNames[i]);
                if (i + 1 < columnNames.length) {
                    System.out.print(separator);
                }
            }
            System.out.println();

            System.out.println("Data:");
            while (csvLoader.hasNextLine()) {
                System.out.print("  ");
                String[] columns = csvLoader.nextLine();
                for (int i = 0; i < columns.length; i++) {
                    System.out.print(columns[i]);
                    if (i + 1 < columns.length) {
                        System.out.print(separator);
                    }
                }
                System.out.println();
            }
        }
    }
}

// End CsvLoader.java

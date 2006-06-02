/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

import mondrian.olap.Connection;
import mondrian.tui.CmdRunner;

import java.io.*;

/**
 * Unit test for {@link mondrian.tui.CmdRunner}.
 *
 * @author jhyde
 * @version $Id$
 * @since Jun 2, 2006
 */
public class CmdRunnerTest extends FoodMartTestCase {
    protected DiffRepository getDiffRepos() {
        return DiffRepository.lookup(CmdRunnerTest.class);
    }

    public void testQuery() throws IOException {
        final DiffRepository diffRepos = getDiffRepos();
        String input = diffRepos.expand("input", "${input}");
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        final CmdRunnerTrojan cmdRunner = new CmdRunnerTrojan(null, pw);
        cmdRunner.commandLoop(new StringReader(input), false);
        pw.flush();
        String output = sw.toString();
        diffRepos.assertEquals("output", "${output}", output);
    }

    private class CmdRunnerTrojan extends CmdRunner {
        public CmdRunnerTrojan(CmdRunner.Options options, PrintWriter out) {
            super(options, out);
        }

        public void commandLoop(Reader in, boolean interactive) {
            super.commandLoop(in, interactive);
        }

        public Connection getConnection() {
            return CmdRunnerTest.this.getConnection();
        }
    }
}

// End CmdRunnerTest.java

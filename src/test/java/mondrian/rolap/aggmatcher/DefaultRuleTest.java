/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.aggmatcher;

import mondrian.recorder.ListRecorder;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import org.eigenbase.xom.*;

import java.io.File;
import java.io.FileReader;
import java.util.Iterator;

/**
 * Testing the default aggregate table recognizer.
 *
 * @author Richard M. Emberson
 */
public class DefaultRuleTest extends TestCase {
    private static final Logger LOGGER =
        Logger.getLogger(DefaultRuleTest.class);
    private static final String DIRECTORY =
        "target/test-classes/mondrian/rolap/aggmatcher";
    private static final String TEST_RULE_XML = "TestRule.xml";

    private DefaultDef.AggRules rules;

    public DefaultRuleTest() {
        super();
    }

    public DefaultRuleTest(String name) {
        super(name);
    }

    private DefaultDef.AggRule getAggRule(String tag) {
        return rules.getAggRule(tag);
    }

    protected void setUp() throws Exception {
        File file = new File(DIRECTORY, TEST_RULE_XML);
        FileReader reader = new FileReader(file);

        Parser xmlParser = XOMUtil.createDefaultParser();

       final DOMWrapper domWrapper = xmlParser.parse(reader);
       rules = new DefaultDef.AggRules(domWrapper);

       ListRecorder msgRecorder = new ListRecorder();
       rules.validate(msgRecorder);
        if (msgRecorder.hasErrors()) {
            LOGGER.error("HAS ERRORS");
            for (Iterator it = msgRecorder.getErrorEntries(); it.hasNext();) {
                ListRecorder.Entry e = (ListRecorder.Entry) it.next();
                LOGGER.error("context=" + e.getContext());
                LOGGER.error("message=" + e.getMessage());
            }
        }
    }

    protected void tearDown() throws Exception {
    }

    private Recognizer.Matcher getTableMatcher(String tag, String tableName) {
        DefaultDef.AggRule rule = getAggRule(tag);
        if (rule == null) {
            LOGGER.info("rule == null for tag=" + tag);
        }
        DefaultDef.TableMatch tableMatch = rule.getTableMatch();
        if (tableMatch == null) {
            LOGGER.info(
                "tableMatch == null for tag=" + tag
                + ", tableName=" + tableName);
        }
        return tableMatch.getMatcher(tableName);
    }

    private Recognizer.Matcher getFactCountMatcher(String tag) {
        DefaultDef.AggRule rule = getAggRule(tag);
        DefaultDef.FactCountMatch factTableName = rule.getFactCountMatch();
        return factTableName.getMatcher();
    }

    private Recognizer.Matcher getForeignKeyMatcher(
        String tag,
        String foreignKeyName)
    {
        DefaultDef.AggRule rule = getAggRule(tag);
        DefaultDef.ForeignKeyMatch foreignKeyMatch = rule.getForeignKeyMatch();
        return foreignKeyMatch.getMatcher(foreignKeyName);
    }


    private Recognizer.Matcher getLevelMatcher(
        String tag,
        String usagePrefix,
        String hierarchyName,
        String levelName,
        String levelColumnName)
    {
        DefaultDef.AggRule rule = getAggRule(tag);
        Recognizer.Matcher matcher =
            rule.getLevelMap().getMatcher(
                usagePrefix,
                hierarchyName,
                levelName,
                levelColumnName);
        return matcher;
    }

    private Recognizer.Matcher getMeasureMatcher(
        String tag,
        String measureName,
        String measureColumnName,
        String aggregateName)
    {
        DefaultDef.AggRule rule = getAggRule(tag);
        Recognizer.Matcher matcher =
            rule.getMeasureMap().getMatcher(
                measureName,
                measureColumnName,
                aggregateName);
        return matcher;
    }

    //////////////////////////////////////////////////////////////////////////
    //
    // tests
    //
    //

    public void testTableNameDefault() {
        final String tag = "default";
        final String factTableName = "FACT_TABLE";

        Recognizer.Matcher matcher = getTableMatcher(tag, factTableName);

        doMatch(matcher, "agg_10_" + factTableName);
        doMatch(matcher, "AGG_10_" + factTableName);
        doMatch(matcher, "agg_this_is_ok_" + factTableName);
        doMatch(matcher, "AGG_THIS_IS_OK_" + factTableName);
        doMatch(matcher, "agg_10_" + factTableName.toLowerCase());
        doMatch(matcher, "AGG_10_" + factTableName.toLowerCase());
        doMatch(matcher, "agg_this_is_ok_" + factTableName.toLowerCase());
        doMatch(matcher, "AGG_THIS_IS_OK_" + factTableName.toLowerCase());

        doNotMatch(matcher, factTableName);
        doNotMatch(matcher, "agg__" + factTableName);
        doNotMatch(matcher, "agg_" + factTableName);
        doNotMatch(matcher, factTableName + "_agg");
        doNotMatch(matcher, "agg_10_Mytable");
    }

    public void testTableNameBBBB() {
        final String tag = "bbbb";
        final String factTableName = "FACT_TABLE";

        Recognizer.Matcher matcher = getTableMatcher(tag, factTableName);

        doMatch(matcher, factTableName + "_agg_10");
        doMatch(matcher, factTableName + "_agg_this_is_ok");

        doNotMatch(matcher, factTableName);
        doNotMatch(matcher, factTableName + "_agg");
        doNotMatch(matcher, factTableName + "__agg");
        doNotMatch(matcher, "agg_" + factTableName);
        doNotMatch(matcher, "Mytable_agg_10");
    }

    public void testTableNameCCCCBAD() {
        final String tag = "cccc";
        final String basename = "WAREHOUSE";
        final String factTableName = "RF_" + basename + "_TABLE";

        // Note that the "basename" and not the fact table name is
        // being used. The Matcher that is return will not match anything
        // because the basename does not match the table basename pattern.
        Recognizer.Matcher matcher = getTableMatcher(tag, basename);

        doNotMatch(matcher, "AGG_10_" + basename);
        doNotMatch(matcher, "agg_this_is_ok_" + basename);

        doNotMatch(matcher, factTableName);
        doNotMatch(matcher, "agg__" + basename);
        doNotMatch(matcher, "agg_" + basename);
        doNotMatch(matcher, basename + "_agg");
        doNotMatch(matcher, "agg_10_Mytable");
    }

    public void testTableNameCCCCGOOD() {
        final String tag = "cccc";
        final String basename = "WAREHOUSE";
        final String factTableName = "RF_" + basename + "_TABLE";

        Recognizer.Matcher matcher = getTableMatcher(tag, factTableName);

        doMatch(matcher, "AGG_10_" + basename);
        doMatch(matcher, "agg_this_is_ok_" + basename);

        doNotMatch(matcher, factTableName);
        doNotMatch(matcher, "agg__" + basename);
        doNotMatch(matcher, "agg_" + basename);
        doNotMatch(matcher, basename + "_agg");
        doNotMatch(matcher, "agg_10_Mytable");
    }

    public void testFactCountDefault() {
        final String tag = "default";
        Recognizer.Matcher matcher = getFactCountMatcher(tag);

        doMatch(matcher, "fact_count");
        doMatch(matcher, "FACT_COUNT");

        doNotMatch(matcher, "my_fact_count");
        doNotMatch(matcher, "MY_FACT_COUNT");
        doNotMatch(matcher, "count");
        doNotMatch(matcher, "COUNT");
        doNotMatch(matcher, "fact_count_my");
        doNotMatch(matcher, "FACT_COUNT_MY");
    }

    public void testFactCountBBBB() {
        final String tag = "bbbb";
        Recognizer.Matcher matcher = getFactCountMatcher(tag);

        doMatch(matcher, "my_fact_count");
        doMatch(matcher, "MY_FACT_COUNT");

        doNotMatch(matcher, "fact_count");
        doNotMatch(matcher, "FACT_COUNT");
        doNotMatch(matcher, "count");
        doNotMatch(matcher, "COUNT");
        doNotMatch(matcher, "fact_count_my");
        doNotMatch(matcher, "FACT_COUNT_MY");
    }

    public void testFactCountCCCC() {
        final String tag = "cccc";
        Recognizer.Matcher matcher = getFactCountMatcher(tag);

        doMatch(matcher, "MY_FACT_COUNT");

        doNotMatch(matcher, "my_fact_count");
        doNotMatch(matcher, "fact_count");
        doNotMatch(matcher, "FACT_COUNT");
        doNotMatch(matcher, "count");
        doNotMatch(matcher, "COUNT");
        doNotMatch(matcher, "fact_count_my");
        doNotMatch(matcher, "FACT_COUNT_MY");
    }

    public void testForeignKeyDefault() {
        final String tag = "default";
        final String foreignKeyName = "foo_key";
        Recognizer.Matcher matcher = getForeignKeyMatcher(tag, foreignKeyName);

        doMatch(matcher, "foo_key");
        doMatch(matcher, "FOO_KEY");

        doNotMatch(matcher, "foo_key_my");
        doNotMatch(matcher, "my_foo_key");
    }

    public void testForeignKeyBBBB() {
        final String tag = "bbbb";
        final String foreignKeyName = "fk_ham_n_eggs";
        Recognizer.Matcher matcher = getForeignKeyMatcher(tag, foreignKeyName);

        doMatch(matcher, "HAM_N_EGGS_FK");

        doNotMatch(matcher, "ham_n_eggs_fk");
        doNotMatch(matcher, "ham_n_eggs");
        doNotMatch(matcher, "fk_ham_n_eggs");
        doNotMatch(matcher, "HAM_N_EGGS");
        doNotMatch(matcher, "FK_HAM_N_EGGS");
    }
/*
        <ForeignKeyMatch id="fkc" basename="(?:FK|fk)_(.*)"
                posttemplate="_[fF][kK]"
                charcase="exact" />
*/
    public void testForeignKeyCCCC() {
        final String tag = "cccc";
        final String foreignKeyName1 = "fk_toast";
        final String foreignKeyName2 = "FK_TOAST";
        final String foreignKeyName3 = "FK_ToAsT";
        Recognizer.Matcher matcher1 =
            getForeignKeyMatcher(tag, foreignKeyName1);
        Recognizer.Matcher matcher2 =
            getForeignKeyMatcher(tag, foreignKeyName2);
        Recognizer.Matcher matcher3 =
            getForeignKeyMatcher(tag, foreignKeyName3);

        doMatch(matcher1, "toast_fk");
        doNotMatch(matcher1, "TOAST_FK");

        doMatch(matcher2, "TOAST_FK");
        doNotMatch(matcher2, "toast_fk");

        doMatch(matcher3, "ToAsT_FK");
        doMatch(matcher3, "ToAsT_fk");
        doMatch(matcher3, "ToAsT_Fk");
        doNotMatch(matcher3, "toast_fk");
        doNotMatch(matcher3, "TOAST_FK");
    }

    public void testLevelDefaultOne() {
        final String tag = "default";
        final String usagePrefix = null;
        final String hierarchyName = "Time";
        final String levelName = "Day in Year";
        final String levelColumnName = "days";
        Recognizer.Matcher matcher = getLevelMatcher(
            tag, usagePrefix, hierarchyName, levelName, levelColumnName);

        doMatch(matcher, "days");
        doMatch(matcher, "time_day_in_year");
        doMatch(matcher, "time_days");

        doNotMatch(matcher, "DAYS");
        doNotMatch(matcher, "Time Day in Year");
    }

    public void testLevelDefaultTwo() {
        final String tag = "default";
        final String usagePrefix = "boo_";
        final String hierarchyName = "Time";
        final String levelName = "Day in Year";
        final String levelColumnName = "days";
        Recognizer.Matcher matcher = getLevelMatcher(
            tag, usagePrefix, hierarchyName, levelName, levelColumnName);

        doMatch(matcher, "days");
        doMatch(matcher, "boo_days");
        doMatch(matcher, "time_day_in_year");
        doMatch(matcher, "time_days");

        doNotMatch(matcher, "boo_time_day_in_year");
        doNotMatch(matcher, "boo_time_days");
        doNotMatch(matcher, "DAYS");
        doNotMatch(matcher, "Time Day in Year");
    }

    public void testLevelBBBB() {
        final String tag = "bbbb";
        final String usagePrefix = "boo_";
        final String hierarchyName = "Time.Period";
        final String levelName = "Day in Year";
        final String levelColumnName = "days";
        Recognizer.Matcher matcher = getLevelMatcher(
            tag, usagePrefix, hierarchyName, levelName, levelColumnName);

        doMatch(matcher, "boo_time_DOT_period_day_SP_in_SP_year_days");
    }

    public void testMeasureDefault() {
        final String tag = "default";
        final String measureName = "Total Sales";
        final String measureColumnName = "sales";
        final String aggregateName = "sum";
        Recognizer.Matcher matcher = getMeasureMatcher(
            tag, measureName, measureColumnName, aggregateName);

        doMatch(matcher, "total_sales");
        doMatch(matcher, "sales");
        doMatch(matcher, "sales_sum");

        doNotMatch(matcher, "Total Sales");
        doNotMatch(matcher, "Total_Sales");
        doNotMatch(matcher, "total_sales_sum");
    }

    //
    //////////////////////////////////////////////////////////////////////////

    //////////////////////////////////////////////////////////////////////////
    //
    // helpers
    //
    //
    private void doMatch(Recognizer.Matcher matcher, String s) {
        assertTrue("Recognizer.Matcher: " + s, matcher.matches(s));
    }

    private void doNotMatch(Recognizer.Matcher matcher, String s) {
        assertTrue("Recognizer.Matcher: " + s, !matcher.matches(s));
    }
    //
    //////////////////////////////////////////////////////////////////////////
}

// End DefaultRuleTest.java

/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import java.net.URL;
import java.io.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.List;
import java.util.Collections;
import java.util.Arrays;

/**
 * Implementation of {@link MondrianServer}.
 *
 * @author jhyde
 * @version $Id$
 * @since Jun 25, 2006
 */
class MondrianServerImpl extends MondrianServer {
    private static MondrianVersion version = null;
    public static final String[] keywords = new String[] {
        "$AdjustedProbability", "$Distance", "$Probability",
        "$ProbabilityStDev", "$ProbabilityStdDeV", "$ProbabilityVariance",
        "$StDev", "$StdDeV", "$Support", "$Variance",
        "AddCalculatedMembers", "Action", "After", "Aggregate", "All",
        "Alter", "Ancestor", "And", "Append", "As", "ASC", "Axis",
        "Automatic", "Back_Color", "BASC", "BDESC", "Before",
        "Before_And_After", "Before_And_Self", "Before_Self_After",
        "BottomCount", "BottomPercent", "BottomSum", "Break", "Boolean",
        "Cache", "Calculated", "Call", "Case", "Catalog_Name", "Cell",
        "Cell_Ordinal", "Cells", "Chapters", "Children",
        "Children_Cardinality", "ClosingPeriod", "Cluster",
        "ClusterDistance", "ClusterProbability", "Clusters",
        "CoalesceEmpty", "Column_Values", "Columns", "Content",
        "Contingent", "Continuous", "Correlation", "Cousin", "Covariance",
        "CovarianceN", "Create", "CreatePropertySet", "CrossJoin", "Cube",
        "Cube_Name", "CurrentMember", "CurrentCube", "Custom", "Cyclical",
        "DefaultMember", "Default_Member", "DESC", "Descendents",
        "Description", "Dimension", "Dimension_Unique_Name", "Dimensions",
        "Discrete", "Discretized", "DrillDownLevel",
        "DrillDownLevelBottom", "DrillDownLevelTop", "DrillDownMember",
        "DrillDownMemberBottom", "DrillDownMemberTop", "DrillTrough",
        "DrillUpLevel", "DrillUpMember", "Drop", "Else", "Empty", "End",
        "Equal_Areas", "Exclude_Null", "ExcludeEmpty", "Exclusive",
        "Expression", "Filter", "FirstChild", "FirstRowset",
        "FirstSibling", "Flattened", "Font_Flags", "Font_Name",
        "Font_size", "Fore_Color", "Format_String", "Formatted_Value",
        "Formula", "From", "Generate", "Global", "Head", "Hierarchize",
        "Hierarchy", "Hierary_Unique_name", "IIF", "IsEmpty",
        "Include_Null", "Include_Statistics", "Inclusive", "Input_Only",
        "IsDescendant", "Item", "Lag", "LastChild", "LastPeriods",
        "LastSibling", "Lead", "Level", "Level_Unique_Name", "Levels",
        "LinRegIntercept", "LinRegR2", "LinRegPoint", "LinRegSlope",
        "LinRegVariance", "Long", "MaxRows", "Median", "Member",
        "Member_Caption", "Member_Guid", "Member_Name", "Member_Ordinal",
        "Member_Type", "Member_Unique_Name", "Members",
        "Microsoft_Clustering", "Microsoft_Decision_Trees", "Mining",
        "Model", "Model_Existence_Only", "Models", "Move", "MTD", "Name",
        "Nest", "NextMember", "Non", "Normal", "Not", "Ntext", "Nvarchar",
        "OLAP", "On", "OpeningPeriod", "OpenQuery", "Or", "Ordered",
        "Ordinal", "Pages", "Pages", "ParallelPeriod", "Parent",
        "Parent_Level", "Parent_Unique_Name", "PeriodsToDate", "PMML",
        "Predict", "Predict_Only", "PredictAdjustedProbability",
        "PredictHistogram", "Prediction", "PredictionScore",
        "PredictProbability", "PredictProbabilityStDev",
        "PredictProbabilityVariance", "PredictStDev", "PredictSupport",
        "PredictVariance", "PrevMember", "Probability",
        "Probability_StDev", "Probability_StdDev", "Probability_Variance",
        "Properties", "Property", "QTD", "RangeMax", "RangeMid",
        "RangeMin", "Rank", "Recursive", "Refresh", "Related", "Rename",
        "Rollup", "Rows", "Schema_Name", "Sections", "Select", "Self",
        "Self_And_After", "Sequence_Time", "Server", "Session", "Set",
        "SetToArray", "SetToStr", "Shape", "Skip", "Solve_Order", "Sort",
        "StdDev", "Stdev", "StripCalculatedMembers", "StrToSet",
        "StrToTuple", "SubSet", "Support", "Tail", "Text", "Thresholds",
        "ToggleDrillState", "TopCount", "TopPercent", "TopSum",
        "TupleToStr", "Under", "Uniform", "UniqueName", "Use", "Value",
        "Value", "Var", "Variance", "VarP", "VarianceP", "VisualTotals",
        "When", "Where", "With", "WTD", "Xor",
    };

    public MondrianVersion getVersion() {
        return getVersionStatic();
    }

    public List<String> getKeywords() {
        return Collections.unmodifiableList(Arrays.asList(keywords));
    }

    private static synchronized MondrianVersion getVersionStatic() {
        if (version == null) {
            final String[] vendorTitleVersion = loadVersionFile();
            String vendor = vendorTitleVersion[0];
            final String title = vendorTitleVersion[1];
            final String versionString = vendorTitleVersion[2];
            if (false) {
                System.out.println(
                    "vendor=" + vendor
                        + ", title=" + title
                        + ", versionString=" + versionString);
            }
            int dot1 = versionString.indexOf('.');
            final int majorVersion =
                dot1 < 0 ? 1 :
                Integer.valueOf(versionString.substring(0, dot1));
            int dot2 = versionString.indexOf('.', dot1 + 1);
            final int minorVersion =
                dot2 < 0 ? 0 :
                Integer.valueOf(versionString.substring(dot1 + 1, dot2));
            version = new MondrianVersion() {
                public String getVersionString() {
                    return versionString;
                }

                public int getMajorVersion() {
                    return majorVersion;
                }

                public int getMinorVersion() {
                    return minorVersion;
                }

                public String getProductName() {
                    return title;
                }
            };
        }
        return version;
    }

    private static String[] loadVersionFile() {
        // First, try to read the version info from the package. If the classes
        // came from a jar, this info will be set from manifest.mf.
        Package pakkage = MondrianServerImpl.class.getPackage();
        String implementationVersion = pakkage.getImplementationVersion();

        // Second, try to read VERSION.txt.
        String version = null;
        String title = null;
        String vendor = null;
        URL resource =
            MondrianServerImpl.class.getClassLoader()
                .getResource("DefaultRules.xml");
        if (resource != null) {
            try {
                String path = resource.getPath();
                String path2 =
                    Util.replace(
                        path, "DefaultRules.xml", "VERSION.txt");
                URL resource2 =
                    new URL(
                        resource.getProtocol(),
                        resource.getHost(),
                        path2);
                String versionString = Util.readURL(resource2);
                Pattern pattern =
                    Pattern.compile(
                        "(?s)Title: (.*)\nVersion: (.*)\nVendor: (.*)\n.*");
                Matcher matcher = pattern.matcher(versionString);
                if (matcher.matches()) {
                    int groupCount = matcher.groupCount();
                    assert groupCount == 3;
                    title = matcher.group(1);
                    version = matcher.group(2);
                    vendor = matcher.group(3);
                }
            } catch (IOException e) {
                // ignore exception - it's OK if file is not found
                Util.discard(e);
            }
        }

        // Version from jar manifest overrides that from VERSION.txt.
        if (implementationVersion != null) {
            version = implementationVersion;
        }
        if (version == null) {
            version = "Unknown version";
        }
        return new String[] {vendor, title, version};
    }
}

// End MondrianServerImpl.java

// This class is generated. Do NOT modify it, or
// add it to source control.

package mondrian.resource;
import java.io.IOException;
import java.util.Locale;
import java.util.ResourceBundle;
import org.eigenbase.resgen.*;

/**
 * This class was generated
 * by class org.eigenbase.resgen.ResourceGen
 * from D:/japps/githome/git/mondrian-fork/src/main/mondrian/resource/MondrianResource.xml
 * on Fri Sep 30 11:47:40 EDT 2016.
 * It contains a list of messages, and methods to
 * retrieve and format those messages.
 */

public class MondrianResource extends org.eigenbase.resgen.ShadowResourceBundle {
    public MondrianResource() throws IOException {
    }
    private static final String baseName = "mondrian.resource.MondrianResource";
    /**
     * Retrieves the singleton instance of {@link MondrianResource}. If
     * the application has called {@link #setThreadLocale}, returns the
     * resource for the thread's locale.
     */
    public static synchronized MondrianResource instance() {
        return (MondrianResource) instance(baseName, getThreadOrDefaultLocale(), ResourceBundle.getBundle(baseName, getThreadOrDefaultLocale()));
    }
    /**
     * Retrieves the instance of {@link MondrianResource} for the given locale.
     */
    public static synchronized MondrianResource instance(Locale locale) {
        return (MondrianResource) instance(baseName, locale, ResourceBundle.getBundle(baseName, locale));
    }

    /**
     * <code>Internal</code> is '<code>Internal error: {0}</code>'
     */
    public final _Def0 Internal = new _Def0("Internal", "Internal error: {0}", null);

    /**
     * <code>MdxCubeNotFound</code> is '<code>MDX cube &#39;&#39;{0}&#39;&#39; not found</code>'
     */
    public final _Def0 MdxCubeNotFound = new _Def0("MdxCubeNotFound", "MDX cube ''{0}'' not found", null);

    /**
     * <code>MdxChildObjectNotFound</code> is '<code>MDX object &#39;&#39;{0}&#39;&#39; not found in {1}</code>'
     */
    public final _Def1 MdxChildObjectNotFound = new _Def1("MdxChildObjectNotFound", "MDX object ''{0}'' not found in {1}", null);

    /**
     * <code>MemberNotFound</code> is '<code>Member &#39;&#39;{0}&#39;&#39; not found</code>'
     */
    public final _Def0 MemberNotFound = new _Def0("MemberNotFound", "Member ''{0}'' not found", null);

    /**
     * <code>MdxCubeName</code> is '<code>cube &#39;&#39;{0}&#39;&#39;</code>'
     */
    public final _Def2 MdxCubeName = new _Def2("MdxCubeName", "cube ''{0}''", null);

    /**
     * <code>MdxHierarchyName</code> is '<code>hierarchy &#39;&#39;{0}&#39;&#39;</code>'
     */
    public final _Def2 MdxHierarchyName = new _Def2("MdxHierarchyName", "hierarchy ''{0}''", null);

    /**
     * <code>MdxDimensionName</code> is '<code>dimension &#39;&#39;{0}&#39;&#39;</code>'
     */
    public final _Def2 MdxDimensionName = new _Def2("MdxDimensionName", "dimension ''{0}''", null);

    /**
     * <code>MdxLevelName</code> is '<code>level &#39;&#39;{0}&#39;&#39;</code>'
     */
    public final _Def2 MdxLevelName = new _Def2("MdxLevelName", "level ''{0}''", null);

    /**
     * <code>MdxMemberName</code> is '<code>member &#39;&#39;{0}&#39;&#39;</code>'
     */
    public final _Def2 MdxMemberName = new _Def2("MdxMemberName", "member ''{0}''", null);

    /**
     * <code>HighCardinalityInDimension</code> is '<code>The highCardinality attribute specified on dimension &#39;&#39;{0}&#39;&#39; is deprecated and will be removed in future versions of Mondrian.The feature will produce wrong results in some scenarios and should be used with caution.</code>'
     */
    public final _Def2 HighCardinalityInDimension = new _Def2("HighCardinalityInDimension", "The highCardinality attribute specified on dimension ''{0}'' is deprecated and will be removed in future versions of Mondrian.The feature will produce wrong results in some scenarios and should be used with caution.", null);

    /**
     * <code>WhileParsingMdx</code> is '<code>Error while parsing MDX statement &#39;&#39;{0}&#39;&#39;</code>'
     */
    public final _Def0 WhileParsingMdx = new _Def0("WhileParsingMdx", "Error while parsing MDX statement ''{0}''", null);

    /**
     * <code>WhileParsingMdxExpression</code> is '<code>Syntax error in MDX expression &#39;&#39;{0}&#39;&#39;</code>'
     */
    public final _Def0 WhileParsingMdxExpression = new _Def0("WhileParsingMdxExpression", "Syntax error in MDX expression ''{0}''", null);

    /**
     * <code>MdxFatalError</code> is '<code>MDX parser cannot recover from previous error(s)</code>'
     */
    public final _Def3 MdxFatalError = new _Def3("MdxFatalError", "MDX parser cannot recover from previous error(s)", null);

    /**
     * <code>FailedToParseQuery</code> is '<code>Failed to parse query &#39;&#39;{0}&#39;&#39;</code>'
     */
    public final _Def0 FailedToParseQuery = new _Def0("FailedToParseQuery", "Failed to parse query ''{0}''", null);

    /**
     * <code>MdxError</code> is '<code>Error: {0}</code>'
     */
    public final _Def0 MdxError = new _Def0("MdxError", "Error: {0}", null);

    /**
     * <code>MdxSyntaxError</code> is '<code>Syntax error at token &#39;&#39;{0}&#39;&#39;</code>'
     */
    public final _Def0 MdxSyntaxError = new _Def0("MdxSyntaxError", "Syntax error at token ''{0}''", null);

    /**
     * <code>MdxSyntaxErrorAt</code> is '<code>Syntax error at line {1}, column {2}, token &#39;&#39;{0}&#39;&#39;</code>'
     */
    public final _Def4 MdxSyntaxErrorAt = new _Def4("MdxSyntaxErrorAt", "Syntax error at line {1}, column {2}, token ''{0}''", null);

    /**
     * <code>MdxFatalSyntaxError</code> is '<code>Couldn&#39;&#39;t repair and continue parse</code>'
     */
    public final _Def5 MdxFatalSyntaxError = new _Def5("MdxFatalSyntaxError", "Couldn''t repair and continue parse", null);

    /**
     * <code>MdxCubeSlicerMemberError</code> is '<code>Failed to add Cube Slicer with member &#39;&#39;{0}&#39;&#39; for hierarchy &#39;&#39;{1}&#39;&#39; on cube &#39;&#39;{2}&#39;&#39;</code>'
     */
    public final _Def4 MdxCubeSlicerMemberError = new _Def4("MdxCubeSlicerMemberError", "Failed to add Cube Slicer with member ''{0}'' for hierarchy ''{1}'' on cube ''{2}''", null);

    /**
     * <code>MdxCubeSlicerHierarchyError</code> is '<code>Failed to add Cube Slicer for hierarchy &#39;&#39;{0}&#39;&#39; on cube &#39;&#39;{1}&#39;&#39;</code>'
     */
    public final _Def1 MdxCubeSlicerHierarchyError = new _Def1("MdxCubeSlicerHierarchyError", "Failed to add Cube Slicer for hierarchy ''{0}'' on cube ''{1}''", null);

    /**
     * <code>MdxInvalidMember</code> is '<code>Invalid member identifier &#39;&#39;{0}&#39;&#39;</code>'
     */
    public final _Def0 MdxInvalidMember = new _Def0("MdxInvalidMember", "Invalid member identifier ''{0}''", null);

    /**
     * <code>MdxCalculatedHierarchyError</code> is '<code>Hierarchy for calculated member &#39;&#39;{0}&#39;&#39; not found</code>'
     */
    public final _Def0 MdxCalculatedHierarchyError = new _Def0("MdxCalculatedHierarchyError", "Hierarchy for calculated member ''{0}'' not found", null);

    /**
     * <code>MdxAxisIsNotSet</code> is '<code>Axis &#39;&#39;{0}&#39;&#39; expression is not a set</code>'
     */
    public final _Def0 MdxAxisIsNotSet = new _Def0("MdxAxisIsNotSet", "Axis ''{0}'' expression is not a set", null);

    /**
     * <code>MdxMemberExpIsSet</code> is '<code>Member expression &#39;&#39;{0}&#39;&#39; must not be a set</code>'
     */
    public final _Def0 MdxMemberExpIsSet = new _Def0("MdxMemberExpIsSet", "Member expression ''{0}'' must not be a set", null);

    /**
     * <code>MdxSetExpNotSet</code> is '<code>Set expression &#39;&#39;{0}&#39;&#39; must be a set</code>'
     */
    public final _Def0 MdxSetExpNotSet = new _Def0("MdxSetExpNotSet", "Set expression ''{0}'' must be a set", null);

    /**
     * <code>MdxFuncArgumentsNum</code> is '<code>Function &#39;&#39;{0}&#39;&#39; must have at least 2 arguments</code>'
     */
    public final _Def0 MdxFuncArgumentsNum = new _Def0("MdxFuncArgumentsNum", "Function ''{0}'' must have at least 2 arguments", null);

    /**
     * <code>MdxFuncNotHier</code> is '<code>Argument &#39;&#39;{0,number}&#39;&#39; of function &#39;&#39;{1}&#39;&#39; must be a hierarchy</code>'
     */
    public final _Def6 MdxFuncNotHier = new _Def6("MdxFuncNotHier", "Argument ''{0,number}'' of function ''{1}'' must be a hierarchy", null);

    /**
     * <code>UnknownParameter</code> is '<code>Unknown parameter &#39;&#39;{0}&#39;&#39;</code>'
     */
    public final _Def0 UnknownParameter = new _Def0("UnknownParameter", "Unknown parameter ''{0}''", null);

    /**
     * <code>MdxFormulaNotFound</code> is '<code>Calculated {0} &#39;&#39;{1}&#39;&#39; has not been found in query &#39;&#39;{2}&#39;&#39;</code>'
     */
    public final _Def4 MdxFormulaNotFound = new _Def4("MdxFormulaNotFound", "Calculated {0} ''{1}'' has not been found in query ''{2}''", null);

    /**
     * <code>MdxCantFindMember</code> is '<code>Cannot find MDX member &#39;&#39;{0}&#39;&#39;. Make sure it is indeed a member and not a level or a hierarchy.</code>'
     */
    public final _Def0 MdxCantFindMember = new _Def0("MdxCantFindMember", "Cannot find MDX member ''{0}''. Make sure it is indeed a member and not a level or a hierarchy.", null);

    /**
     * <code>CalculatedMember</code> is '<code>calculated member</code>'
     */
    public final _Def5 CalculatedMember = new _Def5("CalculatedMember", "calculated member", null);

    /**
     * <code>CalculatedSet</code> is '<code>calculated set</code>'
     */
    public final _Def5 CalculatedSet = new _Def5("CalculatedSet", "calculated set", null);

    /**
     * <code>MdxCalculatedFormulaUsedOnAxis</code> is '<code>Cannot delete {0} &#39;&#39;{1}&#39;&#39;. It is used on {2} axis.</code>'
     */
    public final _Def4 MdxCalculatedFormulaUsedOnAxis = new _Def4("MdxCalculatedFormulaUsedOnAxis", "Cannot delete {0} ''{1}''. It is used on {2} axis.", null);

    /**
     * <code>MdxCalculatedFormulaUsedOnSlicer</code> is '<code>Cannot delete {0} &#39;&#39;{1}&#39;&#39;. It is used on slicer.</code>'
     */
    public final _Def1 MdxCalculatedFormulaUsedOnSlicer = new _Def1("MdxCalculatedFormulaUsedOnSlicer", "Cannot delete {0} ''{1}''. It is used on slicer.", null);

    /**
     * <code>MdxCalculatedFormulaUsedInFormula</code> is '<code>Cannot delete {0} &#39;&#39;{1}&#39;&#39;. It is used in definition of {2} &#39;&#39;{3}&#39;&#39;.</code>'
     */
    public final _Def7 MdxCalculatedFormulaUsedInFormula = new _Def7("MdxCalculatedFormulaUsedInFormula", "Cannot delete {0} ''{1}''. It is used in definition of {2} ''{3}''.", null);

    /**
     * <code>MdxCalculatedFormulaUsedInQuery</code> is '<code>Cannot delete {0} &#39;&#39;{1}&#39;&#39;. It is used in query &#39;&#39;{2}&#39;&#39;.</code>'
     */
    public final _Def4 MdxCalculatedFormulaUsedInQuery = new _Def4("MdxCalculatedFormulaUsedInQuery", "Cannot delete {0} ''{1}''. It is used in query ''{2}''.", null);

    /**
     * <code>MdxAxisShowSubtotalsNotSupported</code> is '<code>Show/hide subtotals operation on axis &#39;&#39;{0,number}&#39;&#39; is not supported.</code>'
     */
    public final _Def8 MdxAxisShowSubtotalsNotSupported = new _Def8("MdxAxisShowSubtotalsNotSupported", "Show/hide subtotals operation on axis ''{0,number}'' is not supported.", null);

    /**
     * <code>NoFunctionMatchesSignature</code> is '<code>No function matches signature &#39;&#39;{0}&#39;&#39;</code>'
     */
    public final _Def0 NoFunctionMatchesSignature = new _Def0("NoFunctionMatchesSignature", "No function matches signature ''{0}''", null);

    /**
     * <code>MoreThanOneFunctionMatchesSignature</code> is '<code>More than one function matches signature &#39;&#39;{0}&#39;&#39;; they are: {1}</code>'
     */
    public final _Def1 MoreThanOneFunctionMatchesSignature = new _Def1("MoreThanOneFunctionMatchesSignature", "More than one function matches signature ''{0}''; they are: {1}", null);

    /**
     * <code>MemberNotInLevelHierarchy</code> is '<code>The member &#39;&#39;{0}&#39;&#39; is not in the same hierarchy as the level &#39;&#39;{1}&#39;&#39;.</code>'
     */
    public final _Def1 MemberNotInLevelHierarchy = new _Def1("MemberNotInLevelHierarchy", "The member ''{0}'' is not in the same hierarchy as the level ''{1}''.", null);

    /**
     * <code>ToggleDrillStateRecursiveNotSupported</code> is '<code>&#39;&#39;RECURSIVE&#39;&#39; is not supported in ToggleDrillState.</code>'
     */
    public final _Def3 ToggleDrillStateRecursiveNotSupported = new _Def3("ToggleDrillStateRecursiveNotSupported", "''RECURSIVE'' is not supported in ToggleDrillState.", null);

    /**
     * <code>CompoundSlicer</code> is '<code>WHERE clause expression returned set with more than one element.</code>'
     */
    public final _Def3 CompoundSlicer = new _Def3("CompoundSlicer", "WHERE clause expression returned set with more than one element.", null);

    /**
     * <code>FunctionMbrAndLevelHierarchyMismatch</code> is '<code>The &lt;level&gt; and &lt;member&gt; arguments to {0} must be from the same hierarchy. The level was from &#39;&#39;{1}&#39;&#39; but the member was from &#39;&#39;{2}&#39;&#39;.</code>'
     */
    public final _Def4 FunctionMbrAndLevelHierarchyMismatch = new _Def4("FunctionMbrAndLevelHierarchyMismatch", "The <level> and <member> arguments to {0} must be from the same hierarchy. The level was from ''{1}'' but the member was from ''{2}''.", null);

    /**
     * <code>CousinHierarchyMismatch</code> is '<code>The member arguments to the Cousin function must be from the same hierarchy. The members are &#39;&#39;{0}&#39;&#39; and &#39;&#39;{1}&#39;&#39;.</code>'
     */
    public final _Def1 CousinHierarchyMismatch = new _Def1("CousinHierarchyMismatch", "The member arguments to the Cousin function must be from the same hierarchy. The members are ''{0}'' and ''{1}''.", null);

    /**
     * <code>HierarchyInIndependentAxes</code> is '<code>Hierarchy &#39;&#39;{0}&#39;&#39; appears in more than one independent axis.</code>'
     */
    public final _Def0 HierarchyInIndependentAxes = new _Def0("HierarchyInIndependentAxes", "Hierarchy ''{0}'' appears in more than one independent axis.", null);

    /**
     * <code>ArgsMustHaveSameHierarchy</code> is '<code>All arguments to function &#39;&#39;{0}&#39;&#39; must have same hierarchy.</code>'
     */
    public final _Def0 ArgsMustHaveSameHierarchy = new _Def0("ArgsMustHaveSameHierarchy", "All arguments to function ''{0}'' must have same hierarchy.", null);

    /**
     * <code>TimeArgNeeded</code> is '<code>Argument to function &#39;&#39;{0}&#39;&#39; must belong to Time hierarchy.</code>'
     */
    public final _Def0 TimeArgNeeded = new _Def0("TimeArgNeeded", "Argument to function ''{0}'' must belong to Time hierarchy.", null);

    /**
     * <code>InvalidAxis</code> is '<code>Invalid axis specification. The axis number must be a non-negative integer, but it was {0,number}.</code>'
     */
    public final _Def8 InvalidAxis = new _Def8("InvalidAxis", "Invalid axis specification. The axis number must be a non-negative integer, but it was {0,number}.", null);

    /**
     * <code>DuplicateAxis</code> is '<code>Duplicate axis name &#39;&#39;{0}&#39;&#39;.</code>'
     */
    public final _Def0 DuplicateAxis = new _Def0("DuplicateAxis", "Duplicate axis name ''{0}''.", null);

    /**
     * <code>NonContiguousAxis</code> is '<code>Axis numbers specified in a query must be sequentially specified, and cannot contain gaps. Axis {0,number} ({1}) is missing.</code>'
     */
    public final _Def6 NonContiguousAxis = new _Def6("NonContiguousAxis", "Axis numbers specified in a query must be sequentially specified, and cannot contain gaps. Axis {0,number} ({1}) is missing.", null);

    /**
     * <code>DupHierarchiesInTuple</code> is '<code>Tuple contains more than one member of hierarchy &#39;&#39;{0}&#39;&#39;.</code>'
     */
    public final _Def0 DupHierarchiesInTuple = new _Def0("DupHierarchiesInTuple", "Tuple contains more than one member of hierarchy ''{0}''.", null);

    /**
     * <code>VisualTotalsAppliedToTuples</code> is '<code>Argument to &#39;&#39;VisualTotals&#39;&#39; function must be a set of members; got set of tuples.</code>'
     */
    public final _Def3 VisualTotalsAppliedToTuples = new _Def3("VisualTotalsAppliedToTuples", "Argument to ''VisualTotals'' function must be a set of members; got set of tuples.", null);

    /**
     * <code>ParameterIsNotModifiable</code> is '<code>Parameter &#39;&#39;{0}&#39;&#39; (defined at &#39;&#39;{1}&#39;&#39; scope) is not modifiable</code>'
     */
    public final _Def1 ParameterIsNotModifiable = new _Def1("ParameterIsNotModifiable", "Parameter ''{0}'' (defined at ''{1}'' scope) is not modifiable", null);

    /**
     * <code>ParameterDefinedMoreThanOnce</code> is '<code>Parameter &#39;&#39;{0}&#39;&#39; is defined more than once in this statement</code>'
     */
    public final _Def0 ParameterDefinedMoreThanOnce = new _Def0("ParameterDefinedMoreThanOnce", "Parameter ''{0}'' is defined more than once in this statement", null);

    /**
     * <code>CycleDuringParameterEvaluation</code> is '<code>Cycle occurred while evaluating parameter &#39;&#39;{0}&#39;&#39;</code>'
     */
    public final _Def0 CycleDuringParameterEvaluation = new _Def0("CycleDuringParameterEvaluation", "Cycle occurred while evaluating parameter ''{0}''", null);

    /**
     * <code>CastInvalidType</code> is '<code>Unknown type &#39;&#39;{0}&#39;&#39;; values are NUMERIC, STRING, BOOLEAN</code>'
     */
    public final _Def0 CastInvalidType = new _Def0("CastInvalidType", "Unknown type ''{0}''; values are NUMERIC, STRING, BOOLEAN", null);

    /**
     * <code>NullNotSupported</code> is '<code>Function does not support NULL member parameter</code>'
     */
    public final _Def3 NullNotSupported = new _Def3("NullNotSupported", "Function does not support NULL member parameter", null);

    /**
     * <code>TwoNullsNotSupported</code> is '<code>Function does not support two NULL member parameters</code>'
     */
    public final _Def3 TwoNullsNotSupported = new _Def3("TwoNullsNotSupported", "Function does not support two NULL member parameters", null);

    /**
     * <code>NoTimeDimensionInCube</code> is '<code>Cannot use the function &#39;&#39;{0}&#39;&#39;, no time dimension is available for this cube.</code>'
     */
    public final _Def0 NoTimeDimensionInCube = new _Def0("NoTimeDimensionInCube", "Cannot use the function ''{0}'', no time dimension is available for this cube.", null);

    /**
     * <code>CannotImplicitlyConvertDimensionToHierarchy</code> is '<code>The &#39;&#39;{0}&#39;&#39; dimension contains more than one hierarchy, therefore the hierarchy must be explicitly specified.</code>'
     */
    public final _Def0 CannotImplicitlyConvertDimensionToHierarchy = new _Def0("CannotImplicitlyConvertDimensionToHierarchy", "The ''{0}'' dimension contains more than one hierarchy, therefore the hierarchy must be explicitly specified.", null);

    /**
     * <code>HierarchyHasNoAccessibleMembers</code> is '<code>Hierarchy &#39;&#39;{0}&#39;&#39; has no accessible members.</code>'
     */
    public final _Def0 HierarchyHasNoAccessibleMembers = new _Def0("HierarchyHasNoAccessibleMembers", "Hierarchy ''{0}'' has no accessible members.", null);

    /**
     * <code>NullValue</code> is '<code>An MDX expression was expected. An empty expression was specified.</code>'
     */
    public final _Def3 NullValue = new _Def3("NullValue", "An MDX expression was expected. An empty expression was specified.", null);

    /**
     * <code>AvgRollupFailed</code> is '<code>Don&#39;&#39;t know how to rollup aggregator &#39;&#39;avg&#39;&#39; because the cube doesn&#39;&#39;t contain at least one &#39;&#39;count&#39;&#39; and one &#39;&#39;sum&#39;&#39; measures based on the same column.</code>'
     */
    public final _Def3 AvgRollupFailed = new _Def3("AvgRollupFailed", "Don''t know how to rollup aggregator ''avg'' because the cube doesn''t contain at least one ''count'' and one ''sum'' measures based on the same column.", null);

    /**
     * <code>DrillthroughDisabled</code> is '<code>Can&#39;&#39;t perform drillthrough operations because &#39;&#39;{0}&#39;&#39; is set to false.</code>'
     */
    public final _Def0 DrillthroughDisabled = new _Def0("DrillthroughDisabled", "Can''t perform drillthrough operations because ''{0}'' is set to false.", null);

    /**
     * <code>DrillthroughCalculatedMember</code> is '<code>Can&#39;&#39;t perform drillthrough operations because &#39;&#39;{0}&#39;&#39; is a calculated member.</code>'
     */
    public final _Def0 DrillthroughCalculatedMember = new _Def0("DrillthroughCalculatedMember", "Can''t perform drillthrough operations because ''{0}'' is a calculated member.", null);

    /**
     * <code>ValidMeasureUsingCalculatedMember</code> is '<code>The function ValidMeasure cannot be used with the measure &#39;&#39;{0}&#39;&#39; because it is a calculated member. The function should be used to wrap the base measure in the source cube.</code>'
     */
    public final _Def0 ValidMeasureUsingCalculatedMember = new _Def0("ValidMeasureUsingCalculatedMember", "The function ValidMeasure cannot be used with the measure ''{0}'' because it is a calculated member. The function should be used to wrap the base measure in the source cube.", null);

    /**
     * <code>UnsupportedCalculatedMember</code> is '<code>Calculated member &#39;&#39;{0}&#39;&#39; is not supported within a compound predicate</code>'
     */
    public final _Def0 UnsupportedCalculatedMember = new _Def0("UnsupportedCalculatedMember", "Calculated member ''{0}'' is not supported within a compound predicate", null);

    /**
     * <code>ConnectStringMandatoryProperties</code> is '<code>Connect string must contain property &#39;&#39;{0}&#39;&#39; or property &#39;&#39;{1}&#39;&#39;</code>'
     */
    public final _Def1 ConnectStringMandatoryProperties = new _Def1("ConnectStringMandatoryProperties", "Connect string must contain property ''{0}'' or property ''{1}''", null);

    /**
     * <code>NonTimeLevelInTimeHierarchy</code> is '<code>Level &#39;&#39;{0}&#39;&#39; belongs to a time hierarchy, so its level-type must be  &#39;&#39;Years&#39;&#39;, &#39;&#39;Quarters&#39;&#39;, &#39;&#39;Months&#39;&#39;, &#39;&#39;Weeks&#39;&#39; or &#39;&#39;Days&#39;&#39;.</code>'
     */
    public final _Def0 NonTimeLevelInTimeHierarchy = new _Def0("NonTimeLevelInTimeHierarchy", "Level ''{0}'' belongs to a time hierarchy, so its level-type must be  ''Years'', ''Quarters'', ''Months'', ''Weeks'' or ''Days''.", null);

    /**
     * <code>TimeLevelInNonTimeHierarchy</code> is '<code>Level &#39;&#39;{0}&#39;&#39; does not belong to a time hierarchy, so its level-type must be &#39;&#39;Standard&#39;&#39;.</code>'
     */
    public final _Def0 TimeLevelInNonTimeHierarchy = new _Def0("TimeLevelInNonTimeHierarchy", "Level ''{0}'' does not belong to a time hierarchy, so its level-type must be ''Standard''.", null);

    /**
     * <code>MustSpecifyPrimaryKeyForHierarchy</code> is '<code>In usage of hierarchy &#39;&#39;{0}&#39;&#39; in cube &#39;&#39;{1}&#39;&#39;, you must specify a primary key.</code>'
     */
    public final _Def1 MustSpecifyPrimaryKeyForHierarchy = new _Def1("MustSpecifyPrimaryKeyForHierarchy", "In usage of hierarchy ''{0}'' in cube ''{1}'', you must specify a primary key.", null);

    /**
     * <code>MustSpecifyPrimaryKeyTableForHierarchy</code> is '<code>Must specify a primary key table for hierarchy &#39;&#39;{0}&#39;&#39;, because it has more than one table.</code>'
     */
    public final _Def0 MustSpecifyPrimaryKeyTableForHierarchy = new _Def0("MustSpecifyPrimaryKeyTableForHierarchy", "Must specify a primary key table for hierarchy ''{0}'', because it has more than one table.", null);

    /**
     * <code>MustSpecifyForeignKeyForHierarchy</code> is '<code>In usage of hierarchy &#39;&#39;{0}&#39;&#39; in cube &#39;&#39;{1}&#39;&#39;, you must specify a foreign key, because the hierarchy table is different from the fact table.</code>'
     */
    public final _Def1 MustSpecifyForeignKeyForHierarchy = new _Def1("MustSpecifyForeignKeyForHierarchy", "In usage of hierarchy ''{0}'' in cube ''{1}'', you must specify a foreign key, because the hierarchy table is different from the fact table.", null);

    /**
     * <code>LevelMustHaveNameExpression</code> is '<code>Level &#39;&#39;{0}&#39;&#39; must have a name expression (a &#39;&#39;column&#39;&#39; attribute or an &lt;Expression&gt; child</code>'
     */
    public final _Def0 LevelMustHaveNameExpression = new _Def0("LevelMustHaveNameExpression", "Level ''{0}'' must have a name expression (a ''column'' attribute or an <Expression> child", null);

    /**
     * <code>PublicDimensionMustNotHaveForeignKey</code> is '<code>Dimension &#39;&#39;{0}&#39;&#39; has a foreign key. This attribute is only valid in private dimensions and dimension usages.</code>'
     */
    public final _Def0 PublicDimensionMustNotHaveForeignKey = new _Def0("PublicDimensionMustNotHaveForeignKey", "Dimension ''{0}'' has a foreign key. This attribute is only valid in private dimensions and dimension usages.", null);

    /**
     * <code>HierarchyMustNotHaveMoreThanOneSource</code> is '<code>Hierarchy &#39;&#39;{0}&#39;&#39; has more than one source (memberReaderClass, &lt;Table&gt;, &lt;Join&gt; or &lt;View&gt;)</code>'
     */
    public final _Def0 HierarchyMustNotHaveMoreThanOneSource = new _Def0("HierarchyMustNotHaveMoreThanOneSource", "Hierarchy ''{0}'' has more than one source (memberReaderClass, <Table>, <Join> or <View>)", null);

    /**
     * <code>DimensionUsageHasUnknownLevel</code> is '<code>In usage of dimension &#39;&#39;{0}&#39;&#39; in cube &#39;&#39;{1}&#39;&#39;, the level &#39;&#39;{2}&#39;&#39; is unknown</code>'
     */
    public final _Def4 DimensionUsageHasUnknownLevel = new _Def4("DimensionUsageHasUnknownLevel", "In usage of dimension ''{0}'' in cube ''{1}'', the level ''{2}'' is unknown", null);

    /**
     * <code>CalcMemberHasBadDimension</code> is '<code>Unknown dimension &#39;&#39;{0}&#39;&#39; for calculated member &#39;&#39;{1}&#39;&#39; in cube &#39;&#39;{2}&#39;&#39;</code>'
     */
    public final _Def4 CalcMemberHasBadDimension = new _Def4("CalcMemberHasBadDimension", "Unknown dimension ''{0}'' for calculated member ''{1}'' in cube ''{2}''", null);

    /**
     * <code>CalcMemberHasBothDimensionAndHierarchy</code> is '<code>Cannot specify both a dimension and hierarchy for calculated member &#39;&#39;{0}&#39;&#39; in cube &#39;&#39;{1}&#39;&#39;</code>'
     */
    public final _Def1 CalcMemberHasBothDimensionAndHierarchy = new _Def1("CalcMemberHasBothDimensionAndHierarchy", "Cannot specify both a dimension and hierarchy for calculated member ''{0}'' in cube ''{1}''", null);

    /**
     * <code>CalcMemberHasUnknownParent</code> is '<code>Cannot find a parent with name &#39;&#39;{0}&#39;&#39; for calculated member &#39;&#39;{1}&#39;&#39; in cube &#39;&#39;{2}&#39;&#39;</code>'
     */
    public final _Def4 CalcMemberHasUnknownParent = new _Def4("CalcMemberHasUnknownParent", "Cannot find a parent with name ''{0}'' for calculated member ''{1}'' in cube ''{2}''", null);

    /**
     * <code>CalcMemberHasDifferentParentAndHierarchy</code> is '<code>The calculated member &#39;&#39;{0}&#39;&#39; in cube &#39;&#39;{1}&#39;&#39; is defined for hierarchy &#39;&#39;{2}&#39;&#39; but its parent member is not part of that hierarchy</code>'
     */
    public final _Def4 CalcMemberHasDifferentParentAndHierarchy = new _Def4("CalcMemberHasDifferentParentAndHierarchy", "The calculated member ''{0}'' in cube ''{1}'' is defined for hierarchy ''{2}'' but its parent member is not part of that hierarchy", null);

    /**
     * <code>CalcMemberNotUnique</code> is '<code>Calculated member &#39;&#39;{0}&#39;&#39; already exists in cube &#39;&#39;{1}&#39;&#39;</code>'
     */
    public final _Def1 CalcMemberNotUnique = new _Def1("CalcMemberNotUnique", "Calculated member ''{0}'' already exists in cube ''{1}''", null);

    /**
     * <code>NeitherExprNorValueForCalcMemberProperty</code> is '<code>Member property must have a value or an expression. (Property &#39;&#39;{0}&#39;&#39; of member &#39;&#39;{1}&#39;&#39; of cube &#39;&#39;{2}&#39;&#39;.)</code>'
     */
    public final _Def4 NeitherExprNorValueForCalcMemberProperty = new _Def4("NeitherExprNorValueForCalcMemberProperty", "Member property must have a value or an expression. (Property ''{0}'' of member ''{1}'' of cube ''{2}''.)", null);

    /**
     * <code>ExprAndValueForMemberProperty</code> is '<code>Member property must not have both a value and an expression. (Property &#39;&#39;{0}&#39;&#39; of member &#39;&#39;{1}&#39;&#39; of cube &#39;&#39;{2}&#39;&#39;.)</code>'
     */
    public final _Def4 ExprAndValueForMemberProperty = new _Def4("ExprAndValueForMemberProperty", "Member property must not have both a value and an expression. (Property ''{0}'' of member ''{1}'' of cube ''{2}''.)", null);

    /**
     * <code>MemberFormatterLoadFailed</code> is '<code>Failed to load formatter class &#39;&#39;{0}&#39;&#39; for level &#39;&#39;{1}&#39;&#39;.</code>'
     */
    public final _Def1 MemberFormatterLoadFailed = new _Def1("MemberFormatterLoadFailed", "Failed to load formatter class ''{0}'' for level ''{1}''.", null);

    /**
     * <code>CellFormatterLoadFailed</code> is '<code>Failed to load formatter class &#39;&#39;{0}&#39;&#39; for member &#39;&#39;{1}&#39;&#39;.</code>'
     */
    public final _Def1 CellFormatterLoadFailed = new _Def1("CellFormatterLoadFailed", "Failed to load formatter class ''{0}'' for member ''{1}''.", null);

    /**
     * <code>PropertyFormatterLoadFailed</code> is '<code>Failed to load formatter class &#39;&#39;{0}&#39;&#39; for property &#39;&#39;{1}&#39;&#39;.</code>'
     */
    public final _Def1 PropertyFormatterLoadFailed = new _Def1("PropertyFormatterLoadFailed", "Failed to load formatter class ''{0}'' for property ''{1}''.", null);

    /**
     * <code>HierarchyMustHaveForeignKey</code> is '<code>Hierarchy &#39;&#39;{0}&#39;&#39; in cube &#39;&#39;{1}&#39;&#39; must have a foreign key, since it is not based on the cube&#39;&#39;s fact table.</code>'
     */
    public final _Def1 HierarchyMustHaveForeignKey = new _Def1("HierarchyMustHaveForeignKey", "Hierarchy ''{0}'' in cube ''{1}'' must have a foreign key, since it is not based on the cube''s fact table.", null);

    /**
     * <code>HierarchyInvalidForeignKey</code> is '<code>Foreign key &#39;&#39;{0}&#39;&#39; of hierarchy &#39;&#39;{1}&#39;&#39; in cube &#39;&#39;{2}&#39;&#39; is not a column in the fact table.</code>'
     */
    public final _Def4 HierarchyInvalidForeignKey = new _Def4("HierarchyInvalidForeignKey", "Foreign key ''{0}'' of hierarchy ''{1}'' in cube ''{2}'' is not a column in the fact table.", null);

    /**
     * <code>UdfClassNotFound</code> is '<code>Failed to load user-defined function &#39;&#39;{0}&#39;&#39;: class &#39;&#39;{1}&#39;&#39; not found</code>'
     */
    public final _Def1 UdfClassNotFound = new _Def1("UdfClassNotFound", "Failed to load user-defined function ''{0}'': class ''{1}'' not found", null);

    /**
     * <code>UdfClassMustBePublicAndStatic</code> is '<code>Failed to load user-defined function &#39;&#39;{0}&#39;&#39;: class &#39;&#39;{1}&#39;&#39; must be public and static</code>'
     */
    public final _Def1 UdfClassMustBePublicAndStatic = new _Def1("UdfClassMustBePublicAndStatic", "Failed to load user-defined function ''{0}'': class ''{1}'' must be public and static", null);

    /**
     * <code>UdfClassWrongIface</code> is '<code>Failed to load user-defined function &#39;&#39;{0}&#39;&#39;: class &#39;&#39;{1}&#39;&#39; does not implement the required interface &#39;&#39;{2}&#39;&#39;</code>'
     */
    public final _Def4 UdfClassWrongIface = new _Def4("UdfClassWrongIface", "Failed to load user-defined function ''{0}'': class ''{1}'' does not implement the required interface ''{2}''", null);

    /**
     * <code>UdfDuplicateName</code> is '<code>Duplicate user-defined function &#39;&#39;{0}&#39;&#39;</code>'
     */
    public final _Def0 UdfDuplicateName = new _Def0("UdfDuplicateName", "Duplicate user-defined function ''{0}''", null);

    /**
     * <code>NamedSetNotUnique</code> is '<code>Named set &#39;&#39;{0}&#39;&#39; already exists in cube &#39;&#39;{1}&#39;&#39;</code>'
     */
    public final _Def1 NamedSetNotUnique = new _Def1("NamedSetNotUnique", "Named set ''{0}'' already exists in cube ''{1}''", null);

    /**
     * <code>UnknownNamedSetHasBadFormula</code> is '<code>Named set in cube &#39;&#39;{0}&#39;&#39; has bad formula</code>'
     */
    public final _Def0 UnknownNamedSetHasBadFormula = new _Def0("UnknownNamedSetHasBadFormula", "Named set in cube ''{0}'' has bad formula", null);

    /**
     * <code>NamedSetHasBadFormula</code> is '<code>Named set &#39;&#39;{0}&#39;&#39; has bad formula</code>'
     */
    public final _Def0 NamedSetHasBadFormula = new _Def0("NamedSetHasBadFormula", "Named set ''{0}'' has bad formula", null);

    /**
     * <code>MeasureOrdinalsNotUnique</code> is '<code>Cube &#39;&#39;{0}&#39;&#39;: Ordinal {1} is not unique: &#39;&#39;{2}&#39;&#39; and &#39;&#39;{3}&#39;&#39;</code>'
     */
    public final _Def7 MeasureOrdinalsNotUnique = new _Def7("MeasureOrdinalsNotUnique", "Cube ''{0}'': Ordinal {1} is not unique: ''{2}'' and ''{3}''", null);

    /**
     * <code>BadMeasureSource</code> is '<code>Cube &#39;&#39;{0}&#39;&#39;: Measure &#39;&#39;{1}&#39;&#39; must contain either a source column or a source expression, but not both</code>'
     */
    public final _Def1 BadMeasureSource = new _Def1("BadMeasureSource", "Cube ''{0}'': Measure ''{1}'' must contain either a source column or a source expression, but not both", null);

    /**
     * <code>DuplicateSchemaParameter</code> is '<code>Duplicate parameter &#39;&#39;{0}&#39;&#39; in schema</code>'
     */
    public final _Def0 DuplicateSchemaParameter = new _Def0("DuplicateSchemaParameter", "Duplicate parameter ''{0}'' in schema", null);

    /**
     * <code>UnknownAggregator</code> is '<code>Unknown aggregator &#39;&#39;{0}&#39;&#39;; valid aggregators are: {1}</code>'
     */
    public final _Def1 UnknownAggregator = new _Def1("UnknownAggregator", "Unknown aggregator ''{0}''; valid aggregators are: {1}", null);

    /**
     * <code>RoleUnionGrants</code> is '<code>Union role must not contain grants</code>'
     */
    public final _Def3 RoleUnionGrants = new _Def3("RoleUnionGrants", "Union role must not contain grants", null);

    /**
     * <code>UnknownRole</code> is '<code>Unknown role &#39;&#39;{0}&#39;&#39;</code>'
     */
    public final _Def0 UnknownRole = new _Def0("UnknownRole", "Unknown role ''{0}''", null);

    /**
     * <code>DescendantsAppliedToSetOfTuples</code> is '<code>Argument to Descendants function must be a member or set of members, not a set of tuples</code>'
     */
    public final _Def3 DescendantsAppliedToSetOfTuples = new _Def3("DescendantsAppliedToSetOfTuples", "Argument to Descendants function must be a member or set of members, not a set of tuples", null);

    /**
     * <code>CannotDeduceTypeOfSet</code> is '<code>Cannot deduce type of set</code>'
     */
    public final _Def3 CannotDeduceTypeOfSet = new _Def3("CannotDeduceTypeOfSet", "Cannot deduce type of set", null);

    /**
     * <code>NotANamedSet</code> is '<code>Not a named set</code>'
     */
    public final _Def3 NotANamedSet = new _Def3("NotANamedSet", "Not a named set", null);

    /**
     * <code>HierarchyHasNoLevels</code> is '<code>Hierarchy &#39;&#39;{0}&#39;&#39; must have at least one level.</code>'
     */
    public final _Def0 HierarchyHasNoLevels = new _Def0("HierarchyHasNoLevels", "Hierarchy ''{0}'' must have at least one level.", null);

    /**
     * <code>HierarchyLevelNamesNotUnique</code> is '<code>Level names within hierarchy &#39;&#39;{0}&#39;&#39; are not unique; there is more than one level with name &#39;&#39;{1}&#39;&#39;.</code>'
     */
    public final _Def1 HierarchyLevelNamesNotUnique = new _Def1("HierarchyLevelNamesNotUnique", "Level names within hierarchy ''{0}'' are not unique; there is more than one level with name ''{1}''.", null);

    /**
     * <code>IllegalLeftDeepJoin</code> is '<code>Left side of join must not be a join; mondrian only supports right-deep joins.</code>'
     */
    public final _Def3 IllegalLeftDeepJoin = new _Def3("IllegalLeftDeepJoin", "Left side of join must not be a join; mondrian only supports right-deep joins.", null);

    /**
     * <code>LevelTableParentNotFound</code> is '<code>The level {0} makes use of the &#39;&#39;parentColumn&#39;&#39; attribute, but a parent member for key {1} is missing. This can be due to the usage of the NativizeSet MDX function with a list of members form a parent-child hierarchy that doesn&#39;&#39;t include all parent members in its definition. Using NativizeSet with a parent-child hierarchy requires the parent members to be included in the set, or the hierarchy cannot be properly built natively.</code>'
     */
    public final _Def9 LevelTableParentNotFound = new _Def9("LevelTableParentNotFound", "The level {0} makes use of the ''parentColumn'' attribute, but a parent member for key {1} is missing. This can be due to the usage of the NativizeSet MDX function with a list of members form a parent-child hierarchy that doesn''t include all parent members in its definition. Using NativizeSet with a parent-child hierarchy requires the parent members to be included in the set, or the hierarchy cannot be properly built natively.", null);

    /**
     * <code>CreateTableFailed</code> is '<code>Mondrian loader could not create table &#39;&#39;{0}&#39;&#39;.</code>'
     */
    public final _Def0 CreateTableFailed = new _Def0("CreateTableFailed", "Mondrian loader could not create table ''{0}''.", null);

    /**
     * <code>CreateIndexFailed</code> is '<code>Mondrian loader could not create index &#39;&#39;{0}&#39;&#39; on table &#39;&#39;{1}&#39;&#39;.</code>'
     */
    public final _Def1 CreateIndexFailed = new _Def1("CreateIndexFailed", "Mondrian loader could not create index ''{0}'' on table ''{1}''.", null);

    /**
     * <code>MissingArg</code> is '<code>Argument &#39;&#39;{0}&#39;&#39; must be specified.</code>'
     */
    public final _Def0 MissingArg = new _Def0("MissingArg", "Argument ''{0}'' must be specified.", null);

    /**
     * <code>InvalidInsertLine</code> is '<code>Input line is not a valid INSERT statement; line {0,number}: {1}.</code>'
     */
    public final _Def6 InvalidInsertLine = new _Def6("InvalidInsertLine", "Input line is not a valid INSERT statement; line {0,number}: {1}.", null);

    /**
     * <code>LimitExceededDuringCrossjoin</code> is '<code>Size of CrossJoin result ({0,number}) exceeded limit ({1,number})</code>'
     */
    public final _Def10 LimitExceededDuringCrossjoin = new _Def10("LimitExceededDuringCrossjoin", "Size of CrossJoin result ({0,number}) exceeded limit ({1,number})", null);

    /**
     * <code>TotalMembersLimitExceeded</code> is '<code>Total number of Members in result ({0,number}) exceeded limit ({1,number})</code>'
     */
    public final _Def10 TotalMembersLimitExceeded = new _Def10("TotalMembersLimitExceeded", "Total number of Members in result ({0,number}) exceeded limit ({1,number})", null);

    /**
     * <code>MemberFetchLimitExceeded</code> is '<code>Number of members to be read exceeded limit ({0,number})</code>'
     */
    public final _Def11 MemberFetchLimitExceeded = new _Def11("MemberFetchLimitExceeded", "Number of members to be read exceeded limit ({0,number})", null);

    /**
     * <code>SegmentFetchLimitExceeded</code> is '<code>Number of cell results to be read exceeded limit of ({0,number})</code>'
     */
    public final _Def11 SegmentFetchLimitExceeded = new _Def11("SegmentFetchLimitExceeded", "Number of cell results to be read exceeded limit of ({0,number})", null);

    /**
     * <code>QueryCanceled</code> is '<code>Query canceled</code>'
     */
    public final _Def12 QueryCanceled = new _Def12("QueryCanceled", "Query canceled", null);

    /**
     * <code>QueryTimeout</code> is '<code>Query timeout of {0,number} seconds reached</code>'
     */
    public final _Def13 QueryTimeout = new _Def13("QueryTimeout", "Query timeout of {0,number} seconds reached", null);

    /**
     * <code>IterationLimitExceeded</code> is '<code>Number of iterations exceeded limit of {0,number}</code>'
     */
    public final _Def11 IterationLimitExceeded = new _Def11("IterationLimitExceeded", "Number of iterations exceeded limit of {0,number}", null);

    /**
     * <code>InvalidHierarchyCondition</code> is '<code>Hierarchy &#39;&#39;{0}&#39;&#39; is invalid (has no members)</code>'
     */
    public final _Def14 InvalidHierarchyCondition = new _Def14("InvalidHierarchyCondition", "Hierarchy ''{0}'' is invalid (has no members)", null);

    /**
     * <code>TooManyMessageRecorderErrors</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: Exceeded number of allowed errors &#39;&#39;{1,number}&#39;&#39;</code>'
     */
    public final _Def15 TooManyMessageRecorderErrors = new _Def15("TooManyMessageRecorderErrors", "Context ''{0}'': Exceeded number of allowed errors ''{1,number}''", null);

    /**
     * <code>ForceMessageRecorderError</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: Client forcing return with errors &#39;&#39;{1,number}&#39;&#39;</code>'
     */
    public final _Def15 ForceMessageRecorderError = new _Def15("ForceMessageRecorderError", "Context ''{0}'': Client forcing return with errors ''{1,number}''", null);

    /**
     * <code>UnknownLevelName</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: The Hierarchy Level &#39;&#39;{1}&#39;&#39; does not have a Level named &#39;&#39;{2}&#39;&#39;</code>'
     */
    public final _Def16 UnknownLevelName = new _Def16("UnknownLevelName", "Context ''{0}'': The Hierarchy Level ''{1}'' does not have a Level named ''{2}''", null);

    /**
     * <code>DuplicateLevelNames</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: Two levels share the same name &#39;&#39;{1}&#39;&#39;</code>'
     */
    public final _Def9 DuplicateLevelNames = new _Def9("DuplicateLevelNames", "Context ''{0}'': Two levels share the same name ''{1}''", null);

    /**
     * <code>DuplicateLevelColumnNames</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: Two levels, &#39;&#39;{1}&#39;&#39; and &#39;&#39;{2}&#39;&#39;,  share the same foreign column name &#39;&#39;{3}&#39;&#39;</code>'
     */
    public final _Def17 DuplicateLevelColumnNames = new _Def17("DuplicateLevelColumnNames", "Context ''{0}'': Two levels, ''{1}'' and ''{2}'',  share the same foreign column name ''{3}''", null);

    /**
     * <code>DuplicateMeasureColumnNames</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: Two measures, &#39;&#39;{1}&#39;&#39; and &#39;&#39;{2}&#39;&#39;,  share the same column name &#39;&#39;{3}&#39;&#39;</code>'
     */
    public final _Def17 DuplicateMeasureColumnNames = new _Def17("DuplicateMeasureColumnNames", "Context ''{0}'': Two measures, ''{1}'' and ''{2}'',  share the same column name ''{3}''", null);

    /**
     * <code>DuplicateLevelMeasureColumnNames</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: The level &#39;&#39;{1}&#39;&#39; and the measuer &#39;&#39;{2}&#39;&#39;,  share the same column name &#39;&#39;{3}&#39;&#39;</code>'
     */
    public final _Def17 DuplicateLevelMeasureColumnNames = new _Def17("DuplicateLevelMeasureColumnNames", "Context ''{0}'': The level ''{1}'' and the measuer ''{2}'',  share the same column name ''{3}''", null);

    /**
     * <code>DuplicateMeasureNames</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: Two measures share the same name &#39;&#39;{1}&#39;&#39;</code>'
     */
    public final _Def9 DuplicateMeasureNames = new _Def9("DuplicateMeasureNames", "Context ''{0}'': Two measures share the same name ''{1}''", null);

    /**
     * <code>DuplicateFactForeignKey</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: Duplicate fact foreign keys &#39;&#39;{1}&#39;&#39; for key &#39;&#39;{2}&#39;&#39;.</code>'
     */
    public final _Def16 DuplicateFactForeignKey = new _Def16("DuplicateFactForeignKey", "Context ''{0}'': Duplicate fact foreign keys ''{1}'' for key ''{2}''.", null);

    /**
     * <code>UnknownLeftJoinCondition</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: Failed to find left join condition in fact table &#39;&#39;{1}&#39;&#39; for foreign key &#39;&#39;{2}&#39;&#39;.</code>'
     */
    public final _Def16 UnknownLeftJoinCondition = new _Def16("UnknownLeftJoinCondition", "Context ''{0}'': Failed to find left join condition in fact table ''{1}'' for foreign key ''{2}''.", null);

    /**
     * <code>UnknownHierarchyName</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: The Hierarchy &#39;&#39;{1}&#39;&#39; does not exist&quot;</code>'
     */
    public final _Def9 UnknownHierarchyName = new _Def9("UnknownHierarchyName", "Context ''{0}'': The Hierarchy ''{1}'' does not exist\"", null);

    /**
     * <code>BadLevelNameFormat</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: The Level name &#39;&#39;{1}&#39;&#39; should be [usage hierarchy name].[level name].</code>'
     */
    public final _Def9 BadLevelNameFormat = new _Def9("BadLevelNameFormat", "Context ''{0}'': The Level name ''{1}'' should be [usage hierarchy name].[level name].", null);

    /**
     * <code>BadMeasureNameFormat</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: The Measures name &#39;&#39;{1}&#39;&#39; should be [Measures].[measure name].</code>'
     */
    public final _Def9 BadMeasureNameFormat = new _Def9("BadMeasureNameFormat", "Context ''{0}'': The Measures name ''{1}'' should be [Measures].[measure name].", null);

    /**
     * <code>BadMeasures</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: This name &#39;&#39;{1}&#39;&#39; must be the string &quot;Measures&quot;.</code>'
     */
    public final _Def9 BadMeasures = new _Def9("BadMeasures", "Context ''{0}'': This name ''{1}'' must be the string \"Measures\".", null);

    /**
     * <code>UnknownMeasureName</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: Measures does not have a measure named &#39;&#39;{1}&#39;&#39;</code>'
     */
    public final _Def9 UnknownMeasureName = new _Def9("UnknownMeasureName", "Context ''{0}'': Measures does not have a measure named ''{1}''", null);

    /**
     * <code>NullAttributeString</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: The value for the attribute &#39;&#39;{1}&#39;&#39; is null.</code>'
     */
    public final _Def9 NullAttributeString = new _Def9("NullAttributeString", "Context ''{0}'': The value for the attribute ''{1}'' is null.", null);

    /**
     * <code>EmptyAttributeString</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: The value for the attribute &#39;&#39;{1}&#39;&#39; is empty (length is zero).</code>'
     */
    public final _Def9 EmptyAttributeString = new _Def9("EmptyAttributeString", "Context ''{0}'': The value for the attribute ''{1}'' is empty (length is zero).", null);

    /**
     * <code>MissingDefaultAggRule</code> is '<code>There is no default aggregate recognition rule with tag &#39;&#39;{0}&#39;&#39;.</code>'
     */
    public final _Def0 MissingDefaultAggRule = new _Def0("MissingDefaultAggRule", "There is no default aggregate recognition rule with tag ''{0}''.", null);

    /**
     * <code>AggRuleParse</code> is '<code>Error while parsing default aggregate recognition &#39;&#39;{0}&#39;&#39;.</code>'
     */
    public final _Def0 AggRuleParse = new _Def0("AggRuleParse", "Error while parsing default aggregate recognition ''{0}''.", null);

    /**
     * <code>BadMeasureName</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: Failed to find Measure name &#39;&#39;{1}&#39;&#39; for cube &#39;&#39;{2}&#39;&#39;.</code>'
     */
    public final _Def16 BadMeasureName = new _Def16("BadMeasureName", "Context ''{0}'': Failed to find Measure name ''{1}'' for cube ''{2}''.", null);

    /**
     * <code>BadRolapStarLeftJoinCondition</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: Bad RolapStar left join condition type: &#39;&#39;{1}&#39;&#39; &#39;&#39;{2}&#39;&#39;.</code>'
     */
    public final _Def16 BadRolapStarLeftJoinCondition = new _Def16("BadRolapStarLeftJoinCondition", "Context ''{0}'': Bad RolapStar left join condition type: ''{1}'' ''{2}''.", null);

    /**
     * <code>SqlQueryFailed</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: Sql query failed to run &#39;&#39;{1}&#39;&#39;.</code>'
     */
    public final _Def9 SqlQueryFailed = new _Def9("SqlQueryFailed", "Context ''{0}'': Sql query failed to run ''{1}''.", null);

    /**
     * <code>AggLoadingError</code> is '<code>Error while loading/reloading aggregates.</code>'
     */
    public final _Def3 AggLoadingError = new _Def3("AggLoadingError", "Error while loading/reloading aggregates.", null);

    /**
     * <code>AggLoadingExceededErrorCount</code> is '<code>Too many errors, &#39;&#39;{0,number}&#39;&#39;, while loading/reloading aggregates.</code>'
     */
    public final _Def8 AggLoadingExceededErrorCount = new _Def8("AggLoadingExceededErrorCount", "Too many errors, ''{0,number}'', while loading/reloading aggregates.", null);

    /**
     * <code>UnknownFactTableColumn</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: For Fact table &#39;&#39;{1}&#39;&#39;, the column &#39;&#39;{2}&#39;&#39; is neither a measure or foreign key&quot;.</code>'
     */
    public final _Def16 UnknownFactTableColumn = new _Def16("UnknownFactTableColumn", "Context ''{0}'': For Fact table ''{1}'', the column ''{2}'' is neither a measure or foreign key\".", null);

    /**
     * <code>AggMultipleMatchingMeasure</code> is '<code>Context &#39;&#39;{0}&#39;&#39;: Candidate aggregate table &#39;&#39;{1}&#39;&#39; for fact table &#39;&#39;{2}&#39;&#39; has &#39;&#39;{3,number}&#39;&#39; columns matching measure &#39;&#39;{4}&#39;&#39;, &#39;&#39;{5}&#39;&#39;, &#39;&#39;{6}&#39;&#39;&quot;.</code>'
     */
    public final _Def18 AggMultipleMatchingMeasure = new _Def18("AggMultipleMatchingMeasure", "Context ''{0}'': Candidate aggregate table ''{1}'' for fact table ''{2}'' has ''{3,number}'' columns matching measure ''{4}'', ''{5}'', ''{6}''\".", null);

    /**
     * <code>CouldNotLoadDefaultAggregateRules</code> is '<code>Could not load default aggregate rules &#39;&#39;{0}&#39;&#39;.</code>'
     */
    public final _Def2 CouldNotLoadDefaultAggregateRules = new _Def2("CouldNotLoadDefaultAggregateRules", "Could not load default aggregate rules ''{0}''.", null);

    /**
     * <code>FailedCreateNewDefaultAggregateRules</code> is '<code>Failed to create new default aggregate rules using property &#39;&#39;{0}&#39;&#39; with value &#39;&#39;{1}&#39;&#39;.</code>'
     */
    public final _Def9 FailedCreateNewDefaultAggregateRules = new _Def9("FailedCreateNewDefaultAggregateRules", "Failed to create new default aggregate rules using property ''{0}'' with value ''{1}''.", null);

    /**
     * <code>CubeRelationNotTable</code> is '<code>The Cube &#39;&#39;{0}&#39;&#39; relation is not a MondrianDef.Table but rather &#39;&#39;{1}&#39;&#39;.</code>'
     */
    public final _Def9 CubeRelationNotTable = new _Def9("CubeRelationNotTable", "The Cube ''{0}'' relation is not a MondrianDef.Table but rather ''{1}''.", null);

    /**
     * <code>AttemptToChangeTableUsage</code> is '<code>JdbcSchema.Table &#39;&#39;{0}&#39;&#39; already set to usage &#39;&#39;{1}&#39;&#39; and can not be reset to usage &#39;&#39;{2}&#39;&#39;.</code>'
     */
    public final _Def4 AttemptToChangeTableUsage = new _Def4("AttemptToChangeTableUsage", "JdbcSchema.Table ''{0}'' already set to usage ''{1}'' and can not be reset to usage ''{2}''.", null);

    /**
     * <code>BadJdbcFactoryClassName</code> is '<code>JdbcSchema Factory classname &#39;&#39;{0}&#39;&#39;, class not found.</code>'
     */
    public final _Def0 BadJdbcFactoryClassName = new _Def0("BadJdbcFactoryClassName", "JdbcSchema Factory classname ''{0}'', class not found.", null);

    /**
     * <code>BadJdbcFactoryInstantiation</code> is '<code>JdbcSchema Factory classname &#39;&#39;{0}&#39;&#39;, can not instantiate.</code>'
     */
    public final _Def0 BadJdbcFactoryInstantiation = new _Def0("BadJdbcFactoryInstantiation", "JdbcSchema Factory classname ''{0}'', can not instantiate.", null);

    /**
     * <code>BadJdbcFactoryAccess</code> is '<code>JdbcSchema Factory classname &#39;&#39;{0}&#39;&#39;, illegal access.</code>'
     */
    public final _Def0 BadJdbcFactoryAccess = new _Def0("BadJdbcFactoryAccess", "JdbcSchema Factory classname ''{0}'', illegal access.", null);

    /**
     * <code>NonNumericFactCountColumn</code> is '<code>Candidate aggregate table &#39;&#39;{0}&#39;&#39; for fact table &#39;&#39;{1}&#39;&#39; has candidate fact count column &#39;&#39;{2}&#39;&#39; has type &#39;&#39;{3}&#39;&#39; that is not numeric.</code>'
     */
    public final _Def17 NonNumericFactCountColumn = new _Def17("NonNumericFactCountColumn", "Candidate aggregate table ''{0}'' for fact table ''{1}'' has candidate fact count column ''{2}'' has type ''{3}'' that is not numeric.", null);

    /**
     * <code>TooManyFactCountColumns</code> is '<code>Candidate aggregate table &#39;&#39;{0}&#39;&#39; for fact table &#39;&#39;{1}&#39;&#39; has &#39;&#39;{2,number}&#39;&#39; fact count columns.</code>'
     */
    public final _Def19 TooManyFactCountColumns = new _Def19("TooManyFactCountColumns", "Candidate aggregate table ''{0}'' for fact table ''{1}'' has ''{2,number}'' fact count columns.", null);

    /**
     * <code>NoFactCountColumns</code> is '<code>Candidate aggregate table &#39;&#39;{0}&#39;&#39; for fact table &#39;&#39;{1}&#39;&#39; has no fact count columns.</code>'
     */
    public final _Def9 NoFactCountColumns = new _Def9("NoFactCountColumns", "Candidate aggregate table ''{0}'' for fact table ''{1}'' has no fact count columns.", null);

    /**
     * <code>NoMeasureColumns</code> is '<code>Candidate aggregate table &#39;&#39;{0}&#39;&#39; for fact table &#39;&#39;{1}&#39;&#39; has no measure columns.</code>'
     */
    public final _Def9 NoMeasureColumns = new _Def9("NoMeasureColumns", "Candidate aggregate table ''{0}'' for fact table ''{1}'' has no measure columns.", null);

    /**
     * <code>TooManyMatchingForeignKeyColumns</code> is '<code>Candidate aggregate table &#39;&#39;{0}&#39;&#39; for fact table &#39;&#39;{1}&#39;&#39; had &#39;&#39;{2,number}&#39;&#39; columns matching foreign key &#39;&#39;{3}&#39;&#39;</code>'
     */
    public final _Def20 TooManyMatchingForeignKeyColumns = new _Def20("TooManyMatchingForeignKeyColumns", "Candidate aggregate table ''{0}'' for fact table ''{1}'' had ''{2,number}'' columns matching foreign key ''{3}''", null);

    /**
     * <code>DoubleMatchForLevel</code> is '<code>Double Match for candidate aggregate table &#39;&#39;{0}&#39;&#39; for fact table &#39;&#39;{1}&#39;&#39; and column &#39;&#39;{2}&#39;&#39; matched two hierarchies: 1) table=&#39;&#39;{3}&#39;&#39;, column=&#39;&#39;{4}&#39;&#39; and 2) table=&#39;&#39;{5}&#39;&#39;, column=&#39;&#39;{6}&#39;&#39;</code>'
     */
    public final _Def21 DoubleMatchForLevel = new _Def21("DoubleMatchForLevel", "Double Match for candidate aggregate table ''{0}'' for fact table ''{1}'' and column ''{2}'' matched two hierarchies: 1) table=''{3}'', column=''{4}'' and 2) table=''{5}'', column=''{6}''", null);

    /**
     * <code>AggUnknownColumn</code> is '<code>Candidate aggregate table &#39;&#39;{0}&#39;&#39; for fact table &#39;&#39;{1}&#39;&#39; has a column &#39;&#39;{2}&#39;&#39; with unknown usage.</code>'
     */
    public final _Def16 AggUnknownColumn = new _Def16("AggUnknownColumn", "Candidate aggregate table ''{0}'' for fact table ''{1}'' has a column ''{2}'' with unknown usage.", null);

    /**
     * <code>NoAggregatorFound</code> is '<code>No aggregator found while converting fact table aggregator: for usage
     * &#39;&#39;{0}&#39;&#39;, fact aggregator &#39;&#39;{1}&#39;&#39; and sibling aggregator &#39;&#39;{2}&#39;&#39;</code>'
     */
    public final _Def16 NoAggregatorFound = new _Def16("NoAggregatorFound", "No aggregator found while converting fact table aggregator: for usage\n        ''{0}'', fact aggregator ''{1}'' and sibling aggregator ''{2}''", null);

    /**
     * <code>NoColumnNameFromExpression</code> is '<code>Could not get a column name from a level key expression: &#39;&#39;{0}&#39;&#39;.</code>'
     */
    public final _Def2 NoColumnNameFromExpression = new _Def2("NoColumnNameFromExpression", "Could not get a column name from a level key expression: ''{0}''.", null);

    /**
     * <code>AggTableZeroSize</code> is '<code>Zero size Aggregate table &#39;&#39;{0}&#39;&#39; for Fact Table &#39;&#39;{1}&#39;&#39;.</code>'
     */
    public final _Def9 AggTableZeroSize = new _Def9("AggTableZeroSize", "Zero size Aggregate table ''{0}'' for Fact Table ''{1}''.", null);

    /**
     * <code>CacheFlushRegionMustContainMembers</code> is '<code>Region of cells to be flushed must contain measures.</code>'
     */
    public final _Def3 CacheFlushRegionMustContainMembers = new _Def3("CacheFlushRegionMustContainMembers", "Region of cells to be flushed must contain measures.", null);

    /**
     * <code>CacheFlushUnionDimensionalityMismatch</code> is '<code>Cannot union cell regions of different dimensionalities. (Dimensionalities are &#39;&#39;{0}&#39;&#39;, &#39;&#39;{1}&#39;&#39;.)</code>'
     */
    public final _Def1 CacheFlushUnionDimensionalityMismatch = new _Def1("CacheFlushUnionDimensionalityMismatch", "Cannot union cell regions of different dimensionalities. (Dimensionalities are ''{0}'', ''{1}''.)", null);

    /**
     * <code>CacheFlushCrossjoinDimensionsInCommon</code> is '<code>Cannot crossjoin cell regions which have dimensions in common. (Dimensionalities are {0}.)</code>'
     */
    public final _Def0 CacheFlushCrossjoinDimensionsInCommon = new _Def0("CacheFlushCrossjoinDimensionsInCommon", "Cannot crossjoin cell regions which have dimensions in common. (Dimensionalities are {0}.)", null);

    /**
     * <code>SegmentCacheIsNotImplementingInterface</code> is '<code>The mondrian.rolap.SegmentCache property points to a class name which is not an
     * implementation of mondrian.spi.SegmentCache.</code>'
     */
    public final _Def3 SegmentCacheIsNotImplementingInterface = new _Def3("SegmentCacheIsNotImplementingInterface", "The mondrian.rolap.SegmentCache property points to a class name which is not an\n        implementation of mondrian.spi.SegmentCache.", null);

    /**
     * <code>SegmentCacheFailedToInstanciate</code> is '<code>An exception was encountered while creating the SegmentCache.</code>'
     */
    public final _Def3 SegmentCacheFailedToInstanciate = new _Def3("SegmentCacheFailedToInstanciate", "An exception was encountered while creating the SegmentCache.", null);

    /**
     * <code>SegmentCacheFailedToLoadSegment</code> is '<code>An exception was encountered while loading a segment from the SegmentCache.</code>'
     */
    public final _Def3 SegmentCacheFailedToLoadSegment = new _Def3("SegmentCacheFailedToLoadSegment", "An exception was encountered while loading a segment from the SegmentCache.", null);

    /**
     * <code>SegmentCacheFailedToSaveSegment</code> is '<code>An exception was encountered while loading a segment from the SegmentCache.</code>'
     */
    public final _Def3 SegmentCacheFailedToSaveSegment = new _Def3("SegmentCacheFailedToSaveSegment", "An exception was encountered while loading a segment from the SegmentCache.", null);

    /**
     * <code>SegmentCacheFailedToLookupSegment</code> is '<code>An exception was encountered while performing a segment lookup in the SegmentCache.</code>'
     */
    public final _Def3 SegmentCacheFailedToLookupSegment = new _Def3("SegmentCacheFailedToLookupSegment", "An exception was encountered while performing a segment lookup in the SegmentCache.", null);

    /**
     * <code>SegmentCacheFailedToScanSegments</code> is '<code>An exception was encountered while getting a list of segment headers in the SegmentCache.</code>'
     */
    public final _Def3 SegmentCacheFailedToScanSegments = new _Def3("SegmentCacheFailedToScanSegments", "An exception was encountered while getting a list of segment headers in the SegmentCache.", null);

    /**
     * <code>SegmentCacheFailedToDeleteSegment</code> is '<code>An exception was encountered while deleting a segment from the SegmentCache.</code>'
     */
    public final _Def3 SegmentCacheFailedToDeleteSegment = new _Def3("SegmentCacheFailedToDeleteSegment", "An exception was encountered while deleting a segment from the SegmentCache.", null);

    /**
     * <code>SegmentCacheReadTimeout</code> is '<code>Timeout reached while reading segment from SegmentCache.</code>'
     */
    public final _Def3 SegmentCacheReadTimeout = new _Def3("SegmentCacheReadTimeout", "Timeout reached while reading segment from SegmentCache.", null);

    /**
     * <code>SegmentCacheWriteTimeout</code> is '<code>Timeout reached while writing segment to SegmentCache.</code>'
     */
    public final _Def3 SegmentCacheWriteTimeout = new _Def3("SegmentCacheWriteTimeout", "Timeout reached while writing segment to SegmentCache.", null);

    /**
     * <code>SegmentCacheLookupTimeout</code> is '<code>Timeout reached while performing a segment lookup in SegmentCache.</code>'
     */
    public final _Def3 SegmentCacheLookupTimeout = new _Def3("SegmentCacheLookupTimeout", "Timeout reached while performing a segment lookup in SegmentCache.", null);

    /**
     * <code>NativeEvaluationUnsupported</code> is '<code>Native evaluation not supported for this usage of function &#39;&#39;{0}&#39;&#39;</code>'
     */
    public final _Def22 NativeEvaluationUnsupported = new _Def22("NativeEvaluationUnsupported", "Native evaluation not supported for this usage of function ''{0}''", null);

    /**
     * <code>NativeSqlInClauseTooLarge</code> is '<code>Cannot use native aggregation constraints for level &#39;&#39;{0}&#39;&#39; because the number of members is larger than the value of &#39;&#39;mondrian.rolap.maxConstraints&#39;&#39; ({1})</code>'
     */
    public final _Def9 NativeSqlInClauseTooLarge = new _Def9("NativeSqlInClauseTooLarge", "Cannot use native aggregation constraints for level ''{0}'' because the number of members is larger than the value of ''mondrian.rolap.maxConstraints'' ({1})", null);

    /**
     * <code>ExecutionStatementCleanupException</code> is '<code>An exception was encountered while trying to cleanup an execution context. A statement failed to cancel gracefully. Locus was : &quot;{0}&quot;.</code>'
     */
    public final _Def0 ExecutionStatementCleanupException = new _Def0("ExecutionStatementCleanupException", "An exception was encountered while trying to cleanup an execution context. A statement failed to cancel gracefully. Locus was : \"{0}\".", null);

    /**
     * <code>QueryLimitReached</code> is '<code>The number of concurrent MDX statements that can be processed simultaneously by this Mondrian server instance ({0,number}) has been reached. To change the limit, set the &#39;&#39;{1}&#39;&#39; property.</code>'
     */
    public final _Def6 QueryLimitReached = new _Def6("QueryLimitReached", "The number of concurrent MDX statements that can be processed simultaneously by this Mondrian server instance ({0,number}) has been reached. To change the limit, set the ''{1}'' property.", null);

    /**
     * <code>SqlQueryLimitReached</code> is '<code>The number of concurrent SQL statements which can be used simultaneously by this Mondrian server instance has been reached. Set &#39;&#39;mondrian.rolap.maxSqlThreads&#39;&#39; to change the current limit.</code>'
     */
    public final _Def3 SqlQueryLimitReached = new _Def3("SqlQueryLimitReached", "The number of concurrent SQL statements which can be used simultaneously by this Mondrian server instance has been reached. Set ''mondrian.rolap.maxSqlThreads'' to change the current limit.", null);

    /**
     * <code>SegmentCacheLimitReached</code> is '<code>The number of concurrent segment cache operations which can be run simultaneously by this Mondrian server instance has been reached. Set &#39;&#39;mondrian.rolap.maxCacheThreads&#39;&#39; to change the current limit.</code>'
     */
    public final _Def3 SegmentCacheLimitReached = new _Def3("SegmentCacheLimitReached", "The number of concurrent segment cache operations which can be run simultaneously by this Mondrian server instance has been reached. Set ''mondrian.rolap.maxCacheThreads'' to change the current limit.", null);

    /**
     * <code>FinalizerErrorRolapSchema</code> is '<code>An exception was encountered while finalizing a RolapSchema object instance.</code>'
     */
    public final _Def3 FinalizerErrorRolapSchema = new _Def3("FinalizerErrorRolapSchema", "An exception was encountered while finalizing a RolapSchema object instance.", null);

    /**
     * <code>FinalizerErrorMondrianServerImpl</code> is '<code>An exception was encountered while finalizing a RolapSchema object instance.</code>'
     */
    public final _Def3 FinalizerErrorMondrianServerImpl = new _Def3("FinalizerErrorMondrianServerImpl", "An exception was encountered while finalizing a RolapSchema object instance.", null);

    /**
     * <code>FinalizerErrorRolapConnection</code> is '<code>An exception was encountered while finalizing a RolapConnection object instance.</code>'
     */
    public final _Def3 FinalizerErrorRolapConnection = new _Def3("FinalizerErrorRolapConnection", "An exception was encountered while finalizing a RolapConnection object instance.", null);


    /**
     * Definition for resources which
     * return a {@link mondrian.olap.MondrianException} exception and
     * take arguments 'String p0'.
     */
    public final class _Def0 extends org.eigenbase.resgen.ResourceDefinition {
        _Def0(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str(String p0) {
            return instantiate(MondrianResource.this, new Object[] {p0}).toString();
        }
        public mondrian.olap.MondrianException ex(String p0) {
            return new mondrian.olap.MondrianException(instantiate(MondrianResource.this, new Object[] {p0}).toString());
        }
        public mondrian.olap.MondrianException ex(String p0, Throwable err) {
            return new mondrian.olap.MondrianException(instantiate(MondrianResource.this, new Object[] {p0}).toString(), err);
        }
    }

    /**
     * Definition for resources which
     * return a {@link mondrian.olap.MondrianException} exception and
     * take arguments 'String p0, String p1'.
     */
    public final class _Def1 extends org.eigenbase.resgen.ResourceDefinition {
        _Def1(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str(String p0, String p1) {
            return instantiate(MondrianResource.this, new Object[] {p0, p1}).toString();
        }
        public mondrian.olap.MondrianException ex(String p0, String p1) {
            return new mondrian.olap.MondrianException(instantiate(MondrianResource.this, new Object[] {p0, p1}).toString());
        }
        public mondrian.olap.MondrianException ex(String p0, String p1, Throwable err) {
            return new mondrian.olap.MondrianException(instantiate(MondrianResource.this, new Object[] {p0, p1}).toString(), err);
        }
    }

    /**
     * Definition for resources which
     * take arguments 'String p0'.
     */
    public final class _Def2 extends org.eigenbase.resgen.ResourceDefinition {
        _Def2(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str(String p0) {
            return instantiate(MondrianResource.this, new Object[] {p0}).toString();
        }
    }

    /**
     * Definition for resources which
     * return a {@link mondrian.olap.MondrianException} exception and
     * take arguments ''.
     */
    public final class _Def3 extends org.eigenbase.resgen.ResourceDefinition {
        _Def3(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str() {
            return instantiate(MondrianResource.this, emptyObjectArray).toString();
        }
        public mondrian.olap.MondrianException ex() {
            return new mondrian.olap.MondrianException(instantiate(MondrianResource.this, emptyObjectArray).toString());
        }
        public mondrian.olap.MondrianException ex(Throwable err) {
            return new mondrian.olap.MondrianException(instantiate(MondrianResource.this, emptyObjectArray).toString(), err);
        }
    }

    /**
     * Definition for resources which
     * return a {@link mondrian.olap.MondrianException} exception and
     * take arguments 'String p0, String p1, String p2'.
     */
    public final class _Def4 extends org.eigenbase.resgen.ResourceDefinition {
        _Def4(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str(String p0, String p1, String p2) {
            return instantiate(MondrianResource.this, new Object[] {p0, p1, p2}).toString();
        }
        public mondrian.olap.MondrianException ex(String p0, String p1, String p2) {
            return new mondrian.olap.MondrianException(instantiate(MondrianResource.this, new Object[] {p0, p1, p2}).toString());
        }
        public mondrian.olap.MondrianException ex(String p0, String p1, String p2, Throwable err) {
            return new mondrian.olap.MondrianException(instantiate(MondrianResource.this, new Object[] {p0, p1, p2}).toString(), err);
        }
    }

    /**
     * Definition for resources which
     * take arguments ''.
     */
    public final class _Def5 extends org.eigenbase.resgen.ResourceDefinition {
        _Def5(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str() {
            return instantiate(MondrianResource.this, emptyObjectArray).toString();
        }
    }

    /**
     * Definition for resources which
     * return a {@link mondrian.olap.MondrianException} exception and
     * take arguments 'Number p0, String p1'.
     */
    public final class _Def6 extends org.eigenbase.resgen.ResourceDefinition {
        _Def6(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str(Number p0, String p1) {
            return instantiate(MondrianResource.this, new Object[] {p0, p1}).toString();
        }
        public mondrian.olap.MondrianException ex(Number p0, String p1) {
            return new mondrian.olap.MondrianException(instantiate(MondrianResource.this, new Object[] {p0, p1}).toString());
        }
        public mondrian.olap.MondrianException ex(Number p0, String p1, Throwable err) {
            return new mondrian.olap.MondrianException(instantiate(MondrianResource.this, new Object[] {p0, p1}).toString(), err);
        }
    }

    /**
     * Definition for resources which
     * return a {@link mondrian.olap.MondrianException} exception and
     * take arguments 'String p0, String p1, String p2, String p3'.
     */
    public final class _Def7 extends org.eigenbase.resgen.ResourceDefinition {
        _Def7(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str(String p0, String p1, String p2, String p3) {
            return instantiate(MondrianResource.this, new Object[] {p0, p1, p2, p3}).toString();
        }
        public mondrian.olap.MondrianException ex(String p0, String p1, String p2, String p3) {
            return new mondrian.olap.MondrianException(instantiate(MondrianResource.this, new Object[] {p0, p1, p2, p3}).toString());
        }
        public mondrian.olap.MondrianException ex(String p0, String p1, String p2, String p3, Throwable err) {
            return new mondrian.olap.MondrianException(instantiate(MondrianResource.this, new Object[] {p0, p1, p2, p3}).toString(), err);
        }
    }

    /**
     * Definition for resources which
     * return a {@link mondrian.olap.MondrianException} exception and
     * take arguments 'Number p0'.
     */
    public final class _Def8 extends org.eigenbase.resgen.ResourceDefinition {
        _Def8(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str(Number p0) {
            return instantiate(MondrianResource.this, new Object[] {p0}).toString();
        }
        public mondrian.olap.MondrianException ex(Number p0) {
            return new mondrian.olap.MondrianException(instantiate(MondrianResource.this, new Object[] {p0}).toString());
        }
        public mondrian.olap.MondrianException ex(Number p0, Throwable err) {
            return new mondrian.olap.MondrianException(instantiate(MondrianResource.this, new Object[] {p0}).toString(), err);
        }
    }

    /**
     * Definition for resources which
     * take arguments 'String p0, String p1'.
     */
    public final class _Def9 extends org.eigenbase.resgen.ResourceDefinition {
        _Def9(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str(String p0, String p1) {
            return instantiate(MondrianResource.this, new Object[] {p0, p1}).toString();
        }
    }

    /**
     * Definition for resources which
     * return a {@link mondrian.olap.ResourceLimitExceededException} exception and
     * take arguments 'Number p0, Number p1'.
     */
    public final class _Def10 extends org.eigenbase.resgen.ResourceDefinition {
        _Def10(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str(Number p0, Number p1) {
            return instantiate(MondrianResource.this, new Object[] {p0, p1}).toString();
        }
        public mondrian.olap.ResourceLimitExceededException ex(Number p0, Number p1) {
            return new mondrian.olap.ResourceLimitExceededException(instantiate(MondrianResource.this, new Object[] {p0, p1}).toString());
        }
    }

    /**
     * Definition for resources which
     * return a {@link mondrian.olap.ResourceLimitExceededException} exception and
     * take arguments 'Number p0'.
     */
    public final class _Def11 extends org.eigenbase.resgen.ResourceDefinition {
        _Def11(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str(Number p0) {
            return instantiate(MondrianResource.this, new Object[] {p0}).toString();
        }
        public mondrian.olap.ResourceLimitExceededException ex(Number p0) {
            return new mondrian.olap.ResourceLimitExceededException(instantiate(MondrianResource.this, new Object[] {p0}).toString());
        }
    }

    /**
     * Definition for resources which
     * return a {@link mondrian.olap.QueryCanceledException} exception and
     * take arguments ''.
     */
    public final class _Def12 extends org.eigenbase.resgen.ResourceDefinition {
        _Def12(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str() {
            return instantiate(MondrianResource.this, emptyObjectArray).toString();
        }
        public mondrian.olap.QueryCanceledException ex() {
            return new mondrian.olap.QueryCanceledException(instantiate(MondrianResource.this, emptyObjectArray).toString());
        }
    }

    /**
     * Definition for resources which
     * return a {@link mondrian.olap.QueryTimeoutException} exception and
     * take arguments 'Number p0'.
     */
    public final class _Def13 extends org.eigenbase.resgen.ResourceDefinition {
        _Def13(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str(Number p0) {
            return instantiate(MondrianResource.this, new Object[] {p0}).toString();
        }
        public mondrian.olap.QueryTimeoutException ex(Number p0) {
            return new mondrian.olap.QueryTimeoutException(instantiate(MondrianResource.this, new Object[] {p0}).toString());
        }
    }

    /**
     * Definition for resources which
     * return a {@link mondrian.olap.InvalidHierarchyException} exception and
     * take arguments 'String p0'.
     */
    public final class _Def14 extends org.eigenbase.resgen.ResourceDefinition {
        _Def14(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str(String p0) {
            return instantiate(MondrianResource.this, new Object[] {p0}).toString();
        }
        public mondrian.olap.InvalidHierarchyException ex(String p0) {
            return new mondrian.olap.InvalidHierarchyException(instantiate(MondrianResource.this, new Object[] {p0}).toString());
        }
    }

    /**
     * Definition for resources which
     * take arguments 'String p0, Number p1'.
     */
    public final class _Def15 extends org.eigenbase.resgen.ResourceDefinition {
        _Def15(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str(String p0, Number p1) {
            return instantiate(MondrianResource.this, new Object[] {p0, p1}).toString();
        }
    }

    /**
     * Definition for resources which
     * take arguments 'String p0, String p1, String p2'.
     */
    public final class _Def16 extends org.eigenbase.resgen.ResourceDefinition {
        _Def16(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str(String p0, String p1, String p2) {
            return instantiate(MondrianResource.this, new Object[] {p0, p1, p2}).toString();
        }
    }

    /**
     * Definition for resources which
     * take arguments 'String p0, String p1, String p2, String p3'.
     */
    public final class _Def17 extends org.eigenbase.resgen.ResourceDefinition {
        _Def17(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str(String p0, String p1, String p2, String p3) {
            return instantiate(MondrianResource.this, new Object[] {p0, p1, p2, p3}).toString();
        }
    }

    /**
     * Definition for resources which
     * take arguments 'String p0, String p1, String p2, Number p3, String p4, String p5, String p6'.
     */
    public final class _Def18 extends org.eigenbase.resgen.ResourceDefinition {
        _Def18(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str(String p0, String p1, String p2, Number p3, String p4, String p5, String p6) {
            return instantiate(MondrianResource.this, new Object[] {p0, p1, p2, p3, p4, p5, p6}).toString();
        }
    }

    /**
     * Definition for resources which
     * take arguments 'String p0, String p1, Number p2'.
     */
    public final class _Def19 extends org.eigenbase.resgen.ResourceDefinition {
        _Def19(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str(String p0, String p1, Number p2) {
            return instantiate(MondrianResource.this, new Object[] {p0, p1, p2}).toString();
        }
    }

    /**
     * Definition for resources which
     * take arguments 'String p0, String p1, Number p2, String p3'.
     */
    public final class _Def20 extends org.eigenbase.resgen.ResourceDefinition {
        _Def20(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str(String p0, String p1, Number p2, String p3) {
            return instantiate(MondrianResource.this, new Object[] {p0, p1, p2, p3}).toString();
        }
    }

    /**
     * Definition for resources which
     * take arguments 'String p0, String p1, String p2, String p3, String p4, String p5, String p6'.
     */
    public final class _Def21 extends org.eigenbase.resgen.ResourceDefinition {
        _Def21(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str(String p0, String p1, String p2, String p3, String p4, String p5, String p6) {
            return instantiate(MondrianResource.this, new Object[] {p0, p1, p2, p3, p4, p5, p6}).toString();
        }
    }

    /**
     * Definition for resources which
     * return a {@link mondrian.olap.NativeEvaluationUnsupportedException} exception and
     * take arguments 'String p0'.
     */
    public final class _Def22 extends org.eigenbase.resgen.ResourceDefinition {
        _Def22(String key, String baseMessage, String[] props) {
            super(key, baseMessage, props);
        }
        public String str(String p0) {
            return instantiate(MondrianResource.this, new Object[] {p0}).toString();
        }
        public mondrian.olap.NativeEvaluationUnsupportedException ex(String p0) {
            return new mondrian.olap.NativeEvaluationUnsupportedException(instantiate(MondrianResource.this, new Object[] {p0}).toString());
        }
    }

}

<?xml version='1.0'?>
<!--
  == This software is subject to the terms of the Eclipse Public License v1.0
  == Agreement, available at the following URL:
  == http://www.eclipse.org/legal/epl-v10.html.
  == You must accept the terms of that agreement to use this software.
  ==
  == Copyright (C) 2000-2005 Julian Hyde
  == Copyright (C) 2005-2006 Pentaho and others
  == All Rights Reserved.
  -->
<xsl:stylesheet version="3.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<!-- Main document -->
<xsl:template match="/">

<HTML>
<HEAD>

<style>

A:link { color:#000066; }
A:visited { color:#666666; }

A.clsIncCpyRt,
A.clsIncCpyRt:visited,
P.clsIncCpyRt {
  font-weight:normal; font-size:75%; font-family:verdana,arial,helvetica,sans-serif;
  color:black;
  text-decoration:none;
}

A.clsLeftMenu,
A.clsLeftMenu:visited {
  color:#000000;
  text-decoration:none;
  font-weight:bold; font-size:8pt;
}

A.clsBackTop,
A.clsBackTop:visited {
  margin-top:10; margin-bottom:0;
  padding-bottom:0;
  font-size:75%;
  color:black;
}

A:hover,
A.clsBackTop:hover,
A.clsIncCpyRt:hover,
A:active { color:blue; }

A.clsGlossary {
  font-size:10pt;
  color:green;
}

BODY { font-size:80%; font-family:verdana,arial,helvetica,sans-serif; }

BUTTON.clsShowme,
BUTTON.clsShowme5 {
  font-weight:bold; font-size:11; font-family:arial;
  width:68; height:23;
  position:relative; top:2;
  background-color:#002F90;
  color:#FFFFFF;
}

DIV.clsBeta {
  font-weight:bold;
  color:red;
}

DIV.clsDocBody { margin-left:10px; margin-right:10px; }

DIV.clsDocBody HR { margin-top:0; }

DIV.clsDesFooter { margin:10px 10px 0px 223px; }

DIV.clsFPfig { font-size:80%; }

DIV.clsHi {
    padding-left:2em;
    text-indent:-2em
}

DIV.clsShowme { margin-bottom:.5em; margin-top:.5em; }

H1{
  font-size:145%;
  margin-top:1.25em; margin-bottom:0em;
}

H2 {
  font-size:135%;
  margin-top:1.25em; margin-bottom:.5em;
}

H3 {
  font-size:128%;
  margin-top:1em; margin-bottom:0em;
}

H4 {
  font-size:120%;
  margin-top:.8em; margin-bottom:0em;
}

H5 {
  font-size:110%;
  margin-top:.8em; margin-bottom:0em;
}

H6 {
  font-size:70%;
  margin-top:.6em; margin-bottom:0em;
}

HR.clsTransHR {
    position:relative; top:20;
    margin-bottom:15;
}

P.clsRef {
  font-weight:bold;
  margin-top:12pt; margin-bottom:0pt;
}

PRE {
  background:#EEEEEE;
  margin-top:1em;   margin-bottom:1em; margin-left:0px;
  padding:5pt;
}

PRE.clsCode, CODE.clsText { font-family:'courier new',courier,serif; font-size:130%; }

PRE.clsSyntax { font-family:verdana,arial,helvetica,sans-serif; font-size:120%; }

SPAN.clsEntryText {
  line-height:12pt;
  font-size:8pt;
}

SPAN.clsHeading {
  color:#00319C;
  font-size:11pt; font-weight:bold;
}

SPAN.clsDefValue, TD.clsDefValue { font-weight:bold; font-family:'courier new' }

SPAN.clsLiteral, TD.clsLiteral { font-family:'courier new'; }

SPAN.clsRange, TD.clsRange { font-style:italic; }

SPAN.clsShowme {
  width:100%;
  filter:dropshadow(color=#000000,OffX=2.5,OffY=2.5,Positive=1);
  position:relative;
  top:-8;
}

TABLE { font-size:100%; }

TABLE.clsStd {
    background-color:#444;
    border:1px none;
    cellspacing:0;
    cellpadding:0
}

TABLE.clsStd TH,
BLOCKQUOTE TH {
    font-size:100%;
    text-align:left; vertical-align:top;
    background-color:#DDD;
    padding:2px;
}

TABLE.clsStd TD,
BLOCKQUOTE TD {
    font-size:100%;
    vertical-align:top;
    background-color:#EEE;
    padding:2px;
}

TABLE.clsParamVls,
TABLE.clsParamVls TD { padding-left:2pt; padding-right:2pt; }

#TOC { visibility:hidden; }

UL UL, OL UL { list-style-type:square; }

.clsHide { display:none; }

.clsShow { }

.clsShowDiv {
  visibility:hidden;
  position:absolute;
  left:230px; top:140px;
  height:0px; width:170px;
  z-index:-1;
}

.#pBackTop { display:none; }

#idTransDiv {
    position:relative;
    width:90%; top:20;
  filter:revealTrans(duration=1.0, transition=23);
}


/*** INDEX-SPECIFIC ***/

A.clsDisabled {
  text-decoration:none;
  color:black;
  cursor:text;
}

A.clsEnabled { cursor:auto; }

SPAN.clsAccess { text-decoration:underline; }

TABLE.clsIndex {
  font-size:100%;
  padding-left:2pt; padding-right:2pt;
    margin-top: 17pt;
}

TABLE.clsIndex TD {
  margin:3pt;
  background-color:#EEEEEE;
}

TR.clsEntry { vertical-align:top; }

TABLE.clsIndex TD.clsLetters {
  background-color:#CCCCCC;
  text-align:center;
}

TD.clsMainHead {
  background-color:#FFFFFF;
  vertical-align:top;
  font-size:145%; font-weight:bold;
  margin-top:1.35em; margin-bottom:.5em;
}

UL.clsIndex { margin-left:20pt; margin-top:0pt; margin-bottom:5pt; }

LI OL { padding-bottom: 1.5em }


/*** GALLERY/TOOLS/SAMPLES ***/

FORM.clsSamples { margin-bottom:0; margin-top:0; }

H1.clsSampH1 {
    font-size:145%;
  margin-top:.25em; margin-bottom:.25em;
}

H1.clsSampHead {
  margin-top:5px; margin-bottom:5px;
  font-size:24px; font-weight:bold; font-family:verdana,arial,helvetica,sans-serif;
}

H2.clsSampTitle {
  font-size:128%;
  margin-top:.2em; margin-bottom:0em;
}

TD.clsDemo {
  font-size:8pt;
  color:#00319C;
  text-decoration:underline;
}

.clsSampDnldMain { font-size:11px; font-family:verdana,arial,helvetica,sans-serif; }

.clsShowDesc { cursor:hand; }

A.clsTools {
  color:#0B3586;
  font-weight:bold;
}

H1.clsTools, H2.clsTools {
  color:#0B3586;
  margin-top:5px;
}

TD.clsToolsHome {
  font-size:9pt;
  line-height:15pt;
}

SPAN.clsToolsTitle {
  color:#00319C;
  font-size:11pt; font-weight:bold;
  text-decoration:none;
}


/*** DESIGN ***/
P.cat {
    font-size:13pt;
    color:#787800;
    text-decoration:none;
    margin-top:18px;
}

P.author {
    font-size:9pt; font-style:italic;
    line-height:13pt;
    margin-top:10px;
}

P.date {
    font-size:8pt;
    line-height:12px;
    margin-top:0px;
    color:#3366FF;
}

P.graph1 {
    line-height:13pt;
    margin-top:-10px;
}

P.col {
    line-height:13pt;
    margin-top:10px; margin-left:5px;
}

P.cal1 {
    text-decoration:none;
    margin-top:-10px;
}

P.cal2 {margin-top:-10px; }
P.photo { font-size:8pt; }


/*** DOCTOP ***/

#tblNavLinks A {
    color:black;
    text-decoration:none;
    font-family:verdana,arial,helvetica,sans-serif;
}
#lnkShowText, #lnkSyncText, #lnkSearchText, #lnkIndexText { font-size:8pt; font-weight:bold; }
#lnkPrevText, #lnkNextText, #lnkUpText { font-size:7.5pt; font-weight:normal; }


DIV.clsBucketBranch {
    margin-left:10px; margin-top:15px; margin-bottom:-10pt;
    font-style:italic; font-size:85%;
}

DIV.clsBucketBranch A,
DIV.clsBucketBranch A:link,
DIV.clsBucketBranch A:active,
DIV.clsBucketBranch A:visited { text-decoration:none; color:black; }
DIV.clsBucketBranch A:hover { color:blue; }


/*** SDK, IE4 ONLY ***/

DIV.clsExpanded, A.clsExpanded { display:inline; color:black; }
DIV.clsCollapsed, A.clsCollapsed { display:none; }
SPAN.clsPropattr { font-weight:bold; }

#pStyles,   #pCode, #pSyntax, #pEvents, #pStyles {display:none; text-decoration:underline; cursor:hand; }

CODE { color:maroon; font-family:'courier new' }

DFN { font-weight:bold; font-style:italic; }


</style>

</HEAD>
<BODY>
<xsl:apply-templates select="Model"/>
</BODY>
</HTML>

</xsl:template>

<!--
  - Doc processing
  -->

<xsl:template match="@*|node()" mode="copy">
 <xsl:copy>
  <xsl:apply-templates select="@*" mode="copy"/>
  <xsl:apply-templates mode="copy"/>
 </xsl:copy>
</xsl:template>

<!-- Model -->
<xsl:template match="Model">
<h1>Mining Meta Model Instance <u><xsl:value-of select="@name"/></u></h1>
<p>
<b>DTD Name:</b>
<a><xsl:attribute name="href"><xsl:value-of select="@dtdName"/>
</xsl:attribute>
<xsl:value-of select="@dtdName"/></a><br/>
<b>Class Name:</b>
<a><xsl:attribute name="href">http://code/bb/main/mining/Broadbase/mining/xml/<xsl:value-of select="@className"/>.java</xsl:attribute>
<xsl:value-of select="@className"/></a> <br/>
<b>Root Element:</b>
<a><xsl:attribute name="href">#<xsl:value-of select="@root"/></xsl:attribute>
<xsl:value-of select="@root"/></a> <br/>
<b>Version:</b> <xsl:value-of select="@version"/> <br/>
</p>

<table width="100%" cellspacing="0" border="1">
<tr>
<td bgcolor="#3366cc" colspan="2">
<font color="white">
<h2>Overview</h2>
</font>
</td>
</tr>
</table>

<blockquote>
<xsl:apply-templates select="Doc" mode="copy"/>
</blockquote>

<h2>Imports</h2>
<ul>
<xsl:for-each select="Import">
<li>
<a><xsl:attribute name="href">#<xsl:value-of select="@type"/></xsl:attribute>
<xsl:value-of select="@type"/>
</a>
</li>
</xsl:for-each>
</ul>

<h2>Element Summary</h2>
<ul>
<xsl:for-each select="Element|StringElement|Plugin">
<xsl:sort select="@type"/>
<li>
<a><xsl:attribute name="href">#<xsl:value-of select="@type"/></xsl:attribute>
<xsl:value-of select="@type"/>
</a>
</li>
</xsl:for-each>
</ul>

<h2>Classes</h2>
<ul>
<xsl:for-each select="Class">
<xsl:sort select="@class"/>
<li>
<a><xsl:attribute name="href">#<xsl:value-of select="@class"/></xsl:attribute>
<xsl:value-of select="@class"/>
</a>
</li>
</xsl:for-each>
</ul>

<xsl:apply-templates select="//Element"/>
<xsl:apply-templates select="//Plugin"/>
<xsl:apply-templates select="//Class"/>
<xsl:apply-templates select="//Import"/>

</xsl:template>

<!--
  -  Element processing
  -->
<xsl:template match="Element">
<p/>
<a><xsl:attribute name="name"><xsl:value-of select="@type"/></xsl:attribute>
<table width="100%" cellspacing="0" border="1">
<tr>
<td bgcolor="#3366cc">
<font color="white">
<h2><i>Element</i> <xsl:value-of select="@type"/></h2>
</font>
</td>
</tr>
</table>
</a>
<blockquote>
<xsl:apply-templates select="Doc" mode="copy"/>
</blockquote>

<xsl:if test="@class">
<h3>Class</h3>
<blockquote>
<a><xsl:attribute name="href">#<xsl:value-of select="@class"/></xsl:attribute>
<xsl:value-of select="@class"/>
</a>
</blockquote>
</xsl:if>

<h3>Attributes</h3>
<blockquote>
<xsl:choose>
<xsl:when test="Attribute">
<table border="1" class="clsStd">
  <tr><th>Attribute</th><th>Type</th><th>Default</th>
      <th>Description</th></tr>
  <xsl:for-each select="Attribute">
  <tr><td>
          <xsl:choose>
         <xsl:when test="@required='true'">
             <b><xsl:value-of select="@name"/></b>
             </xsl:when>
         <xsl:otherwise>
             <xsl:value-of select="@name"/>
             </xsl:otherwise>
      </xsl:choose>
      </td>
      <td><i>
         <xsl:choose>
            <xsl:when test="@type">
              <xsl:value-of select="@type"/>
        </xsl:when>
        <xsl:otherwise>String</xsl:otherwise>
     </xsl:choose>
      </i></td>
      <td>
         <xsl:choose>
        <xsl:when test="@default">
            <xsl:value-of select="@default"/>
        </xsl:when>
        <xsl:otherwise><i>none</i></xsl:otherwise>
     </xsl:choose>
      </td>
      <td>
         <xsl:choose>
        <xsl:when test="Doc">
            <xsl:apply-templates select="Doc" mode="copy"/>
        </xsl:when>
            <xsl:otherwise><i>none</i></xsl:otherwise>
         </xsl:choose>
      </td></tr>
  </xsl:for-each>
</table>
</xsl:when>
<xsl:otherwise><i>none</i></xsl:otherwise>
</xsl:choose>
</blockquote>

<h3>Content</h3>
<blockquote>
<xsl:choose>
<xsl:when test="Object|Array">
<table border="1" class="clsStd">
  <tr><th>Element</th><th>Java Name</th><th>Constraints</th><th>Description</th></tr>
  <xsl:for-each select="Object|Array">
  <tr><td>
         <a><xsl:attribute name="href">#<xsl:value-of select="@type"/>
        </xsl:attribute>
         <xsl:value-of select="@type"/></a>
      </td>
      <td><xsl:value-of select="@name"/></td>
      <td>
         <xsl:choose>
        <xsl:when test="@required='true'">
            <i>Required</i>
        </xsl:when>
        <xsl:when test="@min">
            <i>Array [<xsl:value-of select="@min"/> ..
        <xsl:value-of select="@max"/>]</i>
        </xsl:when>
        <xsl:when test="@name=../Array/@name">
            <i>Array</i>
        </xsl:when>
        <xsl:otherwise>
            <i>Optional</i>
        </xsl:otherwise>
     </xsl:choose>
      </td>
      <td>
         <xsl:choose>
        <xsl:when test="Doc">
            <xsl:apply-templates select="Doc" mode="copy"/>
        </xsl:when>
            <xsl:otherwise><i>none</i></xsl:otherwise>
         </xsl:choose>
  </td></tr>
  </xsl:for-each>
</table>
</xsl:when>
<xsl:when test="Any"><i>Any</i></xsl:when>
<xsl:when test="CData"><i>Text</i></xsl:when>
<xsl:otherwise><i>empty</i></xsl:otherwise>
</xsl:choose>
<xsl:if test="@keepDef='true'">; keep DOM node</xsl:if>

</blockquote>
</xsl:template>


<!--
  -  Plugin processing
  -->
<xsl:template match="Plugin">
<p/>
<a><xsl:attribute name="name"><xsl:value-of select="@type"/></xsl:attribute>
<table width="100%" cellspacing="0" border="0">
<tr>
<td bgcolor="#3366cc">
<font face="arial,sans-serif" size="-1" color="white">
<h2><i>Plugin</i> <xsl:value-of select="@type"/></h2>
</font>
</td>
</tr>
</table>
</a>
<blockquote>
<xsl:apply-templates select="Doc" mode="copy"/>
</blockquote>

<xsl:if test="@class">
<h3>Class</h3>
<blockquote>
<a><xsl:attribute name="href">#<xsl:value-of select="@class"/></xsl:attribute>
<xsl:value-of select="@class"/>
</a>
</blockquote>
</xsl:if>

<h3>Attributes</h3>
<blockquote>
<table border="1" class="clsStd">
  <tr><th>Attribute</th><th>Type</th><th>Default</th>
      <th>Description</th></tr>
  <xsl:for-each select="Attribute">
  <tr><td>
          <xsl:choose>
         <xsl:when test="@required='true'">
             <b><xsl:value-of select="@name"/></b>
             </xsl:when>
         <xsl:otherwise>
             <xsl:value-of select="@name"/>
             </xsl:otherwise>
      </xsl:choose>
      </td>
      <td><i>
         <xsl:choose>
            <xsl:when test="@type">
              <xsl:value-of select="@type"/>
        </xsl:when>
        <xsl:otherwise>String</xsl:otherwise>
     </xsl:choose>
      </i></td>
      <td>
         <xsl:choose>
        <xsl:when test="@default">
            <xsl:value-of select="@default"/>
        </xsl:when>
        <xsl:otherwise><i>none</i></xsl:otherwise>
     </xsl:choose>
      </td>
      <td>
         <xsl:choose>
        <xsl:when test="Doc">
            <xsl:apply-templates select="Doc" mode="copy"/>
        </xsl:when>
            <xsl:otherwise><i>none</i></xsl:otherwise>
         </xsl:choose>
      </td></tr>
  </xsl:for-each>
  <tr><td>defPackage</td><td><i>String</i></td>
      <td>Broadbase.mining.xml</td>
      <td>The <i>defPackage</i> attribute, available to all Plugins, specifies
          the package of the Java Class used to parse all plugin contents.</td>
  </tr>
  <tr><td><b>defClass</b></td><td><i>String</i></td>
      <td>Broadbase.mining.xml</td>
      <td>The <i>defClass</i> attribute, available to all Plugins, specifies
          the class name of the Java Class used to parse all plugin contents.</td>
  </tr>
</table>
</blockquote>

<h3>Content</h3>
<blockquote>
<i>Any</i> from <i>defClass</i>
</blockquote>
</xsl:template>


<!--
  -  Class processing
  -->
<xsl:template match="Class">
<p/>
<a><xsl:attribute name="name"><xsl:value-of select="@class"/></xsl:attribute>
<table width="100%" cellspacing="0" border="0">
<tr>
<td bgcolor="#3366cc">
<font face="arial,sans-serif" size="-1" color="white">
<h2><i>Class</i> <xsl:value-of select="@class"/></h2>
</font>
</td>
</tr>
</table>
</a>
<blockquote>
<xsl:apply-templates select="Doc" mode="copy"/>
</blockquote>

<xsl:if test="@superclass">
<h3>Superclass</h3>
<blockquote>
<a><xsl:attribute name="href">#<xsl:value-of select="@superclass"/></xsl:attribute>
<xsl:value-of select="@superclass"/>
</a>
</blockquote>
</xsl:if>

<h3>Attributes</h3>
<blockquote>
<xsl:choose>
<xsl:when test="Attribute">
<table border="1" class="clsStd">
  <tr><th>Attribute</th><th>Type</th><th>Default</th>
      <th>Description</th></tr>
  <xsl:for-each select="Attribute">
  <tr><td>
          <xsl:choose>
         <xsl:when test="@required='true'">
             <b><xsl:value-of select="@name"/></b>
             </xsl:when>
         <xsl:otherwise>
             <xsl:value-of select="@name"/>
             </xsl:otherwise>
      </xsl:choose>
      </td>
      <td><i>
         <xsl:choose>
            <xsl:when test="@type">
              <xsl:value-of select="@type"/>
        </xsl:when>
        <xsl:otherwise>String</xsl:otherwise>
     </xsl:choose>
      </i></td>
      <td>
         <xsl:choose>
        <xsl:when test="@default">
            <xsl:value-of select="@default"/>
        </xsl:when>
        <xsl:otherwise><i>none</i></xsl:otherwise>
     </xsl:choose>
      </td>
      <td>
         <xsl:choose>
        <xsl:when test="Doc">
            <xsl:apply-templates select="Doc" mode="copy"/>
        </xsl:when>
            <xsl:otherwise><i>none</i></xsl:otherwise>
         </xsl:choose>
      </td></tr>
  </xsl:for-each>
</table>
</xsl:when>
<xsl:otherwise><i>none</i></xsl:otherwise>
</xsl:choose>
</blockquote>

<h3>Content</h3>
<blockquote>
<xsl:choose>
<xsl:when test="Object|Array">
<table border="1" class="clsStd">
  <tr><th>Element</th><th>Java Name</th><th>Constraints</th><th>Description</th></tr>
  <xsl:for-each select="Object|Array">
  <tr><td>
         <a><xsl:attribute name="href">#<xsl:value-of select="@type"/>
        </xsl:attribute>
         <xsl:value-of select="@type"/></a>
      </td>
      <td><xsl:value-of select="@name"/></td>
      <td>
         <xsl:choose>
        <xsl:when test="@required='true'">
            <i>Required</i>
        </xsl:when>
        <xsl:when test="@min">
            <i>Array [<xsl:value-of select="@min"/> ..
        <xsl:value-of select="@max"/>]</i>
        </xsl:when>
        <xsl:when test="@name=../Array/@name">
            <i>Array</i>
        </xsl:when>
        <xsl:otherwise>
            <i>Optional</i>
        </xsl:otherwise>
     </xsl:choose>
      </td>
      <td>
         <xsl:choose>
        <xsl:when test="Doc">
            <xsl:apply-templates select="Doc" mode="copy"/>
        </xsl:when>
            <xsl:otherwise><i>none</i></xsl:otherwise>
         </xsl:choose>
  </td></tr>
  </xsl:for-each>
</table>
</xsl:when>
<xsl:when test="Any"><i>Any</i></xsl:when>
<xsl:when test="CData"><i>Text</i></xsl:when>
<xsl:otherwise><i>empty</i></xsl:otherwise>
</xsl:choose>
</blockquote>
</xsl:template>

<!--
  -  StringElement processing
  -->
<xsl:template match="StringElement">
<p/>
<a><xsl:attribute name="name"><xsl:value-of select="@type"/></xsl:attribute>
<table width="100%" cellspacing="0" border="0">
<tr>
<td bgcolor="#3366cc">
<font face="arial,sans-serif" size="-1" color="white">
<h2><i>StringElement</i> <xsl:value-of select="@type"/></h2>
</font>
</td>
</tr>
</table>
</a>
<blockquote>
<xsl:apply-templates select="Doc" mode="copy"/>
</blockquote>

<h3>Attributes</h3>
<blockquote>
<i>none</i>
</blockquote>

<h3>Content</h3>
<blockquote>
<i>Text</i>
</blockquote>
</xsl:template>

<!--
  -  Import processing
  -->
<xsl:template match="Import">
<p/>
<a><xsl:attribute name="name"><xsl:value-of select="@type"/></xsl:attribute>
<table width="100%" cellspacing="0" border="0">
<tr>
<td bgcolor="#3366cc">
<font face="arial,sans-serif" size="-1" color="white">
<h2><i>Import</i> <xsl:value-of select="@type"/></h2>
</font>
</td>
</tr>
</table>
</a>

<b>Package</b>: <xsl:value-of select="@defPackage"/><br/>
<b>Class</b>: <xsl:value-of select="@defClass"/><br/>
<b>DTD</b>: <xsl:value-of select="@dtdName"/><br/>

<blockquote>
<xsl:apply-templates select="Doc" mode="copy"/>
</blockquote>

</xsl:template>

</xsl:stylesheet>

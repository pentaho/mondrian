<?xml version='1.0'?>
<!-- $Id$ -->
<!--  This software is subject to the terms of the Common Public License -->
<!--  Agreement, available at the following URL: -->
<!--  http://www.opensource.org/licenses/cpl.html. -->
<!-- (C) Copyright 2000-2004 Kana Software, Inc. and others. -->
<!--  All Rights Reserved. -->
<!--  You must accept the terms of that agreement to use this software. -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/TR/WD-xsl">
<xsl:template match="/">

<TABLE BORDER="1">
<TR><TH>infoClass</TH><TD><xsl:value-of select="BaflResourceList/@infoClass"/></TD></TR>
<TR><TH>contextParams</TH><TD><xsl:value-of select="BaflResourceList/@contextParams"/></TD></TR>
<TR><TH>contextArgs</TH><TD><xsl:value-of select="BaflResourceList/@contextArgs"/></TD></TR>
<TR><TH>generateNew?</TH><TD><xsl:value-of select="BaflResourceList/@generateNew"/></TD></TR>
<TR><TH>dynamic?</TH><TD><xsl:value-of select="BaflResourceList/@dynamic"/></TD></TR>
<TR><TH>alwaysContext?</TH><TD><xsl:value-of select="BaflResourceList/@alwaysContext"/></TD></TR>
<TR><TH>argContext</TH><TD><xsl:value-of select="BaflResourceList/@argContext"/></TD></TR>
</TABLE>

<P/>

<TABLE BORDER="1">
<TR>
<TH ROWSPAN="2">Id</TH>
<TH ALIGN="LEFT">Type</TH>
<TH ALIGN="LEFT">Macro name</TH>
</TR>
<TR>
<TH COLSPAN="2" ALIGN="LEFT">Message text</TH>
</TR>
<xsl:for-each select="BaflResourceList/BaflResourceText">
<TR>
<TD ROWSPAN="2"><xsl:value-of select="@id"/></TD>
<TD><xsl:value-of select="@type"/></TD>
<TD><xsl:value-of select="@macroName"/></TD>
</TR>
<TR>
<TD COLSPAN="2"><PRE><xsl:value-of select="cdata()"/></PRE></TD>
</TR>
</xsl:for-each>
</TABLE>
</xsl:template>
</xsl:stylesheet>
<!-- End Resource.xsl -->

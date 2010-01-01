<?xml version="1.0"?>
<!--
$Id$

This software is subject to the terms of the Eclipse Public License v1.0
Agreement, available at the following URL:
http://www.eclipse.org/legal/epl-v10.html.
Copyright (C) 2002-2002 Kana Software, Inc.
Copyright (C) 2002-2009 Julian Hyde and others
All Rights Reserved.
You must accept the terms of that agreement to use this software.
-->

<xsl:stylesheet version="1.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:tt="http://www.tonbeller.com/bii/treetable"
>

<xsl:output method="html" indent="yes"/>

<xsl:template match="/mdxtable">
  <xsl:apply-templates select="slicers/position/member"/>
</xsl:template>

<xsl:template match="member">
  <xsl:if test="preceding-sibling::member">
    <xsl:text>, </xsl:text>
  </xsl:if>
  <xsl:value-of select="@caption"/>
</xsl:template>


</xsl:stylesheet>

<?xml version="1.0"?>
<!--
This software is subject to the terms of the Eclipse Public License v1.0
Agreement, available at the following URL:
http://www.eclipse.org/legal/epl-v10.html.
You must accept the terms of that agreement to use this software.

Copyright (C) 2002-2005 Julian Hyde
Copyright (C) 2005-2009 Pentaho and others
All Rights Reserved.

Prints the text of an MDX query.
-->

<!DOCTYPE xsl:stylesheet [
  <!ENTITY nbsp "&#160;">
  <!ENTITY amp "&#046;">
]>

<xsl:stylesheet version="1.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:tt="http://www.tonbeller.com/bii/treetable"
>

<xsl:output method="html" indent="yes"/>


<xsl:template match="mdxtable">
  <xsl:value-of select="query"/>
</xsl:template>

</xsl:stylesheet>

<?xml version="1.0"?>

<xsl:stylesheet version="1.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:tt="http://www.tonbeller.com/bii/treetable"
>

<xsl:output method="html" indent="yes"/>


<xsl:template match="mdxtable/body/row/cell">
  <xsl:value-of select="@value"/>
</xsl:template>


</xsl:stylesheet>

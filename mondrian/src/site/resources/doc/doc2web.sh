# ******************************************************************************
#
# Pentaho
#
# Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
#
# Use of this software is governed by the Business Source License included
# in the LICENSE.TXT file.
#
# Change Date: 2029-07-20
# ******************************************************************************


pause() {
  xmessage Continue...
}

beep() {
  echo x | tr x \\007
}

doHtml() {
  # LOCALE="en"
  LOCALE="$1"
  # SRCFILE="aggregate_tables.html"
  SRCFILE="$2"
  # DSTFILE="content/en/documentation/aggregate_tables_doc.htm"
  DSTFILE=content/${LOCALE}/documentation/$(echo $SRCFILE|sed -e s/.html$/_doc.htm/)
  if [ "$LOCALE" != "en" ]; then
    SRCFILE=$(echo $SRCFILE | sed -e "s/\.html/_${LOCALE}.html/")
  fi
  mkdir -p content/${LOCALE}/documentation
  echo :: copy $SRCFILE to $DSTFILE
  case "$SRCFILE" in
  xml_schema.html)
    cp "$SRCFILE" "$DSTFILE";;
  *)
    cat "$SRCFILE" |
    awk '
/doc2web start/ {++x;next;}
/doc2web end/ {++x;next;}
{if (x == 1) print;}
        ' >"$DSTFILE";;
  esac
  n=$(awk '/doc2web include/ {print FNR; exit}' $DSTFILE)
  if [ "$n" ]; then
    mv $DSTFILE /tmp/$$
    (
        head --lines=$(expr $n - 1) /tmp/$$
        cat properties.html
        tail --lines=+$(expr $n + 1) /tmp/$$
        rm /tmp/$$
    ) > $DSTFILE
  fi
}

doImg() {
  test -f "$1" || echo "Image '$1' not found"
}

ROOT=$(cd $(dirname $0); pwd -P)
cd $ROOT

site=changeme@mondrian.pentaho.com
javadoc=true
scp=true
deploy=true
headJavadoc=false

# Remove output from previous run.
rm -rf content

# Build javadoc.
if $javadoc; then
  (
  cd $ROOT/..
  rm -rf doc/api
  mkdir -p doc/api
  ant javadoc-with-ydoc xml_schema
  )
fi

# Create, copy and deploy javadoc for the head revision.
if $headJavadoc; then
  if $javadoc; then
    rm -f headJavadoc.tar.gz
    rm -rf headapi
    mv api headapi
    tar -cvz -f headJavadoc.tar.gz headapi
  fi
  if $scp; then
    pause
    rsync -aPr -e 'ssh -oConnectTimeout=300' headJavadoc.tar.gz ${site}:httpdocs
  fi
  if $deploy; then
    pause
    ssh -oConnectTimeout=300 ${site} <<EOF
      cd httpdocs
      tar xvfz headJavadoc.tar.gz

      # Fix up file permissions
      find headapi -type d | xargs chmod go+rx
      find headapi -type f | xargs chmod go+r

      # Replace references to documents from javadoc.
      find headapi -name \*.html |
      xargs perl -p -i -e '
s!architecture.html!../documentation/architecture.php!;
s!mdx.html!../documentation/mdx.php!;
s!xml_schema.html!../documentation/xml_schema.php!;
s!schema.html!../documentation/schema.php!;
                    '
EOF
  fi
  exit
fi

# E.g. doc/aggregate_tables.html
# becomes website/content/en/documentation/aggregate_tables_doc.htm

doHtml en aggregate_tables.html
doHtml en architecture.html
doHtml en cmdrunner.html
doHtml en cache_control.html
doHtml en components.html
doHtml en configuration.html
doHtml en developer.html
doHtml en developer_notes.html
doHtml en faq.html
doHtml en help.html
# skip: doHtml en index.html
doHtml en install.html
doHtml es install.html
doHtml fr install.html
doHtml en install_postgresql.html
doHtml en mdx.html
doHtml en olap.html
doHtml en optimizing_performance.html
doHtml en roadmap.html
doHtml en schema.html
doHtml en xml_schema.html
doHtml en workbench.html

for i in images/*.png; do
  doImg $i
done

# Remove archive.
rm -f mondrianPentaho.tar.gz

# Create archive, containing html, images, and javadoc.
tar -cvz -f mondrianPentaho.tar.gz content images api

# Copy file to server, and deploy.
if $scp; then
  pause
  rsync -aPr -e 'ssh -oConnectTimeout=300' mondrianPentaho.tar.gz ${site}:httpdocs
fi

if $deploy; then
  pause
  ssh -oConnectTimeout=300 ${site} <<EOF
    cd httpdocs
    tar xvfz mondrianPentaho.tar.gz

    # Fix up file permissions
    find api content images -type d | xargs chmod go+rx
    find api content images -type f | xargs chmod go+r

    # Replace references to documents from javadoc.
    find api -name \*.html |
    xargs perl -p -i -e '
s!architecture.html!../documentation/architecture.php!;
s!cache_control.html!../documentation/cache_control.php!;
s!mdx.html!../documentation/mdx.php!;
s!xml_schema.html!../documentation/xml_schema.php!;
s!schema.html!../documentation/schema.php!;
s!configuration.html!../documentation/configuration.php!;
                        '

    # Change references to javadoc from documents.
    find content -name \*.htm |
    xargs perl -p -i -e '
s! src="images/! src="../images/!g;
s! href="images/! href="/images/!g;
s! href="api! href="../api!g;
s! href="cmdrunner\.html! href="command_runner.php!g;
s! href="developer\.html! href="developers_guide.php!g;
s! href="install\.html! href="installation.php!g;
s! href="performance\.html! href="optimizing_performance.php!g;
s! href="([^/]*)\.html! href="\1.php!g;
                        ' 
EOF
fi

# End doc2web.sh

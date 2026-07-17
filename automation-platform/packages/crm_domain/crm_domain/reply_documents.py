from __future__ import annotations

from html import escape
from io import BytesIO
from zipfile import ZIP_DEFLATED, ZIP_STORED, ZipFile

from crm_domain.models import ComplaintCase


ODT_MIME = "application/vnd.oasis.opendocument.text"


def _paragraphs(value: str, style: str = "Body") -> str:
    lines = value.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    return "".join(
        f'<text:p text:style-name="{style}">{escape(line) if line else " "}</text:p>'
        for line in lines
    )


def build_deo_report_odt(case: ComplaintCase, reply: str) -> bytes:
    number = escape(case.complaint_number or "Complaint number pending")
    details = _paragraphs(case.remarks or "No complaint remarks were recorded.")
    response = _paragraphs(reply)
    content = f'''<?xml version="1.0" encoding="UTF-8"?>
<office:document-content office:version="1.3"
 xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0"
 xmlns:style="urn:oasis:names:tc:opendocument:xmlns:style:1.0"
 xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0"
 xmlns:table="urn:oasis:names:tc:opendocument:xmlns:table:1.0"
 xmlns:fo="urn:oasis:names:tc:opendocument:xmlns:xsl-fo-compatible:1.0">
 <office:automatic-styles>
  <style:style style:name="Title" style:family="paragraph"><style:paragraph-properties fo:text-align="center"/><style:text-properties fo:font-size="16pt" fo:font-weight="bold"/></style:style>
  <style:style style:name="Subtitle" style:family="paragraph"><style:paragraph-properties fo:text-align="center" fo:margin-bottom="0.35in"/><style:text-properties fo:font-size="12pt" fo:font-weight="bold"/></style:style>
  <style:style style:name="Heading" style:family="paragraph"><style:text-properties fo:font-size="10pt" fo:font-weight="bold"/></style:style>
  <style:style style:name="Body" style:family="paragraph"><style:paragraph-properties fo:text-align="justify" fo:line-height="140%" fo:margin-bottom="0.08in"/><style:text-properties fo:font-size="10.5pt"/></style:style>
  <style:style style:name="ReportTable" style:family="table"><style:table-properties style:width="6.6in" table:align="margins"/></style:style>
  <style:style style:name="ReportColumn" style:family="table-column"><style:table-column-properties style:column-width="3.3in"/></style:style>
  <style:style style:name="HeaderCell" style:family="table-cell"><style:table-cell-properties fo:background-color="#DDEFF2" fo:border="0.75pt solid #5E8790" fo:padding="0.09in"/></style:style>
  <style:style style:name="BodyCell" style:family="table-cell"><style:table-cell-properties fo:border="0.75pt solid #8FA6AB" fo:padding="0.12in" style:vertical-align="top"/></style:style>
 </office:automatic-styles>
 <office:body><office:text>
  <text:p text:style-name="Title">OFFICE OF THE DISTRICT EDUCATION OFFICER</text:p>
  <text:p text:style-name="Subtitle">MALE ELEMENTARY EDUCATION, LAHORE</text:p>
  <text:p text:style-name="Heading">COMPLAINT NO. {number}</text:p>
  <text:p text:style-name="Body">Compliance report regarding the complaint received through CM Complaint Portal.</text:p>
  <table:table table:name="ComplaintReport" table:style-name="ReportTable">
   <table:table-column table:style-name="ReportColumn"/><table:table-column table:style-name="ReportColumn"/>
   <table:table-row>
    <table:table-cell table:style-name="HeaderCell"><text:p text:style-name="Heading">Complaint Details</text:p></table:table-cell>
    <table:table-cell table:style-name="HeaderCell"><text:p text:style-name="Heading">Remarks</text:p></table:table-cell>
   </table:table-row>
   <table:table-row>
    <table:table-cell table:style-name="BodyCell">{details}</table:table-cell>
    <table:table-cell table:style-name="BodyCell">{response}</table:table-cell>
   </table:table-row>
  </table:table>
  <text:p text:style-name="Body"> </text:p>
  <text:p text:style-name="Heading">DISTRICT EDUCATION OFFICER</text:p>
  <text:p text:style-name="Body">Male Elementary Education, Lahore</text:p>
 </office:text></office:body>
</office:document-content>'''
    styles = '''<?xml version="1.0" encoding="UTF-8"?>
<office:document-styles office:version="1.3" xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0" xmlns:style="urn:oasis:names:tc:opendocument:xmlns:style:1.0" xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0" xmlns:fo="urn:oasis:names:tc:opendocument:xmlns:xsl-fo-compatible:1.0">
 <office:styles><style:default-style style:family="paragraph"><style:text-properties style:font-name="Liberation Serif" fo:font-size="10.5pt"/></style:default-style></office:styles>
 <office:automatic-styles><style:page-layout style:name="Page"><style:page-layout-properties fo:page-width="8.27in" fo:page-height="11.69in" style:print-orientation="portrait" fo:margin="0.7in"/></style:page-layout></office:automatic-styles>
 <office:master-styles><style:master-page style:name="Standard" style:page-layout-name="Page"/></office:master-styles>
</office:document-styles>'''
    manifest = f'''<?xml version="1.0" encoding="UTF-8"?>
<manifest:manifest manifest:version="1.3" xmlns:manifest="urn:oasis:names:tc:opendocument:xmlns:manifest:1.0">
 <manifest:file-entry manifest:full-path="/" manifest:version="1.3" manifest:media-type="{ODT_MIME}"/>
 <manifest:file-entry manifest:full-path="content.xml" manifest:media-type="text/xml"/>
 <manifest:file-entry manifest:full-path="styles.xml" manifest:media-type="text/xml"/>
</manifest:manifest>'''
    output = BytesIO()
    with ZipFile(output, "w") as archive:
        archive.writestr("mimetype", ODT_MIME, compress_type=ZIP_STORED)
        archive.writestr("content.xml", content, compress_type=ZIP_DEFLATED)
        archive.writestr("styles.xml", styles, compress_type=ZIP_DEFLATED)
        archive.writestr("META-INF/manifest.xml", manifest, compress_type=ZIP_DEFLATED)
    return output.getvalue()

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.incad.vdkcommon.solr;

import cz.incad.vdkcommon.DbUtils;
import cz.incad.vdkcommon.Options;
import cz.incad.vdkcommon.SolrIndexerCommiter;
import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import org.w3c.dom.Document;

/**
 *
 * @author alberto
 */
public class Indexer {

    static final Logger logger = Logger.getLogger(Indexer.class.getName());

    int total = 0;

    String sqlZaznamy = "select zaznam_id, identifikator, uniqueCode, codeType, sourceXML, bohemika from zaznam";
    PreparedStatement psZaznamy;

    private final Options opts;
    Transformer transformer;

    public Indexer() throws Exception {
        opts = Options.getInstance();
        TransformerFactory tfactory = TransformerFactory.newInstance();
        StreamSource xslt = new StreamSource(new File(opts.getString("indexerXSL", "vdk_md5.xsl")));
        transformer = tfactory.newTransformer(xslt);

    }

    public void clean() throws Exception {
        logger.log(Level.INFO, "Cleaning index...");
        String s = "<delete><query>*:*</query></delete>";
        SolrIndexerCommiter.postData(s);
        SolrIndexerCommiter.postData("<commit/>");
        logger.log(Level.INFO, "Index cleaned");
    }

    public void reindex(Connection conn) throws Exception {
        clean();
        index(conn);
    }

    public void reindexDoc(Connection conn, String uniqueCode) throws Exception {
        
        logger.log(Level.INFO, "Cleaning doc from index...");
        String s = "<delete><query>code:"+uniqueCode+"</query></delete>";
        SolrIndexerCommiter.postData(s);
        SolrIndexerCommiter.postData("<commit/>");
        
        indexDoc(conn, uniqueCode);
    }
    public void indexDoc(Connection conn, String uniqueCode) throws Exception {

        try {
            logger.log(Level.INFO, "Indexace doc {0}...", uniqueCode);
            String sql = "select zaznam_id, identifikator, codeType, sourceXML, bohemika from zaznam where uniqueCode=?";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, uniqueCode);
            ResultSet rs = ps.executeQuery();
            StringBuilder sb = new StringBuilder();
            sb.append("<add>");
            while (rs.next()) {

                sb.append(transformXML(rs.getString("sourceXML"),
                        uniqueCode,
                        rs.getString("codeType"),
                        rs.getString("identifikator"),
                        rs.getBoolean("bohemika")));
                if (total % 1000 == 0) {
                    sb.append("</add>");
                    SolrIndexerCommiter.postData(sb.toString());
                    SolrIndexerCommiter.postData("<commit/>");
                    sb = new StringBuilder();
                    sb.append("<add>");
                    logger.log(Level.INFO, "Current indexed docs: {0}", total);
                }

                total++;

            }
            SolrIndexerCommiter.postData("<commit/>");
            logger.log(Level.INFO, "REINDEX FINISHED. Total docs: {0}", total);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Error in reindex", ex);
        }
    }

    public void index(Connection conn) throws Exception {

        try {
            logger.log(Level.INFO, "Indexace zaznamu...");
            psZaznamy = conn.prepareStatement(sqlZaznamy);
            ResultSet rs = psZaznamy.executeQuery();
            StringBuilder sb = new StringBuilder();
            sb.append("<add>");
            while (rs.next()) {
                // check interrupted thread
//                if (Thread.currentThread().isInterrupted()) {
//                    logger.log(Level.INFO, "REINDEX INTERRUPTED. Total records: {0}", total);
//                    throw new InterruptedException();
//                }

                sb.append(transformXML(rs.getString("sourceXML"),
                        rs.getString("uniqueCode"),
                        rs.getString("codeType"),
                        rs.getString("identifikator"),
                        rs.getBoolean("bohemika")));
                if (total % 1000 == 0) {
                    sb.append("</add>");
                    SolrIndexerCommiter.postData(sb.toString());
                    SolrIndexerCommiter.postData("<commit/>");
                    sb = new StringBuilder();
                    sb.append("<add>");
                    logger.log(Level.INFO, "Current indexed docs: {0}", total);
                }

                total++;

            }
            SolrIndexerCommiter.postData("<commit/>");
            logger.log(Level.INFO, "REINDEX FINISHED. Total docs: {0}", total);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Error in reindex", ex);
        }
    }

    public void processXML(File file) throws Exception {
        logger.log(Level.FINE, "Sending {0} to index ...", file.getAbsolutePath());
        StreamResult destStream = new StreamResult(new StringWriter());
        transformer.transform(new StreamSource(file), destStream);
        StringWriter sw = (StringWriter) destStream.getWriter();
        SolrIndexerCommiter.postData(sw.toString());
    }

    public void processXML(Document doc) throws Exception {
        logger.log(Level.FINE, "Sending to index ...");
        StreamResult destStream = new StreamResult(new StringWriter());
        transformer.transform(new DOMSource(doc), destStream);
        StringWriter sw = (StringWriter) destStream.getWriter();
        SolrIndexerCommiter.postData(sw.toString());
    }
    
    private String transformXML(String xml, String uniqueCode, String codeType, String identifier, boolean bohemika) throws Exception {
        logger.log(Level.FINE, "Transforming {0} ...", identifier);
        StreamResult destStream = new StreamResult(new StringWriter());
        transformer.setParameter("uniqueCode", uniqueCode);
        transformer.setParameter("codeType", codeType);
        transformer.setParameter("bohemika", Boolean.toString(bohemika));
        transformer.transform(new StreamSource(new StringReader(xml)), destStream);
        logger.log(Level.FINE, "Sending to index ...");
        StringWriter sw = (StringWriter) destStream.getWriter();
        return sw.toString();
    }

    public void processXML(String xml, String uniqueCode, String codeType, String identifier, boolean bohemika) throws Exception {
        logger.log(Level.FINE, "Transforming {0} ...", identifier);
        StreamResult destStream = new StreamResult(new StringWriter());
        transformer.setParameter("uniqueCode", uniqueCode);
        transformer.setParameter("bohemika", Boolean.toString(bohemika));
        transformer.transform(new StreamSource(new StringReader(xml)), destStream);
        logger.log(Level.FINE, "Sending to index ...");
        StringWriter sw = (StringWriter) destStream.getWriter();
        SolrIndexerCommiter.postData(sw.toString());
    }

    public static void main(String[] args) throws SQLException {
        Connection conn = null;
        try {
            org.postgresql.Driver dr;
            conn = DbUtils.getConnection("org.postgresql.Driver",
                    "jdbc:postgresql://localhost:5432/vdk",
                    "vdk",
                    "vdk");
            Indexer indexer = new Indexer();
            indexer.reindex(conn);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, null, ex);
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }

}

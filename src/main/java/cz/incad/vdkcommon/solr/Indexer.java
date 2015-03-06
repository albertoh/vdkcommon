/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.incad.vdkcommon.solr;

import cz.incad.vdkcommon.Options;
import cz.incad.vdkcommon.SolrIndexerCommiter;
import cz.incad.vdkcommon.VDKJobData;
import cz.incad.vdkcommon.oai.HarvesterJobData;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.json.JSONObject;
import org.w3c.dom.Document;

/**
 *
 * @author alberto
 */
public class Indexer {

    static final Logger logger = Logger.getLogger(Indexer.class.getName());

    int total = 0;
    
    private final String LAST_UPDATE = "last_index_update";
    JSONObject statusJson;

    private final Options opts;
    private final VDKJobData jobData;
    String configFile;
    Transformer transformer;
    Transformer trId;

    

    public Indexer() throws Exception {
        this.jobData = null;
        this.configFile = null;
        opts = Options.getInstance();
        init();
    }
    
    public Indexer(VDKJobData jobData) throws Exception {
        this.jobData = jobData;
        this.configFile = jobData.getConfigFile();
        opts = Options.getInstance();
        init();
    }
    
    public Indexer(String configFile) throws Exception {
        this.configFile = configFile;
        this.jobData = new VDKJobData(configFile, new JSONObject());
        opts = Options.getInstance();
        init();

    }
    
    private  void init() throws Exception{
        TransformerFactory tfactory = TransformerFactory.newInstance();
        StreamSource xslt = new StreamSource(new File(opts.getString("indexerXSL", "vdk_md5.xsl")));
        transformer = tfactory.newTransformer(xslt);
        StreamSource xslt2 = new StreamSource(new File(opts.getString("indexerIdXSL", "vdk_id.xsl")));
        trId = tfactory.newTransformer(xslt2);
        readStatus();
    }

    public void clean() throws Exception {
        logger.log(Level.INFO, "Cleaning index...");
        String s = "<delete><query>*:*</query></delete>";
        SolrIndexerCommiter.postData(s);
        SolrIndexerCommiter.postData("<commit/>");
        logger.log(Level.INFO, "Index cleaned");
    }

    public void reindex() throws Exception {
        clean();
        index();
    }

    public void reindexDoc(String uniqueCode) throws Exception {

        logger.log(Level.INFO, "Cleaning doc {0} from index...", uniqueCode);
        String s = "<delete><query>code:" + uniqueCode + "</query></delete>";
        SolrIndexerCommiter.postData(s);
        SolrIndexerCommiter.postData("<commit/>");

        indexDoc(uniqueCode);
    }

    public void indexDoc(String uniqueCode) throws Exception {

        try {
            logger.log(Level.INFO, "Indexace doc {0}...", uniqueCode);
            StringBuilder sb = new StringBuilder();
            sb.append("<add>");

            SolrQuery query = new SolrQuery("code:\"" + uniqueCode + "\"");
            query.addField("id,code,code_type,xml");
            query.setRows(1000);
            SolrDocumentList docs = IndexerQuery.query(opts.getString("solrIdCore", "vdk_id"), query);
            Iterator<SolrDocument> iter = docs.iterator();
            while (iter.hasNext()) {
                if (jobData.isInterrupted()) {
                    logger.log(Level.INFO, "INDEXER INTERRUPTED");
                    break;
                }
                SolrDocument resultDoc = iter.next();

                boolean bohemika = false;
                if (resultDoc.getFieldValue("bohemika") != null) {
                    bohemika = (Boolean) resultDoc.getFieldValue("bohemika");
                }

                sb.append(transformXML((String) resultDoc.getFieldValue("xml"),
                        uniqueCode,
                        (String) resultDoc.getFieldValue("code_type"),
                        (String) resultDoc.getFieldValue("id"),
                        bohemika));

                total++;

            }
            sb.append("</add>");
            SolrIndexerCommiter.postData(sb.toString());
            SolrIndexerCommiter.postData("<commit/>");
            logger.log(Level.INFO, "REINDEX FINISHED. Total docs: {0}", total);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Error in reindex", ex);
        }
    }

    
    public void removeDoc(String identifier) throws Exception {
        String url = String.format("%s/%s/update",
                opts.getString("solrHost", "http://localhost:8080/solr"),
                opts.getString("solrIdCore", "vdk_id"));
        SolrIndexerCommiter.postData("<delete><id>"+identifier+"</id></delete>");
        SolrIndexerCommiter.postData("<commit/>");
    }
    public void store(String id, String code, String codeType, boolean bohemika, String xml) throws Exception {

        StringBuilder sb = new StringBuilder();
        try {
            logger.log(Level.INFO, "Storing document...");
            sb.append("<add>");

            sb.append(doSorlXML(xml,
                    code,
                    codeType,
                    id,
                    bohemika));
            sb.append("</add>");
            String url = String.format("%s/%s/update",
                    opts.getString("solrHost", "http://localhost:8080/solr"),
                    opts.getString("solrIdCore", "vdk_id"));
            SolrIndexerCommiter.postData(url, sb.toString());
            if (total % 1000 == 0) {
                SolrIndexerCommiter.postData(url, "<commit/>");
                logger.log(Level.INFO, "Current stored docs: {0}", total);
            }

            total++;
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Error storing doc with " + sb.toString(), ex);
        }
    }
    
    private void index() throws Exception{
        update(null);
        writeStatus();
    }
    
    public void update() throws Exception{
        
            

            if(statusJson.has(LAST_UPDATE)){
                update("timestamp:[" + statusJson.getString(LAST_UPDATE) + " TO NOW]");
            }else{
                update(null);
            }
        
            writeStatus();
        
    }
    
    private void readStatus() throws IOException{
        File statusFile = new File(System.getProperty("user.home") + File.separator + ".vdkcr" + File.separator + opts.getString("indexerStatus", "indexer.json"));

        if (statusFile.exists()) {
            statusJson = new JSONObject(FileUtils.readFileToString(statusFile, "UTF-8"));
        } else {
            statusJson = new JSONObject();
        }
        
    }
    
    private void writeStatus() throws FileNotFoundException, IOException {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        String to = sdf.format(date);
        statusJson.put(LAST_UPDATE, to);
        File statusFile = new File(System.getProperty("user.home") + File.separator + ".vdkcr" + File.separator + opts.getString("indexerStatus", "indexer.json"));
        FileUtils.writeStringToFile(statusFile, statusJson.toString());
        logger.log(Level.INFO, "writing to file {0}, \n\t{1}, \n\t{2}", new Object[]{statusFile.getAbsolutePath(),
                        to, statusJson.toString()});

    }

    private void update(String fq) throws Exception {

        try {
            StorageBrowser docs = new StorageBrowser();
            docs.setWt("json");
            docs.setFl("id,code,code_type,bohemika,xml");
            if(fq != null){
                docs.setStart(fq);
            }
            Iterator it = docs.iterator();
            StringBuilder sb = new StringBuilder();
            sb.append("<add>");
            while (it.hasNext()) {
                JSONObject doc = (JSONObject) it.next();
                
                boolean bohemika = false;
                if (doc.has("bohemika")) {
                    bohemika = (Boolean) doc.getBoolean("bohemika");
                }

                sb.append(transformXML((String) doc.optString("xml",""),
                        doc.getString("code"),
                        (String) doc.getString("code_type"),
                        (String) doc.getString("id"),
                        bohemika));
                
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
            sb.append("</add>");
            SolrIndexerCommiter.postData(sb.toString());

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

    private String doSorlXML(String xml, String uniqueCode, String codeType, String identifier, boolean bohemika) throws Exception {
        logger.log(Level.FINE, "Transforming {0} ...", identifier);
        StreamResult destStream = new StreamResult(new StringWriter());
        trId.setParameter("uniqueCode", uniqueCode);
        trId.setParameter("codeType", codeType);
        trId.setParameter("bohemika", Boolean.toString(bohemika));
        trId.setParameter("sourceXml", xml);
        trId.transform(new StreamSource(new StringReader(xml)), destStream);
        StringWriter sw = (StringWriter) destStream.getWriter();
        return sw.toString();
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
//            org.postgresql.Driver dr;
//            conn = DbUtils.getConnection("org.postgresql.Driver",
//                    "jdbc:postgresql://localhost:5432/vdk",
//                    "vdk",
//                    "vdk");
            Indexer indexer = new Indexer("vkol");
            indexer.reindex();
        } catch (Exception ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }

}

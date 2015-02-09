package cz.incad.vdkcommon.oai;

import cz.incad.vdkcommon.Bohemika;
import cz.incad.vdkcommon.DbUtils;
import cz.incad.vdkcommon.xml.XMLReader;
import cz.incad.vdkcommon.Options;
import cz.incad.vdkcommon.Interval;
import cz.incad.vdkcommon.Slouceni;
import cz.incad.vdkcommon.db.Zaznam;
import cz.incad.vdkcommon.solr.Indexer;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;

/**
 *
 * @author alberto
 */
public class OAIHarvester {

    private static final Logger logger = Logger.getLogger(OAIHarvester.class.getName());
    private JSONObject opts;
    XMLReader xmlReader;
    Connection conn;
    private String metadataPrefix;
    private int interval;
    String completeListSize;
    int currentDocsSent = 0;
    int currentIndex = 0;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd/HH");
    SimpleDateFormat sdfoai;
    Transformer xformer;

    String homeDir;
    BufferedWriter logFile;
    BufferedWriter errorLogFile;

    private boolean saveToDisk = false;
    private boolean fromDisk = false;
    private boolean fullIndex = false;
    private boolean onlyIdentifiers = false;
    private boolean onlyHarvest = false;
    private boolean continueOnDocError = false;
    private String from;
    private String to;
    private String resumptionToken = null;
    private int startIndex;
    private int maxDocuments = -1;
    private String pathToData;
    String configFile;

    String sqlReindex = "select sourceXML, bohemika from zaznam where uniqueCode=?";
    PreparedStatement psReindex;
    String sqlZaznam = "select zaznam_id, uniquecode from zaznam where identifikator=?";
    PreparedStatement psZaznam;
    private boolean dontIndex;
    String sqlRemoveZaznam = "delete from zaznam where identifikator=?";
    PreparedStatement psRemoveZaznam;
    
    Indexer indexer = new Indexer();

    Zaznam zaznam;

    public OAIHarvester(String configFile) throws Exception {
        this.configFile = configFile;
        conn = DbUtils.getConnection();
        init();
    }

    public OAIHarvester(Connection conn, String configFile) throws Exception {
        this.configFile = configFile;
        this.conn = conn;
        init();
    }

    private void init() throws Exception {
        String path = System.getProperty("user.home") + File.separator + ".vdkcr" + File.separator + configFile + ".json";
        File fdef = FileUtils.toFile(Options.class.getResource("/cz/incad/vdkcommon/oai.json"));

        String json = FileUtils.readFileToString(fdef, "UTF-8");
        opts = new JSONObject(json);
        File f = new File(path);
        if (f.exists() && f.canRead()) {
            json = FileUtils.readFileToString(f, "UTF-8");
            JSONObject confCustom = new JSONObject(json);
            Iterator keys = confCustom.keys();
            while (keys.hasNext()) {
                String key = (String) keys.next();
                logger.log(Level.INFO, "key {0} will be overrided", key);
                opts.put(key, confCustom.get(key));
            }
        }

        this.homeDir = opts.optString("homeDir", ".vdkcr")
                + File.separator;
        this.saveToDisk = opts.optBoolean("saveToDisk", true);
        this.fullIndex = opts.optBoolean("fullIndex", false);
        this.onlyHarvest = opts.optBoolean("onlyHarvest", false);
        this.startIndex = opts.optInt("startIndex", -1);
        
        this.pathToData =opts.getString("indexDirectory");
        
        try {
            File dir = new File(this.homeDir + "logs");
            if (!dir.exists()) {
                boolean success = dir.mkdirs();
                if (!success) {
                    logger.log(Level.WARNING, "Can''t create logs directory");
                }
            }

        } catch (SecurityException ex) {
            logger.log(Level.SEVERE, null, ex);
            throw new Exception(ex);
        }
        xmlReader = new XMLReader();

        psReindex = conn.prepareStatement(sqlReindex);
        psZaznam = conn.prepareStatement(sqlZaznam);

        zaznam = new Zaznam(conn);

        sdfoai = new SimpleDateFormat(opts.getString("oaiDateFormat"));
        sdf = new SimpleDateFormat(opts.getString("filePathFormat"));

        this.setMetadataPrefix(opts.getString("metadataPrefix"));

        interval = Interval.parseString(opts.getString("interval"));
        xformer = TransformerFactory.newInstance().newTransformer();

        logger.info("Harvester initialized");

    }

    private void getRecordsFromDisk() throws Exception {
        logger.info("Processing dowloaded files");
        getRecordsFromDir(new File(this.pathToData));
        
    }

    private void getRecordsFromDir(File dir) throws Exception {
        File[] children = dir.listFiles();
        Arrays.sort(children);
        for (File child : children) {
            if (currentDocsSent >= this.maxDocuments && this.maxDocuments > 0) {
                break;
            }
            if (child.isDirectory()) {
                getRecordsFromDir(child);
            } else {
                String identifier;
                logger.log(Level.INFO, "Loading file {0}", child.getPath());
                xmlReader.loadXmlFromFile(child);
                NodeList nodes = xmlReader.getListOfNodes("//oai:record");
                for (int j = 0; j < nodes.getLength(); j++) {
                    if (currentIndex > this.startIndex) {
                        identifier = xmlReader.getNodeValue("//oai:record[position()=" + (j + 1) + "]/oai:header/oai:identifier/text()");

                        processRecord(nodes.item(j), identifier, j + 1);
                    }
                    currentIndex++;
                }
                logger.log(Level.INFO, "number: {0}", currentDocsSent);

//                if (!arguments.dontIndex) {
//                    indexer.processXML(children[i]);
//                    indexer.commit();
//                }
            }
        }
    }

    public int harvest() throws Exception {

        try {
            logFile = new BufferedWriter(new FileWriter(this.homeDir + "logs" + File.separator + configFile + ".log"));
            errorLogFile = new BufferedWriter(new FileWriter(this.homeDir + "logs" + File.separator + configFile + ".error.log"));

            long startTime = (new Date()).getTime();
            currentIndex = 0;

            if (this.fromDisk) {
                getRecordsFromDisk();
            } else {
                File updateTimeFile = new File(this.homeDir + opts.getString("updateTimeFile"));
                if (resumptionToken != null) {
                    logger.log(Level.INFO, "updating with resumptionToken: {0}", resumptionToken);
                    getRecordWithResumptionToken(this.resumptionToken);
                } else {
                    if (this.fullIndex) {
                        setFrom(getInitialDate());
                    } else {
                        if (updateTimeFile.exists()) {
                            BufferedReader in = new BufferedReader(new FileReader(updateTimeFile));
                            setFrom(in.readLine());
                        } else {
                            setFrom(getInitialDate());
                        }
                    }
                    logger.log(Level.INFO, "updating from: " + from);
                    update(from);
                }
            }

            logFile.newLine();
            logFile.write("Harvest success " + currentDocsSent + " records");

            long timeInMiliseconds = (new Date()).getTime() - startTime;
            logger.log(Level.INFO, "HARVEST SUCCESS {0} records", currentDocsSent);
            logger.info(formatElapsedTime(timeInMiliseconds));

        } catch (Exception ex) {
            logger.log(Level.SEVERE, null, ex);
            throw new Exception(ex);
        } finally {
            try {
                if (logFile != null) {
                    logFile.flush();
                    logFile.close();
                }
                if (errorLogFile != null) {
                    errorLogFile.flush();
                    errorLogFile.close();
                }
            } catch (IOException ex) {
                logger.log(Level.WARNING, null, ex);
            }
        }
        return currentDocsSent;
    }

    private void writeResponseDate(String from) throws FileNotFoundException, IOException {
        File updateTimeFile = new File(this.homeDir + opts.getString("updateTimeFile"));
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(updateTimeFile)));
        out.write(from);
        out.close();
    }

    private void update(String from) throws Exception {
        Calendar c_from = Calendar.getInstance();
        c_from.setTime(sdfoai.parse(from));
        Calendar c_to = Calendar.getInstance();
        c_to.setTime(sdfoai.parse(from));

        c_to.add(interval, 1);

        String to;
        Date date = new Date();
        //sdfoai.setTimeZone(TimeZone.getTimeZone("GMT"));

        if (this.to == null) {
            to = sdfoai.format(date);
        } else {
            to = this.to;
        }
        Date final_date = sdfoai.parse(to);
        Date current = c_to.getTime();

        while (current.before(final_date)) {
            update(sdfoai.format(c_from.getTime()), sdfoai.format(current));
            c_to.add(interval, 1);
            c_from.add(interval, 1);
            current = c_to.getTime();
        }
        update(sdfoai.format(c_from.getTime()), sdfoai.format(final_date));

        writeResponseDate(to);

    }

    private void update(String from, String until) throws Exception {
        logger.log(Level.INFO, "Harvesting from: {0} until: {1}", new Object[]{from, until});
        //responseDate = from;
        writeResponseDate(from);
        getRecords(from, until);
        writeResponseDate(until);
    }

    private void getRecordWithResumptionToken(String resumptionToken) throws Exception {
        while (resumptionToken != null && !resumptionToken.equals("")) {
            resumptionToken = getRecords("?verb=" + opts.getString("verb") + "&resumptionToken=" + resumptionToken);
        }
    }

    private String getInitialDate() throws Exception {
        logger.log(Level.INFO, "Retrieving initial date...");

        String urlString = opts.getString("baseUrl") + "?verb=Identify";

        URL url = new URL(urlString.replace("\n", ""));

        logger.log(Level.FINE, "url: {0}", url.toString());
        xmlReader.readUrl(url.toString());
        return xmlReader.getNodeValue("//oai:Identify/oai:earliestDatestamp/text()");
    }

    private void getRecords(String from, String until) throws Exception {
        String query = String.format("?verb=%s&from=%s&until=%s&metadataPrefix=%s&set=%s",
                opts.getString("verb"),
                from,
                until,
                metadataPrefix,
                opts.getString("set"));
        resumptionToken = getRecords(query);
        while (resumptionToken != null && !resumptionToken.equals("")) {
            resumptionToken = getRecords("?verb=" + opts.getString("verb") + "&resumptionToken=" + resumptionToken);
        }
    }

    private String getRecords(String query) throws Exception {

        // check interrupted thread
        if (Thread.currentThread().isInterrupted()) {
            logger.log(Level.INFO, "HARVESTER INTERRUPTED");
            throw new InterruptedException();
        }

        String urlString = opts.getString("baseUrl") + query;
        URL url = new URL(urlString.replace("\n", ""));
        logFile.newLine();
        logFile.write(url.toString());
        logFile.flush();
        try {
            xmlReader.readUrl(url.toString());
        } catch (Exception ex) {
            logger.log(Level.WARNING, ex.toString());
            logFile.newLine();
            logFile.write("retrying url: " + url.toString());
            xmlReader.readUrl(url.toString());
        }
        String error = xmlReader.getNodeValue("//oai:error/@code");
        if (error.equals("")) {
            completeListSize = xmlReader.getNodeValue("//oai:resumptionToken/@completeListSize");
            String date;
            String identifier;

            String fileName = null;
            if (this.isSaveToDisk()) {
                fileName = writeNodeToFile(xmlReader.getNodeElement(),
                        xmlReader.getNodeValue("//oai:record[position()=1]/oai:header/oai:datestamp/text()"),
                        xmlReader.getNodeValue("//oai:record[position()=1]/oai:header/oai:identifier/text()"));
            }
            NodeList nodes = xmlReader.getListOfNodes("//oai:record");
            if (this.onlyIdentifiers) {
                //TODO
            } else {
                if (!this.onlyHarvest && currentIndex > this.startIndex) {

                    for (int i = 0; i < nodes.getLength(); i++) {
                        identifier = xmlReader.getNodeValue("//oai:record[position()=" + (i + 1) + "]/oai:header/oai:identifier/text()");

                        processRecord(nodes.item(i), identifier, i + 1);
                        currentIndex++;
                        logger.log(Level.FINE, "number: {0} of {1}", new Object[]{(currentDocsSent), completeListSize});
                    }
                    //context.getAplikatorService().processRecords(rc);
                }
            }

            return xmlReader.getNodeValue("//oai:resumptionToken/text()");
        } else {
            logger.log(Level.INFO, "{0} for url {1}", new Object[]{error, urlString});
        }
        return null;
    }

    private String writeNodeToFile(Node node, String date, String identifier) throws Exception {
        String dirName = opts.getString("indexDirectory") + File.separatorChar + sdf.format(sdfoai.parse(date));

        File dir = new File(dirName);
        if (!dir.exists()) {
            boolean success = dir.mkdirs();
            if (!success) {
                logger.log(Level.WARNING, "Can''t create: {0}", dirName);
            }
        }
        String xmlFileName = dirName + File.separatorChar + identifier.substring(opts.getString("identifierPrefix").length()) + ".xml";

        Source source = new DOMSource(node);
        File file = new File(xmlFileName);
        Result result = new StreamResult(file);
        xformer.transform(source, result);
        return xmlFileName;
    }

    private String nodeToString(Node node, int pos) throws Exception {

        String xslt = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><xsl:stylesheet version=\"1.0\" xmlns:oai=\"http://www.openarchives.org/OAI/2.0/\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\"  >"
                + "<xsl:output omit-xml-declaration=\"yes\" method=\"xml\" indent=\"yes\" encoding=\"UTF-8\" />"
                + "<xsl:template  match=\"/\"><xsl:copy-of select=\"//oai:ListRecords/oai:record[position()=" + pos + "]\" /></xsl:template>"
                + "</xsl:stylesheet>";
        Transformer xformer2 = TransformerFactory.newInstance().newTransformer(new StreamSource(new StringReader(xslt)));
        StringWriter sw = new StringWriter();

        Source source = new DOMSource(node);

        xformer2.transform(source, new StreamResult(sw));
        return sw.toString();
    }

    private String nodeToString(Node node) throws Exception {

        StringWriter sw = new StringWriter();

        Source source = new DOMSource(node);

        xformer.transform(source, new StreamResult(sw));
        //System.out.println("sw.toString(): " + sw.toString());
        return sw.toString();
    }

    private String formatElapsedTime(long timeInMiliseconds) {
        long hours, minutes, seconds;
        long timeInSeconds = timeInMiliseconds / 1000;
        hours = timeInSeconds / 3600;
        timeInSeconds = timeInSeconds - (hours * 3600);
        minutes = timeInSeconds / 60;
        timeInSeconds = timeInSeconds - (minutes * 60);
        seconds = timeInSeconds;
        return hours + " hour(s) " + minutes + " minute(s) " + seconds + " second(s)";
    }

    public static void main(String[] args) throws Exception {

        Connection conn = null;
        try {
            org.postgresql.Driver dr;
            conn = DbUtils.getConnection("org.postgresql.Driver",
                    "jdbc:postgresql://localhost:5432/vdk",
                    "vdk",
                    "vdk");
            OAIHarvester oh = new OAIHarvester(conn, "nkc_vdk");
            //oh.setSaveToDisk(true);
            oh.setFromDisk(true);
            oh.setPathToData("/home/alberto/.vdkcr/OAI/harvest/NKC/2014/08/28/09/20/45");
            
            oh.harvest();

            //oh.harvest("-cfgFile nkp_vdk -dontIndex -fromDisk ", null, null);
            //oh.harvest("-cfgFile VKOL -dontIndex -saveToDisk -onlyHarvest", null, null);201304191450353201304220725599VKOLOAI:VKOL-M
            //oh.harvest("-cfgFile VKOL -dontIndex -saveToDisk -onlyHarvest resumptionToken 201304191450353201304220725599VKOLOAI:VKOL-M", null, null);
            //oh.harvest("-cfgFile MZK01-VDK -dontIndex -saveToDisk -onlyHarvest", null, null);
            //oh.harvest("-cfgFile MZK03-VDK -dontIndex -saveToDisk -onlyHarvest", null, null);
            //oh.harvest("-cfgFile nkp_vdk -dontIndex -saveToDisk -onlyHarvest ", null, null);
            //oh.harvest("-cfgFile nkp_vdk -dontIndex -saveToDisk -onlyHarvest -resumptionToken 201305160612203201305162300009NKC-VDK:NKC-VDKM", null, null);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, null, ex);
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }

    }

    /**
     * @param metadataPrefix the metadataPrefix to set
     */
    public void setMetadataPrefix(String metadataPrefix) {
        this.metadataPrefix = metadataPrefix;
    }

    /**
     * @param fromDisk the saveToDisk to set
     */
    public void setFromDisk(boolean fromDisk) {
        this.fromDisk = fromDisk;
    }

    /**
     * @return the saveToDisk
     */
    public boolean isSaveToDisk() {
        return saveToDisk;
    }

    /**
     * @param saveToDisk the saveToDisk to set
     */
    public void setSaveToDisk(boolean saveToDisk) {
        this.saveToDisk = saveToDisk;
    }

    /**
     * @param from the from to set
     */
    public void setFrom(String from) {
        this.from = from;
    }

    /**
     * @param to the to to set
     */
    public void setTo(String to) {
        this.to = to;
    }

    /**
     * @param pathToData the from to set
     */
    public void setPathToData(String pathToData) {
        this.pathToData = pathToData;
    }

    /**
     * @param fullIndex the fullIndex to set
     */
    public void setFullIndex(boolean fullIndex) {
        this.fullIndex = fullIndex;
    }

    /**
     * @param resumptionToken the resumptionToken to set
     */
    public void setResumptionToken(String resumptionToken) {
        this.resumptionToken = resumptionToken;
    }

    /**
     * @param onlyIdentifiers the onlyIdentifiers to set
     */
    public void setOnlyIdentifiers(boolean onlyIdentifiers) {
        this.onlyIdentifiers = onlyIdentifiers;
    }

    /**
     * @param onlyHarvest the onlyHarvest to set
     */
    public void setOnlyHarvest(boolean onlyHarvest) {
        this.onlyHarvest = onlyHarvest;
    }

    /**
     * @param maxDocuments the startIndex to set
     */
    public void setMaxDocuments(int maxDocuments) {
        this.maxDocuments = maxDocuments;
    }

    /**
     * @param continueOnDocError the onlyHarvest to set
     */
    public void setContinueOnDocError(boolean continueOnDocError) {
        this.continueOnDocError = continueOnDocError;
    }

    /**
     * @param startIndex the startIndex to set
     */
    public void setStartIndex(int startIndex) {
        this.startIndex = startIndex;
    }

    /**
     *
     * @param node the xml node in the file
     * @param identifier the identifier of the oai record
     * @param index the index of the node in the xml file
     */
    private void processRecord(Node node, String identifier, int index) throws InterruptedException, Exception {
        // check interrupted thread
        if (Thread.currentThread().isInterrupted()) {

            logger.log(Level.INFO, "HARVESTER INTERRUPTED");
            throw new InterruptedException();
        }
        if (node != null) {
            zaznam.clearParams();

//            JSONObject json = org.json.XML.toJSONObject(nodeToString(node));
//            logger.log(Level.INFO, "JSON for identifier {0} is: ", json);
            String error = xmlReader.getNodeValue(node, "/oai:error/@code");
            if (error == null || error.equals("")) {
                ZaznamData oldZaznam = getZaznam(identifier);
                
                

                String urlZdroje = opts.getString("baseUrl")
                        + "?verb=GetRecord&identifier=" + identifier
                        + "&metadataPrefix=" + metadataPrefix
                        + "#set=" + opts.getString("set");

                if ("deleted".equals(xmlReader.getNodeValue(node, "./oai:header/@status"))) {
                    if (this.fullIndex) {
                        logger.log(Level.FINE, "Skip deleted record when fullindex");
                        return;
                    }
                    //zpracovat deletes
                    if (!this.isDontIndex()) {
                        removeZaznam(identifier);
                        if(oldZaznam != null){
                            indexer.reindexDoc(conn, oldZaznam.getUniqueCode());
                           
                        }
                    }
                } else {
//                    String xmlStr = nodeToString(xmlReader.getNodeElement(), index);
                    String xmlStr = nodeToString(node);
//                    System.out.println(xmlStr);

                    String hlavninazev = xmlReader.getNodeValue(node, "./oai:metadata/marc:record/marc:datafield[@tag='245']/marc:subfield[@code='a']/text()");

                    String cnbStr = xmlReader.getNodeValue(node, "./oai:metadata/marc:record/marc:datafield[@tag='015']/marc:subfield[@code='a']/text()");

                    JSONObject slouceni = Slouceni.fromXml(xmlStr);

                    zaznam.knihovna = opts.getString("knihovna");
                    zaznam.identifikator = identifier;
                    zaznam.uniqueCode = slouceni.getString("docCode");
                    zaznam.codeType = slouceni.getString("codeType");
                    zaznam.urlZdroje = urlZdroje;
                    zaznam.hlavniNazev = hlavninazev;
                    zaznam.bohemika = Bohemika.isBohemika(xmlStr);

                    String typDokumentu = "";
                    String leader = xmlReader.getNodeValue(node, "./oai:metadata/marc:record/marc:leader/text()");
                    if (leader != null && leader.length() > 9) {
                        typDokumentu = typDokumentu(leader);
                    }
                    zaznam.typDokumentu = typDokumentu;
                    zaznam.sourceXML = xmlStr;

                    try {
                        if(oldZaznam == null){
                            zaznam.insert();
                        }else{
                            zaznam.update(oldZaznam.getId());
                        }
                        currentDocsSent++;
                    } catch (Exception ex) {
                        if (this.continueOnDocError) {
                            errorLogFile.newLine();
                            errorLogFile.write("Error writing docs to db. Id: " + identifier);
                            errorLogFile.flush();
                            logger.log(Level.WARNING, "Error writing doc to db. Id: {0}", identifier);
                        } else {
                            throw new Exception(ex);
                        }
                    }

                    //try {
                    if (!this.isDontIndex()) {
                        if(oldZaznam != null && oldZaznam.getUniqueCode().equals(zaznam.uniqueCode)){
                            indexer.reindexDoc(conn, oldZaznam.getUniqueCode());
                        }
                        if (oldZaznam != null) {
                            indexer.reindexDoc(conn, zaznam.uniqueCode);
                        } else {
                            //indexer.processXML(xmlStr, uniqueCode, codeType, identifier, Bohemika.isBohemika(xmlStr));
                        }

                    }

                }
            } else {
                logger.log(Level.SEVERE, "Can't proccess xml {0}", error);
            }
        }
    }
    
    private ZaznamData getZaznam(String identifier) throws SQLException{
        ZaznamData ret = null;
        psZaznam.setString(1, identifier);
        ResultSet rs = psZaznam.executeQuery();
        if(rs.next()){
            ret = new ZaznamData();
            ret.setId(rs.getInt(1));
            ret.setUniqueCode(rs.getString(2));
        }
        rs.close();
        return ret;
    }

    
    private void removeZaznam(String identifier) throws SQLException{
        psRemoveZaznam.setString(1, identifier);
        psRemoveZaznam.executeUpdate();
        
    }

    private String typDokumentu(String leader) {
        if (leader != null && leader.length() > 9) {
            String code = leader.substring(6, 8);
            if ("aa".equals(code)
                    || "ac".equals(code)
                    || "ad".equals(code)
                    || "am".equals(code)
                    || code.startsWith("t")) {
                return "BK";
            } else if ("bi".equals(code)
                    || "bs".equals(code)) {
                return "SE";
            } else if (code.startsWith("p")) {
                return "MM";
            } else if (code.startsWith("e")) {
                return "MP";
            } else if (code.startsWith("f")) {
                return "MP";
            } else if (code.startsWith("g")) {
                return "VM";
            } else if (code.startsWith("k")) {
                return "VM";
            } else if (code.startsWith("o")) {
                return "VM";
            } else if (code.startsWith("c")) {
                return "MU";
            } else if (code.startsWith("d")) {
                return "MU";
            } else if (code.startsWith("i")) {
                return "MU";
            } else if (code.startsWith("j")) {
                return "MU";
            } else {
                return code;
            }
        } else {
            return "none";
        }
    }

    /**
     * @return the dontIndex
     */
    public boolean isDontIndex() {
        return dontIndex;
    }

    /**
     * @param dontIndex the dontIndex to set
     */
    public void setDontIndex(boolean dontIndex) {
        this.dontIndex = dontIndex;
    }

    private static class ZaznamData {
        private String uniqueCode;
        private int id;
        public ZaznamData() {
        }

        /**
         * @return the uniqueCode
         */
        public String getUniqueCode() {
            return uniqueCode;
        }

        /**
         * @param uniqueCode the uniqueCode to set
         */
        public void setUniqueCode(String uniqueCode) {
            this.uniqueCode = uniqueCode;
        }

        /**
         * @return the id
         */
        public int getId() {
            return id;
        }

        /**
         * @param id the id to set
         */
        public void setId(int id) {
            this.id = id;
        }
    }
}

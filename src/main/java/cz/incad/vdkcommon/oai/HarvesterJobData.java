/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.incad.vdkcommon.oai;

import cz.incad.vdkcommon.Interval;
import cz.incad.vdkcommon.Options;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;

/**
 *
 * @author alberto
 */
public class HarvesterJobData {
    
    private static Logger logger = Logger.getLogger(HarvesterJobData.class.getName());

    
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd/HH");
    private SimpleDateFormat sdfoai;
    
    
    private JSONObject opts;
    private String homeDir;
    

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
    private String configFile;
    private String metadataPrefix;
    
    private boolean dontIndex;
    private String name;
    private int interval;
    
    private boolean interrupted = false;
    
    public HarvesterJobData(String name, String conf) throws Exception{
        this.configFile = conf;
        this.name = name;
        init();
    }
    
    private void init() throws Exception {
        String path = System.getProperty("user.home") + File.separator + ".vdkcr" + File.separator + getConfigFile() + ".json";
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

        this.setHomeDir(getOpts().optString("homeDir", ".vdkcr") + File.separator);
        this.setSaveToDisk(getOpts().optBoolean("saveToDisk", true));
        this.setFullIndex(getOpts().optBoolean("fullIndex", false));
        this.setOnlyHarvest(getOpts().optBoolean("onlyHarvest", false));
        this.setStartIndex(getOpts().optInt("startIndex", -1));

        this.setPathToData(getOpts().getString("indexDirectory"));

        
        this.sdfoai = new SimpleDateFormat(opts.getString("oaiDateFormat"));
        this.sdf = new SimpleDateFormat(opts.getString("filePathFormat"));

        

        this.setMetadataPrefix(getOpts().getString("metadataPrefix"));

        setInterval(Interval.parseString(getOpts().getString("interval")));
        

        logger.info("HarvesterJobData initialized");

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
     * @return the fromDisk
     */
    public boolean isFromDisk() {
        return fromDisk;
    }

    /**
     * @param fromDisk the fromDisk to set
     */
    public void setFromDisk(boolean fromDisk) {
        this.fromDisk = fromDisk;
    }

    /**
     * @return the fullIndex
     */
    public boolean isFullIndex() {
        return fullIndex;
    }

    /**
     * @param fullIndex the fullIndex to set
     */
    public void setFullIndex(boolean fullIndex) {
        this.fullIndex = fullIndex;
    }

    /**
     * @return the resumptionToken
     */
    public String getResumptionToken() {
        return resumptionToken;
    }

    /**
     * @param resumptionToken the resumptionToken to set
     */
    public void setResumptionToken(String resumptionToken) {
        this.resumptionToken = resumptionToken;
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

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the sdf
     */
    public SimpleDateFormat getSdf() {
        return sdf;
    }

    /**
     * @param sdf the sdf to set
     */
    public void setSdf(SimpleDateFormat sdf) {
        this.sdf = sdf;
    }

    /**
     * @return the sdfoai
     */
    public SimpleDateFormat getSdfoai() {
        return sdfoai;
    }

    /**
     * @param sdfoai the sdfoai to set
     */
    public void setSdfoai(SimpleDateFormat sdfoai) {
        this.sdfoai = sdfoai;
    }

    /**
     * @return the opts
     */
    public JSONObject getOpts() {
        return opts;
    }

    /**
     * @param opts the opts to set
     */
    public void setOpts(JSONObject opts) {
        this.opts = opts;
    }

    /**
     * @return the homeDir
     */
    public String getHomeDir() {
        return homeDir;
    }

    /**
     * @param homeDir the homeDir to set
     */
    public void setHomeDir(String homeDir) {
        this.homeDir = homeDir;
    }

    /**
     * @return the onlyIdentifiers
     */
    public boolean isOnlyIdentifiers() {
        return onlyIdentifiers;
    }

    /**
     * @param onlyIdentifiers the onlyIdentifiers to set
     */
    public void setOnlyIdentifiers(boolean onlyIdentifiers) {
        this.onlyIdentifiers = onlyIdentifiers;
    }

    /**
     * @return the onlyHarvest
     */
    public boolean isOnlyHarvest() {
        return onlyHarvest;
    }

    /**
     * @param onlyHarvest the onlyHarvest to set
     */
    public void setOnlyHarvest(boolean onlyHarvest) {
        this.onlyHarvest = onlyHarvest;
    }

    /**
     * @return the continueOnDocError
     */
    public boolean isContinueOnDocError() {
        return continueOnDocError;
    }

    /**
     * @param continueOnDocError the continueOnDocError to set
     */
    public void setContinueOnDocError(boolean continueOnDocError) {
        this.continueOnDocError = continueOnDocError;
    }

    /**
     * @return the from
     */
    public String getFrom() {
        return from;
    }

    /**
     * @param from the from to set
     */
    public void setFrom(String from) {
        this.from = from;
    }

    /**
     * @return the to
     */
    public String getTo() {
        return to;
    }

    /**
     * @param to the to to set
     */
    public void setTo(String to) {
        this.to = to;
    }

    /**
     * @return the startIndex
     */
    public int getStartIndex() {
        return startIndex;
    }

    /**
     * @param startIndex the startIndex to set
     */
    public void setStartIndex(int startIndex) {
        this.startIndex = startIndex;
    }

    /**
     * @return the maxDocuments
     */
    public int getMaxDocuments() {
        return maxDocuments;
    }

    /**
     * @param maxDocuments the maxDocuments to set
     */
    public void setMaxDocuments(int maxDocuments) {
        this.maxDocuments = maxDocuments;
    }

    /**
     * @return the pathToData
     */
    public String getPathToData() {
        return pathToData;
    }

    /**
     * @param pathToData the pathToData to set
     */
    public void setPathToData(String pathToData) {
        this.pathToData = pathToData;
    }

    /**
     * @return the configFile
     */
    public String getConfigFile() {
        return configFile;
    }

    /**
     * @param configFile the configFile to set
     */
    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

    /**
     * @return the metadataPrefix
     */
    public String getMetadataPrefix() {
        return metadataPrefix;
    }

    /**
     * @param metadataPrefix the metadataPrefix to set
     */
    public void setMetadataPrefix(String metadataPrefix) {
        this.metadataPrefix = metadataPrefix;
    }

    /**
     * @return the interval
     */
    public int getInterval() {
        return interval;
    }

    /**
     * @param interval the interval to set
     */
    public void setInterval(int interval) {
        this.interval = interval;
    }

    /**
     * @return the interrupted
     */
    public boolean isInterrupted() {
        return interrupted;
    }

    /**
     * @param interrupted the interrupted to set
     */
    public void setInterrupted(boolean interrupted) {
        this.interrupted = interrupted;
    }
}

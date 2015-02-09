/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.incad.vdkcommon.oai;

/**
 *
 * @author alberto
 */
public class HarvesterJobData {
    private final String conf;
    
    public HarvesterJobData(String conf){
        this.conf = conf;
    }

    /**
     * @return the conf
     */
    public String getConf() {
        return conf;
    }
}

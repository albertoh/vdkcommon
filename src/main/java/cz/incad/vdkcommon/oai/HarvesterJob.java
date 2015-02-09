/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.incad.vdkcommon.oai;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;

/**
 *
 * @author alberto
 */
public class HarvesterJob implements Job {

    private static final Logger LOGGER = Logger.getLogger(HarvesterJob.class.getName());

    @Override
    public void execute(JobExecutionContext jec) throws JobExecutionException {
        try {
            String jobKey = jec.getJobDetail().getKey().toString();
            int i = 0;
            for (JobExecutionContext j : jec.getScheduler().getCurrentlyExecutingJobs()) {
                if (jobKey.equals(j.getJobDetail().getKey().toString())) {
                    i++;
                }
            }
            if (i > 1) {
                LOGGER.log(Level.INFO, "jobKey {0} is still running. Nothing to do.", jobKey);
                return;
            }

            
            JobDataMap data = jec.getJobDetail().getJobDataMap();
            HarvesterJobData jobdata = (HarvesterJobData) data.get("jobdata");
            
            OAIHarvester oh = new OAIHarvester(jobdata.getConf());
                                    //oh.setSaveToDisk(jobdata.isSetFullIndex());
                                    //oh.setFullIndex(jobdata.isSetFullIndex());
                                    oh.harvest();
            
            LOGGER.log(Level.INFO, "jobKey: {0}", jobKey);

            

        } catch (SchedulerException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(HarvesterJob.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
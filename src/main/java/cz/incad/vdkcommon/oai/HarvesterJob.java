/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.incad.vdkcommon.oai;

import cz.incad.vdkcommon.VDKScheduler;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.quartz.DateBuilder.evenMinuteDate;
import org.quartz.InterruptableJob;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import static org.quartz.TriggerBuilder.newTrigger;
import org.quartz.UnableToInterruptJobException;
import org.quartz.core.jmx.JobDataMapSupport;
import org.quartz.impl.StdSchedulerFactory;

/**
 *
 * @author alberto
 */
public class HarvesterJob implements InterruptableJob {

    private static final Logger LOGGER = Logger.getLogger(HarvesterJob.class.getName());
    HarvesterJobData jobdata;

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
            jobdata = (HarvesterJobData) data.get("jobdata");

            OAIHarvester oh = new OAIHarvester(jobdata);
            
            oh.harvest();

            LOGGER.log(Level.INFO, "jobKey: {0}", jobKey);

        } catch (SchedulerException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(HarvesterJob.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void harvestScheduled(HarvesterJobData jobdata) {

        try {
            String name = jobdata.getName();
            Scheduler sched;
            sched = VDKScheduler.getInstance().getScheduler();

            Map<String, Object> map = new HashMap<String, Object>();

            map.put("jobdata", jobdata);
            JobDataMap data = JobDataMapSupport.newJobDataMap(map);

            // compute a time that is on the next round minute   
            Date runTime = evenMinuteDate(new Date());
            // Trigger the job to run on the next round minute   
            Trigger trigger = newTrigger()
                    .withIdentity("job_" + name, "Zdroj")
                    .startAt(runTime)
                    .build();

            JobDetail job = JobBuilder.newJob(HarvesterJob.class)
                    .withIdentity("job_" + name, "Zdroj")
                    .setJobData(data)
                    .build();
            if (sched.checkExists(job.getKey())) {
                sched.deleteJob(job.getKey());
            }

            sched.scheduleJob(job, trigger);
            LOGGER.log(Level.INFO, "Cron for {0} scheduled with {1}", new Object[]{name, runTime});

            //sched.start();

        } catch (SchedulerException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        }

    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        //Thread.currentThread().interrupt();
        jobdata.setInterrupted(true);
    }

}

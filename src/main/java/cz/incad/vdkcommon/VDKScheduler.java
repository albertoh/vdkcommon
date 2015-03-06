
package cz.incad.vdkcommon;

import cz.incad.vdkcommon.VDKJob;
import cz.incad.vdkcommon.VDKJobData;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.TriggerBuilder;
import org.quartz.core.jmx.JobDataMapSupport;
import org.quartz.impl.StdSchedulerFactory;

/**
 *
 * @author alberto
 */
public class VDKScheduler {
    
    static final Logger LOGGER = Logger.getLogger(VDKScheduler.class.getName());
    private static VDKScheduler _sharedInstance = null;
    private org.quartz.Scheduler scheduler;
    
    public synchronized static VDKScheduler getInstance() {
        if (_sharedInstance == null) {
            _sharedInstance = new VDKScheduler();
        }
        return _sharedInstance;
    }
    
    public VDKScheduler(){
        try {
            SchedulerFactory sf = new StdSchedulerFactory();
            scheduler = sf.getScheduler();
        }catch (SchedulerException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        } 
    }

    /**
     * @return the scheduler
     */
    public org.quartz.Scheduler getScheduler() {
        return scheduler;
    }
    
    public static void addJob(String name, String cronVal, 
            String conf) throws SchedulerException, Exception {

        org.quartz.Scheduler sched = VDKScheduler.getInstance().getScheduler();
        Map<String, Object> map = new HashMap<String, Object>();
        VDKJobData jobdata = new VDKJobData(conf, new JSONObject());
        map.put("jobdata", jobdata);
        JobDataMap data = JobDataMapSupport.newJobDataMap(map);

        JobDetail job = JobBuilder.newJob(VDKJob.class)
                .withIdentity("job_" + name)
                .setJobData(data)
                .build();
        if (sched.checkExists(job.getKey())) {
            sched.deleteJob(job.getKey());
        }
        if(cronVal.equals("")){
            LOGGER.log(Level.INFO, "Cron for {0} cleared ", name);
        }else{
            CronTrigger trigger = TriggerBuilder.newTrigger()
                    .withIdentity("trigger_" + name)
                    .withSchedule(CronScheduleBuilder.cronSchedule(cronVal))
                    .build();
            sched.scheduleJob(job, trigger);
            LOGGER.log(Level.INFO, "Cron for {0} scheduled with {1}", new Object[]{name, cronVal});
        }
    }
    
    public static void addIndexerJob() throws SchedulerException, Exception {

        org.quartz.Scheduler sched = VDKScheduler.getInstance().getScheduler();
        Map<String, Object> map = new HashMap<String, Object>();
        VDKJobData jobdata = new VDKJobData("indexer", new JSONObject());
        map.put("jobdata", jobdata);
        JobDataMap data = JobDataMapSupport.newJobDataMap(map);

        JobDetail job = JobBuilder.newJob(VDKJob.class)
                .withIdentity("job_index")
                .setJobData(data)
                .build();
        if (sched.checkExists(job.getKey())) {
            sched.deleteJob(job.getKey());
        }
        
        File statusFile = new File(System.getProperty("user.home") + File.separator + 
                ".vdkcr" + File.separator + 
                Options.getInstance().getString("indexerStatus", "indexer.json"));

        if (statusFile.exists()) {
            JSONObject statusJson = new JSONObject(FileUtils.readFileToString(statusFile, "UTF-8"));
            String cronVal = statusJson.optString("cron", "");
        
            if(cronVal.equals("")){
                LOGGER.log(Level.INFO, "Cron for index cleared ");
            }else{
                CronTrigger trigger = TriggerBuilder.newTrigger()
                        .withIdentity("trigger_index")
                        .withSchedule(CronScheduleBuilder.cronSchedule(cronVal))
                        .build();
                sched.scheduleJob(job, trigger);
                LOGGER.log(Level.INFO, "Cron for index scheduled with {0}", cronVal);
            }
        } 
    }
    
}


package cz.incad.vdkcommon;

import cz.incad.vdkcommon.oai.HarvesterJob;
import cz.incad.vdkcommon.oai.HarvesterJobData;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
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
        HarvesterJobData jobdata = new HarvesterJobData(conf);
        map.put("jobdata", jobdata);
        JobDataMap data = JobDataMapSupport.newJobDataMap(map);

        JobDetail job = JobBuilder.newJob(HarvesterJob.class)
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
    
}


package cz.incad.vdkcommon;

import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

/**
 *
 * @author alberto
 */
public class VDKScheduler {
    
    static final Logger logger = Logger.getLogger(VDKScheduler.class.getName());
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
            logger.log(Level.SEVERE, null, ex);
        } 
    }

    /**
     * @return the scheduler
     */
    public org.quartz.Scheduler getScheduler() {
        return scheduler;
    }
    
}

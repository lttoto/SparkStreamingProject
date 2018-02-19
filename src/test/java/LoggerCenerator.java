import org.apache.log4j.Logger;

/**
 * Created by taoshiliu on 2018/2/19.
 *
 * 模拟日志产生
 */
public class LoggerCenerator {

    private static Logger logger = Logger.getLogger(LoggerCenerator.class);

    public static void main(String[] args) throws Exception {
        int index = 0;
        while(true) {
            Thread.sleep(1000);
            logger.info("value : " + index++);
        }
    }

}

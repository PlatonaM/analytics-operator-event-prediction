package util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.*;


public class Logger {

    static class MsgFormatter extends Formatter {

        @Override
        public String format(LogRecord rec) {
            return calcDate(rec.getMillis()) +
                    " - " +
                    rec.getLevel() +
                    ": [" +
                    rec.getLoggerName() +
                    "] " +
                    formatMessage(rec) +
                    "\n";
        }

        public String getHead(Handler h) {
            return super.getHead(h);
        }

        public String getTail(Handler h) {
            return super.getTail(h);
        }

        private String calcDate(long millisecs) {
            SimpleDateFormat date_format = new SimpleDateFormat("MM.dd.yyyy KK:mm:ss a");
            Date resultdate = new Date(millisecs);
            return date_format.format(resultdate);
        }
    }

    static private final Map<String, Level> levelMap;
    static {
        levelMap = new HashMap<>();
        levelMap.put("error", Level.SEVERE);
        levelMap.put("warning", Level.WARNING);
        levelMap.put("info", Level.INFO);
        levelMap.put("debug", Level.FINEST);
    }

    static private Level loggerLevel;
    static private Handler loggerHandler;

    static public void setup(String level) {
        java.util.logging.Logger rootLogger = java.util.logging.Logger.getLogger("");
        Formatter formatter = new MsgFormatter();
        loggerHandler = new ConsoleHandler();
        loggerHandler.setFormatter(formatter);
        for (Handler h : rootLogger.getHandlers()) {
            h.setFormatter(formatter);
        }
        loggerHandler.setLevel(levelMap.get(level));
        loggerLevel = levelMap.get(level);
    }

    static public java.util.logging.Logger getLogger(String name) {
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger(name);
        logger.addHandler(loggerHandler);
        logger.setLevel(loggerLevel);
        logger.setUseParentHandlers(false);
        return logger;
    }
}

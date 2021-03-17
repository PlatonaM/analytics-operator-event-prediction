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

    static public void setup(String level) {
        java.util.logging.Logger rootLogger = java.util.logging.Logger.getLogger("");
        MsgFormatter msgFormatter = new MsgFormatter();
        for (Handler h : rootLogger.getHandlers()) {
            h.setFormatter(msgFormatter);
        }
        rootLogger.setLevel(levelMap.get(level));
    }
}

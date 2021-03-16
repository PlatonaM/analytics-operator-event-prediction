package util;

import java.util.Map;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Logger {

    private final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(Logger.class.getName());

    Map<String, Level> levelMap = Stream.of(new Object[][] {
            { "severe", Level.SEVERE },
            { "info", Level.INFO },
            { "warning", Level.WARNING },
            { "config", Level.CONFIG },
            { "fine", Level.FINE },
            { "finer", Level.FINER },
            { "finest", Level.FINEST },
    }).collect(Collectors.toMap(data -> (String) data[0], data -> (Level) data[1]));

    public Logger(String level) {
        logger.setLevel(levelMap.get(level));
    }

    public java.util.logging.Logger getLogger() {
        return logger;
    }
}

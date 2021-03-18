/*
 * Copyright 2021 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package handlers;

import java.util.*;
import java.util.logging.Logger;

import static org.infai.ses.platonam.util.Logger.getLogger;


public class DataHandler {

    private final static Logger logger = getLogger(DataHandler.class.getName());
    private final String timeField;
    private final String emptyPlaceholder;
    private final String delimiter;

    public DataHandler(String timeField, String emptyPlaceholder, String delimiter) {
        if (timeField == null || timeField.isBlank()) {
            throw new RuntimeException("invalid time_field");
        }
        if (delimiter == null || delimiter.isBlank()) {
            throw new RuntimeException("invalid delimiter");
        }
        this.timeField = timeField;
        this.emptyPlaceholder = emptyPlaceholder;
        this.delimiter = delimiter;
    }

    private String getValue(Object obj, Object defaultValue) {
        if (obj != null) {
            if (obj instanceof String) {
                return (String) obj;
            }
            return String.valueOf(obj);
        } else {
            if (defaultValue != null) {
                return String.valueOf(defaultValue);
            }
            return emptyPlaceholder;
        }
    }

    private List<String> buildHeader(List<String> columns) {
        columns.remove(timeField);
        Collections.sort(columns);
        List<String> header = new ArrayList<>();
        header.add(timeField);
        header.addAll(columns);
        return header;
    }

    private List<String> getHeader(List<Map<String, Object>> data) {
        return buildHeader(new ArrayList<>(data.get(0).keySet()));
    }

    private List<String> getHeader(List<Map<String, Object>> data, List<String> safeColumns) {
        List<String> columns = new ArrayList<>(data.get(0).keySet());
        if (columns.retainAll(safeColumns)) {
            logger.warning("removed unknown features");
        }
        List<String> missingCols = new ArrayList<>(safeColumns);
        missingCols.removeAll(columns);
        if (!missingCols.isEmpty()) {
            columns.addAll(missingCols);
            logger.warning("added missing features");
        }
        return buildHeader(columns);
    }

    private Map<Integer, String> getLineMap(List<String> header) {
        Map<Integer, String> lineMap = new HashMap<>();
        int lineLength = header.size();
        for (int i = 0; i < lineLength; i++) {
            lineMap.put(i, header.get(i));
        }
        return lineMap;
    }

    private String buildCSV(List<Map<String, Object>> data, List<String> header, Map<?, ?> defaultValues) {
        Map<Integer, String> lineMap = getLineMap(header);
        int lineLength = header.size();
        StringBuilder csvData = new StringBuilder(String.join(delimiter, header) + "\n");
        for (Map<String, Object> item : data) {
            StringBuilder line = new StringBuilder();
            for (int i = 0; i < lineLength; i++) {
                line.append(getValue(item.get(lineMap.get(i)), defaultValues.get(lineMap.get(i))));
                if (i < lineLength - 1) {
                    line.append(delimiter);
                }
            }
            csvData.append(line);
            csvData.append("\n");
        }
        return csvData.toString();
    }

    public String getCSV(List<Map<String, Object>> data, Map<?, ?> defaultValues) {
        return buildCSV(data, getHeader(data), defaultValues);
    }

    public String getCSV(List<Map<String, Object>> data, Map<?, ?> defaultValues, List<String> safeColumns) {
        return buildCSV(data, getHeader(data, safeColumns), defaultValues);
    }

    public String getTimeField() {
        return timeField;
    }
}

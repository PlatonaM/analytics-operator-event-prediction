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


public class DataHandler {

    private final String timeField;
    private final String emptyPlaceholder;
    private final String delimiter;

    public DataHandler(String timeField, String emptyPlaceholder, String delimiter) {
        this.timeField = timeField;
        this.emptyPlaceholder = emptyPlaceholder;
        this.delimiter = delimiter;
    }

    private String getValue(Object obj) {
        if (obj instanceof Number) {
            return String.valueOf(obj).replaceAll("\\.0+$", "");
        } else if (obj instanceof String) {
            return (String) obj;
        } else {
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

    private List<String> getHeader(List<Map<String, ?>> data) {
        return buildHeader(new ArrayList<>(data.get(0).keySet()));
    }

    private List<String> getHeader(List<Map<String, ?>> data, List<String> safeColumns) {
        List<String> columns = new ArrayList<>(data.get(0).keySet());
        if (columns.retainAll(safeColumns)) {
            System.out.println("removed unknown columns");
        }
        List<String> missingCols = new ArrayList<>(safeColumns);
        missingCols.removeAll(columns);
        if (!missingCols.isEmpty()) {
            columns.addAll(missingCols);
            System.out.println("added missing columns");
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

    private String buildCSV(List<Map<String, ?>> data, List<String> header) {
        Map<Integer, String> lineMap = getLineMap(header);
        int lineLength = header.size();
        StringBuilder csvData = new StringBuilder(String.join(delimiter, header) + "\n");
        for (Map<String, ?> item : data) {
            StringBuilder line = new StringBuilder();
            for (int i = 0; i < lineLength; i++) {
                line.append(getValue(item.get(lineMap.get(i))));
                if (i < lineLength - 1) {
                    line.append(delimiter);
                }
            }
            csvData.append(line);
            csvData.append("\n");
        }
        return csvData.toString();
    }

    public String getCSV(List<Map<String, ?>> data) {
        return buildCSV(data, getHeader(data));
    }

    public String getCSV(List<Map<String, ?>> data, List<String> safeColumns) {
        return buildCSV(data, getHeader(data, safeColumns));
    }

    public String getTimeField() {
        return timeField;
    }
}

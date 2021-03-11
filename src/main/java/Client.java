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


import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.infai.ses.senergy.operators.BaseOperator;
import org.infai.ses.senergy.operators.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class Client extends BaseOperator {

    private final String timeField;
    private final String emptyPlaceholder;
    private final String delimiter;
    private final String workerConfig;
    private final String workerURL;
    private final String trainerURL;
    private final boolean compressedInput;
    private final CloseableHttpClient httpclient = HttpClients.createDefault();

    public Client(String timeField, String emptyPlaceholder, String delimiter, String workerConfig, String workerURL, String trainerURL, boolean compressedInput) {
        this.timeField = timeField;
        this.emptyPlaceholder = emptyPlaceholder;
        this.delimiter = delimiter;
        this.workerConfig = workerConfig;
        this.workerURL = workerURL;
        this.trainerURL = trainerURL;
        this.compressedInput = compressedInput;
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

    private String getCSV(List<Map<String, ?>> data) {
        List<String> columns = new ArrayList<>(data.get(0).keySet());
        columns.remove(timeField);
        Collections.sort(columns);
        List<String> header = new ArrayList<>();
        header.add(timeField);
        header.addAll(columns);
        Map<Integer, String> lineMap = new HashMap<>();
        int lineLength = header.size();
        for (int i = 0; i < lineLength; i++) {
            lineMap.put(i, header.get(i));
        }
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

    private String httpGet(String url, String contentType) throws IOException {
        //CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet request = new HttpGet(url);
        request.addHeader("content-type", contentType);
        CloseableHttpResponse response = httpclient.execute(request);
        try {
            if (response.getStatusLine().getStatusCode() == 200) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    return Util.stringFromStream(entity.getContent());
                } else {
                    throw new RuntimeException("empty response");
                }
            } else {
                throw new RuntimeException(url + " - " + response.getStatusLine().getStatusCode());
            }
        } finally {
            response.close();
        }
    }

    private String httpPost(String url, String contentType, String data) throws IOException {
        HttpPost request = new HttpPost(url);
        request.addHeader("content-type", contentType);
        request.setEntity(new StringEntity(data));
        CloseableHttpResponse response = httpclient.execute(request);
        try {
            if (response.getStatusLine().getStatusCode() == 200) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    return Util.stringFromStream(entity.getContent());
                } else {
                    throw new RuntimeException("empty response");
                }
            } else {
                throw new RuntimeException(url + " - " + response.getStatusLine().getStatusCode());
            }
        } finally {
            response.close();
        }
    }

    private String getModel(String serviceID) throws InterruptedException {
        while (true) {
            try {
                return httpGet(trainerURL + "/" + serviceID, "application/json");
            } catch (Exception e) {
                System.out.println("retrieving model failed - " + e.getMessage());
            }
            TimeUnit.SECONDS.sleep(5);
        }
    }

    private String postJobData(String modelData, String csvData) throws InterruptedException {
        String jobData = "{\"model\":" + modelData + ",\"config\":" + workerConfig + "}";
        while (true) {
            try {
                Map<String, String> resp = new Gson().fromJson(httpPost(workerURL, "application/json", jobData), new TypeToken<Map<String, String>>(){}.getType());
                while (true) {
                    try {
                        httpPost(workerURL + "/" + resp.get("id"), "text/csv", csvData);
                        return resp.get("id");
                    } catch (Exception e) {
                        System.out.println("transmitting data failed - " + e.getMessage());
                    }
                    TimeUnit.SECONDS.sleep(5);
                }
            } catch (Exception e) {
                System.out.println("creating job failed - " + e.getMessage());
            }
            TimeUnit.SECONDS.sleep(5);
        }
    }

    private Object getJobResult(String jobID) throws InterruptedException {
        Map<String, ?> resp;
        while (true) {
            try {
                resp = new Gson().fromJson(httpGet(workerURL + "/" + jobID, "application/json"), new TypeToken<Map<String, ?>>() {
                }.getType());
                if (resp.get("status").equals("finished")) {
                    return resp.get("result");
                } else if (resp.get("status").equals("failed")) {
                    throw new RuntimeException("job failed - worker reason: " + resp.get("reason"));
                }
                TimeUnit.SECONDS.sleep(5);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                System.out.println("retrieving job result failed - " + e.getMessage());
            }
            TimeUnit.SECONDS.sleep(5);
        }
    }

    @Override
    public void run(Message message) {
        Map<String, ?> metaData;
        List<Map<String, ?>> data;
        Map<?, ?> inputSource;
        try {
            metaData = new Gson().fromJson(message.getInput("meta_data").getString(), new TypeToken<Map<String, ?>>(){}.getType());
            List<?> inputSources = (ArrayList<?>) metaData.get("input_sources");
            if (inputSources == null) {
                throw new RuntimeException("missing input source");
            }
            if (inputSources.size() > 1) {
                throw new RuntimeException("multiple input sources not supported");
            }
            inputSource = (LinkedTreeMap<?, ?>) inputSources.get(0);
            if (compressedInput) {
                data = new Gson().fromJson(Util.decompress(message.getInput("data").getString()), new TypeToken<LinkedList<Map<String, ?>>>(){}.getType());
            } else {
                data = new Gson().fromJson(message.getInput("data").getString(), new TypeToken<LinkedList<Map<String, ?>>>(){}.getType());
            }
            String windowStart = (String) data.get(0).get(timeField);
            String windowEnd = (String) data.get(data.size() - 1).get(timeField);
            int dataPoints = data.size();
            System.out.println("received message with window from " + windowStart + " to " + windowEnd + " containing " + dataPoints + " data points");
            String csvData = getCSV(data);
            System.out.println(getJobResult(postJobData(getModel((String) inputSource.get("name")), csvData)));
        } catch (Throwable t) {
            System.out.println("error handling message:");
            t.printStackTrace();
        }
    }

    @Override
    public Message configMessage(Message message) {
        message.addInput("data");
        message.addInput("meta_data");
        return message;
    }
}

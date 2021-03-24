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


import models.ModelData;
import org.infai.ses.platonam.util.HttpRequest;
import org.infai.ses.platonam.util.Json;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.infai.ses.platonam.util.HttpRequest.httpGet;
import static org.infai.ses.platonam.util.HttpRequest.httpPost;


public class JobHandler {
    private final String workerURL;
    private final String timeField;

    public JobHandler(String workerURL, String timeField) {
        if (workerURL == null || workerURL.isBlank()) {
            throw new RuntimeException("invalid worker_url: " + workerURL);
        }
        this.workerURL = workerURL;
        this.timeField = timeField;
    }

    public String createJob(List<ModelData> models) throws HttpRequest.HttpRequestException {
        Map<String, Object> data = new HashMap<>();
        data.put("time_field", timeField);
        data.put("sorted_data", true);
        data.put("models", models);
        return httpPost(
                workerURL,
                "application/json",
                Json.toString(data)
        );
    }

    public void addDataToJob(String csvData, String jobID) throws HttpRequest.HttpRequestException {
        httpPost(workerURL + "/" + jobID, "text/csv", csvData);
    }

    public Map<String, List<Object>> getJobResult(String jobID) throws HttpRequest.HttpRequestException, JobFailedException, JobNotDoneException {
        Map<String, Object> jobData = Json.typeSafeMapFromString(httpGet(workerURL + "/" + jobID, "application/json"));
        if (jobData.get("status").equals("finished")) {
            return (Map<String, List<Object>>) jobData.get("result");
        } else if (jobData.get("status").equals("failed")) {
            throw new JobFailedException("worker reason: " + jobData.get("reason"));
        } else {
            throw new JobNotDoneException((String) jobData.get("status"));
        }
    }

    public static class JobNotDoneException extends Exception {
        public JobNotDoneException(String errorMessage) {
            super(errorMessage);
        }
    }

    public static class JobFailedException extends Exception {
        public JobFailedException(String errorMessage) {
            super(errorMessage);
        }
    }
}

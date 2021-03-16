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


import models.JobData;
import models.ModelData;
import util.Util;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import handlers.*;
import org.infai.ses.senergy.operators.BaseOperator;
import org.infai.ses.senergy.operators.Message;

import java.util.*;
import java.util.concurrent.TimeUnit;


public class Client extends BaseOperator {

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

    private final DataHandler dataHandler;
    private final ModelHandler modelHandler;
    private final String workerURL;
    private final boolean compressedInput;
    private final long requestPollDelay;
    private final long requestMaxRetries;

    public Client(DataHandler dataHandler, ModelHandler modelHandler, String workerURL, boolean compressedInput, long requestPollDelay, long requestMaxRetries) {
        if (workerURL == null || workerURL.isBlank()) {
            throw new RuntimeException("invalid worker_url: " + workerURL);
        }
        this.dataHandler = dataHandler;
        this.modelHandler = modelHandler;
        this.workerURL = workerURL;
        this.compressedInput = compressedInput;
        this.requestPollDelay = requestPollDelay;
        this.requestMaxRetries = requestMaxRetries;
    }

    private String createJob(List<ModelData> models, String timeField) throws Util.HttpRequestException {
        return Util.httpPost(
                workerURL,
                "application/json",
                new Gson().toJson(new JobData(timeField, true, models), JobData.class)
        );
    }

    private void addDataToJob(String csvData, String jobID) throws Util.HttpRequestException {
        Util.httpPost(workerURL + "/" + jobID, "text/csv", csvData);
    }

    private JobData getJobResult(String jobID) throws Util.HttpRequestException, JobFailedException, JobNotDoneException {
        JobData jobRes;
        jobRes = new Gson().fromJson(
                Util.httpGet(workerURL + "/" + jobID, "application/json"),
                JobData.class
        );
        if (jobRes.status.equals("finished")) {
            return jobRes;
        } else if (jobRes.status.equals("failed")) {
            throw new JobFailedException("worker reason: " + jobRes.reason);
        } else {
            throw new JobNotDoneException(jobRes.status);
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
            System.out.println("received message with " + data.size() + " data points");
            List<List<String>> modelIDs = modelHandler.getModelIDs((String) inputSource.get("name"));
            Map<Integer, List<ModelData>> models = new HashMap<>();
            for (String modelID: modelIDs.get(0)) {
                getAndStoreModel(models, modelID);
            }
            if (!modelIDs.get(1).isEmpty()) {
                System.out.println("waiting for missing models ..");
                for (String modelID: modelIDs.get(1)) {
                    getAndStoreModel(models, modelID);
                }
            }
            Map<String, List<Map<String, Number>>> predictions = new HashMap<>();
            for (int key: models.keySet()) {
                String jobID = createJob(models.get(key), dataHandler.getTimeField());
                addDataToJob(dataHandler.getCSV(data, models.get(key).get(0).columns), jobID);
                JobData jobResult = getJobResult(jobID);
                for (String resKey: jobResult.result.keySet()) {
                    if (!predictions.containsKey(resKey)) {
                        predictions.put(resKey, new ArrayList<>());
                    }
                    predictions.get(resKey).addAll(jobResult.result.get(resKey));
                }
            }
            System.out.println("predictions: " + predictions);
        } catch (Throwable t) {
            System.out.println("error handling message:");
            t.printStackTrace();
        }
    }

    private void getAndStoreModel(Map<Integer, List<ModelData>> models, String modelID) throws Util.HttpRequestException, InterruptedException {
        for (int i=0; i < requestMaxRetries; i++) {
            try {
                ModelData model = modelHandler.getModel(modelID);
                int colsHashCode = modelHandler.getColsHashCode(model.columns);
                if (!models.containsKey(colsHashCode)) {
                    models.put(colsHashCode, new ArrayList<>());
                }
                models.get(colsHashCode).add(model);
                break;
            } catch (Util.HttpRequestException e) {
                if (i == requestMaxRetries - 1) {
                    throw e;
                }
                TimeUnit.SECONDS.sleep(requestPollDelay);
            }
        }
    }

    @Override
    public Message configMessage(Message message) {
        message.addInput("data");
        message.addInput("meta_data");
        return message;
    }
}

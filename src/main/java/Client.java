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
import java.util.logging.Logger;


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
    private final boolean fixFeatures;
    private final static Logger logger = util.Logger.getLogger(Client.class.getName());

    public Client(DataHandler dataHandler, ModelHandler modelHandler, String workerURL, boolean compressedInput, long requestPollDelay, long requestMaxRetries, boolean fixFeatures) {
        if (workerURL == null || workerURL.isBlank()) {
            throw new RuntimeException("invalid worker_url: " + workerURL);
        }
        this.dataHandler = dataHandler;
        this.modelHandler = modelHandler;
        this.workerURL = workerURL;
        this.compressedInput = compressedInput;
        this.requestPollDelay = requestPollDelay;
        this.requestMaxRetries = requestMaxRetries;
        this.fixFeatures = fixFeatures;
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
            logger.info("received message containing " + data.size() + " data points ...");
            logger.info("retrieving model IDs ...");
            List<List<String>> modelIDs = new ArrayList<>();
            for (int i=0; i <= requestMaxRetries; i++) {
                try {
                    modelIDs.addAll(modelHandler.getModelIDs((String) inputSource.get("name")));
                    break;
                } catch (Util.HttpRequestException e) {
                    if (i == requestMaxRetries) {
                        logger.severe("retrieving model IDs failed");
                        throw e;
                    }
                    TimeUnit.SECONDS.sleep(requestPollDelay);
                }
            }
            Map<Integer, List<ModelData>> models = new HashMap<>();
            logger.info("retrieving " + modelIDs.get(0).size() + " models ...");
            for (String modelID: modelIDs.get(0)) {
                getAndStoreModel(models, modelID);
            }
            if (!modelIDs.get(1).isEmpty()) {
                logger.info("waiting for " + modelIDs.get(1).size() + " models ...");
                for (String modelID: modelIDs.get(1)) {
                    logger.fine("waiting for model " + modelID);
                    getAndStoreModel(models, modelID);
                }
            }
            Map<String, List<Map<String, Number>>> predictions = new HashMap<>();
            if (models.keySet().size() > 1) {
                logger.warning("using models with diverging feature sets");
                logger.info("starting " + models.keySet().size() + " jobs ...");
            } else {
                logger.info("starting job ...");
            }
            for (int key: models.keySet()) {
                for (int i=0; i <= requestMaxRetries; i++) {
                    try {
                        String jobID = createJob(models.get(key), dataHandler.getTimeField());
                        logger.fine("created job " + jobID);
                        for (int y=0; y <= requestMaxRetries; y++) {
                            try {
                                addDataToJob(dataHandler.getCSV(data, models.get(key).get(0).columns), jobID);
                                logger.fine("added data to job " + jobID);
                                for (int x=0; x <= requestMaxRetries; x++) {
                                    try {
                                        JobData jobResult = getJobResult(jobID);
                                        logger.fine("retrieved results from job " + jobID);
                                        for (String resKey: jobResult.result.keySet()) {
                                            if (!predictions.containsKey(resKey)) {
                                                predictions.put(resKey, new ArrayList<>());
                                            }
                                            predictions.get(resKey).addAll(jobResult.result.get(resKey));
                                        }
                                        break;
                                    } catch (Util.HttpRequestException e) {
                                        if (x == requestMaxRetries) {
                                            logger.severe("retrieved results from job " + jobID + " failed");
                                            throw e;
                                        }
                                        TimeUnit.SECONDS.sleep(requestPollDelay);
                                    } catch (JobNotDoneException e) {
                                        if (x == requestMaxRetries) {
                                            logger.severe("job " + jobID + " took to long - try changing 'request_poll_delay' or 'request_max_retries'");
                                            throw e;
                                        }
                                        TimeUnit.SECONDS.sleep(requestPollDelay);
                                    }
                                }
                                break;
                            } catch (Util.HttpRequestException e) {
                                if (y == requestMaxRetries) {
                                    logger.severe("adding data to job " + jobID + " failed");
                                    throw e;
                                }
                                TimeUnit.SECONDS.sleep(requestPollDelay);
                            }
                        }
                        break;
                    } catch (Util.HttpRequestException e) {
                        if (i == requestMaxRetries) {
                            logger.severe("creating job failed");
                            throw e;
                        }
                        TimeUnit.SECONDS.sleep(requestPollDelay);
                    }
                }
            }
            logger.info("outputting results message ...");
        } catch (Throwable t) {
            logger.severe("error handling message:");
            t.printStackTrace();
        }
    }

    private void getAndStoreModel(Map<Integer, List<ModelData>> models, String modelID) throws Util.HttpRequestException, InterruptedException {
        for (int i=0; i <= requestMaxRetries; i++) {
            try {
                ModelData model = modelHandler.getModel(modelID);
                int colsHashCode = modelHandler.getColsHashCode(model.columns);
                if (!models.containsKey(colsHashCode)) {
                    models.put(colsHashCode, new ArrayList<>());
                }
                models.get(colsHashCode).add(model);
                logger.fine("retrieved model " + model.id + " (" + model.created + ")");
                break;
            } catch (Util.HttpRequestException e) {
                if (i == requestMaxRetries) {
                    logger.severe("retrieving model " + modelID + " failed");
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

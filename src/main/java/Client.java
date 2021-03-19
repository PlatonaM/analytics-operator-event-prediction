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


import com.google.gson.internal.LinkedTreeMap;
import handlers.DataHandler;
import handlers.JobHandler;
import handlers.ModelHandler;
import models.ModelData;
import org.infai.ses.platonam.util.HttpRequest;
import org.infai.ses.senergy.operators.BaseOperator;
import org.infai.ses.senergy.operators.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.infai.ses.platonam.util.Compression.decompress;
import static org.infai.ses.platonam.util.Json.typeSafeMapFromJson;
import static org.infai.ses.platonam.util.Json.typeSafeMapListFromJson;
import static org.infai.ses.platonam.util.Logger.getLogger;


public class Client extends BaseOperator {

    private final static Logger logger = getLogger(Client.class.getName());
    private final DataHandler dataHandler;
    private final ModelHandler modelHandler;
    private final JobHandler jobHandler;
    private final boolean compressedInput;
    private final long requestPollDelay;
    private final long requestMaxRetries;
    private final boolean fixFeatures;

    public Client(DataHandler dataHandler, ModelHandler modelHandler, JobHandler jobHandler, boolean compressedInput, long requestPollDelay, long requestMaxRetries, boolean fixFeatures) {
        this.dataHandler = dataHandler;
        this.modelHandler = modelHandler;
        this.jobHandler = jobHandler;
        this.compressedInput = compressedInput;
        this.requestPollDelay = requestPollDelay;
        this.requestMaxRetries = requestMaxRetries;
        this.fixFeatures = fixFeatures;
    }

    private void getAndStoreModel(Map<Integer, List<ModelData>> models, String modelID) throws HttpRequest.HttpRequestException, InterruptedException {
        for (int i = 0; i <= requestMaxRetries; i++) {
            try {
                ModelData model = modelHandler.getModel(modelID);
                int colsHashCode = modelHandler.getColsHashCode(model.columns);
                if (!models.containsKey(colsHashCode)) {
                    models.put(colsHashCode, new ArrayList<>());
                }
                models.get(colsHashCode).add(model);
                logger.fine("retrieved model " + model.id + " (" + model.created + ")");
                break;
            } catch (HttpRequest.HttpRequestException e) {
                if (i == requestMaxRetries) {
                    logger.severe("retrieving model " + modelID + " failed");
                    throw e;
                }
                TimeUnit.SECONDS.sleep(requestPollDelay);
            }
        }
    }

    private String createJob(List<ModelData> models) throws InterruptedException, HttpRequest.HttpRequestException {
        String jobID;
        for (int i = 0; i <= requestMaxRetries; i++) {
            try {
                jobID = jobHandler.createJob(models);
                logger.fine("created job " + jobID);
                return jobID;
            } catch (HttpRequest.HttpRequestException e) {
                if (i == requestMaxRetries) {
                    logger.severe("creating job failed");
                    throw e;
                }
                TimeUnit.SECONDS.sleep(requestPollDelay);
            }
        }
        throw new InterruptedException();
    }

    private void addDataToJob(String csvData, String jobID) throws InterruptedException, HttpRequest.HttpRequestException {
        for (int i = 0; i <= requestMaxRetries; i++) {
            try {
                jobHandler.addDataToJob(csvData, jobID);
                logger.fine("added data to job " + jobID);
                break;
            } catch (HttpRequest.HttpRequestException e) {
                if (i == requestMaxRetries) {
                    logger.severe("adding data to job " + jobID + " failed");
                    throw e;
                }
                TimeUnit.SECONDS.sleep(requestPollDelay);
            }
        }
    }

    private Map<String, List<Object>> getJobResult(String jobID) throws InterruptedException, HttpRequest.HttpRequestException, JobHandler.JobNotDoneException, JobHandler.JobFailedException {
        Map<String, List<Object>> jobResult;
        logger.fine("waiting for job " + jobID + " to complete ...");
        for (int i = 0; i <= requestMaxRetries; i++) {
            try {
                jobResult = jobHandler.getJobResult(jobID);
                logger.fine("retrieved results from job " + jobID);
                return jobResult;
            } catch (HttpRequest.HttpRequestException e) {
                if (i == requestMaxRetries) {
                    logger.severe("retrieving results from job " + jobID + " failed");
                    throw e;
                }
                TimeUnit.SECONDS.sleep(requestPollDelay);
            } catch (JobHandler.JobNotDoneException e) {
                if (i == requestMaxRetries) {
                    logger.severe("job " + jobID + " took to long - try changing 'request_poll_delay' or 'request_max_retries'");
                    throw e;
                }
                TimeUnit.SECONDS.sleep(requestPollDelay);
            } catch (JobHandler.JobFailedException e) {
                logger.severe("job " + jobID + " failed - " + e.getMessage());
                throw e;
            }
        }
        throw new InterruptedException();
    }

    @Override
    public void run(Message message) {
        Map<String, Object> metaData;
        List<Map<String, Object>> data;
        Map<?, ?> inputSource;
        try {
            metaData = typeSafeMapFromJson(message.getInput("meta_data").getString());
            List<?> inputSources = (ArrayList<?>) metaData.get("input_sources");
            Map<?, ?> defaultValues = (Map<?, ?>) metaData.getOrDefault("default_values", new HashMap<>());
            if (inputSources == null) {
                throw new RuntimeException("missing input source");
            }
            if (inputSources.size() > 1) {
                throw new RuntimeException("multiple input sources not supported");
            }
            inputSource = (LinkedTreeMap<?, ?>) inputSources.get(0);
            if (compressedInput) {
                data = typeSafeMapListFromJson(decompress(message.getInput("data").getString()));
            } else {
                data = typeSafeMapListFromJson(message.getInput("data").getString());
            }
            logger.info("received message containing " + data.size() + " data points ...");
            logger.info("retrieving model IDs ...");
            List<List<String>> modelIDs = new ArrayList<>();
            for (int i = 0; i <= requestMaxRetries; i++) {
                try {
                    modelIDs.addAll(modelHandler.getModelIDs((String) inputSource.get("name")));
                    break;
                } catch (HttpRequest.HttpRequestException e) {
                    if (i == requestMaxRetries) {
                        logger.severe("retrieving model IDs failed");
                        throw e;
                    }
                    TimeUnit.SECONDS.sleep(requestPollDelay);
                }
            }
            Map<Integer, List<ModelData>> models = new HashMap<>();
            logger.info("retrieving " + modelIDs.get(0).size() + " models ...");
            for (String modelID : modelIDs.get(0)) {
                getAndStoreModel(models, modelID);
            }
            if (!modelIDs.get(1).isEmpty()) {
                logger.info("waiting for " + modelIDs.get(1).size() + " models ...");
                for (String modelID : modelIDs.get(1)) {
                    logger.fine("waiting for model " + modelID);
                    getAndStoreModel(models, modelID);
                }
            }
            if (models.keySet().size() > 1) {
                logger.warning("using models with diverging feature sets");
                logger.info("starting " + models.keySet().size() + " jobs ...");
            } else {
                logger.info("starting job ...");
            }
            Map<String, List<Object>> predictions = new HashMap<>();
            for (int key : models.keySet()) {
                String jobID = createJob(models.get(key));
                String csvData;
                if (fixFeatures) {
                    csvData = dataHandler.getCSV(data, defaultValues, models.get(key).get(0).columns);
                } else {
                    csvData = dataHandler.getCSV(data, defaultValues);
                }
                addDataToJob(csvData, jobID);
                Map<String, List<Object>> jobResult = getJobResult(jobID);
                for (String resKey : jobResult.keySet()) {
                    if (!predictions.containsKey(resKey)) {
                        predictions.put(resKey, new ArrayList<>());
                    }
                    predictions.get(resKey).addAll(jobResult.get(resKey));
                }
            }
        } catch (HttpRequest.HttpRequestException | JobHandler.JobFailedException | JobHandler.JobNotDoneException e) {
            logger.severe("error handling message");
        } catch (Throwable t) {
            logger.severe("error handling message:");
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

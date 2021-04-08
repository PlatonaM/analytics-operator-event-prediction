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


import com.google.gson.reflect.TypeToken;
import handlers.DataHandler;
import handlers.JobHandler;
import handlers.ModelHandler;
import models.Job;
import models.Model;
import models.ModelIDs;
import org.infai.ses.platonam.util.Compression;
import org.infai.ses.platonam.util.HttpRequest;
import org.infai.ses.platonam.util.Json;
import org.infai.ses.senergy.operators.BaseOperator;
import org.infai.ses.senergy.operators.Message;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

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
    private final String deviceID;
    private final String serviceID;
    private final boolean skipOnMissing;

    public Client(DataHandler dataHandler, ModelHandler modelHandler, JobHandler jobHandler, boolean compressedInput, long requestPollDelay, long requestMaxRetries, boolean fixFeatures, String deviceID, String serviceID, boolean skipOnMissing) {
        this.dataHandler = dataHandler;
        this.modelHandler = modelHandler;
        this.jobHandler = jobHandler;
        this.compressedInput = compressedInput;
        this.requestPollDelay = requestPollDelay;
        this.requestMaxRetries = requestMaxRetries;
        this.fixFeatures = fixFeatures;
        this.deviceID = deviceID;
        this.serviceID = serviceID;
        this.skipOnMissing = skipOnMissing;
    }

    private void getAndStoreModel(Map<Integer, List<Model>> models, String modelID) throws HttpRequest.HttpRequestException, InterruptedException, ModelHandler.GetModelException {
        for (int i = 0; i <= requestMaxRetries; i++) {
            try {
                Model model = modelHandler.getModel(modelID);
                int colsHashCode = modelHandler.getColsHashCode(model.columns);
                if (!models.containsKey(colsHashCode)) {
                    models.put(colsHashCode, new ArrayList<>());
                }
                models.get(colsHashCode).add(model);
                logger.fine("retrieved model " + model.id + " (" + model.created + ")");
                break;
            } catch (HttpRequest.HttpRequestException | ModelHandler.GetModelException e) {
                if (i == requestMaxRetries) {
                    logger.severe("retrieving model " + modelID + " failed");
                    throw e;
                }
                TimeUnit.SECONDS.sleep(requestPollDelay);
            }
        }
    }

    private String createJob(List<Model> models) throws InterruptedException, HttpRequest.HttpRequestException {
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
        logger.fine("waiting for job " + jobID + " to complete ...");
        for (int i = 0; i <= requestMaxRetries; i++) {
            try {
                Job.Reduced job = jobHandler.getJob(jobID);
                logger.fine("retrieved results from job " + jobID);
                return job.result;
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
        List<Map<String, Object>> data;
        Map<?, ?> inputSource;
        try {
            if (compressedInput) {
                InputStream inputStream = Compression.decompressToStream(message.getInput("data").getString());
                data = Json.fromStreamToList(inputStream, new TypeToken<>() {
                });
            } else {
                data = Json.fromString(message.getInput("data").getString(), new TypeToken<>() {
                });
            }
            logger.info("received message containing " + data.size() + " data points ...");
            logger.info("retrieving model IDs ...");
            ModelIDs modelIDs = null;
            for (int i = 0; i <= requestMaxRetries; i++) {
                try {
                    modelIDs = modelHandler.getModelIDs();
                    break;
                } catch (HttpRequest.HttpRequestException e) {
                    if (i == requestMaxRetries) {
                        logger.severe("retrieving model IDs failed");
                        throw e;
                    }
                    TimeUnit.SECONDS.sleep(requestPollDelay);
                }
            }
            Map<Integer, List<Model>> models = new HashMap<>();
            logger.info("retrieving " + modelIDs.available.size() + " models ...");
            for (String modelID : modelIDs.available) {
                getAndStoreModel(models, modelID);
            }
            if (!modelIDs.pending.isEmpty() && !skipOnMissing) {
                logger.info("waiting for " + modelIDs.pending.size() + " models ...");
                for (String modelID : modelIDs.pending) {
                    logger.fine("waiting for model " + modelID + " ...");
                    getAndStoreModel(models, modelID);
                }
            }
            if (models.keySet().size() > 1) {
                logger.warning("using models with diverging feature sets");
                logger.info("starting " + models.keySet().size() + " jobs ...");
            } else if (models.keySet().size() == 1) {
                logger.info("starting job ...");
            } else {
                throw new Exception("no models available");
            }
            Map<String, Object> predictions = new HashMap<>();
            for (int key : models.keySet()) {
                String jobID = createJob(models.get(key));
                String csvData;
                if (fixFeatures) {
                    csvData = dataHandler.getCSV(data, models.get(key).get(0).default_values, models.get(key).get(0).columns);
                } else {
                    csvData = dataHandler.getCSV(data);
                }
//                BufferedWriter writer = new BufferedWriter(new FileWriter("output/csv_" + System.currentTimeMillis() +"_.csv"));
//                writer.write(csvData);
//                writer.close();
                addDataToJob(csvData, jobID);
                Map<String, List<Object>> jobResult = getJobResult(jobID);
                for (String resKey : jobResult.keySet()) {
                    if (!predictions.containsKey(resKey)) {
                        predictions.put(resKey, new ArrayList<>());
                    }
                    List<Object> result = (List<Object>) predictions.get(resKey);
                    result.addAll(jobResult.get(resKey));
                }
            }
            List<String> startAndEndTime = dataHandler.getStartAndEndTimestamp(data);
            message.output("start_time", startAndEndTime.get(0));
            message.output("end_time", startAndEndTime.get(1));
            message.output("device_id", deviceID);
            message.output("service_id", serviceID);
            message.output("predictions", Json.toString(new TypeToken<Map<String, Object>>() {
            }.getType(), predictions));
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
        return message;
    }
}

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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class Client extends BaseOperator {

    private final DataHandler dataHandler;
    private final ModelHandler modelHandler;
    private final String workerURL;
    private final boolean compressedInput;

    public Client(DataHandler dataHandler, ModelHandler modelHandler, String workerURL, boolean compressedInput) {
        this.dataHandler = dataHandler;
        this.modelHandler = modelHandler;
        this.workerURL = workerURL;
        this.compressedInput = compressedInput;
    }

    private String createJob(List<ModelData> models, String timeField) throws IOException {
        return Util.httpPost(
                workerURL,
                "application/json",
                new Gson().toJson(new JobData(timeField, true, models), JobData.class)
        );
    }

    private void addDataToJob(String csvData, String jobID) throws IOException {
        Util.httpPost(workerURL + "/" + jobID, "text/csv", csvData);
    }

    private JobData getJobResult(String jobID) throws InterruptedException {
        JobData jobRes;
        while (true) {
            try {
                jobRes = new Gson().fromJson(
                        Util.httpGet(workerURL + "/" + jobID, "application/json"),
                        JobData.class
                );
                if (jobRes.status.equals("finished")) {
                    return jobRes;
                } else if (jobRes.status.equals("failed")) {
                    throw new RuntimeException("job failed - worker reason: " + jobRes.reason);
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
            System.out.println("received message with " + data.size() + " data points");
            List<List<String>> modelIDs = modelHandler.getModelIDs((String) inputSource.get("name"));
            List<ModelData> models = new ArrayList<>();
            for (String modelID: modelIDs.get(0)) {
                ModelData model = modelHandler.getModel(modelID);
                models.add(model);
            }
            if (!modelIDs.get(1).isEmpty()) {
                System.out.println("waiting for missing models ..");
            }
            String jobID = createJob(models, dataHandler.getTimeField());
            addDataToJob(dataHandler.getCSV(data), jobID);
            JobData jobResult = getJobResult(jobID);
            System.out.println("prediction: " + jobResult.result);
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

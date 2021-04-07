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


import com.google.gson.reflect.TypeToken;
import models.Model;
import models.ModelIDs;
import models.ModelRequest;
import org.infai.ses.platonam.util.HttpRequest;
import org.infai.ses.platonam.util.Json;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class ModelHandler {

    private final String trainerURL;
    private final Map<String, Object> mlConfig;
    private final String timeField;
    private final String sourceID;
    private final String serviceID;

    public ModelHandler(String trainerURL, String mlConfig, String timeField, String sourceID, String serviceID) {
        this.serviceID = serviceID;
        if (trainerURL == null || trainerURL.isBlank()) {
            throw new RuntimeException("invalid trainer_url");
        }
        if (mlConfig == null || mlConfig.isBlank()) {
            throw new RuntimeException("invalid ml_config");
        }
        if (timeField == null || timeField.isBlank()) {
            throw new RuntimeException("invalid time_field");
        }
        if (sourceID == null || sourceID.isBlank()) {
            throw new RuntimeException("invalid source_id");
        }
        this.trainerURL = trainerURL;
        this.mlConfig = Json.fromString(mlConfig, new TypeToken<>() {
        });
        this.timeField = timeField;
        this.sourceID = sourceID;
    }

    public ModelIDs getModelIDs() throws HttpRequest.HttpRequestException {
        ModelRequest modelRequest = new ModelRequest();
        modelRequest.service_id = serviceID;
        modelRequest.ml_config = mlConfig;
        modelRequest.time_field = timeField;
        modelRequest.source_id = sourceID;
        return Json.fromString(
                HttpRequest.httpPost(trainerURL, "application/json", Json.toString(ModelRequest.class, modelRequest)),
                ModelIDs.class
        );
    }

    public Model getModel(String modelID) throws HttpRequest.HttpRequestException, GetModelException {
        Model model = Json.fromString(
                HttpRequest.httpGet(trainerURL + "/" + modelID, "application/json"),
                Model.class
        );
        if (model.data == null) {
            throw new GetModelException("no data available for " + modelID);
        }
        return model;
    }

    public int getColsHashCode(List<String> columns) {
        List<String> colsCopy = new ArrayList<>(columns);
        Collections.sort(colsCopy);
        String colsStr = String.join("", colsCopy);
        return colsStr.hashCode();
    }

    public static class GetModelException extends Exception {
        public GetModelException(String errorMessage) {
            super(errorMessage);
        }
    }
}

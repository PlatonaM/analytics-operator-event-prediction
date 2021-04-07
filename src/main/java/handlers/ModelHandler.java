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
import org.infai.ses.platonam.util.HttpRequest;
import org.infai.ses.platonam.util.Json;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class ModelHandler {

    private final String trainerURL;
    private final String mlConfig;

    public ModelHandler(String trainerURL, String mlConfig) {
        if (trainerURL == null || trainerURL.isBlank()) {
            throw new RuntimeException("invalid trainer_url");
        }
        if (mlConfig == null || mlConfig.isBlank()) {
            throw new RuntimeException("invalid ml_config");
        }
        this.trainerURL = trainerURL;
        this.mlConfig = mlConfig;
    }

    public List<List<String>> getModelIDs(String serviceID) throws HttpRequest.HttpRequestException {
        String reqData = "{\"service_id\":\"" + serviceID + "\",\"ml_config\":" + mlConfig + "}";
        Map<String, List<String>> respData = Json.fromString(
                HttpRequest.httpPost(trainerURL, "application/json", reqData),
                new TypeToken<>() {
                }
        );
        List<List<String>> modelIDs = new ArrayList<>();
        modelIDs.add(respData.get("available"));
        modelIDs.add(respData.get("pending"));
        return modelIDs;
    }

    public Model getModel(String modelID) throws HttpRequest.HttpRequestException {
        return Json.fromString(
                HttpRequest.httpGet(trainerURL + "/" + modelID, "application/json"),
                Model.class
        );
    }

    public int getColsHashCode(List<String> columns) {
        List<String> colsCopy = new ArrayList<>(columns);
        Collections.sort(colsCopy);
        String colsStr = String.join("", colsCopy);
        return colsStr.hashCode();
    }
}

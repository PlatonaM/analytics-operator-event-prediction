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


package models;

import java.util.List;
import java.util.Map;

class JobBase {
    public String id;
    public String created;
    public String status;
    public String data_source;
    public Map<String, List<Object>> result;
    public String reason;
    public boolean sorted_data;
}

public class Job {
    public static class Extended extends JobBase {
        public List<Model> models;
    }

    public static class Reduced extends JobBase {
        public List<String> models;
    }
}

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


import org.infai.ses.senergy.operators.Config;
import org.infai.ses.senergy.operators.Stream;
import org.infai.ses.senergy.utils.ConfigProvider;

public class Operator {

    public static void main(String[] args) throws Exception {
        Config config = ConfigProvider.getConfig();
        Client client = new Client(
                config.getConfigValue("time_field", null),
                config.getConfigValue("empty_placeholder", ""),
                config.getConfigValue("delimiter", null),
                config.getConfigValue("worker_config", null),
                config.getConfigValue("worker_url", null),
                config.getConfigValue("trainer_url", null),
                Boolean.parseBoolean(config.getConfigValue("compressed_input", "false"))
        );
        Stream stream  = new Stream();
        stream.start(client);
    }
}

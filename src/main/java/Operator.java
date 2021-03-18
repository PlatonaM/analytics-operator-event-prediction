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


import handlers.DataHandler;
import handlers.ModelHandler;
import org.infai.ses.platonam.util.Logger;
import org.infai.ses.senergy.operators.Config;
import org.infai.ses.senergy.operators.Stream;
import org.infai.ses.senergy.utils.ConfigProvider;


public class Operator {

    public static void main(String[] args) {
        Config config = ConfigProvider.getConfig();
        Logger.setup(config.getConfigValue("logging_level", "info"));
        DataHandler dataHandler = new DataHandler(
                config.getConfigValue("time_field", null),
                config.getConfigValue("empty_placeholder", ""),
                config.getConfigValue("delimiter", null)
        );
        ModelHandler modelHandler = new ModelHandler(
                config.getConfigValue("trainer_url", null),
                config.getConfigValue("ml_config", null)
        );
        Client client = new Client(
                dataHandler,
                modelHandler,
                config.getConfigValue("worker_url", null),
                Boolean.parseBoolean(config.getConfigValue("compressed_input", "false")),
                Long.parseLong(config.getConfigValue("request_poll_delay", "15")),
                Long.parseLong(config.getConfigValue("request_max_retries", "240")),
                Boolean.parseBoolean(config.getConfigValue("fix_features", "false"))
        );
        Stream stream = new Stream();
        stream.start(client);
    }
}

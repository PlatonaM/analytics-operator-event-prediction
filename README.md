## analytics-operator-event-prediction

Takes sets of messages and transforms them to CSV, which serve as inputs for [analytics-worker-event-prediction](https://github.com/PlatonaM/analytics-worker-event-prediction). 
The structure of each contained message must be simple and flat. In addition the necessary ML models for performing predictions on the dataset are obtained from [analytics-trainer-event-prediction](https://github.com/PlatonaM/analytics-trainer-event-prediction).

### Configuration

`time_field`: Field containing timestamps. **required**

`delimiter`: Delimiter to use for CSV. **required**

`worker_url`: URL of analytics-worker-event-prediction.  **required**

`trainer_url`: URL of analytics-trainer-event-prediction. **required**

`device_id`: ID of the device for which predictions are to be performed. **required**

`service_id`: ID of the data service as defined in the device type to which the device belongs. **required**

`ml_config`: Machine learning configuration. See data structures below for details. **required**

`compressed_input`: Set if input messages are compressed or not.

`request_poll_delay`: Determines the delay between result queries.

`request_max_retries`: Set the amount of maximum queries before a pending result is ignored.

`fix_features`: Add or remove features to match model.

`logging_level`: Set logging level to `info`, `warning`, `error` or `debug`.

`skip_on_missing`: Skip prediction if model is not available.

`empty_placeholder`: Placeholder used for empty fields when building CSV.

### Inputs

`data`: A set of messages as provided for example by [analytics-operator-cache](https://github.com/PlatonaM/analytics-operator-cache).

### Outputs

`start_time`: Timestamp of first message in set.

`end_time`: Timestamp of last message in set.

`device_id`: ID of the device for which predictions was performed.

`service_id`: ID of the data service as defined in the device type to which the device belongs.

`predictions`: Prediction results. See data structures below for details.

### Data Structures

#### Ml config

    {
        "sampling_frequency": [<string>],
        "imputations_technique_str": [<string>],
        "imputation_technique_num": [<string>],
        "ts_fresh_window_length": [<number>],
        "ts_fresh_window_end": [<number>],
        "ts_fresh_minimal_features": [<boolean>],
        "balance_ratio": [<number>],
        "random_state": [[<number>]],
        "cv": [<number>],
        "oversampling_method": [<boolean>],
        "target_col": [<string>],                     # REQUIRED
        "target_errorCode": [<number>],               # REQUIRED
        "scaler": [<string>],
        "ml_algorithm": [<string>]
    }

#### Prediction results

    [
        {
        "target": <number>,
        "result": <number>
        }
    ]

package models;

import java.util.List;
import java.util.Map;

public class JobData {
    public String time_field;
    public boolean sorted_data;
    public List<ModelData> models;
    public String status;
    public Map<String, List<Map<String, Number>>> result;
    public String reason;

    public JobData(String time_field, boolean sorted_data, List<ModelData> models) {
        this.time_field = time_field;
        this.sorted_data = sorted_data;
        this.models = models;
    }
}

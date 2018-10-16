
package org.arrah.framework.jsonparser;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
	"sparkconfig",
    "datasources",
    "transformations",
    "outputs"
})
public class RootConfigParser implements Serializable
{

	@JsonProperty("sparkconfig")
    private SparkConfigParser sparkconfig = null;
    @JsonProperty("datasources")
    private List<DatasourceParser> datasources = null;
    @JsonProperty("transformations")
    private List<TransformationParser> transformations = null;
    @JsonProperty("outputs")
    private List<OutputParser> outputs = null;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();
    private final static long serialVersionUID = -5413454822542948357L;

    @JsonProperty("sparkconfig")
    public SparkConfigParser getSparkConfig() {
        return sparkconfig;
    }

    @JsonProperty("sparkconfig")
    public void setSparkConfig(SparkConfigParser sparkconfig) {
        this.sparkconfig = sparkconfig;
    }

    @JsonProperty("datasources")
    public List<DatasourceParser> getDatasources() {
        return datasources;
    }

    @JsonProperty("datasources")
    public void setDatasources(List<DatasourceParser> datasources) {
        this.datasources = datasources;
    }

    @JsonProperty("transformations")
    public List<TransformationParser> getTransformations() {
        return transformations;
    }

    @JsonProperty("transformations")
    public void setTransformations(List<TransformationParser> transformations) {
        this.transformations = transformations;
    }

    @JsonProperty("outputs")
    public List<OutputParser> getOutputs() {
        return outputs;
    }

    @JsonProperty("outputs")
    public void setOutputs(List<OutputParser> outputs) {
        this.outputs = outputs;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

}

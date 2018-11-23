
package org.arrah.framework.jsonparser;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "appname",
    "master",
    "verbose",
    "noexecutor",
    "nocore",
    "executormemory",
    "confparam"
})
public class SparkConfigParser implements Serializable
{

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	@JsonProperty("appname")
    private String appname;
    @JsonProperty("master")
    private String master;
    @JsonProperty("verbose")
    private String verbose;
    @JsonProperty("noexecutor")
    private String noexecutor;
    @JsonProperty("nocore")
    private String nocore;
    @JsonProperty("executormemory")
    private String executormemory;
    @JsonProperty("confparam")
    private String confparam;
   
    @JsonProperty("appname")
    public String getAppName() {
        return appname;
    }

    @JsonProperty("appname")
    public void setAppName(String name) {
        this.appname = name;
    }

    @JsonProperty("master")
    public String getMaster() {
        return master;
    }

    @JsonProperty("master")
    public void setMaster(String master) {
        this.master = master;
    }

    @JsonProperty("noexecutor")
    public String getNoOfExecutor() {
        return noexecutor;
    }

    @JsonProperty("noexecutor")
    public void setNoOfExecutor(String noexecutor) {
        this.noexecutor = noexecutor;
    }
    

    @JsonProperty("nocore")
    public String getNoOfCore() {
        return nocore;
    }

    @JsonProperty("nocore")
    public void setNoOfCore(String nocore) {
        this.nocore = nocore;
    }

    @JsonProperty("executormemory")
    public String getExecutorMem() {
        return executormemory;
    }

    @JsonProperty("executormemory")
    public void setExecutorMem(String excutormemory) {
        this.executormemory = excutormemory;
    }

    @JsonProperty("verbose")
	public String getVerbose() {
		return verbose;
	}
    
    @JsonProperty("confparam")
	public void setConfparam(String confparam) {
		this.confparam = confparam;
	}
    
    @JsonProperty("confparam")
	public String getConfparam() {
		return confparam;
	}

   
}

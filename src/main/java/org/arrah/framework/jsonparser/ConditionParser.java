
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
    "column",
    "condition",
    "value",
    "datatype",
    "joinType",
    "leftcolumn",
    "rightcolumn",
    "dropcolumn",
    "sql",
    "aggrcondition",
    "funcname"
})
public class ConditionParser implements Serializable
{

    @JsonProperty("column")
    private String column;
    @JsonProperty("condition")
    private String condition;
    @JsonProperty("value")
    private List<String> value = null;
    @JsonProperty("datatype")
    private String datatype;
    @JsonProperty("joinType")
    private String joinType;
    @JsonProperty("leftcolumn")
    private String leftcolumn;
    @JsonProperty("rightcolumn")
    private String rightcolumn;
    @JsonProperty("dropcolumn")
    private String dropcolumn;
    @JsonProperty("sql")
    private String sql;
    @JsonProperty("aggrcondition")
    private String aggrcondition;
    @JsonProperty("funcname")
    private String funcname;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();
    private final static long serialVersionUID = 7944448796714875419L;

    @JsonProperty("column")
    public String getColumn() {
        return column;
    }

    @JsonProperty("column")
    public void setColumn(String column) {
        this.column = column;
    }

    @JsonProperty("condition")
    public String getCondition() {
        return condition;
    }

    @JsonProperty("condition")
    public void setCondition(String condition) {
        this.condition = condition;
    }

    @JsonProperty("value")
    public List<String> getValue() {
        return value;
    }

    @JsonProperty("value")
    public void setValue(List<String> value) {
        this.value = value;
    }

    @JsonProperty("datatype")
    public String getDatatype() {
        return datatype;
    }

    @JsonProperty("datatype")
    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }

    @JsonProperty("joinType")
    public String getJoinType() {
        return joinType;
    }

    @JsonProperty("joinType")
    public void setJoinType(String joinType) {
        this.joinType = joinType;
    }

    @JsonProperty("leftcolumn")
    public String getLeftcolumn() {
        return leftcolumn;
    }

    @JsonProperty("leftcolumn")
    public void setLeftcolumn(String leftcolumn) {
        this.leftcolumn = leftcolumn;
    }

    @JsonProperty("rightcolumn")
    public String getRightcolumn() {
        return rightcolumn;
    }

    @JsonProperty("rightcolumn")
    public void setRightcolumn(String rightcolumn) {
        this.rightcolumn = rightcolumn;
    }

    @JsonProperty("dropcolumn")
    public String getDropcolumn() {
        return dropcolumn;
    }

    @JsonProperty("dropcolumn")
    public void setDropcolumn(String dropcolumn) {
        this.dropcolumn = dropcolumn;
    }

    @JsonProperty("aggrcondition")
    public String getAggrcondition() {
        return aggrcondition;
    }

    @JsonProperty("aggrcondition")
    public void setAggrcondition(String aggrcondition) {
        this.aggrcondition = aggrcondition;
    }
    
    @JsonProperty("funcname")
    public String getFuncName() {
        return funcname;
    }

    @JsonProperty("funcname")
    public void setFuncName(String funcname) {
        this.funcname = funcname;
    }
    
    @JsonProperty("sql")
    public String getSql() {
        return sql;
    }

    @JsonProperty("sql")
    public void setSql(String sql) {
        this.sql = sql;
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

package org.arrah.framework.spark.udfs;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ArrayAggUDAF extends UserDefinedAggregateFunction{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private StructType _inputDataType;

    private StructType _bufferSchema;

    private DataType _returnDataType;



    public ArrayAggUDAF() {
        List<StructField> inputFields = new LinkedList<>();
        inputFields.add(DataTypes.createStructField("inputArray", DataTypes.createArrayType(DataTypes.StringType), true));
        _inputDataType = DataTypes.createStructType(inputFields);

        List<StructField> bufferFields = new LinkedList<>();
        bufferFields.add(DataTypes.createStructField("bufferArray",  DataTypes.createArrayType(DataTypes.StringType), true));
        _bufferSchema = DataTypes.createStructType(bufferFields);

        _returnDataType = DataTypes.createArrayType(DataTypes.StringType);
    }

    @Override
    public StructType inputSchema() {
        return _inputDataType;
    }

    @Override
    public StructType bufferSchema() {
        return _bufferSchema;
    }

    @Override
    public DataType dataType() {
        return _returnDataType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0,null);
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        // This input Row only has a single column storing the input value in Double.
        // We only update the buffer when the input value is not null.
        if (!input.isNullAt(0)) {
            if (buffer.isNullAt(0)) {
                // If the buffer value (the intermediate result of the sum) is still null,
                // we set the input value to the buffer.
                buffer.update(0, input.getList(0));
            } else {
                // Otherwise, we add the input value to the buffer value.
                // Need to new ArrayList first, or convert to scala collection unable to set value.
                List<String> newValue = new LinkedList<>();
                newValue.addAll(input.<String>getList(0));
                List<String> iniVal = new LinkedList<>();
                iniVal.addAll(buffer.<String>getList(0));
                for (int i=0;i<newValue.size();i++) {
                	String iniStr = iniVal.get(i);
                	String newStr = newValue.get(i);
                	String finalStr;
                	
                	if(iniStr != null){
                		if(newStr != null){
                			finalStr = (new Long(newStr.split(":")[0]).longValue()+new Long(iniStr.split(":")[0]).intValue())+":"+(new Long(newStr.split(":")[1]).longValue()+new Long(iniStr.split(":")[1]).longValue());
                		}else{
                			finalStr = iniStr;
                		}
                	}else{
                		finalStr = null;
                	}
                	
                	
                    newValue.set(i,finalStr);
                    }
                buffer.update(0, newValue);
            }
        }
    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        // buffer1 and buffer2 have the same structure.
        // We only update the buffer1 when the input buffer2's value is not null.
        if (!buffer2.isNullAt(0)) {
            if (buffer1.isNullAt(0)) {
                // If the buffer value (intermediate result of the sum) is still null,
                // we set the it as the input buffer's value.
                buffer1.update(0, buffer2.getList(0));

            } else {
                // Otherwise, we add the input value to the buffer value.
                List<String> newValue = buffer2.getList(0);
                List<String> newValue1 = new ArrayList<>();
                List<String> iniVal = buffer1.getList(0);
                for (int i=0;i<newValue.size();i++) {
                	
                	String iniStr = iniVal.get(i);
                	String newStr = newValue.get(i);
                	String finalStr;
                	
                	if(iniStr != null){
                		if(newStr != null){
                			finalStr = (new Long(newStr.split(":")[0]).longValue()+new Long(iniStr.split(":")[0]).longValue())+":"+(new Long(newStr.split(":")[1]).longValue()+new Long(iniStr.split(":")[1]).longValue());
                		}else{
                			finalStr = iniStr;
                		}
                	}else{
                		finalStr = null;
                	}
                	//newValue1.set(i,finalStr);
                	newValue1.add(i, finalStr);
                }
                buffer1.update(0, newValue1);
            }
        }
    }

    @Override
    public Object evaluate(Row buffer) {
        if (buffer.isNullAt(0)) {
            // If the buffer value is still null, we return null.
            return null;
        } else {
            // Otherwise, the intermediate sum is the final result.
            return buffer.getList(0);
        }
    }
}

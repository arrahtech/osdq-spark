package org.arrah.framework.jsontest;

import java.io.File;
import java.io.IOException;

import org.arrah.framework.jsonparser.RootConfigParser;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RootConfigTest {

	public static void main(String[] args){
		
		ObjectMapper mapper = new ObjectMapper();
		try {

			// Convert JSON string from file to Object
			RootConfigParser jsonConfig = mapper.readValue(new File("/Users/vsingh007c/Documents/workspace/sparkvalidation/src/org/arrah/framework/samplejson/config_stb_base.json"), RootConfigParser.class);
			System.out.println(jsonConfig.getDatasources().get(0).getSelectedColumns().size());

		
			//Pretty print
			String jsonConfig1 = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonConfig);
			System.out.println(jsonConfig1);

		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	}

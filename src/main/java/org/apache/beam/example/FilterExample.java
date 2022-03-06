package org.apache.beam.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

public class FilterExample {
	/*
	 * This program is an example of creating csv file from system with following
	 * data id,first_name,last_name,email,gender,contry,phone,dob Find out data with
	 * gender as Male write that data to the file system
	 */
	static class GetMaleFilter implements SerializableFunction<String, Boolean> {

		@Override
		public Boolean apply(String input) {
			String[] csvArray = input.split(",");
			return csvArray[4].equals("Male");
		}

	}

	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		PCollection<String> csvStr = pipeline.apply("Reading data from file",
				TextIO.read().from("D:\\JAVA_WORLD\\Projects\\Beam_in\\MOCK_DATA.csv"));
		csvStr.apply("Get data with gender as male", Filter.by(new GetMaleFilter())).apply("Write to file with header",
				TextIO.write().to("D:\\JAVA_WORLD\\Projects\\Beam_out\\MOCK_DATA_male_data.csv").withNumShards(1)
						.withSuffix(".csv").withHeader("id,first_name,last_name,email,gender,contry,phone,dob"));
		pipeline.run();
	}

}

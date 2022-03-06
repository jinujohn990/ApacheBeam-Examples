package org.apache.beam.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

public class MapElementUsingSimpleFunction {
	/*
	 * This program is an example of creating csv file from system with following
	 * data id,first_name,last_name,email,gender,contry,phone,dob Then convert Male
	 * to M and Female to F and write the data to file system
	 */

	static class ConvertGenderValue extends SimpleFunction<String, String> {
		@Override
		public String apply(String input) {
			
			String[] csvArray = input.split(",");
			if (csvArray[4].equals("Male")) {
				csvArray[4] = "M";
			} else if (csvArray[4].equals("Female")) {
				csvArray[4] = "F";
			}
			return String.join(",", csvArray);
		}
	}

	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		PCollection<String> csvStr = pipeline.apply("Reading data from file",
				TextIO.read().from("D:\\JAVA_WORLD\\Projects\\Beam_in\\MOCK_DATA.csv"));
		csvStr.apply("Convert gender value", MapElements.via(new ConvertGenderValue())).apply(
				"Write to file with header",
				TextIO.write().to("D:\\JAVA_WORLD\\Projects\\Beam_out\\MOCK_DATA_gender_converted.csv")
						.withNumShards(1).withSuffix(".csv")
						.withHeader("id,first_name,last_name,email,gender,contry,phone,dob"));
	pipeline.run();
	
	}

}

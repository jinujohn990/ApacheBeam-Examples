package org.apache.beam.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MapElementSample {
	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();

		PCollection<String> txtStr = pipeline.apply("Reading data from file",
				TextIO.read().from("D:\\JAVA_WORLD\\Projects\\Beam_in\\MOCK_DATA.csv"));
		txtStr.apply(MapElements.into(TypeDescriptors.strings()).via((String str) -> str.toUpperCase()))
		.apply("Write to file with header",
				TextIO.write().to("D:\\JAVA_WORLD\\Projects\\Beam_out\\MOCK_DATA_MapElement_transformed.csv")
				.withNumShards(1).withSuffix(".csv")
				.withHeader("id,first_name,last_name,email,gender,contry,phone,dob"));
		pipeline.run().waitUntilFinish();
	}
}

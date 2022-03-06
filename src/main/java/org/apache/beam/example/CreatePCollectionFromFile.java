package org.apache.beam.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;

/*
 * This program is an example of creating PCollection by reading 
 * file from a file and write  the elements to another file
 */
public class CreatePCollectionFromFile {
	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();

		PCollection<String> txtStr = pipeline
				.apply("Reading data from file",TextIO.read().from("D:\\JAVA_WORLD\\Projects\\Beam_in\\MOCK_DATA.csv"));

		txtStr.apply("Write to file without header", TextIO.write()
				.to("D:\\JAVA_WORLD\\Projects\\Beam_out\\MOCK_DATA_write.csv").withNumShards(1).withSuffix(".csv"));

		txtStr.apply("Write to file with header",
				TextIO.write().to("D:\\JAVA_WORLD\\Projects\\Beam_out\\MOCK_DATA_write_with_header.csv")
						.withNumShards(1).withSuffix(".csv")
						.withHeader("id,first_name,last_name,email,gender,contry,phone,dob"));
		pipeline.run().waitUntilFinish();

	}
}

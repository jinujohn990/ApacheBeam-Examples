package org.apache.beam.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class ParDoExample {
	static class ConvertStringToUppercase extends DoFn<String, String> {
		@ProcessElement
		public void processElement(@Element String line, OutputReceiver<String> out) {
			out.output(line.toUpperCase());
		}
	}

	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		PCollection<String> csvStr = pipeline.apply("Reading data from file",
				TextIO.read().from("D:\\JAVA_WORLD\\Projects\\Beam_in\\MOCK_DATA.csv"));
		csvStr.apply(ParDo.of(new ConvertStringToUppercase())).apply("Write to file with header",
				TextIO.write().to("D:\\JAVA_WORLD\\Projects\\Beam_out\\MOCK_DATA_uppercase_converted_using_pardo.csv")
						.withNumShards(1).withSuffix(".csv")
						.withHeader("id,first_name,last_name,email,gender,contry,phone,dob"));
		pipeline.run();
	}

}

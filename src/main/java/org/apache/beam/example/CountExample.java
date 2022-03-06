package org.apache.beam.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class CountExample {
	static class PrintCount extends DoFn<Long, Void> {
		@ProcessElement
		public void processElement(@Element Long count, OutputReceiver<Void> out) {
			System.out.println("Count :" + count);
		}
	}
	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		PCollection<String> csvStr = pipeline.apply("Reading data from file",
				TextIO.read().from("D:\\JAVA_WORLD\\Projects\\Beam_in\\MOCK_DATA.csv"));
		csvStr.apply(Count.globally())
		.apply(ParDo.of(new PrintCount()));
		pipeline.run();
	}
}

package org.apache.beam.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class PrintElementsOfPCollectionMain {
	static class PrintElementsOfPCollection extends DoFn<String, Void> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			System.out.println(c.element());
		}
	}

	public static void main(String[] args) {
		System.out.println("Method 1: Using MapElements ..................");
		Pipeline pipeline = Pipeline.create();

		PCollection<String> txtStr = pipeline
				.apply("Reading data from file", TextIO.read().from("D:\\JAVA_WORLD\\Projects\\Beam_in\\MOCK_DATA.csv"))
				.apply("Printing elements of PCollection to console",MapElements.into(TypeDescriptors.strings()).via((String str) -> {
					System.out.println(str);
					return str;
				}));
		pipeline.run().waitUntilFinish();
		
		System.out.println("Method 2: Using ParDo...................");
		Pipeline pipeline2 = Pipeline.create();
		pipeline2
				.apply("Reading data from file", TextIO.read().from("D:\\JAVA_WORLD\\Projects\\Beam_in\\MOCK_DATA.csv"))
				.apply("Printing elements of PCollection to console",ParDo.of(new PrintElementsOfPCollection()));
		pipeline2.run().waitUntilFinish();
	}
}

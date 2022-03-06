package org.apache.beam.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class DistinctExample {
	static class PrintCount extends DoFn<Long, Void> {
		@ProcessElement
		public void processElement(@Element Long count, OutputReceiver<Void> out) {
			System.out.println("Count :" + count);
		}
	}

	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		PCollection<String> pCollection1 = pipeline.apply("Reading data from file",
				TextIO.read().from("D:\\JAVA_WORLD\\Projects\\Beam_in\\MOCK_DATA.csv"));
		PCollection<String> pCollection2 = pipeline.apply("Reading data from file",
				TextIO.read().from("D:\\JAVA_WORLD\\Projects\\Beam_in\\MOCK_DATA.csv"));
		PCollection<String> pCollection3 = pipeline.apply("Reading data from file",
				TextIO.read().from("D:\\JAVA_WORLD\\Projects\\Beam_in\\MOCK_DATA.csv"));

		PCollectionList<String> pCollectionList = PCollectionList.of(pCollection1).and(pCollection2).and(pCollection3);
		PCollection<String> mergedCollection = pCollectionList.apply(Flatten.pCollections());
		PCollection<String> distinctCollection = mergedCollection.apply(Distinct.create());

		
		
		pCollection1.apply(Count.globally()).apply(ParDo.of(new PrintCount()));
		
		
		PCollection<Long> count = distinctCollection.apply(Count.globally());
		count.apply(ParDo.of(new PrintCount()));
		pipeline.run().waitUntilFinish();

	}

}

package org.apache.beam.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class FlattenExample {
	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		PCollection<String> csvStr = pipeline.apply("Reading data from file",
				TextIO.read().from("D:\\JAVA_WORLD\\Projects\\Beam_in\\MOCK_DATA.csv"));
		PCollection<String> collection1 = csvStr.apply(Sample.any(100));
		PCollection<String> collection2 = csvStr.apply(Sample.any(100));
		PCollection<String> collection3 = csvStr.apply(Sample.any(100));
		PCollectionList<String> pList = PCollectionList.of(collection1).and(collection2).and(collection3);
		PCollection<String> mergedList = pList.apply(Flatten.pCollections());
		mergedList.apply("Write to file with header",
				TextIO.write().to("D:\\JAVA_WORLD\\Projects\\Beam_out\\MOCK_DATA_MapElement_flatten.csv")
						.withNumShards(1).withSuffix(".csv")
						.withHeader("id,first_name,last_name,email,gender,contry,phone,dob"));
		pipeline.run();

	}
}

package org.apache.beam.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class PartitionExample {
	static class PartitionBasedOnGender implements PartitionFn<String> {

		@Override
		public int partitionFor(String elem, int numPartitions) {
			String[] csvArray = elem.split(",");
			if (csvArray[4].equals("Male")) {
				return 0;
			} else if (csvArray[4].equals("Female")) {
				return 1;
			} else {
				return 2;
			}

		}

	}

	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		PCollection<String> csvStr = pipeline.apply("Reading data from file",
				TextIO.read().from("D:\\JAVA_WORLD\\Projects\\Beam_in\\MOCK_DATA.csv"));
		PCollectionList<String> partition = csvStr.apply(Partition.of(3, new PartitionBasedOnGender()));
		PCollection<String> pMaleCollection = partition.get(0);
		PCollection<String> pFemaleCollection = partition.get(1);
		pMaleCollection.apply("Write to file with header",
				TextIO.write().to("D:\\JAVA_WORLD\\Projects\\Beam_out\\MOCK_DATA_MapElement_flatten_partition_male.csv")
						.withNumShards(1).withSuffix(".csv")
						.withHeader("id,first_name,last_name,email,gender,contry,phone,dob"));
		pFemaleCollection.apply("Write to file with header",
				TextIO.write().to("D:\\JAVA_WORLD\\Projects\\Beam_out\\MOCK_DATA_MapElement_partition_female.csv")
						.withNumShards(1).withSuffix(".csv")
						.withHeader("id,first_name,last_name,email,gender,contry,phone,dob"));
		pipeline.run();
	}

}

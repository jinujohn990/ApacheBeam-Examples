package org.apache.beam.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;

/*
 * This program is an example of creating PCollection from list 
 * and write  the PCollection to  file
 */
public class CreatePCollectionFromList {
	public static void main(String[] args) {
		List<String> strList = new ArrayList<>();
		strList.add("str1");
		strList.add("str2");
		strList.add("str3");

		Pipeline pipeline = Pipeline.create();
		pipeline.apply("Creating PCollection from list", Create.of(strList)).apply("Writing PCollecton to file", TextIO
				.write().to("D:\\JAVA_WORLD\\Projects\\Beam_out\\listtofile.txt").withNumShards(1).withSuffix("txt"));
		pipeline.run().waitUntilFinish();
	}
}

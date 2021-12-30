package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StatefulFilter {


    private static final Logger LOG = LoggerFactory.getLogger(FilterAndCountQuery.FilterQueryFn.class);

    public static class StatefulFilterFn extends DoFn<String, KV<String, Double>> {


        @ProcessElement
        public void processElement(ProcessContext c) {

            String[] elems = c.element().split(",");
            String state_code = elems[0];
            String dateLocalYear = elems[11].substring(0, 4);


            if (state_code.equals("06") && dateLocalYear.equals("2017")) {
                String countryCode = elems[1];
                Double arithmeticMean = Double.valueOf(elems[16]);
                c.output(KV.of(countryCode, arithmeticMean));
            }
        }
    }

    public static class StatefulFilterTransform extends PTransform<PCollection<String>, PCollection<KV<String, Double>>> {
        @Override
        public PCollection<KV<String, Double>> expand(
                PCollection<String> lines) {
            return lines.apply(ParDo.of(new StatefulFilterFn()));
        }
    }

    public interface StatefulFilterOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("sample.csv")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Validation.Required
        @Default.String("stateful-filter")
        String getOutput();

        void setOutput(String value);
    }

    static void runWordCount(StatefulFilter.StatefulFilterOptions options) {
        Pipeline p = Pipeline.create(options);
        long startTime = System.currentTimeMillis();
        LOG.info("Started evaluation at: " + startTime);
        p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply(new StatefulFilterTransform())
                .apply(Sum.doublesPerKey())
                .apply(MapElements
                        .into(TypeDescriptors.strings())
                        .via(x -> x.getKey() + ": " + x.
                                getValue()))
                .apply("WriteCounts", TextIO.write().to(options.
                        getOutput()));


        p.run().waitUntilFinish();
        long endTime = System.currentTimeMillis();
        LOG.info("Ended evaluation at: " + endTime);
        LOG.info("Total execution time: " + (float) (endTime - startTime) / 1000 + " seconds");
    }

    public static void main(String[] args) {
        StatefulFilter.StatefulFilterOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(StatefulFilter.StatefulFilterOptions.class);

        runWordCount(options);
    }


}

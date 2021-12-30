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


public class FilterAndCountQuery {

    private static final Logger LOG = LoggerFactory.getLogger(FilterAndCountQuery.FilterQueryFn.class);

    public static class FilterQueryFn extends DoFn<KV<String, Long>, KV<String, Long>> {


        @ProcessElement
        public void processElement(ProcessContext c) {
            if (c.element().getKey().equals("031")) {
                c.output(c.element());
            }
        }
    }

    public interface FilterQueryOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("epa_co_daily_summary.csv")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Validation.Required
        @Default.String("abc")
        String getOutput();

        void setOutput(String value);
    }

    static void runWordCount(FilterAndCountQuery.FilterQueryOptions options) {
        Pipeline p = Pipeline.create(options);
        long startTime = System.currentTimeMillis();
        LOG.info("Started evaluation at: " + startTime);
        p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply(new PTransform<PCollection<String>, PCollection<
                        KV<String, Long>>>() {
                    @Override
                    public PCollection<KV<String, Long>> expand(
                            PCollection<String> input) {

                        LOG.info("Processing: " + input.getName());
                        return input.apply(
                                MapElements.into(
                                                TypeDescriptors.kvs(
                                                        TypeDescriptors.strings(),
                                                        TypeDescriptors.longs()))
                                        .via(line -> KV.of(line.split(",")[1], 1L))); //1 is county_code since we need  filter on that;
                    }
                })
                .apply(ParDo.of(new FilterAndCountQuery.FilterQueryFn()))
                .apply(Filter.by(obj -> obj.getKey().equals("031")))
                .apply(Count.perKey())
                .apply(MapElements.via(new WordCount.FormatAsTextFn()))
                .apply("WriteCounts", TextIO.write().to(options.
                        getOutput()));


        p.run().waitUntilFinish();
        long endTime = System.currentTimeMillis();
        LOG.info("Ended evaluation at: " + endTime);
        LOG.info("Total execution time: " + (float) (endTime - startTime) / 1000 + " seconds");
    }

    public static void main(String[] args) {
        FilterAndCountQuery.FilterQueryOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(FilterAndCountQuery.FilterQueryOptions.class);

        runWordCount(options);
    }
}

/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import static java.lang.Runtime.getRuntime;
import static java.util.stream.Collectors.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CalculateAverage_conoroneill {

    private static final String FILE = "./measurements.txt";

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
        private static Measurement fromLine(String line) {
            int semi = line.indexOf(';');
            if (semi == -1) throw new RuntimeException("No semicolon found");
//            return new Measurement(line.substring(0, semi), parseBoth(line.substring(semi + 1)));
            return new Measurement(line.substring(0, semi), parseSimple(line.substring(semi + 1)));
//            return new Measurement(line.substring(0, semi), parseBytes(line.getBytes(), semi + 1));
//            return new Measurement(line.substring(0, semi - 1), Double.parseDouble(line.substring(semi + 1)));
        }
    }


    private static double parseBoth(String s) {
        double quick = parseSimple(s);
        double slow = Double.parseDouble(s);
        if (quick != slow) {
            throw new RuntimeException(STR."Parsing; string is: \"\{s}\"; quick: \{quick}; slow: \{slow}");
        }
        double fromBytes = parseBytes(s.getBytes(), 0);
        if (fromBytes != slow) {
            throw new RuntimeException(STR."Parsing; string is: \"\{s}\"; fromBytes: \{fromBytes}; slow: \{slow}");
        }
        return quick;
    }

    private static double parseBytes(byte[] bytes, int startIndex) {
        byte char1 = bytes[startIndex];
        if (char1 == '-') {
            char1 = bytes[startIndex+1];
            byte char2 = bytes[startIndex+2];
            if (char2 == '.') {
                byte charFraction = bytes[startIndex+3];
                // -n.f
                return (double) (-(char1 - '0')) - ((double) (charFraction - '0')) / 10;
            }
            else {
                byte charFraction = bytes[startIndex+4];
                // -nn.f
                return (double) (-((char1 - '0') * 10 + (char2 - '0'))) - ((double) (charFraction - '0')) / 10;
            }
        }
        else {
            byte char2 = bytes[startIndex+1];
            if (char2 == '.') {
                byte charFraction = bytes[startIndex+2];
                // n.f
                return (double) ((char1 - '0')) + ((double) (charFraction - '0')) / 10;
            }
            else {
                byte charFraction = bytes[startIndex+3];
                // nn.f
                return (double) (((char1 - '0') * 10 + (char2 - '0'))) + ((double) (charFraction - '0')) / 10;
            }
        }

    }

    private static double parseSimple(String s) {
        char char1 = s.charAt(0);
        if (char1 == '-') {
            char1 = s.charAt(1);
            char char2 = s.charAt(2);
            if (char2 == '.') {
                char charFraction = s.charAt(3);
                // -n.f
                return (double) (-(char1 - '0')) - ((double) (charFraction - '0')) / 10;
            }
            else {
                char charFraction = s.charAt(4);
                // -nn.f
                return (double) (-((char1 - '0') * 10 + (char2 - '0'))) - ((double) (charFraction - '0')) / 10;
            }
        }
        else {
            char char2 = s.charAt(1);
            if (char2 == '.') {
                char charFraction = s.charAt(2);
                // n.f
                return (double) ((char1 - '0')) + ((double) (charFraction - '0')) / 10;
            }
            else {
                char charFraction = s.charAt(3);
                // nn.f
                return (double) (((char1 - '0') * 10 + (char2 - '0'))) + ((double) (charFraction - '0')) / 10;
            }
        }
    }

    private static record ResultRow(double min, double mean, double max, long count) {

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    private static class MeasurementAggregator {
        private double min;
        private double max;
        private double sum;
        private long count;

        public MeasurementAggregator() {
            this(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0);
        }

        public MeasurementAggregator(double min, double max, double sum, long count) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }
    }

    public static void main(String[] args) throws IOException {
        // Map<String, Double> measurements1 = Files.lines(Paths.get(FILE))
        // .map(l -> l.split(";"))
        // .collect(groupingBy(m -> m[0], averagingDouble(m -> Double.parseDouble(m[1]))));
        //
        // measurements1 = new TreeMap<>(measurements1.entrySet()
        // .stream()
        // .collect(toMap(e -> e.getKey(), e -> Math.round(e.getValue() * 10.0) / 10.0)));
        // System.out.println(measurements1);

        long start = System.currentTimeMillis();
        Collector<Measurement, MeasurementAggregator, ResultRow> collector = Collector.of(
                MeasurementAggregator::new,
                (a, m) -> {
                    if (a.count == 0) {
                        a.min = m.value;
                        a.max = m.value;
                    } else if (m.value < a.min) {
                        a.min = m.value;
                    } else if (m.value > a.max) {
                        a.max = m.value;
                    }
                    // a.min = Math.min(a.min, m.value);
                    // a.max = Math.max(a.max, m.value);
                    a.sum += m.value;
                    a.count++;
                },
//                (agg1, agg2) -> {
//                    var res = new MeasurementAggregator();
//                    res.min = Math.min(agg1.min, agg2.min);
//                    res.max = Math.max(agg1.max, agg2.max);
//                    res.sum = agg1.sum + agg2.sum;
//                    res.count = agg1.count + agg2.count;
//
//                    return res;
//                },
                (agg1, agg2) -> new MeasurementAggregator(
                        Math.min(agg1.min, agg2.min),
                        Math.max(agg1.max, agg2.max),
                        agg1.sum + agg2.sum,
                        agg1.count + agg2.count),
                agg -> new ResultRow(agg.min,
                        (Math.round(agg.sum * 10.0) / 10.0) / agg.count,
                        agg.max,
                        agg.count
                )
                ,Collector.Characteristics.CONCURRENT, Collector.Characteristics.UNORDERED
        );

        Stream<String> lineStream = Files.lines(Paths.get(FILE));
        Stream<String> parallelStream = lineStream.parallel();
        Stream<Measurement> measurementStream = parallelStream.map(Measurement::fromLine);
        Map<String, ResultRow> measurementsMap = measurementStream.collect(
                groupingBy(Measurement::station, collector)
        );
        // Finally, put into a TreeMap to sort the map by station name, ready for printing the results
        Map<String, ResultRow> measurements = new TreeMap<>(measurementsMap);

        long end = System.currentTimeMillis();

        System.out.println(measurements);
        long countValues = measurements.values().stream().mapToLong(x -> x.count).sum();
        System.out.println(STR."Number of keys: \{measurements.size()}; values: \{countValues} (= \{countValues/1000000} million)");
        System.out.println(STR."Elapsed: \{end - start} millis");
        System.out.println(STR."Processors: \{getRuntime().availableProcessors()}");
    }
}

package com;

import org.apache.commons.csv.*;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.StreamSupport;

public class quantile_normalized_expression {

    public static void main(String[] args) throws IOException {
        // Path to the input CSV file
        String inputFilePath = "D:/SCLC-SlPj/NanoString microRNA Expression Data_2019.csv";
        // Path to save the normalized CSV file
        String normalizedFilePath = "D:/SCLC-SlPj/Normalized_Data.csv";
        // Path to save the filtered NCI data CSV file
        String nciFilteredFilePath = "D:/SCLC-SlPj/NCI_Filtered_Data.csv";

        // Read the CSV file
        List<CSVRecord> records = readCSV(inputFilePath);

        // Extract headers and data
        String[] headers = StreamSupport.stream(records.get(0).spliterator(), false)
                                .toArray(String[]::new);

        List<double[]> data = new ArrayList<>();
        for (int i = 1; i < records.size(); i++) {
            double[] row = StreamSupport.stream(records.get(i).spliterator(), false)
                            .skip(1)
                            .map(value -> {
                                try {
                                    return Double.parseDouble(value.trim());
                                } catch (NumberFormatException e) {
                                    System.err.println("Skipping non-numeric value: " + value);
                                    return Double.NaN; // Replace invalid values with NaN
                                }
                            })
                            .filter(Double::isFinite) // Remove NaN values
                            .mapToDouble(Double::doubleValue)
                            .toArray();

            data.add(row);
        }

        // Perform quantile normalization
        double[][] normalizedData = quantileNormalize(data);

        // Write normalized data to a new CSV file
        writeCSV(normalizedFilePath, headers, normalizedData, records);

        // Filter rows related to NCI cell lines
        List<CSVRecord> nciRecords = new ArrayList<>();
        for (int i = 1; i < records.size(); i++) {
            if (records.get(i).get(1).contains("NCI")) { // Check if CellLineName contains "NCI"
                nciRecords.add(records.get(i));
            }
        }

        // Write filtered NCI data to a new CSV file
        writeFilteredCSV(nciFilteredFilePath, headers, nciRecords);
    }

    // Read CSV file
    private static List<CSVRecord> readCSV(String filePath) throws IOException {
        try (FileReader reader = new FileReader(filePath);
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build());)
        {
            return csvParser.getRecords();
        }
    }

    // Perform quantile normalization
    private static double[][] quantileNormalize(List<double[]> data) {
        int rows = data.size();
        int cols = data.get(0).length;

        // Create a matrix for the data
        double[][] matrix = new double[rows][cols];
        for (int i = 0; i < rows; i++) {
            matrix[i] = data.get(i);
        }

        // Rank each column
        double[][] rankedData = new double[rows][cols];
        for (int j = 0; j < cols; j++) {
            double[] column = new double[rows];
            for (int i = 0; i < rows; i++) {
                column[i] = matrix[i][j];
            }
            Arrays.sort(column);
            for (int i = 0; i < rows; i++) {
                rankedData[i][j] = Arrays.binarySearch(column, matrix[i][j]) + 1; // Rank starts from 1
            }
        }

        // Calculate the mean of each rank across columns
        double[] meanRanks = new double[rows];
        for (int i = 0; i < rows; i++) {
            double sum = 0;
            for (int j = 0; j < cols; j++) {
                sum += rankedData[i][j];
            }
            meanRanks[i] = sum / cols;
        }

        // Sort the original data and replace with the mean ranks
        double[][] normalizedData = new double[rows][cols];
        for (int j = 0; j < cols; j++) {
            double[] column = new double[rows];
            for (int i = 0; i < rows; i++) {
                column[i] = matrix[i][j];
            }
            Arrays.sort(column);
            for (int i = 0; i < rows; i++) {
                normalizedData[i][j] = column[(int) meanRanks[i] - 1];
            }
        }

        return normalizedData;
    }

    // Write normalized data to CSV
    private static void writeCSV(String filePath, String[] headers, double[][] normalizedData, List<CSVRecord> records) throws IOException {
        try (FileWriter writer = new FileWriter(filePath);
        CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.builder().setHeader(headers).build());
        ) {
            for (int i = 0; i < normalizedData.length; i++) {
                List<String> row = new ArrayList<>();
                row.add(records.get(i + 1).get(0)); // Add miRNA name
                for (double value : normalizedData[i]) {
                    row.add(String.valueOf(value));
                }
                csvPrinter.printRecord(row);
            }
        }
    }

    // Write filtered NCI data to CSV
    private static void writeFilteredCSV(String filePath, String[] headers, List<CSVRecord> nciRecords) throws IOException {
        try (FileWriter writer = new FileWriter(filePath);
        CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.builder().setHeader(headers).build());
        ) {
            for (CSVRecord record : nciRecords) {
                csvPrinter.printRecord(record);
            }
        }
    }
}
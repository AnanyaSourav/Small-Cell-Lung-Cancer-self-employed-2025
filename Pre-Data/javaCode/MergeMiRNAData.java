package com;

import org.apache.commons.csv.*;
import java.io.*;
import java.util.*;

public class MergeMiRNAData {

    public static void main(String[] args) throws IOException {
        // File paths
        String cleanedDataFilePath = "D:/SCLC-SlPj/cleaned_nci_data.csv"; // Cleaned drug response data
        String mirnaDataFilePath = "D:/SCLC-SlPj/NCI_Filtered_Data.csv"; // miRNA expression data
        String finalMergedFilePath = "D:/SCLC-SlPj/final_merged_data.csv"; // Output file

        // Reshape miRNA data into a map (Key: CellLineName, Value: {miRNA1: Expr, miRNA2: Expr, ...})
        Map<String, Map<String, String>> mirnaExpressionMap = reshapeMiRNAData(mirnaDataFilePath);

        // Merge cleaned drug response data with reshaped miRNA expression data
        List<List<String>> mergedData = mergeData(cleanedDataFilePath, mirnaExpressionMap);

        // Write final merged dataset
        writeMergedCSV(finalMergedFilePath, mergedData);

        System.out.println("Final merged data saved successfully to: " + finalMergedFilePath);
    }

    // Reshape miRNA data: Convert long format to wide format
    private static Map<String, Map<String, String>> reshapeMiRNAData(String filePath) throws IOException {
        Map<String, Map<String, String>> reshapedData = new HashMap<>();

        try (Reader reader = new FileReader(filePath);
             @SuppressWarnings("deprecation")
            CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

            for (CSVRecord record : parser) {
                String cellLine = record.get("CellLineName");
                String mirna = record.get("miRNAName");
                String expression = record.get("Expression");

                // If cell line is not already in map, create new entry
                reshapedData.putIfAbsent(cellLine, new HashMap<>());
                reshapedData.get(cellLine).put(mirna, expression);
            }
        }
        return reshapedData;
    }

    // Merge cleaned drug response data with reshaped miRNA expression data using cell_line
    private static List<List<String>> mergeData(String cleanedDataFile, Map<String, Map<String, String>> mirnaMap) throws IOException {
        List<List<String>> mergedData = new ArrayList<>();
        Set<String> allMirnaNames = new HashSet<>();

        try (Reader reader = new FileReader(cleanedDataFile);
             @SuppressWarnings("deprecation")
            CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

            // Create new header by combining both CSV headers
            List<String> headers = new ArrayList<>(parser.getHeaderNames());

            // Collect all miRNA names from the reshaped miRNA data
            for (Map<String, String> mirnaValues : mirnaMap.values()) {
                allMirnaNames.addAll(mirnaValues.keySet());
            }
            headers.addAll(allMirnaNames);
            mergedData.add(headers);

            for (CSVRecord record : parser) {
                String cellLine = record.get("cell_line");
                List<String> row = new ArrayList<>(record.toMap().values());

                // If miRNA data exists for the cell line, add expression values
                Map<String, String> mirnaValues = mirnaMap.getOrDefault(cellLine, new HashMap<>());
                for (String mirna : allMirnaNames) {
                    row.add(mirnaValues.getOrDefault(mirna, "NA")); // Fill missing values with "NA"
                }

                mergedData.add(row);
            }
        }
        return mergedData;
    }

    // Write merged data to CSV
    private static void writeMergedCSV(String filePath, List<List<String>> data) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
             CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT)) {
            for (List<String> row : data) {
                printer.printRecord(row);
            }
        }
    }
}

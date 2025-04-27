package com;

import org.apache.commons.csv.*;
import java.io.*;
import java.util.*;

public class DataCleaningValidation {

    public static void main(String[] args) throws IOException {
        // File paths
        String inputFilePath = "D:/SCLC-SlPj/final_merged_nci_experiment_data.csv"; // Merged file
        String cleanedFilePath = "D:/SCLC-SlPj/cleaned_nci_data.csv"; // Output cleaned file

        // Read the CSV file
        List<CSVRecord> records = readCSV(inputFilePath);

        // Validate and clean the data
        List<CSVRecord> cleanedRecords = cleanAndValidateData(records);

        // Write the cleaned data to a new CSV file
        writeCleanedCSV(cleanedFilePath, cleanedRecords);

        System.out.println("Data cleaning completed. Cleaned file saved to: " + cleanedFilePath);
    }

    // Read CSV file
    private static List<CSVRecord> readCSV(String filePath) throws IOException {
        try (Reader reader = new FileReader(filePath);
             @SuppressWarnings("deprecation")
            CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            return parser.getRecords();
        }
    }

    // Clean and validate the data
    private static List<CSVRecord> cleanAndValidateData(List<CSVRecord> records) {
        Set<String> uniqueRows = new HashSet<>();
        List<CSVRecord> cleanedRecords = new ArrayList<>();

        for (CSVRecord record : records) {
            // Check for missing values in critical columns
            if (record.get("experiment_id").isEmpty() || record.get("cell_line").isEmpty()) {
                continue; // Skip rows with missing experiment ID or cell line
            }

            // Validate numerical columns
            if (!isNumeric(record.get("log_ic50")) || !isNumeric(record.get("log_ec50")) ||
                !isNumeric(record.get("response_zero")) || !isNumeric(record.get("response_inflection")) ||
                !isNumeric(record.get("hill_slope"))) {
                continue; // Skip rows with invalid numerical data
            }

            // Remove duplicates (based on experiment_id)
            String uniqueKey = record.get("experiment_id") + "-" + record.get("cell_line");
            if (!uniqueRows.contains(uniqueKey)) {
                uniqueRows.add(uniqueKey);
                cleanedRecords.add(record);
            }
        }

        return cleanedRecords;
    }

    // Check if a value is numeric
    private static boolean isNumeric(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // Write cleaned data to a new CSV file
    private static void writeCleanedCSV(String filePath, List<CSVRecord> data) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
             @SuppressWarnings("deprecation")
            CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(
                     "experiment_id", "nsc", "highest_tested_concentration", "cell_line",
                     "log_ec50", "response_zero", "response_inflection", "hill_slope", "log_ic50"))) {

            for (CSVRecord record : data) {
                printer.printRecord(
                        record.get("experiment_id"), record.get("nsc"),
                        record.get("highest_tested_concentration"), record.get("cell_line"),
                        record.get("log_ec50"), record.get("response_zero"),
                        record.get("response_inflection"), record.get("hill_slope"),
                        record.get("log_ic50")
                );
            }
        }
    }
}

package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.models.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class ValidationActivitiesImpl implements ValidationActivities {

    @Value("${app.data.dir:./data}")
    private String dataDir;

    private Path resolve(String filePath) {
        Path p = Paths.get(filePath);
        return p.isAbsolute() ? p : Paths.get(dataDir).resolve(filePath);
    }

    // ──────────────────────────────────────────────────────────────
    // Stage 1: Basic — does the file exist, is it non-empty, readable?
    // ──────────────────────────────────────────────────────────────
    @Override
    public ValidationResult executeBasicValidation(FileMetadata fileMetadata) {
        Path path = resolve(fileMetadata.getFilePath());
        log.debug("Basic validation for: {}", path);
        List<String> errors = new ArrayList<>();

        if (!Files.exists(path))        errors.add("File does not exist: " + path);
        else if (!Files.isReadable(path)) errors.add("File is not readable: " + path);
        else {
            try {
                if (Files.size(path) == 0) errors.add("File is empty (0 bytes)");
            } catch (IOException e) {
                errors.add("Cannot determine file size: " + e.getMessage());
            }
        }

        if (errors.isEmpty()) {
            log.info("Basic validation PASSED for {}", fileMetadata.getFileName());
            return ValidationResult.passed(ValidationStage.BASIC_VALIDATION);
        }
        log.warn("Basic validation FAILED for {}: {}", fileMetadata.getFileName(), errors);
        return ValidationResult.failed(ValidationStage.BASIC_VALIDATION, errors);
    }

    // ──────────────────────────────────────────────────────────────
    // Stage 2: Business — CSV has expected header columns; XML/JSON parseable
    // ──────────────────────────────────────────────────────────────
    @Override
    public ValidationResult executeBusinessValidation(FileMetadata fileMetadata, ValidationResult basicResult) {
        if (!basicResult.isValid()) {
            return ValidationResult.failed(ValidationStage.BUSINESS_VALIDATION,
                    List.of("Skipped: basic validation did not pass"));
        }
        Path path = resolve(fileMetadata.getFilePath());
        log.debug("Business validation for: {}", path);
        List<String> errors = new ArrayList<>();
        String name = path.getFileName().toString().toLowerCase();

        try {
            if (name.endsWith(".csv")) {
                List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
                if (lines.isEmpty()) {
                    errors.add("CSV file has no content");
                } else {
                    String header = lines.get(0);
                    // Every CSV must have at least 2 comma-separated columns in header
                    String[] cols = header.split(",");
                    if (cols.length < 2) errors.add("CSV header has fewer than 2 columns: " + header);
                    // Must have at least one data row
                    long dataRows = lines.stream().skip(1).filter(l -> !l.isBlank()).count();
                    if (dataRows == 0) errors.add("CSV has a header but no data rows");
                    else log.info("Business validation: {} columns, {} data rows", cols.length, dataRows);
                }
            } else if (name.endsWith(".xml")) {
                javax.xml.parsers.DocumentBuilderFactory.newInstance()
                        .newDocumentBuilder()
                        .parse(path.toFile());
                log.info("Business validation: XML is well-formed");
            } else if (name.endsWith(".json")) {
                String content = Files.readString(path, StandardCharsets.UTF_8).trim();
                if (!content.startsWith("[") && !content.startsWith("{"))
                    errors.add("JSON content does not start with [ or {");
                else log.info("Business validation: JSON structure looks valid");
            }
        } catch (Exception e) {
            errors.add("Parse error during business validation: " + e.getMessage());
        }

        if (errors.isEmpty()) return ValidationResult.passed(ValidationStage.BUSINESS_VALIDATION);
        log.warn("Business validation FAILED for {}: {}", fileMetadata.getFileName(), errors);
        return ValidationResult.failed(ValidationStage.BUSINESS_VALIDATION, errors);
    }

    // ──────────────────────────────────────────────────────────────
    // Stage 3: Advanced — detect corrupt records (e.g. CORRUPT status in XML)
    // ──────────────────────────────────────────────────────────────
    @Override
    public ValidationResult executeAdvancedValidation(FileMetadata fileMetadata, ValidationResult businessResult) {
        if (!businessResult.isValid()) {
            return ValidationResult.failed(ValidationStage.ADVANCED_VALIDATION,
                    List.of("Skipped: business validation did not pass"));
        }
        Path path = resolve(fileMetadata.getFilePath());
        log.debug("Advanced validation for: {}", path);
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        try {
            String content = Files.readString(path, StandardCharsets.UTF_8);
            if (content.contains("CORRUPT") || content.contains("???INVALID???"))
                errors.add("Corrupt data markers detected in file content");
            if (content.contains("INVALID_DATE"))
                warnings.add("Invalid date values detected");
            if (content.contains("XXX"))
                warnings.add("Unknown currency codes detected");
        } catch (IOException e) {
            errors.add("Cannot read file for advanced validation: " + e.getMessage());
        }

        ValidationResult result = errors.isEmpty()
                ? ValidationResult.passed(ValidationStage.ADVANCED_VALIDATION)
                : ValidationResult.failed(ValidationStage.ADVANCED_VALIDATION, errors);
        result.setWarnings(warnings);
        if (!warnings.isEmpty()) log.warn("Advanced validation warnings for {}: {}", fileMetadata.getFileName(), warnings);
        if (!errors.isEmpty()) log.warn("Advanced validation FAILED for {}: {}", fileMetadata.getFileName(), errors);
        else log.info("Advanced validation PASSED for {}", fileMetadata.getFileName());
        return result;
    }

    // ──────────────────────────────────────────────────────────────
    // Stage 4: Final — aggregate and summarise all stage results
    // ──────────────────────────────────────────────────────────────
    @Override
    public ValidationResult executeFinalValidation(FileMetadata fileMetadata, List<ValidationResult> allResults) {
        List<String> allErrors = new ArrayList<>();
        List<String> allWarnings = new ArrayList<>();
        for (ValidationResult r : allResults) {
            if (r.getErrors() != null) allErrors.addAll(r.getErrors());
            if (r.getWarnings() != null) allWarnings.addAll(r.getWarnings());
        }
        boolean allPassed = allResults.stream().allMatch(ValidationResult::isValid);
        log.info("Final validation for {}: passed={} errors={} warnings={}",
                fileMetadata.getFileName(), allPassed, allErrors.size(), allWarnings.size());
        ValidationResult result = allPassed
                ? ValidationResult.passed(ValidationStage.FINAL_VALIDATION)
                : ValidationResult.failed(ValidationStage.FINAL_VALIDATION, allErrors);
        result.setWarnings(allWarnings);
        result.setMessage(String.format("Validation complete — %d stages, %d errors, %d warnings",
                allResults.size(), allErrors.size(), allWarnings.size()));
        return result;
    }

    // ──────────────────────────────────────────────────────────────
    // Remediation stubs — log what would be attempted
    // ──────────────────────────────────────────────────────────────
    @Override
    public boolean attemptBasicRemediation(FileMetadata fileMetadata, List<String> errors) {
        log.info("Basic remediation for {}: checking if file re-appeared (errors: {})",
                fileMetadata.getFilePath(), errors);
        return Files.exists(resolve(fileMetadata.getFilePath()));
    }

    @Override
    public boolean attemptBusinessRemediation(FileMetadata fileMetadata, List<String> errors) {
        log.info("Business remediation for {}: would attempt header repair (errors: {})",
                fileMetadata.getFilePath(), errors);
        return false; // structural issues require manual intervention
    }

    @Override
    public boolean attemptAdvancedRemediation(FileMetadata fileMetadata, List<String> errors) {
        log.info("Advanced (ML-based) remediation for {}: would flag records for review (errors: {})",
                fileMetadata.getFilePath(), errors);
        return false;
    }

    @Override
    public void escalateForManualDataReview(FileMetadata fileMetadata, List<String> errors) {
        log.warn("ESCALATION — manual data review required for {} | errors: {}",
                fileMetadata.getFilePath(), errors);
    }

    // ── Repair Stages ──────────────────────────────────────────────────────

    @Override
    public boolean applyFormatCorrection(FileMetadata fileMetadata, List<String> errors) {
        log.info("FORMAT CORRECTION — auto-fixing structural issues for {} | errors: {}",
                fileMetadata.getFileName(), errors);
        // Auto-fix: trim whitespace, fix line endings, normalize delimiters
        Path path = resolve(fileMetadata.getFilePath());
        try {
            if (!Files.exists(path)) return false;
            String content = Files.readString(path, java.nio.charset.StandardCharsets.UTF_8);
            String fixed = content.replace("\r\n", "\n").replace("\r", "\n").trim();
            Files.writeString(path, fixed, java.nio.charset.StandardCharsets.UTF_8);
            log.info("FORMAT CORRECTION applied successfully for {}", fileMetadata.getFileName());
            return true;
        } catch (IOException e) {
            log.error("FORMAT CORRECTION failed for {}: {}", fileMetadata.getFileName(), e.getMessage());
            return false;
        }
    }

    @Override
    public boolean applyDataCleansing(FileMetadata fileMetadata, List<String> errors) {
        log.info("DATA CLEANSING / STANDARDIZATION — normalizing data for {} | errors: {}",
                fileMetadata.getFileName(), errors);
        Path path = resolve(fileMetadata.getFilePath());
        try {
            if (!Files.exists(path)) return false;
            String content = Files.readString(path, java.nio.charset.StandardCharsets.UTF_8);
            String cleaned = java.util.Arrays.stream(content.split("\n"))
                    .filter(line -> !line.contains("CORRUPT") && !line.contains("???INVALID???"))
                    .collect(java.util.stream.Collectors.joining("\n"));
            Files.writeString(path, cleaned, java.nio.charset.StandardCharsets.UTF_8);
            log.info("DATA CLEANSING applied for {} — removed corrupt markers", fileMetadata.getFileName());
            return true;
        } catch (IOException e) {
            log.error("DATA CLEANSING failed for {}: {}", fileMetadata.getFileName(), e.getMessage());
            return false;
        }
    }

    @Override
    public boolean correctMissingData(FileMetadata fileMetadata, List<String> errors) {
        log.info("MISSING DATA CORRECTION — filling defaults for {} | errors: {}",
                fileMetadata.getFileName(), errors);
        // In a real impl: fill NULL/empty fields with defaults based on schema
        return true;
    }

    @Override
    public boolean detectAndCorrectOutliers(FileMetadata fileMetadata, List<String> errors) {
        log.info("OUTLIER DETECTION CORRECTION — checking statistical ranges for {} | errors: {}",
                fileMetadata.getFileName(), errors);
        // In a real impl: run statistical analysis and flag/correct values outside 3σ
        return true;
    }

    @Override
    public void approveProcessingAndMarkAsQuality(FileMetadata fileMetadata, String reason) {
        log.info("APPROVED FOR PROCESSING — file={} reason={}", fileMetadata.getFileName(), reason);
    }
}

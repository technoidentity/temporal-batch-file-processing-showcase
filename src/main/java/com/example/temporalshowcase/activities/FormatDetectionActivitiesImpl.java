package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.models.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class FormatDetectionActivitiesImpl implements FormatDetectionActivities {

    private final ObjectMapper objectMapper;

    @Value("${app.data.dir:./data}")
    private String dataDir;

    private Path resolve(String filePath) {
        Path p = Paths.get(filePath);
        return p.isAbsolute() ? p : Paths.get(dataDir).resolve(filePath);
    }

    // ──────────────────────────────────────────────────────────────

    @Override
    public FileFormat analyzeAndDetectFormat(FileMetadata fileMetadata) {
        Path path = resolve(fileMetadata.getFilePath());
        String name = fileMetadata.getFileName() != null ? fileMetadata.getFileName().toLowerCase() : "";

        FileFormatType typeFromBytes = detectByMagicBytes(path);

        FileFormatType typeFromContent = (typeFromBytes == FileFormatType.UNKNOWN)
                ? detectByContentInspection(path) : typeFromBytes;

        String confidence = validateFormatSchema(path, typeFromContent);

        // Extension fallback when content inspection is also inconclusive
        FileFormatType finalType = typeFromContent;
        if (finalType == FileFormatType.UNKNOWN) {
            if (name.endsWith(".csv"))                             finalType = FileFormatType.CSV;
            else if (name.endsWith(".xml"))                       finalType = FileFormatType.XML;
            else if (name.endsWith(".json"))                      finalType = FileFormatType.JSON;
            else if (name.endsWith(".txt") || name.endsWith(".dat")) finalType = FileFormatType.FIXED_WIDTH;
        }

        log.info("Format detection for {}: magicBytes={} content={} final={} confidence={}",
                fileMetadata.getFileName(), typeFromBytes, typeFromContent, finalType, confidence);
        return FileFormat.builder()
                .type(finalType)
                .encoding("UTF-8")
                .detectionConfidence(confidence)
                .build();
    }

    /** Step 1: Read first few bytes and match known file signatures (magic bytes). */
    private FileFormatType detectByMagicBytes(Path path) {
        if (!Files.exists(path)) return FileFormatType.UNKNOWN;
        try {
            byte[] header = new byte[16];
            try (var is = Files.newInputStream(path)) {
                int read = is.read(header);
                if (read < 1) return FileFormatType.UNKNOWN;
            }
            String headerStr = new String(header, StandardCharsets.UTF_8).trim();
            if (headerStr.startsWith("<?xml") || headerStr.startsWith("<"))  return FileFormatType.XML;
            if (headerStr.startsWith("{")      || headerStr.startsWith("["))  return FileFormatType.JSON;
        } catch (IOException e) {
            log.debug("Magic bytes detection failed for {}: {}", path, e.getMessage());
        }
        return FileFormatType.UNKNOWN;
    }

    /** Step 2: Content Inspection — scan first few lines for structural patterns. */
    private FileFormatType detectByContentInspection(Path path) {
        if (!Files.exists(path)) return FileFormatType.UNKNOWN;
        try {
            List<String> sample = Files.lines(path, StandardCharsets.UTF_8).limit(5).toList();
            if (sample.isEmpty()) return FileFormatType.UNKNOWN;
            String firstLine = sample.get(0).trim();
            if (firstLine.startsWith("{") || firstLine.startsWith("[")) return FileFormatType.JSON;
            if (firstLine.startsWith("<")) return FileFormatType.XML;
            long commaLines = sample.stream().filter(l -> l.contains(",")).count();
            if (commaLines >= Math.min(2, sample.size())) return FileFormatType.CSV;
            boolean consistentLength = sample.stream().mapToInt(String::length)
                    .distinct().count() <= 2;
            if (consistentLength && sample.size() >= 2) return FileFormatType.FIXED_WIDTH;
        } catch (IOException e) {
            log.debug("Content inspection failed for {}: {}", path, e.getMessage());
        }
        return FileFormatType.UNKNOWN;
    }

    /** Step 3: Schema Validation — attempt to parse and validate format-specific rules. */
    private String validateFormatSchema(Path path, FileFormatType type) {
        if (!Files.exists(path)) return "LOW";
        try {
            return switch (type) {
                case XML -> {
                    DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(path.toFile());
                    yield "HIGH";
                }
                case JSON -> {
                    objectMapper.readTree(Files.readString(path, StandardCharsets.UTF_8));
                    yield "HIGH";
                }
                case CSV -> {
                    List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
                    if (lines.isEmpty()) yield "LOW";
                    int cols = lines.get(0).split(",").length;
                    boolean consistent = lines.stream().skip(1)
                            .filter(l -> !l.isBlank())
                            .allMatch(l -> l.split(",", -1).length == cols);
                    yield consistent ? "HIGH" : "MEDIUM";
                }
                case FIXED_WIDTH -> "MEDIUM";
                default -> "LOW";
            };
        } catch (Exception e) {
            log.debug("Schema validation failed for {} ({}): {}", path, type, e.getMessage());
            return "LOW";
        }
    }

    @Override
    public StandardizedData parseCsvFile(FileMetadata fileMetadata, FileFormat format) {
        Path path = resolve(fileMetadata.getFilePath());
        log.debug("Parsing CSV file: {}", path);
        try {
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
            if (lines.isEmpty()) {
                return emptyResult(fileMetadata, FileFormatType.CSV);
            }
            String[] headers = lines.get(0).split(",");
            List<Map<String, String>> rows = new ArrayList<>();
            for (int i = 1; i < lines.size(); i++) {
                String line = lines.get(i).trim();
                if (line.isBlank()) continue;
                String[] values = line.split(",", -1);
                Map<String, String> row = new LinkedHashMap<>();
                for (int h = 0; h < headers.length; h++) {
                    row.put(headers[h].trim(), h < values.length ? values[h].trim() : "");
                }
                rows.add(row);
            }
            log.info("Parsed CSV {}: {} records, headers={}", fileMetadata.getFileName(), rows.size(),
                    List.of(headers));
            return StandardizedData.builder()
                    .sourceFile(fileMetadata.getFilePath())
                    .originalFormat(FileFormatType.CSV)
                    .recordCount(rows.size())
                    .data(Map.of("headers", headers, "rows", rows))
                    .standardizedFormat("JSON")
                    .build();
        } catch (IOException e) {
            log.error("Failed to parse CSV {}: {}", path, e.getMessage());
            return emptyResult(fileMetadata, FileFormatType.CSV);
        }
    }

    @Override
    public StandardizedData parseXmlFile(FileMetadata fileMetadata, FileFormat format) {
        Path path = resolve(fileMetadata.getFilePath());
        log.debug("Parsing XML file: {}", path);
        try {
            Document doc = DocumentBuilderFactory.newInstance()
                    .newDocumentBuilder()
                    .parse(path.toFile());
            doc.getDocumentElement().normalize();
            String rootTag = doc.getDocumentElement().getTagName();
            NodeList children = doc.getDocumentElement().getChildNodes();
            long elementCount = 0;
            for (int i = 0; i < children.getLength(); i++) {
                if (children.item(i).getNodeType() == org.w3c.dom.Node.ELEMENT_NODE) elementCount++;
            }
            log.info("Parsed XML {}: root=<{}>, {} child elements",
                    fileMetadata.getFileName(), rootTag, elementCount);
            return StandardizedData.builder()
                    .sourceFile(fileMetadata.getFilePath())
                    .originalFormat(FileFormatType.XML)
                    .recordCount(elementCount)
                    .data(Map.of("root", rootTag, "elementCount", elementCount))
                    .standardizedFormat("JSON")
                    .build();
        } catch (Exception e) {
            log.error("Failed to parse XML {}: {}", path, e.getMessage());
            return emptyResult(fileMetadata, FileFormatType.XML);
        }
    }

    @Override
    public StandardizedData parseJsonFile(FileMetadata fileMetadata, FileFormat format) {
        Path path = resolve(fileMetadata.getFilePath());
        log.debug("Parsing JSON file: {}", path);
        try {
            String content = Files.readString(path, StandardCharsets.UTF_8);
            long recordCount;
            Map<String, Object> data;
            if (content.trim().startsWith("[")) {
                List<Object> list = objectMapper.readValue(content, new TypeReference<>() {});
                recordCount = list.size();
                data = Map.of("type", "array", "count", recordCount);
            } else {
                Map<String, Object> obj = objectMapper.readValue(content, new TypeReference<>() {});
                recordCount = 1;
                data = Map.of("type", "object", "keys", obj.keySet().size());
            }
            log.info("Parsed JSON {}: {} records", fileMetadata.getFileName(), recordCount);
            return StandardizedData.builder()
                    .sourceFile(fileMetadata.getFilePath())
                    .originalFormat(FileFormatType.JSON)
                    .recordCount(recordCount)
                    .data(data)
                    .standardizedFormat("JSON")
                    .build();
        } catch (IOException e) {
            log.error("Failed to parse JSON {}: {}", path, e.getMessage());
            return emptyResult(fileMetadata, FileFormatType.JSON);
        }
    }

    @Override
    public StandardizedData parseFixedWidthFile(FileMetadata fileMetadata, FileFormat format) {
        Path path = resolve(fileMetadata.getFilePath());
        log.debug("Parsing Fixed-Width file: {}", path);
        try {
            long lineCount = Files.lines(path, StandardCharsets.UTF_8)
                    .filter(l -> !l.isBlank())
                    .count();
            log.info("Parsed fixed-width {}: {} lines", fileMetadata.getFileName(), lineCount);
            return StandardizedData.builder()
                    .sourceFile(fileMetadata.getFilePath())
                    .originalFormat(FileFormatType.FIXED_WIDTH)
                    .recordCount(lineCount)
                    .data(Map.of("lines", lineCount))
                    .standardizedFormat("JSON")
                    .build();
        } catch (IOException e) {
            log.error("Failed to parse fixed-width {}: {}", path, e.getMessage());
            return emptyResult(fileMetadata, FileFormatType.FIXED_WIDTH);
        }
    }

    @Override
    public StandardizedData attemptFormatConversion(FileMetadata fileMetadata, FileFormat format) {
        log.info("Attempting format conversion for unknown-format file: {}", fileMetadata.getFilePath());
        // Best-effort: try to read as text and count lines
        Path path = resolve(fileMetadata.getFilePath());
        try {
            long lines = Files.lines(path, StandardCharsets.UTF_8).count();
            return StandardizedData.builder()
                    .sourceFile(fileMetadata.getFilePath())
                    .originalFormat(FileFormatType.UNKNOWN)
                    .recordCount(lines)
                    .data(Map.of("rawLines", lines))
                    .standardizedFormat("JSON")
                    .build();
        } catch (IOException e) {
            return emptyResult(fileMetadata, FileFormatType.UNKNOWN);
        }
    }

    @Override
    public void escalateUnknownFormat(FileMetadata fileMetadata, FileFormat format, List<String> errors) {
        log.error("ESCALATION — unknown format for {} (detected: {}, confidence: {}) errors: {}",
                fileMetadata.getFilePath(), format.getType(), format.getDetectionConfidence(), errors);
    }

    @Override
    public void handleFormatError(FileMetadata fileMetadata, FileFormat format, String errorMessage) {
        log.error("FORMAT ERROR HANDLER — file={} detectedFormat={} error={}",
                fileMetadata.getFileName(), format.getType(), errorMessage);
    }

    @Override
    public void handleConversionError(FileMetadata fileMetadata, String errorMessage) {
        log.error("CONVERSION ERROR HANDLER — file={} error={}",
                fileMetadata.getFileName(), errorMessage);
    }

    @Override
    public void handleValidationError(FileMetadata fileMetadata, FileFormat format, String errorMessage) {
        log.error("VALIDATION ERROR HANDLER — file={} format={} error={}",
                fileMetadata.getFileName(), format.getType(), errorMessage);
    }

    // ──────────────────────────────────────────────────────────────

    private StandardizedData emptyResult(FileMetadata meta, FileFormatType type) {
        return StandardizedData.builder()
                .sourceFile(meta.getFilePath())
                .originalFormat(type)
                .recordCount(0)
                .data(Map.of())
                .standardizedFormat("JSON")
                .build();
    }
}

package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.models.*;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

import java.util.List;
import java.util.Map;

@ActivityInterface
public interface DependencyActivities {

    // ── Dependency analysis ────────────────────────────────────────────────

    /** Step 1: Parse File Headers — reads the file's header line to discover its declared dependencies. */
    @ActivityMethod
    Map<String, List<String>> parseFileHeaders(List<FileMetadata> files);

    /** Step 2: Extract Dependency Metadata — uses header info to build per-file dependency lists. */
    @ActivityMethod
    List<String> extractFileDependencies(FileMetadata fileMetadata);

    /** Step 3 is done in the workflow (graph construction). */

    /** Step 4: Validate Graph Cycles — returns list of nodes involved in a cycle, empty if none. */
    @ActivityMethod
    List<String> detectGraphCycles(DependencyGraph graph);

    /** Step 6: Cycle Resolution — break cycles by removing lowest-priority edges. */
    @ActivityMethod
    void resolveCycles(DependencyGraph graph, List<String> cycle);

    /** Step 5: Topological Sort — returns nodes in safe processing order. */
    @ActivityMethod
    List<String> topologicalSort(DependencyGraph graph);

    // ── Processing pipeline queue management ───────────────────────────────

    /** Move a file into the Ready Queue. */
    @ActivityMethod
    void enqueueReady(FileMetadata file);

    /** Move a file from Ready → Processing Queue. */
    @ActivityMethod
    void markProcessing(FileMetadata file);

    /** Move a file from Processing → Completed Queue. */
    @ActivityMethod
    void markCompleted(FileMetadata file);

    /** Move a file from Processing → Failed Queue. */
    @ActivityMethod
    void markFailed(FileMetadata file, String reason);

    /** Report a dependency chain failure. */
    @ActivityMethod
    void handleFileProcessingFailure(FileMetadata fileMetadata, String errorMessage);
}

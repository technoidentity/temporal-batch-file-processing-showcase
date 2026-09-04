package com.example.temporalshowcase.workflows.pattern12;
import com.example.temporalshowcase.config.TemporalRuntime;

import com.example.temporalshowcase.activities.DependencyActivities;
import com.example.temporalshowcase.models.*;
import io.temporal.workflow.*;

import java.util.*;

/**
 * Pattern 12: Dependency Graph — main orchestrator.
 *
 * Flow:
 *  File Dependency Analyzer:
 *    1. Parse File Headers
 *    2. Extract Dependency Metadata
 *    3. Build Dependency Graph
 *    4. Validate Graph Cycles
 *       ├─ No Cycles  → 5. Topological Sort → Ordered Processing Workflow
 *       └─ Cycles     → 6. Cycle Resolution → Topological Sort → Ordered Processing Workflow
 *  Dependency Graph Manager:
 *    ├─ Dependencies Resolved → OrderedProcessingWorkflow (child)
 *    └─ Dependencies Missing  → DependencyWaitWorkflow (child) per waiting file
 */
public class DependencyGraphWorkflowImpl implements DependencyGraphWorkflow {

    private volatile int    completedCount  = 0;
    private          String processingStage = "STARTING";

    private final DependencyActivities dependencyActivities = Workflow.newActivityStub(
            DependencyActivities.class,
            TemporalRuntime.activity("dependency"));

    @Override
    public void manageFileDependencies(List<FileMetadata> files) {

        // ── Step 1: Parse File Headers ────────────────────────────────────────
        processingStage = "PARSING_HEADERS";
        Map<String, List<String>> headerDeps = dependencyActivities.parseFileHeaders(files);

        // ── Step 2: Extract Dependency Metadata ───────────────────────────────
        processingStage = "EXTRACTING_METADATA";
        List<FileMetadata> enriched = new ArrayList<>();
        for (FileMetadata f : files) {
            List<String> deps = headerDeps.getOrDefault(f.getFilePath(), Collections.emptyList());
            enriched.add(f.toBuilder().dependencies(deps).build());
        }

        // ── Step 3: Build Dependency Graph ────────────────────────────────────
        processingStage = "BUILDING_GRAPH";
        DependencyGraph graph = new DependencyGraph();
        for (FileMetadata f : enriched) graph.addNode(f.getFilePath());
        for (FileMetadata f : enriched) {
            List<String> deps = dependencyActivities.extractFileDependencies(f);
            for (String dep : deps) graph.addEdge(dep, f.getFilePath());
        }

        // ── Step 4: Validate Graph Cycles ─────────────────────────────────────
        processingStage = "VALIDATING_CYCLES";
        List<String> cycle = dependencyActivities.detectGraphCycles(graph);

        if (!cycle.isEmpty()) {
            // ── Step 6: Cycle Resolution — Cycles Found ──────────────────────
            processingStage = "CYCLE_RESOLUTION";
            dependencyActivities.resolveCycles(graph, cycle);
        }

        // ── Step 5: Topological Sort ──────────────────────────────────────────
        processingStage = "TOPOLOGICAL_SORT";
        List<String> processingOrder = dependencyActivities.topologicalSort(graph);

        Map<String, FileMetadata> fileMap = new HashMap<>();
        for (FileMetadata f : enriched) fileMap.put(f.getFilePath(), f);

        // ── Classify: Dependencies Resolved vs Dependencies Missing ───────────
        processingStage = "CLASSIFYING";
        List<FileMetadata> waiting  = new ArrayList<>();

        for (String path : processingOrder) {
            FileMetadata f = fileMap.get(path);
            if (f == null) continue;
            List<String> deps = graph.getDependencies(path);
            // "Dependencies Missing" = one or more upstream files not in this batch
            boolean anyMissing = deps.stream().anyMatch(dep -> !fileMap.containsKey(dep));
            if (anyMissing) waiting.add(f);
        }

        // ── Dependencies Missing → DependencyWaitWorkflow (one child per file) ─
        if (!waiting.isEmpty()) {
            processingStage = "WAITING_FOR_DEPENDENCIES";
            List<Promise<Void>> waitPromises = new ArrayList<>();
            for (FileMetadata waitFile : waiting) {
                List<String> missing = graph.getDependencies(waitFile.getFilePath())
                        .stream().filter(dep -> !fileMap.containsKey(dep)).toList();
                DependencyWaitWorkflow waitChild = Workflow.newChildWorkflowStub(
                        DependencyWaitWorkflow.class,
                        ChildWorkflowOptions.newBuilder()
                                .setWorkflowId("dep-wait-" + waitFile.getFileName() + "-"
                                        + Workflow.getInfo().getWorkflowId())
                                .setTaskQueue(TemporalRuntime.queue("dependency-graph"))
                                .build());
                waitPromises.add(Async.procedure(waitChild::waitUntilDependenciesMet, waitFile, missing));
            }
            Promise.allOf(waitPromises).get();
        }

        // ── Dependencies Resolved → OrderedProcessingWorkflow (child) ─────────
        processingStage = "PROCESSING";
        OrderedProcessingWorkflow orderedChild = Workflow.newChildWorkflowStub(
                OrderedProcessingWorkflow.class,
                ChildWorkflowOptions.newBuilder()
                        .setWorkflowId("ordered-" + Workflow.getInfo().getWorkflowId())
                        .setTaskQueue(TemporalRuntime.queue("dependency-graph"))
                        .build());

        List<FileMetadata> allOrdered = new ArrayList<>();
        for (String path : processingOrder) {
            FileMetadata f = fileMap.get(path);
            if (f != null) allOrdered.add(f);
        }

        orderedChild.processInOrder(allOrdered);
        completedCount = allOrdered.size();
        processingStage = "DONE";
    }

    @Override
    public int getCompletedFileCount() {
        return completedCount;
    }

    @Override
    public String getProcessingStage() {
        return processingStage;
    }
}

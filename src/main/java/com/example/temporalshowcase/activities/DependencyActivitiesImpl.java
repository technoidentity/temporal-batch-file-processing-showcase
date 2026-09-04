package com.example.temporalshowcase.activities;

import com.example.temporalshowcase.models.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class DependencyActivitiesImpl implements DependencyActivities {

    /** In-memory queues that represent the processing pipeline. */
    private static final Queue<String>       readyQueue      = new LinkedList<>();
    private static final Set<String>         processingQueue = ConcurrentHashMap.newKeySet();
    private static final Set<String>         completedQueue  = ConcurrentHashMap.newKeySet();
    private static final Map<String, String> failedQueue     = new ConcurrentHashMap<>();

    /**
     * Step 1: Parse File Headers.
     * For the showcase, returns a pre-defined dependency map:
     * E=none, A=none, B=[A], C=[A,B], D=[C].
     * In production this would read actual file headers from the landing zone.
     */
    @Override
    public Map<String, List<String>> parseFileHeaders(List<FileMetadata> files) {
        Map<String, String> pathByName = new LinkedHashMap<>();
        for (FileMetadata f : files) {
            pathByName.put(f.getFileName(), f.getFilePath());
        }

        Map<String, List<String>> headers = new LinkedHashMap<>();
        for (FileMetadata f : files) {
            List<String> logicalDeps;
            if (f.getDependencies() != null && !f.getDependencies().isEmpty()) {
                logicalDeps = f.getDependencies();
            } else {
                logicalDeps = defaultDepsForName(f.getFileName());
            }
            headers.put(f.getFilePath(), resolveDepKeys(logicalDeps, pathByName));
        }
        log.info("Parsed file headers for {} files — dependency map built", files.size());
        return headers;
    }

    /** Diagram default graph: E/A none, B→A, C→A+B, D→C. Missing names stay as unresolved keys. */
    private List<String> defaultDepsForName(String name) {
        if (name == null) return Collections.emptyList();
        if (name.contains("fileB") && !name.contains("fileA")) return List.of("fileA.csv");
        if (name.contains("fileC")) return List.of("fileA.csv", "fileB.csv");
        if (name.contains("fileD")) return List.of("fileC.csv");
        return Collections.emptyList();
    }

    /**
     * Maps a declared dependency to a file path when that file is in this batch.
     * If it is not in the batch the original key is kept so the graph can mark it missing.
     */
    private List<String> resolveDepKeys(List<String> deps, Map<String, String> pathByName) {
        List<String> resolved = new ArrayList<>();
        for (String dep : deps) {
            resolved.add(pathByName.getOrDefault(dep, dep));
        }
        return resolved;
    }

    /** Step 2: Extract per-file dependency list from the already-populated graph's metadata. */
    @Override
    public List<String> extractFileDependencies(FileMetadata fileMetadata) {
        log.debug("Extracting dependencies for file: {}", fileMetadata.getFilePath());
        if (fileMetadata.getDependencies() != null) {
            return fileMetadata.getDependencies();
        }
        return Collections.emptyList();
    }

    /** Step 4: Detect graph cycles using DFS colour-marking. */
    @Override
    public List<String> detectGraphCycles(DependencyGraph graph) {
        log.info("Detecting cycles in dependency graph with {} nodes", graph.getNodes().size());
        Set<String> white = new HashSet<>(graph.getNodes()); // unvisited
        Set<String> grey  = new HashSet<>();                 // in current DFS path
        List<String> cycle = new ArrayList<>();

        for (String node : graph.getNodes()) {
            if (white.contains(node)) {
                if (dfsCycleDetect(node, graph, white, grey, cycle)) {
                    log.warn("Cycle detected involving: {}", cycle);
                    return cycle;
                }
            }
        }
        log.info("No cycles detected in dependency graph");
        return Collections.emptyList();
    }

    private boolean dfsCycleDetect(String node, DependencyGraph graph,
                                    Set<String> white, Set<String> grey, List<String> cycle) {
        white.remove(node);
        grey.add(node);
        for (String dep : graph.getDependencies(node)) {
            if (grey.contains(dep)) {
                cycle.add(dep);
                cycle.add(node);
                return true;
            }
            if (white.contains(dep) && dfsCycleDetect(dep, graph, white, grey, cycle)) {
                return true;
            }
        }
        grey.remove(node);
        return false;
    }

    /** Step 6: Cycle Resolution — removes the edge that closes the cycle. */
    @Override
    public void resolveCycles(DependencyGraph graph, List<String> cycle) {
        if (cycle.size() < 2) return;
        String from = cycle.get(cycle.size() - 1);
        String to   = cycle.get(0);
        graph.removeEdge(from, to);
        log.warn("Cycle Resolution: removed edge {} → {} to break cycle {}", from, to, cycle);
    }

    /** Step 5: Topological sort — iterative Kahn's algorithm. */
    @Override
    public List<String> topologicalSort(DependencyGraph graph) {
        log.info("Performing topological sort on {} nodes", graph.getNodes().size());

        // inDegree[node] = number of upstream dependencies node must wait for
        Map<String, Integer> inDegree = new HashMap<>();
        for (String node : graph.getNodes()) {
            inDegree.put(node, graph.getDependencies(node).size());
        }

        Queue<String> queue = new LinkedList<>();
        for (Map.Entry<String, Integer> e : inDegree.entrySet()) {
            if (e.getValue() == 0) queue.offer(e.getKey());
        }

        List<String> sorted = new ArrayList<>();
        while (!queue.isEmpty()) {
            String node = queue.poll();
            sorted.add(node);
            for (String dependent : graph.getDependents(node)) {
                inDegree.merge(dependent, -1, Integer::sum);
                if (inDegree.get(dependent) == 0) queue.offer(dependent);
            }
        }

        if (sorted.size() != graph.getNodes().size()) {
            log.warn("Topological sort incomplete — {}/{} nodes sorted (remaining cycle)",
                    sorted.size(), graph.getNodes().size());
            graph.getNodes().stream().filter(n -> !sorted.contains(n)).forEach(sorted::add);
        }

        log.info("Topological sort result ({}): {}", sorted.size(), sorted);
        return sorted;
    }

    // ── Processing Pipeline queue operations ─────────────────────────────────

    @Override
    public void enqueueReady(FileMetadata file) {
        readyQueue.offer(file.getFilePath());
        log.info("READY QUEUE  ← {}", file.getFileName());
    }

    @Override
    public void markProcessing(FileMetadata file) {
        readyQueue.remove(file.getFilePath());
        processingQueue.add(file.getFilePath());
        log.info("PROCESSING QUEUE ← {} (ready={} processing={} completed={} failed={})",
                file.getFileName(), readyQueue.size(), processingQueue.size(),
                completedQueue.size(), failedQueue.size());
    }

    @Override
    public void markCompleted(FileMetadata file) {
        processingQueue.remove(file.getFilePath());
        completedQueue.add(file.getFilePath());
        log.info("COMPLETED QUEUE ← {} (completed={})", file.getFileName(), completedQueue.size());
    }

    @Override
    public void markFailed(FileMetadata file, String reason) {
        processingQueue.remove(file.getFilePath());
        failedQueue.put(file.getFilePath(), reason);
        log.error("FAILED QUEUE ← {} reason={}", file.getFileName(), reason);
    }

    @Override
    public void handleFileProcessingFailure(FileMetadata fileMetadata, String errorMessage) {
        log.error("Dependency chain failure — file={} error={}", fileMetadata.getFilePath(), errorMessage);
    }
}

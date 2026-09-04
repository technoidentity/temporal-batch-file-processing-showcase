package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DependencyGraph {
    @Builder.Default
    private List<String> nodes = new ArrayList<>();
    @Builder.Default
    private Map<String, List<String>> edges = new HashMap<>();

    public void addNode(String filePath) {
        if (!nodes.contains(filePath)) {
            nodes.add(filePath);
            edges.putIfAbsent(filePath, new ArrayList<>());
        }
    }

    public void addEdge(String from, String to) {
        edges.computeIfAbsent(from, k -> new ArrayList<>()).add(to);
    }

    public List<String> getDependencies(String filePath) {
        List<String> deps = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : edges.entrySet()) {
            if (entry.getValue().contains(filePath)) {
                deps.add(entry.getKey());
            }
        }
        return deps;
    }

    public List<String> getDependents(String filePath) {
        return edges.getOrDefault(filePath, new ArrayList<>());
    }

    public void removeEdge(String from, String to) {
        List<String> targets = edges.get(from);
        if (targets != null) targets.remove(to);
    }
}

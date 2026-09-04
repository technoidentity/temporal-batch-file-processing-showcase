package com.example.temporalshowcase.workflows.pattern12;
import com.example.temporalshowcase.config.TemporalRuntime;

import com.example.temporalshowcase.activities.DependencyActivities;
import com.example.temporalshowcase.models.FileMetadata;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.Workflow;

import java.time.Duration;
import java.util.List;

/**
 * Pattern 12 – Dependency Wait Workflow (Dependencies Missing path).
 *
 * Polls with backoff until missing upstream files are treated as available
 * (showcase: simulated arrival after a few poll cycles) or the wait times out.
 */
public class DependencyWaitWorkflowImpl implements DependencyWaitWorkflow {

    private final Duration POLL_INTERVAL     = TemporalRuntime.patterns().getDependency().getPollInterval();
    private final int      POLLS_UNTIL_READY = TemporalRuntime.patterns().getDependency().getPollsUntilReady();
    private final int      MAX_POLLS         = TemporalRuntime.patterns().getDependency().getMaxPolls();

    private final DependencyActivities depActivities = Workflow.newActivityStub(
            DependencyActivities.class,
            TemporalRuntime.activity("quick"));

    private String waitStatus = "STARTING";

    @Override
    public void waitUntilDependenciesMet(FileMetadata file, List<String> missingDeps) {
        waitStatus = "WAITING missing=" + missingDeps;
        depActivities.enqueueReady(file);

        for (int poll = 1; poll <= MAX_POLLS; poll++) {
            waitStatus = "POLLING " + poll + "/" + MAX_POLLS + " missing=" + missingDeps;
            Workflow.sleep(POLL_INTERVAL);

            if (poll >= POLLS_UNTIL_READY) {
                waitStatus = "READY — upstream dependencies satisfied for " + file.getFileName();
                return;
            }
        }

        waitStatus = "TIMED_OUT — dependencies never arrived for " + file.getFileName();
        depActivities.markFailed(file, "Dependency wait timeout");
        throw ApplicationFailure.newFailure(
                "Timeout waiting for dependencies: " + missingDeps + " for file: " + file.getFileName(),
                "DEPENDENCY_TIMEOUT");
    }

    @Override
    public String getWaitStatus() {
        return waitStatus;
    }
}

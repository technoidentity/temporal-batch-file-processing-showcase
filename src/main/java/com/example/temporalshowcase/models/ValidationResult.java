package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ValidationResult {
    private boolean valid;
    private ValidationStage stage;
    private List<String> errors;
    private List<String> warnings;
    private String message;

    public static ValidationResult passed(ValidationStage stage) {
        return ValidationResult.builder().valid(true).stage(stage).errors(new ArrayList<>()).build();
    }

    public static ValidationResult failed(ValidationStage stage, List<String> errors) {
        return ValidationResult.builder().valid(false).stage(stage).errors(errors).build();
    }
}

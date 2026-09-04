package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PermissionIssue {
    private String filePath;
    private String requiredPermission;
    private String currentPermission;
    private String errorCode;
    private String description;
}

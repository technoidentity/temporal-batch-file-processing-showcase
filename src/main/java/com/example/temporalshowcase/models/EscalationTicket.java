package com.example.temporalshowcase.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EscalationTicket {
    private String ticketId;
    private String filePath;
    private String assignedTeam;
    private String priority;
    private long createdAt;
    private String status;
}

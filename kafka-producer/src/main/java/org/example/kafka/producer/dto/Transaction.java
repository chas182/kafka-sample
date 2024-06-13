package org.example.kafka.producer.dto;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.LocalDateTime;

@Getter
@Setter
@EqualsAndHashCode
@ToString
public class Transaction {

    @NotEmpty
    private String bank;
    @NotNull
    @Positive
    private Long clientId;
    @NotNull
    private TransactionType orderType;
    @NotNull
    private Integer quantity;
    @NotNull
    private Double price;
    @NotNull
    private LocalDateTime createdAt;

}

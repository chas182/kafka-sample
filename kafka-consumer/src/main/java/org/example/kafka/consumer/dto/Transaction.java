package org.example.kafka.consumer.dto;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.example.kafka.consumer.domain.TransactionType;

import java.time.LocalDateTime;

@Getter
@Setter
@EqualsAndHashCode
@ToString
public class Transaction {

    private String bank;
    private Long clientId;
    private TransactionType orderType;
    private Integer quantity;
    private Double price;
    private LocalDateTime createdAt;

}

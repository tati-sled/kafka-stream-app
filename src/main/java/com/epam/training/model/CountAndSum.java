package com.epam.training.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Utility class to populate average values
 *
 * @author Tatiana_Slednikova
 * @version 1.0.0
 * @since 1.0.0
 */
@Data
@AllArgsConstructor(access = AccessLevel.PUBLIC)
public class CountAndSum {
    private Long count;
    private Double sum;

    @Override
    public String toString() {
        return count + " " + sum;
    }
}

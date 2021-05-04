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
    private long count;
    private double sum;

    @Override
    public String toString() {
        return count + " " + sum;
    }

    /**
     * Increment item
     *
     * @param value current value
     * @return CountAndSum instance
     */
    public CountAndSum incrementCountAndSum(double value) {
        this.count++;
        this.sum += value;
        return this;
    }

    /**
     * Evaluate average value
     *
     * @return average value
     */
    public double evaluateAverage() {
        return sum / count;
    }
}

package com.epam.training.util;

public class CountAndSum {
    private Long count;
    private Double sum;

    public CountAndSum(Long count, Double sum) {
        this.count = count;
        this.sum = sum;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Double getSum() {
        return sum;
    }

    public void setSum(Double sum) {
        this.sum = sum;
    }

    @Override
    public String toString() {
        return count + " " + sum;
    }
}

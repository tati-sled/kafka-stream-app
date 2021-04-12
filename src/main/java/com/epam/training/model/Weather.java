package com.epam.training.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.json.JSONObject;

/**
 * Weather data model.
 * Class refers to topic data represented in json.
 * Example: {"lng":-77.5231,"lat":16.0477,"avg_tmpr_f":84.7,"avg_tmpr_c":29.3,"wthr_date":"2017-09-27"}
 *
 * @author Tatiana_Slednikova
 * @version 1.0.0
 * @since 1.0.0
 */
@Data
@AllArgsConstructor
@Builder
public class Weather {

    double longitude;
    double latitude;
    double averageTemperatureF;
    double averageTemperatureC;
    String weatherDate;
    String geoHash;

    public Weather(double averageTemperatureC, double averageTemperatureF, String weatherDate, String geoHash) {
        this.averageTemperatureC = averageTemperatureC;
        this.averageTemperatureF = averageTemperatureF;
        this.weatherDate = weatherDate;
        this.geoHash = geoHash;
    }

    @Override
    public String toString() {
        return new JSONObject().put("lng", getLongitude())
                .put("lat", getLatitude())
                .put("avg_tmpr_f", getAverageTemperatureF())
                .put("avg_tmpr_c", getAverageTemperatureC())
                .put("wthr_date", getWeatherDate())
                .put("geoHash", getGeoHash())
                .toString();
    }

}

package com.epam.training.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.json.JSONObject;

/**
 * Hotel data model.
 * Class refers to topic data represented as json.
 * Example: {"Id":"3401614098432","Name":"London Suites","Country":"GB","City":"London","Address":"230 A Mile End Road Tower Hamlets London E1 4LJ United Kingdom","Latitude":"51.5215508","Longitude":"-0.0469238","Geohash":"gcpv"}
 *
 * @author Tatiana_Slednikova
 * @version 1.0.0
 * @since 1.0.0
 */
@Data
@AllArgsConstructor
@Builder
public class Hotel {

    String id;
    String name;
    String country;
    String city;
    String address;
    double latitude;
    double longitude;
    String geoHash;
    String date;
    String averageTemperatureC;
    String averageTemperatureF;

    @Override
    public String toString() {
        return new JSONObject()
                .put("Id", getId())
                .put("Name", getName())
                .put("Country", getCountry())
                .put("City", getCity())
                .put("Address", getAddress())
                .put("Latitude", getLatitude())
                .put("Longitude", getLongitude())
                .put("Geohash", getGeoHash())
                .put("Date", getDate())
                .put("Average Temperature C", getAverageTemperatureC())
                .put("Average Temperature F", getAverageTemperatureF())
                .toString();
    }

}

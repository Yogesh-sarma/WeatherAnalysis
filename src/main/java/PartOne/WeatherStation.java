package PartOne;

public class WeatherStation {
    private String id;
    private String state;
    private String name;

    public WeatherStation(String id, double latitude, String state, String name, String gsnFlag) {
        this.id = id;
        this.state = state;
        this.name = name;
    }

    public WeatherStation() {
        this.id = "";
        this.state = "state";
        this.name = "name";
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return id + ", " + state + ", " +
                "," + name ;
    }
}

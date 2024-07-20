package PartOne;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FilterAugmentReducer extends Reducer<Text, TempDetails, Text, FilteredAugmentedData> {
    private FileSystem fs;
    private Map<String, LocationWritable> locationMap;
    private Gson gson = new Gson();
    int tenCount=0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        fs = FileSystem.get(conf);
        Type mapType = new TypeToken<Map<String, LocationWritable>>() {}.getType();
        locationMap = gson.fromJson(conf.get("stationDetails"), mapType);
    }

    public void reduce(Text key, Iterable<TempDetails> values, Context context) throws IOException,
            InterruptedException {
        String prevDate = "", currDate;
        FilteredAugmentedData result = new FilteredAugmentedData();
        Map<String, List<FilteredAugmentedData>> dataMap = new HashMap<>();
        List<FilteredAugmentedData> dataList = new ArrayList<>();

        for(TempDetails tempDetails : values){
            currDate = tempDetails.getDate().toString();
            if(prevDate.isEmpty()){
                prevDate=currDate;
            }

            LocationWritable locationDetails;
            if(!currDate.equals(prevDate)) {
                if (locationMap.get(key.toString()) != null) {
                    locationDetails = locationMap.get(key.toString());
                }
                else {
                    locationDetails = getLocationDetails(key.toString(), context);
                }
                result.setLocation(locationDetails);
                prevDate = currDate;
                // Handling Missing or incomplete data
                if (result.getTavg().get() == -999 || result.getTmin().get() == -999 || result.getTmax().get() == -999 || result.getPrcp().get()== -999){
                    tenCount++;
                }
                else {
                    tenCount=0;
                }
                if(tenCount==10){
                    dataMap.remove(key.toString());
                    break;
                }
                dataList.add(result);
                dataMap.put(key.toString(), dataList);
                result = new FilteredAugmentedData();
            }
            result.setDate(tempDetails.getDate());
            if(tempDetails.getElement().toString().equalsIgnoreCase("tmax")){
                result.setTmax(new FloatWritable(Float.parseFloat(tempDetails.getValue().toString())));
            }
            else if(tempDetails.getElement().toString().equalsIgnoreCase("tmin")){
                result.setTmin(new FloatWritable(Float.parseFloat(tempDetails.getValue().toString())));
            }
            else if(tempDetails.getElement().toString().equalsIgnoreCase("tavg")){
                result.setTavg(new FloatWritable(Float.parseFloat(tempDetails.getValue().toString())));
            }
            else if(tempDetails.getElement().toString().equalsIgnoreCase("prcp")) {
                result.setPrcp(new IntWritable(Integer.parseInt(tempDetails.getValue().toString())));
            }
        }


        if(tenCount!=10 && dataMap.containsKey(key.toString())){
            //impute missing Temperature data and handling temperature outliers
            for(FilteredAugmentedData data : dataMap.get(key.toString())){
                float min = data.getTmin().get();
                float max = data.getTmax().get();
                float avg = data.getTavg().get();
                if(avg==-999 && min ==-999 && max!=-999){
                    data.setTavg(new FloatWritable(max));
                    data.setTmin(new FloatWritable(max));
                }
                if(avg==-999 && max ==-999 && min!=-999){
                    data.setTavg(new FloatWritable(min));
                    data.setTmax(new FloatWritable(min));
                }
                if(max==-999 && min==-999 && avg!=-999){
                    data.setTmin(new FloatWritable(avg));
                    data.setTmax(new FloatWritable(avg));
                }
                if(max==-999 && avg==-999 && min==-999){
                    data.setTmax(new FloatWritable(0));
                    data.setTmin(new FloatWritable(0));
                    data.setTavg(new FloatWritable(0));
                }
                if((avg==-999 || avg>=150) && min!=-999 && max!=-999){
                    data.setTavg(new FloatWritable((min+max)/2));
                }
                //min doesn't exist or has lesser value than -100(outlier)
                //managing both in the same way
                if(min<-100 && avg!=-999 && max!=-999){
                    data.setTmin(new FloatWritable(avg - (max/2)));
                }
                //max doesn't exist or has greater value than 250(outlier)
                //managing both in the same way
                if((max==-999 || max>=150) && avg!=-999 && min!=-999) {
                    data.setTmax(new FloatWritable(avg + (Math.abs(min) / 2)));
                }
            }

            for(int i=0; i<3 && dataMap.get(key.toString()).size()>=i; i++){
                    if(dataMap.get(key.toString()).get(i).getPrcp().get()==-999){
                        dataMap.get(key.toString()).get(i).setPrcp(new IntWritable(0));
                    }
            }
            //handling missing precipitation and outliers
            for(int i=3; i<dataMap.get(key.toString()).size();i++){
                int prcp = dataMap.get(key.toString()).get(i).getPrcp().get();
                if(prcp==-999 || prcp>150){
                    int avg=0;
                    for(int j=i-3;j<i;j++){
                        avg += dataMap.get(key.toString()).get(j).getPrcp().get();
                    }
                    avg/=3;
                    dataMap.get(key.toString()).get(i).setPrcp(new IntWritable(avg));
                }
            }

            for(FilteredAugmentedData data : dataMap.get(key.toString())){
                context.write(key, data);
            }
        }
    }

    private LocationWritable getLocationDetails(String stationId, Context context) throws IOException {
        LocationWritable details = new LocationWritable();
        URI[] cacheFiles = DistributedCache.getCacheFiles(context.getConfiguration());
        if(cacheFiles != null && cacheFiles.length==1){
            Path filePath = new Path(cacheFiles[0].getPath());
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));

            String line;
            while ((line = reader.readLine()) != null) {
                if(line.split("\\s+ ")[0].equalsIgnoreCase(stationId)){
                    WeatherStation station = parseWeatherStation(line);
                    details.setLocation(new Text(station.getName()));
                    details.setState(new Text(station.getState()));
                    details.setCountry(new Text(stationId.substring(0,2)));
                }
            }
            locationMap.put(stationId, details);
            reader.close();
        }
        return details;
    }

    private WeatherStation parseWeatherStation(String line) {
        WeatherStation station = new WeatherStation();
        System.out.println("parse-"+line.length());
        try {
            station.setId(line.substring(0, 11).trim());
            if(!line.substring(38, 40).trim().isEmpty()){
                station.setState(line.substring(38, 40).trim());
            }
            if(!line.substring(41, 71).trim().isEmpty()){
                station.setName(line.substring(41, 71).trim());
            }
        } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
            station.setId(line.substring(0, 11).trim());
            station.setState("state");
            station.setName("name");
        }
        return station;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        if (fs != null) {
            fs.close();
        }
        context.getConfiguration().set("stationDetails", gson.toJson(locationMap));
    }
}

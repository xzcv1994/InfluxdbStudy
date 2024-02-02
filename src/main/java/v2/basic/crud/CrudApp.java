package v2.basic.crud;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import config.CONFIG;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CrudApp {
    public static void main(String[] args) throws InterruptedException {
        //1.influxdb 연결 생성
        String url = CONFIG.DB_URL;
        String token = CONFIG.DB_TOKEN;
        String org = CONFIG.DB_ORG;
        String bucket = CONFIG.DB_BUCKET;

        InfluxDBClient influxDBClient = InfluxDBClientFactory.create(url, token.toCharArray(), org, bucket);
        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();

        //2.insert thread 생성
        Thread insertThread = new Thread(() -> {
            Point point1 = Point.measurement("iot")
                    .addTag("location", "1F")
                    .addTag("type", "sensor")
                    .addTag("model", "temper")
                    .addField("value", 20.0 + (30.0 - 20.0) * (new Random().nextDouble()));

            writeApi.writePoint(point1);

            Point point2 = Point.measurement("iot")
                    .addTag("location", "1F")
                    .addTag("type", "sensor")
                    .addTag("model", "humidity")
                    .addField("value", 40.0 + (70.0 - 40.0) * (new Random().nextDouble()));

            writeApi.writePoint(point2);
        });

        //3.select thread 생성
        String flux = "from(bucket:\"ktDB\") |> range(start:-10s) |> filter(fn: (r) => r._measurement == \"iot\")";
        QueryApi queryApi = influxDBClient.getQueryApi();

        Thread selectThread = new Thread(() -> {
            System.out.println("============================ getSensor Value ============================");
            List<FluxTable> tables = queryApi.query(flux);
            for (FluxTable fluxTable : tables) {
                List<FluxRecord> records = fluxTable.getRecords();
                for (FluxRecord fluxRecord : records) {
                    System.out.println(fluxRecord.getTime() + ": " + fluxRecord.getValueByKey("_value"));
                }
            }
        });

        //4. insert, select 주기 실행
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);
        executorService.scheduleAtFixedRate(insertThread, 0, 6, TimeUnit.SECONDS);
        executorService.scheduleAtFixedRate(selectThread, 0, 3, TimeUnit.SECONDS);
    }
}

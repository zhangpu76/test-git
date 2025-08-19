import com.stream.common.utils.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package PACKAGE_NAME.Test
 * @Author zhou.han
 * @Date 2024/12/17 15:04
 * @description:
 */
public class Test {


    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
//        MySqlSource<String> mySQLDbMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
//                "realtime_v3",
//                "realtime_v3.cdc_test_tbl",
//                ConfigUtils.getString("mysql.user"),
//                ConfigUtils.getString("mysql.pwd"),
//                StartupOptions.initial()
//        );


//        DataStreamSource<String> ds = env.fromSource(mySQLDbMainCdcSource, WatermarkStrategy.noWatermarks(), "test-cdc-source");
//        ds.print();


        env.execute();
    }



}

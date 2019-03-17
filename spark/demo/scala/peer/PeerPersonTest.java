import org.apache.spark.rdd.RDD;
import scala.Option;
import scala.Tuple2;

import java.util.HashSet;
import java.util.Set;

public class PeerPersonTest {

    public static void main(String[] args) {
//        System.setProperty("HADOOP_USER_NAME", "root");
        Set<String> faceLibIds = new HashSet<>();
        faceLibIds.add("cccj1");
        faceLibIds.add("I");


//        String jars = "D:/peer-person/spark-peer-person/target/spark-peer-person-1.0-SNAPSHOT.jar," +
//                "D:/peer-person/spark-peer-person/output/spark-peer-person-1.0-SNAPSHOT/lib/*";
        String jars = "local:/usr/spark-peer-person-output/lib/*";
        PeerPersonBuilder builder = PeerPersonBuilder.applySimple("hdh132", "hdh130", "hdh130,hdh132,hdh134", jars);
        Option<PeerPerson> option = builder.build();
        int version = 1;

        try {
            if (option.nonEmpty()) {
                PeerPerson peerPerson = option.get();
                PeerPersonParam param = PeerPersonParam.applySimple(faceLibIds, 5000, 5000, 30, 3);
                RDD<Tuple2<BaseData, BaseData>> rdd = peerPerson.loadRdd(param);
                System.out.println("计算结束==========================================");
                peerPerson.sendToKafkaSimple(rdd, "hdh130:9092,hdh132:9092,hdh134:9092", version);
                System.out.println("发送数据结束==========================================");
            }
        } finally {
            if (option.nonEmpty()) {
                option.get().close();
            }
        }

    }

}

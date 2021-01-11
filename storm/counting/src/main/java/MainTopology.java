import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class MainTopology {
    public static void main(String[] args) throws Exception {
        MainTopology topology = new MainTopology();
        topology.runLocal(60);
    }

    public void runLocal(int waitSeconds) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("wordSpout", new WordSpout(), 1);
        builder.setBolt("countBolt", new CountBolt(), 1).shuffleGrouping("wordSpout");

        Config config = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word_count", config, builder.createTopology());

        try {
            Thread.sleep(waitSeconds * 1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        cluster.killTopology("word_count");
        cluster.shutdown();
    }
}

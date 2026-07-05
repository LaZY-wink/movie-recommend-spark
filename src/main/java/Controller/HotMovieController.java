package Controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class HotMovieController {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    // ============ 阶段4：实时大屏接口 ============

    @GetMapping("/hot/ranking")
    public List<Map<String, Object>> getRanking() {
        // ✅ 累加所有历史数据，按 movie_id 分组求和
        String sql = "SELECT movie_id, SUM(click_count) as click_count " +
                "FROM hot_movie_ranking " +
                "GROUP BY movie_id " +
                "ORDER BY click_count DESC " +
                "LIMIT 10";
        return jdbcTemplate.queryForList(sql);
    }

    // ============ 阶段5：推荐系统接口 ============

    @GetMapping("/recommend/{userId}")
    public List<Map<String, Object>> getRecommendations(@PathVariable String userId) {
        List<Map<String, Object>> result = new ArrayList<>();
        Connection connection = null;
        Table table = null;

        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "192.168.102.211");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf("movie_reco"));

            Get get = new Get(Bytes.toBytes(userId));
            get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("recommendations"));
            Result hbaseResult = table.get(get);

            if (!hbaseResult.isEmpty()) {
                byte[] value = hbaseResult.getValue(Bytes.toBytes("info"), Bytes.toBytes("recommendations"));
                if (value != null) {
                    String recStr = Bytes.toString(value);
                    String[] recs = recStr.split("\\|");
                    for (String rec : recs) {
                        String[] parts = rec.split(":");
                        if (parts.length == 3) {
                            Map<String, Object> item = new HashMap<>();
                            item.put("movieId", parts[0]);
                            item.put("title", parts[1]);
                            item.put("rating", Double.parseDouble(parts[2]));
                            result.add(item);
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try { if (table != null) table.close(); } catch (Exception e) {}
            try { if (connection != null) connection.close(); } catch (Exception e) {}
        }

        return result;
    }

    @GetMapping("/users")
    public List<String> getUsers() {
        List<String> users = new ArrayList<>();
        Connection connection = null;
        Table table = null;

        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "192.168.102.211");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf("movie_reco"));

            Scan scan = new Scan();
            scan.addFamily(Bytes.toBytes("info"));
            ResultScanner scanner = table.getScanner(scan);

            for (Result result : scanner) {
                String rowKey = Bytes.toString(result.getRow());
                users.add(rowKey);
            }
            scanner.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try { if (table != null) table.close(); } catch (Exception e) {}
            try { if (connection != null) connection.close(); } catch (Exception e) {}
        }

        Collections.sort(users);
        return users;
    }
}
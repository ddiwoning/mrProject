package mongo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.log4j.Log4j;
import mongo.conn.MongoDBConnection;
import mongo.dto.AccessLogDTO;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j
public class MonthLog2Reducer extends Reducer<Text, Text, Text, IntWritable> {

    private MongoDatabase mongodb;

    private Map<String, String> months = new HashMap<>();

    public MonthLog2Reducer() {

        this.months.put("Jan", "LOG_01");
        this.months.put("Feb", "LOG_02");
        this.months.put("Mar", "LOG_03");
        this.months.put("Apr", "LOG_04");
        this.months.put("May", "LOG_05");
        this.months.put("Jun", "LOG_06");
        this.months.put("Jul", "LOG_07");
        this.months.put("Aug", "LOG_08");
        this.months.put("Sep", "LOG_09");
        this.months.put("Oct", "LOG_10");
        this.months.put("Nov", "LOG_11");
        this.months.put("Dec", "LOG_12");
    }

    @Override
    protected void setup(Reducer<Text, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {

        this.mongodb = new MongoDBConnection().getMongoDB();

        for (String month : this.months.keySet()) {

            String colNm = this.months.get(month);

            boolean create = true;

            for (String s : this.mongodb.listCollectionNames()) {

                if (colNm.equals(s)) {
                    create = false;
                    break;
                }
            }

            if (create) {

                this.mongodb.createCollection(colNm);
            }
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        List<Document> pList = new ArrayList<>();

        String colNm = this.months.get(key.toString());

        log.info("key : " + key);
        log.info("colNm : " + colNm);

        MongoCollection<Document> col = mongodb.getCollection(colNm);

        for (Text value : values) {

            String json = value.toString();

            AccessLogDTO pDTO = new ObjectMapper().readValue(json, AccessLogDTO.class);

            Document doc = new Document(new ObjectMapper().convertValue(pDTO, Map.class));

            pList.add(doc);

            doc = null;
        }

        int pListSize = pList.size();
        int blockSize = 50000;

        for (int i = 0; i < pListSize; i += blockSize) {

            col.insertMany(new ArrayList<>(pList.subList(i, Math.min(i + blockSize, pListSize))));


        }

        col = null;
        pList = null;
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        // MongoDB 접속 해제
        this.mongodb = null;
    }
}

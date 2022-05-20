package maponly;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ImageCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * 부모 Mapper 자바 파일에 작성된 map 함수를 덮어쓰기 수행
     * map 함수는 분석할 파일의 레코드 1줄마다 실행됨
     * 파일의 라인수가 100개라면, map 함수는 100번 실행됨
     */
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

            // 분석할 파일의 한 줄 값
            String[] fields = value.toString().split("\"");

            if (fields.length > 1) {
                String request = fields[1];
                fields = request.split(" ");

                if (fields.length > 1) {

                    String fileName = fields[1].toLowerCase();

                    if (fileName.endsWith(".jpg")) {
                        context.getCounter("imageCount", "jpg").increment(1);

                    } else if (fileName.endsWith(".gif")) {
                        context.getCounter("imageCount", "gif").increment(1);

                    } else {
                        context.getCounter("imageCount", "other").increment(1);
                }
            }
        }
    }
}

package partition;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * 리듀스 역할을 수행하기 위해서는 Reducer 자바 파일을 상속받아야함
 * Reducer 파일의 앞의 2개 데이터 타입(Text, IntWritable)은 Suffle and Sort에 보낸 데이터의 키와 값의 데이터 타입
 * 보통 Mapper에서 보낸 데이터타입과 동일함
 * Reducer 파일의 뒤의 2개 데이터 타입(Text, IntWritable)은 결과 파일 생성에 사용도리 키와 값
 *
 */
public class MonthLogReducer extends Reducer<Text, Text, Text, IntWritable> {

    /**
     * 부모 Reducer 자바 파일에 작성된 reduce 함수를 덮으쓰기 수행
     * reduce 함수는 suffle and sort로 처리된 데이터마다 실행됨
     * 처리된 데이터의 수가 500개라면, reduce 함수는 500번 실행됨
     *
     * reduce 객체는 기본값이 1개로 1개의 쓰레드로 처리함
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        //IP별 빈도수를 계산하기 위한 변수
        int ipCount = 0;

        //suffle and sort로 인해 단어별로 데이터들의 값들이 List 구조로 저장됨
        //파티셔너를 통해 같은 월에 해당되는 ip만 넘어옴
        //192.168.0.1 : {'Mon', 'Mon', 'Mon', 'Mon', 'Mon', }
        //배열의 수는 IP 호출 수이며, 배열의 수를 계산함
        for (Text value : values) {
            ipCount++;
        }

        //분석 결과가 파일에 데이터 저장하기
        context.write(key, new IntWritable(ipCount));
    }


}
package partition;


import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * 맵리듀스를 실행하기 위한 main 함수가 존재하는 자바 파일
 * 드라이버 파일로 부름
 */
@Log4j
public class TimeLog extends Configuration implements Tool {


    //맵리듀스 실행 함수
    public static void main(String[] args) throws Exception {

        //파라미터는 분석할 파일과 분석 겨로가가 저장될파일로 2개를 받음
        if (args.length != 2) {
            log.info("분석할 폴더 및 분석결과가 저장될 폴더를 입력해야 합니다.");
            System.exit(-1);

        }
        int exitCode = ToolRunner.run(new TimeLog(), args);

        System.exit(exitCode);

    }

    @Override
    public void setConf(Configuration configuration) {

        //App이름 정의
        configuration.set("AppName", "Partitioner Test");

    }

    @Override
    public Configuration getConf() {

        //맵리듀스 전체에 적용될 변수를 정의할 때 사용
        Configuration conf = new Configuration();

        //변수 정의
        this.setConf(conf);

        return conf;
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        String appName = conf.get("AppName");

        log.info("appName : " + appName);

        //맵리듀스 실행을 위한 잡 객체를 가져오기
        //하둡이 실행되면, 기본적으로 잡 객체를 메모리에 올림
        Job job = Job.getInstance();

        //호출이 발생하면, 메모리에 저장하여 캐시 처리 수행
        //하둡분산파일시스템에 저장된 파일만 가능함
        job.addCacheFile(new Path("/access_log").toUri());


        // 맵리듀스 잡이 시작되는 main함수가 존재하는 파일 설정
        job.setJarByClass(TimeLog.class);

        //맵리듀스 잡 이름 설정 , 리소스 매니저 등 맵리듀스 실행 결과 및 로그 확인 시 편리함
        job.setJobName(appName);

        // 분석할 폴더 -- 첫번재 파라미터
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 분석 결과가 저장되는 폴더 -- 두번째 파라미터
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 맵리듀스의 맵 역할을 수행하는 Mapper 자바 파일 설정
        job.setMapperClass(TimeLogMapper.class);

        // 맵리듀스의 리듀스 역할을 수행하는 Reducer 자바 파일 설정
        job.setReducerClass(TimeLogReducer.class);

        //리듀스를 여러 개 분산해서 실행하기 위해 사용되는 파티셔너 객체 설정
        job.setPartitionerClass(TimeLogPartitioner.class);

        //1일은 24시간으로 리듀스 24를 생성하여 리듀스 1개당 1시간씩 ㅣ처리
        //예를 들어 0시는 0번 리듀스, 23월을 23번 리듀스가 데이터 처리함
        job.setNumReduceTasks(24);

        //Mapper에서 suffle and sort로 전달하는 키의 데이터 타입
        job.setMapOutputKeyClass(Text.class);

        //Mapper에서 suffle and sort로 전달하는 값의 데이터 타입
        job.setMapOutputValueClass(Text.class);

        //분석 결과가 저장될 때 사용될 키의 데이터 타입
        job.setOutputKeyClass(Text.class);

        //분석 결과가 저장될 때 사용될 값의 데이터 타입
        job.setOutputValueClass(IntWritable.class);

        //맵리듀스 실행
        boolean success = job.waitForCompletion(true);

        return (success ? 0 : 1);
    }

}

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;

/**
 * Description：
 * Created by luolinjie on 2018/4/25.
 */
public class TestPrintSet {
    public  static  void main(String[] args) throws ParseException {
        String date = "2017:04:27 17:00:00";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss");
        Date parse = simpleDateFormat.parse(date);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String format = sdf.format(parse);
        System.out.println(format);
    }
}

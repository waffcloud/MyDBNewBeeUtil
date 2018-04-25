import java.util.HashSet;

/**
 * Description：
 * Created by luolinjie on 2018/4/25.
 */
public class TestPrintSet {
    public  static  void main(String[] args){
        HashSet<String> reports = new HashSet<String>();
        reports.add(" \r\n执行SQL成功数： singleFileSqlCounter_success + -----失败数：  singleFileSqlCounter_success)|");
        System.out.println(reports);
    }
}

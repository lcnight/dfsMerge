import java.util.Date;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.text.SimpleDateFormat;

public class DateUtil
{
    private static Date inDate = new Date();
    private static Calendar calendar = new GregorianCalendar();
    private static SimpleDateFormat ymdFmt = new SimpleDateFormat("yyyyMMdd");
    private static SimpleDateFormat ymdStrFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * @brief return day identifier offset of baseDay
     * @param baseDay ::= YYYYMMDD
     * @param offset ::= +/-n
     * @return 
     */
    public static int getDayOff(int baseDay, int offset)
    {
        int year = baseDay / 10000;
        int month = baseDay % 10000 / 100;
        int day = baseDay % 100;
        calendar.set(year, month, day);
        calendar.add(Calendar.DAY_OF_MONTH, offset);
        return calendar.get(Calendar.YEAR) * 10000 + 
            calendar.get(Calendar.MONTH) * 100 + calendar.get(Calendar.DAY_OF_MONTH);
    }

    /**
     * @brief return month identifier offset of baseMonth
     * @param baseDay ::= YYYYMM
     * @param offset ::= +/-n
     * @return 
     */
    public static int getMonthOff(int baseMonth, int offset)
    {
        int year = baseMonth / 100;
        int month = baseMonth % 100;
        calendar.set(year, month, 1);
        calendar.add(Calendar.MONTH, offset);
        return calendar.get(Calendar.YEAR) * 100 + calendar.get(Calendar.MONTH);
    }

    public static String getYMDFormat(long timestamp)
    {
        inDate.setTime(timestamp * 1000L);
        return ymdFmt.format(inDate);
    }

    public static String getYMDFormat(String timestr) 
    {
        long timestamp = 0;
        try { 
            timestamp = Long.valueOf(timestr).longValue();
        } catch (Exception e) {
            timestamp = 0;
        } 

        return getYMDFormat(timestamp);
    }

    public static String getCurStr() {
        return ymdStrFmt.format(new Date());
    }

    public static void printArray(String[] objs)
    {
        System.out.printf("String size: %d\n", objs.length);
        for (int i = 0 ; i < objs.length ; ++i) {
            System.out.println(objs[i]);
        }
    }
}

package gr.imsi.athenarc.experiments.util;



import java.text.ParseException;

import gr.imsi.athenarc.middleware.domain.DateTimeUtil;


public class QueryUtils {

    public static Long convertToEpoch(String s) throws ParseException {
        return DateTimeUtil.parseDateTimeString(s);
    }

    public static Long convertToEpoch(String s, String timeFormat) throws ParseException {
        return DateTimeUtil.parseDateTimeString(s, timeFormat);
    }

}
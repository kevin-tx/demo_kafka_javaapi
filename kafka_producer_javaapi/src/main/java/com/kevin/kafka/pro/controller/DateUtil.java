package com.kevin.kafka.pro.controller;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Optional;

/**
 * com.tvtcloud.basictools.util.DateUtil
 *
 * @author hewei
 * @version [1.0.0, 2022/06/24]
 */
public class DateUtil {

    public static final String YMD = "yyyy-MM-dd";

    public static final String HMS = "HH:mm:ss";

    public static final String HM = "HH:mm";

    public static final String DEFAULT_FORMAT = YMD + " " + HMS;
    private static final DateTimeFormatter DF = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DF_HMS = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private static final DateTimeFormatter DF_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    /**
     * 默认年
     */
    private static final Integer DEFAULT_YEAR = 1970;

    /**
     * 默认月
     */
    private static final Integer DEFAULT_MOUTH = 1;

    /**
     * 默认日
     */
    private static final Integer DEFAULT_DAY = 1;

    /**
     * 根据给定的Date转为LocalDateTime
     *
     * @param date 日期时间对象
     * @return LocalDateTime
     */
    public static LocalDateTime toLocalDateTime(Date date) {
        return Optional.ofNullable(date)
                .map(Date::toInstant)
                .map(Timestamp::from)
                .map(Timestamp::toLocalDateTime)
                .orElse(null);
    }

    /**
     * 根据给定的LocalDateTime转为Date
     *
     * @param localDateTime 日期时间对象
     * @return Date
     */
    public static Date toDate(LocalDateTime localDateTime) {
        return Optional.ofNullable(localDateTime)
                .map(Timestamp::valueOf)
                .map(Timestamp::toInstant)
                .map(Date::from)
                .orElse(null);
    }

    /**
     * 根据字符日期时间解析为LocalDateTime 默认时间格式 yyyy-MM-dd HH:mm:ss
     *
     * @param date 日期时间字符串
     * @return LocalDateTime
     */
    public static LocalDateTime parseLocalDateTime(String date) {
        return parseLocalDateTime(date, DEFAULT_FORMAT);
    }

    /**
     * 根据字符日期时间解析为LocalDateTime
     *
     * @param date    日期时间字符串
     * @param pattern 日期时间格式化模式
     * @return LocalDateTime
     */
    public static LocalDateTime parseLocalDateTime(String date, String pattern) {
        return Optional.ofNullable(date)
                .map(DateTimeFormatter.ofPattern(pattern)::parse)
                .map(LocalDateTime::from)
                .orElse(null);
    }

    /**
     * 时分秒转换为1970年1月1日的时间（10:00 -> 1970-01-01 10:00）
     *
     * @param date 时分秒（10:00:00）
     * @return 时间
     */
    public static LocalDateTime parseLocalDateTimeByHM(String date) {
        DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder().appendPattern(HM)
                .parseDefaulting(ChronoField.YEAR, DEFAULT_YEAR)
                .parseDefaulting(ChronoField.MONTH_OF_YEAR, DEFAULT_MOUTH)
                .parseDefaulting(ChronoField.DAY_OF_MONTH, DEFAULT_DAY).toFormatter();
        return LocalDateTime.parse(date, dateTimeFormatter);
    }

    /**
     * 获取1970年1月1日当天的hms时间
     *
     * @param data 待处理数据
     * @return LocalDateTime
     */
    public static LocalDateTime getHMSLocalDateTime(LocalDateTime data) {
        return data.withYear(DEFAULT_YEAR).withMonth(DEFAULT_MOUTH).withDayOfMonth(DEFAULT_DAY);
    }

    /**
     * 返回当前时间的距离次日零点的秒数
     */
    public static Integer getRemainSecondsOneDay(Date currentDate) {
        //使用plusDays加传入的时间加1天，将时分秒设置成0
        LocalDateTime midnight = LocalDateTime.ofInstant(currentDate.toInstant(),
                        ZoneId.systemDefault()).plusDays(1).withHour(0).withMinute(0)
                .withSecond(0).withNano(0);
        LocalDateTime currentDateTime = LocalDateTime.ofInstant(currentDate.toInstant(),
                ZoneId.systemDefault());
        //使用ChronoUnit.SECONDS.between方法，传入两个LocalDateTime对象即可得到相差的秒数
        long seconds = ChronoUnit.SECONDS.between(currentDateTime, midnight);
        return (int) seconds;
    }

    /**
     * 时间字符串转LocalDateTime类型
     */
    public static LocalDateTime String2LocalDateTime(String timeString) {
        return LocalDateTime.parse(timeString, DF);
    }

    public static String localDateTime2HmsString(LocalDateTime time) {
        if (time == null) {
            return "";
        }
        return DF_HMS.format(time);
    }

    /**
     * 获得UTC时间LocalDateTime
     */
    public static LocalDateTime getUTCDate() {
        return LocalDateTime.now(ZoneId.of("UTC"));
    }

    /**
     * 获取对应时区的LocalDateTime
     */
    public static LocalDateTime getDateByZoneId(long utcTimeMillis, String zoneId) {
        Instant instant = Instant.ofEpochMilli(utcTimeMillis);
        return LocalDateTime.ofInstant(instant, ZoneId.of(zoneId));
    }

    /**
     * 获得UTC时间字符串
     */
    public static String getUTCDateString() {
        return DF.format(getUTCDate());
    }

    /**
     * 将long类型的timestamp转为LocalDateTime UTC时间
     */
    public static LocalDateTime getUTCDate(long utcTimeMillis) {
        Instant instant = Instant.ofEpochMilli(utcTimeMillis);
        return LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));
    }

    /**
     * 将long类型的timestamp转为 UTC时间字符串
     */
    public static String getUTCDateString(long utcTimeMillis) {
        return DF.format(getUTCDate(utcTimeMillis));
    }

    /**
     * UTC LocalDateTime 转 Long 类型 UTC时间戳
     */
    public static long getUTCTimeMillis(LocalDateTime utcDateTime) {
        return utcDateTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
    }

    public static LocalDateTime timestampToDatetime(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
    }

    /**
     * UTC LocalDateTime 转 Long 类型 UTC时间戳
     */
    public static long getDefaultTimeMillis(LocalDateTime utcDateTime) {
        return utcDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    /**
     * 获取当前UTC时间戳
     */
    public static long getUTCCurrentMillis() {
        LocalDateTime localDateTime = getUTCDate();
        return getUTCTimeMillis(localDateTime);
    }



    /**
     * 获取当前UTC时间戳
     */
    public static long getUTCCurrentSecond() {
        LocalDateTime localDateTime = DateUtil.getUTCDate();
        localDateTime = localDateTime.withNano(0);
        return getUTCTimeMillis(localDateTime)/1000;
    }

    /**
     * 根据UTC时间戳计算两个日期的间隔天数
     *
     * @param endUtcMillis   utc结束时间
     * @param startUtcMillis utc开始时间
     */
    public static int getBetweenDay(long endUtcMillis, long startUtcMillis) {
        return Math.abs((int) (getUTCDate(endUtcMillis).toLocalDate().toEpochDay() - getUTCDate(startUtcMillis).toLocalDate().toEpochDay()));
    }

    /**
     * utc时间戳增加秒数
     */
    public static long plusSeconds(long utcMillis, long seconds) {
        return utcMillis + seconds * 1000L;
    }

    /**
     * utc时间戳增加天数
     */
    public static long plusDays(long utcMillis, long days) {
        return utcMillis + days * 24 * 60 * 60 * 1000L;
    }

    /**
     * 获取UTC当前日期的前一天:年-月-日
     */
    public static String getbeforeDayUTCStr() {
        LocalDateTime localDateTime = getUTCDate().minusDays(1);
        return DF_DATE.format(localDateTime);
    }

    /**
     * 获取当前日期的前一天:年-月-日
     */
    public static String getbeforeDayStr() {
        LocalDate localDate = LocalDate.now().minusDays(1);
        return DF_DATE.format(localDate);
    }

    /**
     * 获取当天日期字符串:年-月-日 格式
     */
    public static String getDayStr() {
        return DF_DATE.format(LocalDate.now());
    }

    /**
     * 获取当天日期的零点零分零秒
     */
    public static LocalDateTime getNowDayZero() {
        //获取当前日期年月日时分秒
        String nowDayStr = getDayStr() + " 00:00:00";
        //转为LocalDateTime
        return LocalDateTime.parse(nowDayStr, DF);
    }

    /**
     * 计算两个LocalDateTime时间差值，返回秒
     */
    public static long getTwoTimeDiff(LocalDateTime startTime, LocalDateTime endTime) {
        Duration duration = Duration.between(startTime, endTime);
        return duration.toMillis() / 1000;
    }

}

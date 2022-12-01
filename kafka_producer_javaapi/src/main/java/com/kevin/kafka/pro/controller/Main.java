package com.kevin.kafka.pro.controller;

import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * com.tvt.kafka.pro.controller.Main
 *
 * @author TX
 * @version [1.0.0, 2022/09/17]
 */
@Slf4j
public class Main {

    /*
    设备SN：
    NVR01: 7645418863A5EE9A17C8463933A8119B
    NVR02: 0CD8032C026AE8AC5A027472C9BD73FB

    通道SN：
    IPC01: 379A11A371F173C5E3A5026FA0433656
    IPC02: 3E1EE4A7EE0398FA0D712E0EBDDFCCC7
    IPC03: 760E910ABDCC7603139B2D945EDC9CFB
    */

    private static String getTimeHex(long utcCurrentSecond) {
        return Long.toHexString(utcCurrentSecond);
    }

    private static String getDevSn(int id) {
        switch (id) {
            case 1:
                return "7645418863A5EE9A17C8463933A8119B";
            case 2:
                return "0CD8032C026AE8AC5A027472C9BD73FB";
        }
        return null;
    }

    private static String getChlSn(int id) {
        switch (id) {
            case 1:
                return "379A11A371F173C5E3A5026FA0433656";
            case 2:
                return "3E1EE4A7EE0398FA0D712E0EBDDFCCC7";
            case 3:
                return "760E910ABDCC7603139B2D945EDC9CFB";
            case 4:
                return "056FDE4D6122D3C9069E7A0C8210941A";
            case 5:
                return "B0E0C0006494F7A76A1E4EEE9A5C487B";
            case 6:
                return "8B05E6F0F6A895A6A19FEE30A6C0E3B2";
            case 7:
                return "6FBBEF72A3E78C5686EC0E140193EB4B";
            case 8:
                return "A586798FAB0C23B2F784BA9966911684";
            case 9:
                return "7F87DD68B8C491146E2B8F6998679B05";
            case 10:
                return "B06745C2D97B1C9A06D24A9030BEEB28";
            case 11:
                return "D70E69FCBFB62DF57A1D8123ED0FC064";
            case 12:
                return "3A5B70B3A69DE246A35178C62B1E0C42";
            case 13:
                return "3200C965AAE4BA688BB7911DC06FB1FF";
        }
        return null;
    }

    public static void main(String[] args) throws InterruptedException {
        long utcCurrentSecond = DateUtil.getUTCCurrentSecond();
//        String cmd = getCmd_sendDevUpgradeCheck(utcCurrentSecond,
//            "2EFA54FAB40171C8BA11BCEBC4005C0A",
//            "1.4.8.53749B220928.N0R.U1(8A418).beta");
////        sendDevData("2EFA54FAB40171C8BA11BCEBC4005C0A", "{\"url\":\"/innerapi/device/upgrade/check\",\"data\":\"{\\\"basic\\\":{\\\"sn\\\":\\\"2EFA54FAB40171C8BA11BCEBC4005C0A\\\",\\\"devType\\\":\\\"2\\\",\\\"devVersion\\\":\\\"1.4.8.53749B220928.N0R.U1(8A418).beta\\\",\\\"natInstId\\\":4,\\\"ver\\\":\\\"1.0\\\",\\\"id\\\":\\\"20\\\",\\\"time\\\":1666604597,\\\"receiveTime\\\":1666604597,\\\"nonce\\\":138171},\\\"data\\\":{\\\"sn\\\":\\\"2EFA54FAB40171C8BA11BCEBC4005C0A\\\",\\\"mac\\\":\\\"00:18:AE:00:6D:8C\\\",\\\"countryCode\\\":\\\"US\\\",\\\"customerId\\\":205,\\\"model\\\":\\\"TD-3308H1-8P-A2\\\",\\\"version\\\":\\\"1.4.8.53749B220928.N0R.U1(8A418).beta\\\",\\\"versionId\\\":\\\"1BoHBmL9hPPoYDWmsLGYJg\\\",\\\"workMode\\\":\\\"bind,connect\\\",\\\"registerTime\\\":[2022,10,24,9,27,23],\\\"readyTime\\\":[2022,10,24,9,27,23],\\\"lang\\\":\\\"zh-TW\\\",\\\"onlineStatus\\\":1}}\",\"devUrl\":\"/device/upgrade/check\"}");
//        sendWithPartition("device_upgrade", 2, "18AF034EF64565574CF09D0E3C0DE2DA", "{\"url\":\"/innerapi/device/upgrade-download/report\",\"data\":\"{\\\"basic\\\":{\\\"sn\\\":\\\"18AF034EF64565574CF09D0E3C0DE2DA\\\",\\\"devType\\\":\\\"2\\\",\\\"devVersion\\\":\\\"1.4.8.53663B220926.N0N.U1(8A418).beta\\\",\\\"natInstId\\\":5,\\\"ver\\\":\\\"1.0\\\",\\\"id\\\":\\\"239\\\",\\\"time\\\":1666614362,\\\"receiveTime\\\":1666614658,\\\"nonce\\\":6814521},\\\"data\\\":{\\\"downloadReport\\\":{\\\"ver\\\":\\\"1.4.8.53836B220930.N0N.U1(8A418).beta\\\",\\\"checkInfo\\\":\\\"7079e5c4715768e410f6d05980a92d20\\\",\\\"msg\\\":\\\"download info empty\\\",\\\"code\\\":9027,\\\"startTime\\\":1666614359,\\\"endTime\\\":0,\\\"retryNum\\\":0},\\\"deviceInfo\\\":{\\\"sn\\\":\\\"18AF034EF64565574CF09D0E3C0DE2DA\\\",\\\"mac\\\":\\\"00:18:AE:00:6D:88\\\",\\\"countryCode\\\":\\\"US\\\",\\\"customerId\\\":205,\\\"model\\\":\\\"TD-3308B1-8P-A1\\\",\\\"version\\\":\\\"1.4.8.53663B220926.N0N.U1(8A418).beta\\\",\\\"versionId\\\":\\\"Dj1PK2B9OZl1nb4EabrXqQ\\\",\\\"workMode\\\":\\\"bind,connect\\\",\\\"registerTime\\\":[2022,10,24,12,14,3],\\\"readyTime\\\":[2022,10,24,12,14,3],\\\"lang\\\":\\\"zh-TW\\\",\\\"onlineStatus\\\":1}}}\",\"devUrl\":\"/device/upgrade/report\"}");


//        String cmd_sendDevUpgradeCheck = getCmd_sendDevUpgradeCheck(utcCurrentSecond, getDevSn(1),
//            "1.4.8.00002B220921.N0N.U1(16A420).beta");
//        sendDevData(getDevSn(1), cmd_sendDevUpgradeCheck);

        chlOnline();
//        sendDownloadFailed();

//        chlOnlineAndCaps(getDevSn(2), "1.4.8.00002B220921.N0N.U1(16A420).beta", "verid-ipc2", "5.2.0.00002B220921");

//        sendUpgradeReport(getDevSn(1), "1.4.8.00003B220921.N0N.U1(16A420).beta",
//            "8ed7dfcff4e017e4f61d36bbbf1f08bc", null, 200);
//        sendUpgradeReport(getDevSn(1), "1.4.8.00003B220921.N0N.U1(16A420).beta",
//            "8ed7dfcff4e017e4f61d36bbbf1f08bc", null, 9024);

//        sendWithInvalidPartition();
//        sendWithPartition();
//        sendNoPartition();
    }


    private static void sendDownloadFailed(){
//        List<Integer> chls = Arrays.asList(1);
        List<Integer> chls = null;
        long utcCurrentSecond = DateUtil.getUTCCurrentSecond();
        String tmp = "^devSN^#2#^devVer^#"
            + getTimeHex(utcCurrentSecond)
            + "#4#{\"basic\":{\"ver\":\"1.0\",\"id\":\"33\",\"time\":" + utcCurrentSecond
            + ",\"nonce\":3579723},\"url\":\"/device/upgrade/report\",\"data\":{\"ver\":\"^devVer^\",\"checkInfo\":\"^md5^\",\"planTime\":"
            + (utcCurrentSecond + 10)
            + ",\"msg\":\"download success\",\"code\":" + 9027 + ",\"startTime\":"
            + utcCurrentSecond
            + ",\"endTime\":" + (utcCurrentSecond + 5) + ",\"retryNum\":1" + (chls != null ?
            ",\"chls\":[" + StringUtils.join(chls, ",") + "]" : "") + "}}";
        String cmd = tmp.replace("^devSN^", "7645418863A5EE9A17C8463933A8119B").replace("^devVer^", "1.4.8.00003B220921.N0N.U1(16A420).beta")
            .replace("^md5^", "8ed7dfcff4e017e4f61d36bbbf1f08bc");
        sendDevData("7645418863A5EE9A17C8463933A8119B", cmd);
    }

    private static void chlOnline() throws InterruptedException {
        long utcCurrentSecond = DateUtil.getUTCCurrentSecond();
        String tmp = "^devSN^#2#^devVer^#" + getTimeHex(utcCurrentSecond)
            + "#4#{\"basic\":{\"ver\":\"1.0\",\"id\":\"20\",\"time\":" + utcCurrentSecond
            + ",\"nonce\":138171},\"url\":\"/dev/chl/onlineStatus/report\",\"data\":{\"online\":["
//            + "{\"chlIndex\":7,\"verID\":\"verid-ipc1" + "\",\"chlSn\":\"" + "379A11A371F173C5E3A5026FA0433656" + "\"},"
            + "{\"chlIndex\":1,\"verID\":\"verid-ipc3" + "\",\"chlSn\":\"" + "379A11A371F173C5E3A5026FA0433656" + "\"}"
            + "]}}";
        String cmd = tmp.replace("^devSN^", "7645418863A5EE9A17C8463933A8119B").replace("^devVer^", "1.4.8.54208B221020.N0R.U1(8A410).beta");
        sendDevData("7645418863A5EE9A17C8463933A8119B", cmd);

        Thread.sleep(3000);

        utcCurrentSecond = DateUtil.getUTCCurrentSecond();
        tmp = "^devSN^#2#^devVer^#" + getTimeHex(utcCurrentSecond)
            + "#4#{\"basic\":{\"ver\":\"1.0\",\"id\":\"612\",\"time\":" + utcCurrentSecond
            + ",\"code\":200,\"msg\":\"success\"},\"url\":\"/device/camera/caps/get#response\",\"data\":{\"chlCaps\":["
//            + "{\"chlIndex\":7,\"verID\":\"verid-ipc1" + "\",\"version\":\"" + "5.2.0.00001B220916\",\"model\":\"C2 4MP 5.2\",\"date\":\"\",\"manufacturer\":\"2\","
//            + "\"chlSn\":\"379A11A371F173C5E3A5026FA0433656\",\"mac\":\"70:00:00:00:00:01\",\"name\":\"chl-1\"},"
            + "{\"chlIndex\":1,\"verID\":\"verid-ipc3" + "\",\"version\":\"" + "5.2.0.00003B220916\",\"model\":\"C2 4MP 5.2\",\"date\":\"\",\"manufacturer\":\"2\","
            + "\"chlSn\":\"379A11A371F173C5E3A5026FA0433656\",\"mac\":\"70:00:00:00:00:01\",\"name\":\"chl-1\"}"
            + "]}}";
        cmd = tmp.replace("^devSN^", "7645418863A5EE9A17C8463933A8119B").replace("^devVer^", "1.4.8.54208B221020.N0R.U1(8A410).beta");
        sendDevData("7645418863A5EE9A17C8463933A8119B", cmd);
    }


    private static void chlOnlineAndCaps(String devSn, String devVer, String chlVerId,
        String chlVer)
        throws InterruptedException {
//        chlOnline(devSn, devVer, chlVerId);
//        Thread.sleep(500);
        chlCapsRsp(devSn, devVer, chlVerId, chlVer);

    }

    private static void chlOnline(String devSn, String devVer, String chlVerId) {
        long utcCurrentSecond = DateUtil.getUTCCurrentSecond();
        String cmd = getCmd_sendChlOnline(utcCurrentSecond, devSn, devVer, chlVerId);
        sendDevData(devSn, cmd);

    }

    private static void chlCapsRsp(String devSn, String devVer, String chlVerId, String chlVer) {
        long utcCurrentSecond = DateUtil.getUTCCurrentSecond();
        String cmd = getCmd_sendChlCapRsp(utcCurrentSecond, devSn, devVer, chlVerId, chlVer);
        sendDevData(devSn, cmd);

    }


    private static void oldDevOnline(int devId) throws InterruptedException {
        sendRegister(getDevSn(devId), "1.4.8.00002B220921.N0N.U1(16A420).beta", "NVR-Product1");
        Thread.sleep(500);
        sendReady(getDevSn(devId), "1.4.8.00002B220921.N0N.U1(16A420).beta", "verid-nvr2");
        Thread.sleep(500);
        sendDevInfo(getDevSn(devId), "1.4.8.00002B220921.N0N.U1(16A420).beta", "verid-nvr2",
            "NVR-Product1", "dev-" + devId);
    }

    private static void newDevOnlne(int devId) throws InterruptedException {
        sendRegister(getDevSn(devId), "1.4.8.00003B220921.N0N.U1(16A420).beta", "NVR-Product1");
        Thread.sleep(500);
        sendReady(getDevSn(devId), "1.4.8.00003B220921.N0N.U1(16A420).beta", "verid-dev3");
        Thread.sleep(500);
        sendDevInfo(getDevSn(devId), "1.4.8.00003B220921.N0N.U1(16A420).beta", "verid-dev3",
            "NVR-Product1", "dev-" + devId);
    }


    private static void sendRegister(String sn, String devVer, String devModel) {
        long utcCurrentSecond = DateUtil.getUTCCurrentSecond();
        String cmd = getCmd_Register(utcCurrentSecond, sn, devVer, devModel);
        sendDevData(sn, cmd);
    }


    private static void sendReady(String sn, String devVer, String devVerId) {
        long utcCurrentSecond = DateUtil.getUTCCurrentSecond();
        String cmd = getCmd_Ready(utcCurrentSecond, sn, devVer, devVerId);
        sendDevData(sn, cmd);
    }


    private static void sendDevInfo(String sn, String devVer, String devVerId, String devModel,
        String devName) {
        long utcCurrentSecond = DateUtil.getUTCCurrentSecond();
        String cmd = getCmd_DevInfo(utcCurrentSecond, sn, devVer, devVerId, devModel, devName);
        sendDevData(sn, cmd);
    }

    private static void sendUpgradeReport(String sn, String devVer, String md5, List<Integer> chls,
        int code) {
        long utcCurrentSecond = DateUtil.getUTCCurrentSecond();
        String cmd = getCmd_sendUpgradeReport(utcCurrentSecond, sn, devVer, md5, chls, code);
        sendDevData(sn, cmd);
    }

    private static String getCmd_Register(long utcCurrentSecond, String devSn, String devVer,
        String devModel) {
        String tmp = "^devSN^#2#^devVer^#" + getTimeHex(utcCurrentSecond)
            + "#4#{\"basic\":{\"ver\":\"1.0\",\"id\":5421,\"time\":" + utcCurrentSecond
            + ",\"nonce\":2028191893},\"url\":\"/device/register\",\"data\":{\"ip\":\"10.100.20.189\",\"port\":54808,\"mac\":\"00:18:AE:00:66:88\",\"type\":2,\"model\":\"^devModel^\",\"natVersion\":\"1.1.3\",\"devVersion\":\"^devVer^\",\"customerId\":\"213\",\"info\":\"\",\"lastBootTime\":"
            + utcCurrentSecond + ",\"dn\":\"\"}}";
        String value = tmp.replace("^devSN^", devSn).replace("^devVer^", devVer)
            .replace("^devModel^", devModel);
        return value;
    }

    private static String getCmd_Ready(long utcCurrentSecond, String devSn, String devVer,
        String devVerId) {
        String tmp = "^devSN^#2#^devVer^#"
            + getTimeHex(utcCurrentSecond)
            + "#4#{\"basic\":{\"ver\":\"1.0\",\"id\":\"16\",\"time\":" + utcCurrentSecond
            + ",\"nonce\":137011},\"url\":\"/device/ready\",\"data\":{\"workMode\":\"unbind,connect\",\"cfgId\":0,\"verID\":\"^devVerId^\",\"lang\":\"0x0409\",\"licenseMd5\":\"\"}}";
        String value = tmp.replace("^devSN^", devSn).replace("^devVer^", devVer)
            .replace("^devVerId^", devVerId);
        return value;
    }

    private static String getCmd_DevInfo(long utcCurrentSecond, String devSn, String devVer,
        String devVerId, String devModel, String devName) {
        String tmp = "^devSN^#2#^devVer^#"
            + getTimeHex(utcCurrentSecond)
            + "#4#{\"basic\":{\"ver\":\"1.0\",\"id\":\"273\",\"time\":" + utcCurrentSecond
            + ",\"nonce\":20362560,\"receiveTime\":" + utcCurrentSecond
            + ",\"sn\":\"^devSN^\",\"devType\":\"2\",\"devVersion\":\"^devVer^\",\"natInstId\":4},\"url\":\"/device/info\",\"data\":{\"devInfo\":{\"ver\":\"^devVer^\",\"name\":\"^devName^\",\"deviceNumber\":\"1\",\"verID\":\"^devVerId^\",\"hVer\":\"3000421-V0\",\"model\":\"^devModel^\",\"originalModel\":\"^devModel^\",\"verDate\":\"20220928\",\"cfgId\":23,\"onvifVer\":\"17.06\",\"pluginVer\":\"2.4.5.1360\",\"apiVer\":\"1.6.0 build20170725\",\"aiVer\":\"3_2050\",\"PCUI\":\"00414243000000004445464711CD001000170000A100\",\"PN\":\"202206212\",\"PCBAV\":\"11\",\"MCU\":\"---\",\"kernelVer\":\"L863-M4EC-M9S7\",\"customerId\":\"213\",\"packContentFlag\":0,\"platformType\":\"N0R\",\"mac\":\"00:18:AE:00:66:88\",\"sdkVer\":\"2.0\",\"type\":2,\"codeId\":\"53749\"},\"devCaps\":{\"chlNum\":32,\"poeChlNum\":8,\"alarmInNum\":16,\"alarmOutNum\":4,\"posNum\":8,\"maxConnNum\":64,\"maxMainstreamNum\":12,\"maxSubstreamNum\":32,\"maxPlaybackNum\":8,\"supportFun\":[\"m\",\"ta\",\"fm\",\"vhc\",\"cls\"],\"cloudStorageChlNum\":4,\"diskInterface\":[{\"name\":\"STAT\",\"num\":1}]}}}";
        String value = tmp.replace("^devSN^", devSn).replace("^devVer^", devVer)
            .replace("^devVerId^", devVerId).replace("^devModel^", devModel)
            .replace("^devName^", devName);
        return value;
    }

    private static String getCmd_sendUpgradeReport(long utcCurrentSecond, String devSn,
        String devVer, String md5, List<Integer> chls, int code) {
        String tmp = "^devSN^#2#^devVer^#"
            + getTimeHex(utcCurrentSecond)
            + "#4#{\"basic\":{\"ver\":\"1.0\",\"id\":\"33\",\"time\":" + utcCurrentSecond
            + ",\"nonce\":3579723},\"url\":\"/device/upgrade/report\",\"data\":{\"ver\":\"^devVer^\",\"checkInfo\":\"^md5^\",\"planTime\":"
            + (utcCurrentSecond + 10)
            + ",\"msg\":\"download success\",\"code\":" + code + ",\"startTime\":"
            + utcCurrentSecond
            + ",\"endTime\":" + (utcCurrentSecond + 5) + ",\"retryNum\":1" + (chls != null ?
            ",\"chls\":[" + StringUtils.join(chls, ",") + "]" : "") + "}}";
        String value = tmp.replace("^devSN^", devSn).replace("^devVer^", devVer)
            .replace("^md5^", md5);
        return value;
    }

    private static String getCmd_sendChlOnline(long utcCurrentSecond, String devSn,
        String devVer, String chlVerId) {
        String tmp = "^devSN^#2#^devVer^#" + getTimeHex(utcCurrentSecond)
            + "#4#{\"basic\":{\"ver\":\"1.0\",\"id\":\"20\",\"time\":" + utcCurrentSecond
            + ",\"nonce\":138171},\"url\":\"/dev/chl/onlineStatus/report\",\"data\":{\"syncOnline\":[{\"chlIndex\":1,\"verID\":\""
            + chlVerId + "\",\"chlSn\":\"" + getChlSn(1) + "\"},{\"chlIndex\":2,\"verID\":\""
            + chlVerId + "\",\"chlSn\":\"" + getChlSn(2) + "\"},{\"chlIndex\":3,\"verID\":\""
            + chlVerId + "\",\"chlSn\":\"" + getChlSn(2) + "\"}]}}";
        String value = tmp.replace("^devSN^", devSn).replace("^devVer^", devVer);
        return value;
    }


    private static String getCmd_sendChlCapRsp(long utcCurrentSecond, String devSn,
        String devVer, String chlVerId, String chlVer) {
        String tmp = "^devSN^#2#^devVer^#" + getTimeHex(utcCurrentSecond)
            + "#4#{\"basic\":{\"ver\":\"1.0\",\"id\":\"612\",\"time\":" + utcCurrentSecond
            + ",\"code\":200,\"msg\":\"success\"},\"url\":\"/device/camera/caps/get#response\",\"data\":{\"chlCaps\":[{\"chlIndex\":1,\"verID\":\""
            + chlVerId + "\",\"version\":\"" + chlVer
            + "\",\"model\":\"C2 4MP 5.2\",\"date\":\"\",\"manufacturer\":\"2\",\"chlSn\":\""
            + getChlSn(1)
            + "\",\"mac\":\"70:00:00:00:00:01\",\"name\":\"chl-1\"},{\"chlIndex\":2,\"verID\":\""
            + chlVerId + "\",\"version\":\"" + chlVer
            + "\",\"model\":\"C2 4MP 5.2\",\"date\":\"\",\"manufacturer\":\"2\",\"chlSn\":\""
            + getChlSn(2)
            + "\",\"mac\":\"70:00:00:00:00:02\",\"name\":\"chl-2\"},{\"chlIndex\":3,\"verID\":\""
            + chlVerId + "\",\"version\":\"" + chlVer
            + "\",\"model\":\"C2 4MP 5.2\",\"date\":\"\",\"manufacturer\":\"2\",\"chlSn\":\""
            + getChlSn(3) + "\",\"mac\":\"70:00:00:00:00:03\",\"name\":\"chl-3\"}]}}";
        String value = tmp.replace("^devSN^", devSn).replace("^devVer^", devVer);
        return value;
    }

    private static String getCmd_sendDevUpgradeCheck(long utcCurrentSecond, String devSn,
        String devVer) {
        String tmp = "^devSN^#2#^devVer^#" + getTimeHex(utcCurrentSecond)
            + "#4#{\"basic\":{\"ver\":\"1.0\",\"id\":\"20\",\"time\":" + utcCurrentSecond
            + ",\"nonce\":138171},\"url\":\"/device/upgrade/check\",\"data\":{\"lang\":\"0x0404\"}}";
        String value = tmp.replace("^devSN^", devSn).replace("^devVer^", devVer);
        return value;
    }

    private static String getCmd_sendChlUpgradeCheck(long utcCurrentSecond, String devSn,
        String devVer, List<Integer> chlIndexList) {
        String chlIndexStr = StringUtils.join(chlIndexList, ",");
        String tmp = "^devSN^#2#^devVer^#" + getTimeHex(utcCurrentSecond)
            + "#4#{\"basic\":{\"ver\":\"1.0\",\"id\":\"20\",\"time\":" + utcCurrentSecond
            + ",\"nonce\":138171},\"url\":\"/device/chl/upgrade/check\",\"data\":{\"lang\":\"0x0404\",\"chls\":["+chlIndexStr+"]}}";
        String value = tmp.replace("^devSN^", devSn).replace("^devVer^", devVer);
        return value;
    }


    private static void sendDevData(String sn, String message) {
        try {
            KafkaProducer<String, String> producer = getProducer();
            producer
                .send(new ProducerRecord<>("nat-proxy-1", sn, message), (metadata, exception) -> {
                    if (exception == null) {
                        System.out
                            .println("主题：" + metadata.topic() + "->" + "分区：" + metadata.partition()
                                + ", key: " + sn + ", value: " + message
                            );
                    } else {
                        exception.printStackTrace();
                    }
                });
            producer.flush();
        } catch (Exception ex) {
            log.error("发送异常", ex);
        }
    }

    private static void sendWithPartition(String topic, int partition, String key ,String value) {
        try {
            KafkaProducer<String, String> producer = getProducer();
            producer
                .send(new ProducerRecord<>(topic, partition, key, value), (metadata, exception) -> {
                    if (exception == null) {
                        System.out
                            .println("主题：" + metadata.topic() + "->" + "分区：" + metadata.partition()
                                + ", key: " + key + ", value: " + value
                            );
                    } else {
                        exception.printStackTrace();
                    }
                });
            producer.flush();
        } catch (Exception ex) {
            log.error("发送异常", ex);
        }
    }

    private static void sendWithInvalidPartition() {
        try {
            KafkaProducer<String, String> producer = getProducer();
            String k = "k-for-p3";
            String v = "v-for=p3";
            producer
                .send(new ProducerRecord<>("demo-01", 3, k, v), (metadata, exception) -> {
                    if (exception == null) {
                        System.out
                            .println("主题：" + metadata.topic() + "->" + "分区：" + metadata.partition()
                                + ", key: " + k + ", value: " + v
                            );
                    } else {
                        exception.printStackTrace();
                    }
                });
            producer.flush();
        } catch (Exception ex) {
            log.error("发送异常", ex);
        }
    }

    private static void sendWithPartition() {
        KafkaProducer<String, String> producer = getProducer();
        for (int i = 0; i < 10; i++) {
            String iStr = String.format("%08d", i);
            String k = "k-w-p-" + iStr;
            String v = "v-w-p-" + iStr;
            Integer partition = 0;
            if (i < 6) {
                partition = 0;
            } else if (i < 9) {
                partition = 1;
            } else {
                partition = 2;
            }
            producer
                .send(new ProducerRecord<>("demo-01", partition, k, v), (metadata, exception) -> {
                    if (exception == null) {
                        System.out
                            .println("主题：" + metadata.topic() + "->" + "分区：" + metadata.partition()
                                + ", key: " + k + ", value: " + v
                            );
                    } else {
                        exception.printStackTrace();
                    }
                });
        }
        producer.flush();
    }

    private static void sendNoPartition() {
        KafkaProducer<String, String> producer = getProducer();
        for (int i = 0; i < 10; i++) {
            String k = "k" + i;
            String v = "v" + i;
            producer
                .send(new ProducerRecord<>("demo-01", k, "v" + v), (metadata, exception) -> {
                    if (exception == null) {
                        System.out
                            .println("主题：" + metadata.topic() + "->" + "分区：" + metadata.partition()
                                + ", key: " + k + ", value: " + v
                            );
                    } else {
                        exception.printStackTrace();
                    }
                });
        }
        producer.flush();
    }

    private static KafkaProducer<String, String> getProducer() {
        //10.50.10.134:9092,10.50.10.135:9092,10.50.10.136:9092
        //10.50.10.38:9092,10.50.10.37:9092,10.50.10.45:9092
        //10.50.10.12:9093,10.50.10.13:9093,10.50.10.16:9093
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            "10.50.10.38:9092,10.50.10.37:9092,10.50.10.45:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }

    private static void testTimeHex() {
        long utcCurrentSecond = DateUtil.getUTCCurrentSecond();
        System.out.println(utcCurrentSecond);
        String timeHex = getTimeHex(utcCurrentSecond);
        System.out.println(timeHex);
        Long time = Long.parseLong(timeHex, 16);
        System.out.println(time);
    }
}

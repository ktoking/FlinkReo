package com.atguigu.hotItems_analysis.hotitems;

import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.util.Properties;

public class MyTest {
    public static final String ip="39.96.86.4";
    public static final String port="80";
    public static final String inet="http://39.96.86.4:8239/gw-kkk-test1/getIp";


    public static void main(String[] args) throws InterruptedException, IOException {
        // 目标网址，会返回发起请求的ip
        URL url1 = new URL(inet);
        // 不设置任何代理
        String result1 = printInputstream(url1.openStream());
        System.out.println(" 不设置任何代理:" + result1);

        /**
         * 方式一
         */
        Properties prop = System.getProperties();
        // 设置http访问要使用的代理服务器的地址
        prop.setProperty("http.proxyHost", ip);
        // 设置http访问要使用的代理服务器的端口
        prop.setProperty("http.proxyPort", port);
        System.setProperties(prop);
        URL url2 = new URL(inet);
        String result2 = printInputstream(url2.openStream());
        System.out.println(" 设置全局代理:" + result2);

        /**
         * 方法二
         */
        // 创建代理服务器
        InetSocketAddress addr = new InetSocketAddress(ip, Integer.parseInt(port));
        // http 代理
        Proxy proxy = new Proxy(Proxy.Type.HTTP, addr);
        URL url3 = new URL(inet);
        URLConnection conn = url3.openConnection(proxy);
        conn.setReadTimeout(5000);
        String result3 = printInputstream(conn.getInputStream());
        System.out.println(" 为当前请求设置代理:" + result3);
    }

    public static String printInputstream(InputStream in) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        reader = new BufferedReader(new InputStreamReader(in));
        String s = null;
        StringBuffer sb = new StringBuffer();
        try {
            while ((s = reader.readLine()) != null) {
                sb.append(s);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }
}

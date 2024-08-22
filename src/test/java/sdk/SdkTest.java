package sdk;

import com.github.dts.cluster.MessageTypeEnum;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class SdkTest {
    public static void main(String[] args) throws IOException {
//        test2();
        test1();
    }

    private static void test2() throws IOException {
        String def = encodeBasicAuth("def", "123", Charset.forName("UTF-8"));
        String encode = URLEncoder.encode("Basic " + def, "utf-8");
        URL url = new URL("http://localhost:8080/dts/sdk/subscriber?Authorization=" + encode);
        Socket socket = new Socket(url.getHost(), url.getPort());
        OutputStream outputStream = socket.getOutputStream();
        outputStream.write(("GET " + url.getFile() + " HTTP/1.1\r\n").getBytes());
        outputStream.write(("Host: " + url.getHost() + "\r\n").getBytes());
        outputStream.write("\r\n".getBytes());
        outputStream.flush();

        InputStream inputStream = socket.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, Charset.forName("UTF-8")));
        List<String> headers = new ArrayList<>();
        while (true) {
            String s1 = reader.readLine();
            headers.add(s1);
//            if (s1.isEmpty()) {
//                break;
//            }
        }
//        byte[] head = new byte[10];
//        while (true) {
//            int read = inputStream.read(head);
//            if (read == -1) {
//                break;
//            }
//            MessageTypeEnum byCode = MessageTypeEnum.getByCode(head[0]);
//            int length = (head[1] << 8 | head[2] & 0xFF) & 0xFF;
//            long id = ((long) (head[3] & 0xff) << 24 | (head[4] & 0xff) << 16 | (head[5] & 0xff) << 8 | head[6] & 0xff) & 0xFF;
//            byte[] payload = new byte[length];
//            inputStream.read(payload);
//
//            String s = new String(payload, Charset.forName("utf-8"));
//            System.out.println("s = " + s);
//        }

    }

    private static void test1() throws IOException {
        String def = encodeBasicAuth("def", "123", Charset.forName("UTF-8"));
        String encode = URLEncoder.encode("Basic " + def, "utf-8");

        URL url = new URL("http://localhost:8080/dts/sdk/subscriber?Authorization=" + encode);
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        InputStream inputStream = urlConnection.getInputStream();

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, Charset.forName("UTF-8")));

        while (true) {
            String s1 = reader.readLine();

            System.out.println("s = " + s1);
        }

    }

    static short getShort(byte[] memory, int index) {
        return (short) (memory[index] << 8 | memory[index + 1] & 0xFF);
    }


    public static String encodeBasicAuth(String username, String password, Charset charset) {
        String credentialsString = username + ":" + password;
        byte[] encodedBytes = Base64.getEncoder().encode(credentialsString.getBytes(charset));
        return new String(encodedBytes, charset);
    }
}

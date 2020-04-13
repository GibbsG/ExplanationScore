import java.io.*;
public class test {
    public static void main(String[] args) {
        int[] a = new int[]{1, 1};
        int[] b = a;
        int i = b[1];
        b[0] = 6;
        System.out.println(a[0]);
    }

}

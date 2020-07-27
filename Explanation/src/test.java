import java.util.Arrays;

public class test {
    public static void main(String[] args) {
        int i = 0;
        double[] a = new double[]{0, 1, 0};
        while (i < 100) {
            i++;
            System.out.println(Arrays.toString(a));
            double[] b = new double[3];
            b[0] = 0.7*a[0]+0.1*a[1];
            b[1] = 0.3*a[0]+0.7*a[1]+0.4*a[2];
            b[2] = 0.2*a[1]+0.6*a[2];
            a = b;
        }
    }
}

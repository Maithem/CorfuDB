public class Main {
    public static void main(String[] args) throws Exception {

        Thread t = new Thread(() -> {
            System.out.println("In new thread!");

            Thread t2 = new Thread(() -> {
                System.out.println("Thread2 started by thread1!!!");
            });

            t2.setName("shread 2!!");

            t2.start();

            try {
                Thread.sleep(5000);
            } catch (Exception e) {
                System.out.println("Exception " + e);
            }
            System.out.println("In new threadend!");
        });
        t.setName("whoa!");

        t.start();

        System.out.println("waiting");

        Thread.sleep(1000 * 2);
        t.interrupt();
        System.out.println("interrupting");
    }
}

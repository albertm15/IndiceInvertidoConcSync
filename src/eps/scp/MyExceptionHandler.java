
package eps.scp;

public class MyExceptionHandler implements Thread.UncaughtExceptionHandler{
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        // Maneja la excepción no capturada aquí
        System.out.println("Excepción no capturada en el hilo " + t.getName() + ": " + e.getMessage());
    }
}

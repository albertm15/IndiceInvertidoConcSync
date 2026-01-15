/* ---------------------------------------------------------------
Práctica 3.
Código fuente : IndiceInvertidoConcPRA3
Grado Informática
49381774S Albert Martín López.
49380060A Pau Barahona Setó.
--------------------------------------------------------------- */

package eps.scp;

public class MyExceptionHandler implements Thread.UncaughtExceptionHandler{
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        // Maneja la excepción no capturada aquí
        System.out.println("Excepción no capturada en el hilo " + t.getName() + ": " + e.getMessage());
    }
}

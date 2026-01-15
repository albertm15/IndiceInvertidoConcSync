/* ---------------------------------------------------------------
Práctica 3.
Código fuente : IndiceInvertidoConcPRA3
Grado Informática
49381774S Albert Martín López.
49380060A Pau Barahona Setó.
--------------------------------------------------------------- */

package eps.scp;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.*;

public class LoadInvertedIndex implements Runnable{
    private final String DIndexFilePrefix = "IndexFile";
    private ArrayList<Thread> threadList = new ArrayList<>();
    private ArrayList <RecorrerFicheroDeLoad> taskList = new ArrayList<>();
    private String inputDirectory;
    private Map<String, HashSet<Location>> Hash;
    public static CyclicBarrier barrier;
    private Phaser phaser;
    public LoadInvertedIndex(String indexDirectory, Map<String, HashSet <Location>> Hash, Phaser phaser){
        this.inputDirectory = indexDirectory;
        this.Hash = Hash;
        this.phaser = phaser;
    }
    @Override
    public void run() {

        loadInvertedIndex(inputDirectory);
        barrier = new CyclicBarrier(threadList.size()+1);
        for (Thread virtualThread : threadList){
            virtualThread.start();
        }

        try {
            barrier.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (BrokenBarrierException e) {
            throw new RuntimeException(e);
        }

        this.phaser.arriveAndDeregister();

    }
    public void loadInvertedIndex(String inputDirectory){
        File folder = new File(inputDirectory);
        File[] listOfFiles = folder.listFiles((d, name) -> name.startsWith(DIndexFilePrefix));

        // Recorremos todos los ficheros del directorio de Indice y los procesamos.
        int actualTaskNumber = 0;
        for (File file : listOfFiles) {
            if (file.isFile()) {
                taskList.add(new RecorrerFicheroDeLoad(file, Hash));
                threadList.add(Thread.ofVirtual().unstarted(taskList.get(actualTaskNumber)));
            }
            actualTaskNumber++;
        }
    }
}

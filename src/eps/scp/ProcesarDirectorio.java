
package eps.scp;
import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ProcesarDirectorio implements Runnable{

    private String path;
    private List<File> FilesList;
    private Map<Integer,String> Files;
    private int TotalFiles = 0;
    private long TotalLines = 0;
    private long TotalWords = 0;
    private long TotalLocations = 0;
    private ArrayList <Thread> threadList = new ArrayList<>();
    private ArrayList <ProcesarFichero> taskList = new ArrayList<>();

    private int fileId = 0;

    private Map<String, HashSet<Location>> Hash;

    private Map<Location, String> IndexFilesLines;

    public static CountDownLatch latch;
    public static Phaser filesPhaser;

    public static Lock lockParaFichero = new ReentrantLock(true);


    protected ProcesarDirectorio(String path, List<File> FilesList, Map<Integer,String> Files, Map<String, HashSet <Location>> Hash, Map<Location, String> IndexFilesLines) {
        this.path = path;
        this.FilesList = FilesList;
        this.Files = Files;
        this.Hash = Hash;
        this.IndexFilesLines = IndexFilesLines;
    }

    @Override
    public void run(){
        TotalFiles = 0;
        searchDirectoryFiles(path);
        filesPhaser = new Phaser(TotalFiles+1);
        notificarCreacionPhaser();

        for (Thread virtualThread : threadList){
            virtualThread.start();
        }
    }

    public void searchDirectoryFiles(String dirpath){
        File file=new File(dirpath);
        File content[] = file.listFiles();
        if (content != null) {
            for (int i = 0; i < content.length; i++) {
                if (content[i].isDirectory()) {
                    // Si es un directorio, procesarlo recursivamente.
                    searchDirectoryFiles(content[i].getAbsolutePath());
                }
                else {
                    // Si es un fichero de texto, añadirlo a la lista para su posterior procesamiento.
                    if (checkFile(content[i].getName())){
                        FilesList.add(content[i]);
                        fileId++;
                        Files.put(fileId, content[i].getAbsolutePath());
                        taskList.add(new ProcesarFichero(fileId, content[i], Hash));
                        threadList.add(Thread.ofVirtual().unstarted(taskList.get(TotalFiles)));
                        TotalFiles++;
                        InvertedIndex.addProcessedFiles(1);
                    }
                }
            }
        }
        else
            System.err.printf("Directorio %s no existe.\n",file.getAbsolutePath());
    }

    private boolean checkFile (String name)
    {
        if (name.endsWith("txt")) {
            return true;
        }
        return false;
    }

    public void notificarCreacionPhaser(){
        InvertedIndex.lock.lock();
        try {
            InvertedIndex.condicionCumplida = true;
            InvertedIndex.phaserCreado.signalAll();
        }
        finally { InvertedIndex.lock.unlock(); }
    }

}

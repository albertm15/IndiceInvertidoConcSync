
package eps.scp;
import java.io.*;
import java.text.Normalizer;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import org.apache.commons.io.FileUtils;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class InvertedIndex
{
    // Constantes
    public final String ANSI_RED = "\u001B[31m";
    public final String ANSI_GREEN = "\u001B[32m";
    public final String ANSI_BLUE = "\u001B[34m";
    public final String ANSI_GREEN_YELLOW_UNDER = "\u001B[32;40;4m";
    public final String ANSI_RESET = "\u001B[0m";

    private final String DDefaultIndexDir = "./Index/";   // Directorio por defecto donde se guarda el indice invertido.

    private final float DMatchingPercentage = 0.80f;  // Porcentaje mínimo de matching entre el texto original y la consulta (80%)
    private final float DNearlyMatchingPercentage = 0.60f;  // Porcentaje mínimo de matching entre el texto original y la consulta (80%)

    // Members
    private String InputDirPath = null;       // Contiene la ruta del directorio que contiene los ficheros a Indexar.
    private String IndexDirPath = null;       // Contiene la ruta del directorio donde guardar el indice.
    private static List<File> FilesList = new ArrayList<>();

    // Hash Map convertir de ids ficheros a su ruta
    private static Map<Integer,String> Files = new HashMap<Integer,String>();

    // Hash Map para acceder a las líneas de todos los ficheros del indice.
    private static Map<Location, String> IndexFilesLines = new TreeMap<Location, String>();

    // Hash Map que implementa el Índice Invertido: key=word, value=Locations(Listof(file,line)).
    private static Map<String, HashSet <Location>> Hash =  new TreeMap<String, HashSet <Location>>();

    // Estadisticas para verificar la correcta contrucción del indice invertido.
    private static long TotalLocations = 0;
    private static long TotalWords = 0;
    private static long TotalLines = 0;
    private static int TotalProcessedFiles = 0;
    private static long TotalKeys = 0;

    public static int M = 1000;


    public Map<Integer, String> getFiles() { return Files; }
    public Map<Location, String> getIndexFilesLines() { return IndexFilesLines; }
    public static Map<String, HashSet<Location>> getHash() { return Hash; }
    public void setIndexDirPath(String indexDirPath) {
        IndexDirPath = indexDirPath;
    }
    public long getTotalWords(Map hash){ return(hash.size()); }
    public long getTotalLocations() { return TotalLocations; }
    public long getTotalWords() { return InvertedIndex.TotalWords; }
    public long getTotalLines() { return TotalLines; }
    public int getTotalProcessedFiles() { return TotalProcessedFiles; }
    public long getTotalKeys(){ return TotalKeys; }

    public static Lock lock = new ReentrantLock(true);
    public static Condition phaserCreado = lock.newCondition();
    public static Statistics GlobalStatistics = new Statistics("=");


    public static boolean condicionCumplida = false;
    public void esperarPhaser(){
        lock.lock();
        try {
            while(!condicionCumplida){
                phaserCreado.await();
            }
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        finally {
            lock.unlock();
        }
    }

    public static synchronized void addLocationLoad(String key, HashSet<Location> locationList) {
        Hash.put(key, locationList);
    }

    public static synchronized void addProcessedFiles(int num){
        TotalProcessedFiles+=num;
    }
    public static synchronized void addLocations(long num){
        TotalLocations+=num;
    }
    public static void addWords(long num){
        TotalWords+=num;
    }
    public static synchronized void addLines(long num){
        TotalLines+=num;
    }
    public static synchronized void addKeys(long num){
        TotalKeys+=num;
    }
    // Método para realizar el merge de Map<String, HashSet<Location>>

    public static synchronized void mergeLines(Map<Location, String> IndexLines){
        IndexFilesLines.putAll(IndexLines);
    }


    // Constructores
    public InvertedIndex() {
    }
    public InvertedIndex(String InputPath) {
        this.InputDirPath = InputPath;
        this.IndexDirPath = DDefaultIndexDir;
    }

    public InvertedIndex(String inputDir, String indexDir) {
        this.InputDirPath = inputDir;
        this.IndexDirPath = indexDir;
    }

    // Método para la construcción del indice invertido.
    //  1. Busca los ficheros de texto recursivamente en el directorio de entrada.
    //  2. Construye el indice procesando las palabras del fichero.
    public void buidIndex()
    {
        Instant start = Instant.now();

        TotalProcessedFiles = 0;
        TotalLocations = 0;
        TotalLines=0;
        TotalWords=0;
        ProcesarDirectorio taskDir = new ProcesarDirectorio(InputDirPath, FilesList, Files, Hash, IndexFilesLines);
        Thread dirThread = new Thread(taskDir);
        dirThread.setUncaughtExceptionHandler(new MyExceptionHandler());
        dirThread.start();


        esperarPhaser();

        while(ProcesarDirectorio.filesPhaser.getRegisteredParties() > 1){
            ProcesarDirectorio.filesPhaser.arriveAndAwaitAdvance();

            String maxWord = Collections.max(Hash.entrySet(), (entry1, entry2) -> entry1.getValue().size() - entry2.getValue().size()).getKey();
            GlobalStatistics.setMostPopularWord(maxWord);
            GlobalStatistics.setMostPopularWordLocations(Hash.get(maxWord).size());
            GlobalStatistics.print("Global");

            ProcesarDirectorio.filesPhaser.arriveAndAwaitAdvance();
        }

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();  //in millis
        System.out.printf("[Build Index with %d files] Total execution time: %.3f secs.\n", FilesList.size(), timeElapsed/1000.0);

        // Comprobar que el resultado sea correcto.
        try {
            assertEquals(getTotalWords(Hash), getTotalKeys());
            assertEquals(getTotalLocations(Hash), getTotalLocations());
            assertEquals(getTotalFiles(Files), getTotalProcessedFiles());
            assertEquals(getTotalLines(IndexFilesLines), getTotalLines());

        }catch (AssertionError e){
            System.out.println(ANSI_RED+ e.getMessage() + " "+ ANSI_RESET);
        }
    }

    // Calcula el número de ubicaciones diferentes de las palabras en los ficheros.
    // Si una palabra aparece varias veces en un linea de texto, solo se cuenta una vez.
    public long getTotalLocations(Map hash)
    {
        long locations=0;
        Set<String> keySet = hash.keySet();

        Iterator keyIterator = keySet.iterator();
        while (keyIterator.hasNext() ) {
            String word = (String) keyIterator.next();
            locations += Hash.get(word).size();
        }
        return(locations);
    }
    public long getTotalFiles(Map files){
        return(files.size());
    }
    public long getTotalLines(Map filesLines){
        return(filesLines.size());
    }
    public void printIndex()
    {
        Set<String> keySet = Hash.keySet();
        Iterator keyIterator = keySet.iterator();
        while (keyIterator.hasNext() ) {
            String word = (String) keyIterator.next();
            System.out.print(word + "\t");
            HashSet<Location> locations = Hash.get(word);
            for(Location loc: locations){
                System.out.printf("(%d,%d) ", loc.getFileId(), loc.getLine());
            }
            System.out.println();
        }
    }


    public void saveIndex()
    {
        saveIndex(IndexDirPath);
    }

    // Método para salvar el indice invertido en el directorio pasado como parámetro.
    // Se salva:
    //  + Indice Invertido (Hash)
    //  + Hash map de conversión de idFichero->RutaNombreFichero (Files)
    //  + Hash de acceso indexado a las lineas de los ficheros (IndexFilesLines)
    public void saveIndex(String indexDirectory)
    {
        CountDownLatch latch = new CountDownLatch(3);
        Instant start = Instant.now();
        ArrayList<Thread> saveTasksThreads = new ArrayList<>();

        resetDirectory(indexDirectory);

        SaveInvertedIndex taskInvertedIndexSave = new SaveInvertedIndex(indexDirectory, Hash, latch);
        Thread saveThread1 = new Thread(taskInvertedIndexSave);
        saveThread1.setUncaughtExceptionHandler(new MyExceptionHandler());
        saveTasksThreads.add(saveThread1);
        saveThread1.start();

        SaveFilesIds taskFileIdSave = new SaveFilesIds(indexDirectory, Files, latch);
        Thread saveThread2 = new Thread(taskFileIdSave);
        saveThread2.setUncaughtExceptionHandler(new MyExceptionHandler());
        saveTasksThreads.add(saveThread2);
        saveThread2.start();

        SaveFilesLines taskFilesLinesSave = new SaveFilesLines(indexDirectory, IndexFilesLines, latch);
        Thread saveThread3 = new Thread(taskFilesLinesSave);
        saveThread3.setUncaughtExceptionHandler(new MyExceptionHandler());
        saveTasksThreads.add(saveThread3);
        saveThread3.start();

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();  //in millis
        System.out.printf("[Save Index with %d keys] Total execution time: %.3f secs.\n", Hash.size(), timeElapsed/1000.0);
    }

    public void resetDirectory(String outputDirectory)
    {
        File path = new File(outputDirectory);
        if (!path.exists())
            path.mkdir();
        else if (path.isDirectory()) {
            try {
                FileUtils.cleanDirectory(path);
            } catch (IOException e) {
                System.err.printf("Error borrando contenido directorio indice %s.\n",path.getAbsolutePath());
                e.printStackTrace();
            }
        }
    }

    public void loadIndex()
    {
        loadIndex(IndexDirPath);
    }

    // Método para carga el indice invertido del directorio pasado como parámetro.
    // Se carga:
    //  + Indice Invertido (Hash)
    //  + Hash map de conversión de idFichero->RutaNombreFichero (Files)
    //  + Hash de acceso indexado a las lineas de los ficheros (IndexFilesLines)
    public void loadIndex(String indexDirectory)
    {
        ArrayList<Thread> loadTasksThreads = new ArrayList<>();
        Phaser phaser = new Phaser(4);


        Instant start = Instant.now();

        resetIndex();

        LoadInvertedIndex taskInvertedIndexLoad = new LoadInvertedIndex(indexDirectory, Hash, phaser);
        Thread loadThread1 = new Thread(taskInvertedIndexLoad);
        loadThread1.setUncaughtExceptionHandler(new MyExceptionHandler());
        loadTasksThreads.add(loadThread1);
        loadThread1.start();

        LoadFilesIds taskFileIdLoad = new LoadFilesIds(indexDirectory, Files, phaser);
        Thread loadThread2 = new Thread(taskFileIdLoad);
        loadThread2.setUncaughtExceptionHandler(new MyExceptionHandler());
        loadTasksThreads.add(loadThread2);
        loadThread2.start();

        LoadFilesLines taskFilesLinesLoad = new LoadFilesLines(indexDirectory, IndexFilesLines, phaser);
        Thread loadThread3 = new Thread(taskFilesLinesLoad);
        loadThread3.setUncaughtExceptionHandler(new MyExceptionHandler());
        loadTasksThreads.add(loadThread3);
        loadThread3.start();

        phaser.arriveAndAwaitAdvance();

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();  //in millis
        System.out.printf("[Load Index with %d keys] Total execution time: %.3f secs.\n", Hash.size(), timeElapsed/1000.0);
    }

    public void resetIndex()
    {
        Hash.clear();
        Files.clear();
        IndexFilesLines.clear();
    }


    // Implentar una consulta sobre el indice invertido:
    //  1. Descompone consulta en palabras.
    //  2. Optiene las localizaciones de cada palabra en el indice invertido.
    //  3. Agrupa palabras segun su localizacion en una hash de coincidencias.
    //  4. Recorremos la tabla de coincidencia y mostramos las coincidencias en función del porcentaje de matching.
    public void query(String queryString)
    {
        String queryResult=null;
        Map<Location, Integer> queryMatchings = new TreeMap<Location, Integer>();
        Instant start = Instant.now();

        System.out.println ("Searching for query: "+queryString);

        // Pre-procesamiento query
        queryString = Normalizer.normalize(queryString, Normalizer.Form.NFD);
        queryString = queryString.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
        String filter_line = queryString.replaceAll("[^a-zA-Z0-9áÁéÉíÍóÓúÚäÄëËïÏöÖüÜñÑ ]","");
        // Dividimos la línea en palabras.
        String[] words = filter_line.split("\\W+");
        int querySize = words.length;

        // Procesar cada palabra de la query
        for(String word: words)
        {
            word = word.toLowerCase();
            if (Indexing.Verbose) System.out.printf("Word %s matching: ",word);
            // Procesar las distintas localizaciones de esta palabra
            if (Hash.get(word)==null)
                continue;
            for(Location loc: Hash.get(word))
            {
                // Si no existe esta localización en la tabla de coincidencias, entonces la añadimos con valor inicial a 1.
                Integer value = queryMatchings.putIfAbsent(loc, 1);
                if (value != null) {
                    // Si existe, incrementamos el número de coincidencias para esta localización.
                    queryMatchings.put(loc, value+1);
                }
                if (Indexing.Verbose) System.out.printf("%s,",loc);
            }
            if (Indexing.Verbose) System.out.println(".");
        }

        if (queryMatchings.size()==0)
            System.out.printf(ANSI_RED+"Not matchings found.\n"+ANSI_RESET);

        // Recorremos la tabla de coincidencia y mostramos las líneas en donde aparezca más de un % de las palabras de la query.
        for(Map.Entry<Location, Integer> matching : queryMatchings.entrySet())
        {
            Location location = matching.getKey();
            if ((matching.getValue()/(float)querySize)==1.0)
                System.out.printf(ANSI_GREEN_YELLOW_UNDER+"%.2f%% Full Matching found in line %d of file %s: %s.\n"+ANSI_RESET,(matching.getValue()/(float)querySize)*100.0,location.getLine(), location.getFileId(), getIndexFilesLine(location));
            else if ((matching.getValue()/(float)querySize)>=DMatchingPercentage)
                System.out.printf(ANSI_GREEN+"%.2f%% Matching found in line %d of file %s: %s.\n"+ANSI_RESET,(matching.getValue()/(float)querySize)*100.0,location.getLine(), location.getFileId(), getIndexFilesLine(location));
            else if ((matching.getValue()/(float)querySize)>=DNearlyMatchingPercentage)
                System.out.printf(ANSI_RED+"%.2f%% Weak Matching found in line %d of file %s: %s.\n"+ANSI_RESET,(matching.getValue()/(float)querySize)*100.0,location.getLine(), location.getFileId(), getIndexFilesLine(location));
        }

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();  //in millis
        System.out.printf("[Query with %d words] Total execution time: %.3f secs.\n", querySize, timeElapsed/1000.0);
    }

    private String getIndexFilesLine(Location loc){
        return(IndexFilesLines.get(loc));
    }

}

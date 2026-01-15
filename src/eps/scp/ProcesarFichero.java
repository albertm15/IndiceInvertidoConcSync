/* ---------------------------------------------------------------
Práctica 3.
Código fuente : IndiceInvertidoConcPRA3
Grado Informática
49381774S Albert Martín López.
49380060A Pau Barahona Setó.
--------------------------------------------------------------- */

package eps.scp;
import java.io.*;
import java.text.Normalizer;
import java.util.*;

public class ProcesarFichero implements Runnable{
    private int fileId;
    private File file;
    private Map<String, HashSet <Location>> Hash;
    private Map<String, HashSet <Location>> HashPrivado = new TreeMap<String, HashSet <Location>>();

    private Map<Location, String> IndexFilesLines = new TreeMap<>();
    private long TotalLines = 0;
    private long TotalWords = 0;
    private long TotalLocations = 0;
    private long TotalKeysFound = 0;


    public ProcesarFichero (int fileId, File file, Map<String, HashSet<Location>> Hash){
        this.fileId = fileId;
        this.file = file;
        this.Hash = Hash;
    }

    @Override
    public void run(){
        addFileWordsModificado(fileId, file);
    }
    public void addFileWordsModificado(int fileId, File file)
    {
        Statistics FileStatistics = new Statistics("_");
        System.out.printf("Processing %3dth file %s (Path: %s)\n", fileId, file.getName(), file.getAbsolutePath());
        FileStatistics.incProcessingFiles();

        // Crear buffer reader para leer el fichero a procesar.
        try(BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            String line;
            int lineNumber = 0;  // inicializa contador de líneas a 0.
            while( (line = br.readLine()) !=null)   // Leemos siguiente línea de texto del fichero.
            {
                lineNumber++;
                TotalLines++;
                FileStatistics.incProcessedLines();
                if (Indexing.Verbose) System.out.printf("Procesando linea %d fichero %d: ",lineNumber,fileId);
                Location newLocation = new Location(fileId, lineNumber);
                addIndexFilesLine(newLocation, line);
                // Eliminamos carácteres especiales de la línea del fichero.
                line = Normalizer.normalize(line, Normalizer.Form.NFD);
                line = line.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
                String filter_line = line.replaceAll("[^a-zA-Z0-9áÁéÉíÍóÓúÚäÄëËïÏöÖüÜñÑ ]","");
                // Dividimos la línea en palabras.
                String[] words = filter_line.split("\\W+");
                //String[] words = line.split("(?U)\\p{Space}+");
                // Procesar cada palabra
                for(String word:words)
                {
                    if (Indexing.Verbose) System.out.printf("%s ",word);
                    word = word.toLowerCase();
                    // Obtener entrada correspondiente en el Indice Invertido
                    ProcesarDirectorio.lockParaFichero.lock();
                    HashSet<Location> locations = Hash.get(word);
                    if (locations == null)
                    {   // Si no existe esa palabra en el indice invertido, creamos una lista vacía de Localizaciones y la añadimos al Indice
                        locations = new HashSet<Location>();
                        if (!Hash.containsKey(word)) {
                            FileStatistics.incKeysFound();
                            TotalKeysFound++; // Modificado!!
                        }
                        InvertedIndex.addLocationLoad(word, locations);
                    }
                    TotalWords++;   // Modificado!!
                    FileStatistics.incProcessedWords();   // Modificado!!
                    // Añadimos nueva localización en la lista de localizaciomes asocidada con ella.
                    int oldLocSize = locations.size();
                    locations.add(newLocation);
                    if (locations.size()>oldLocSize) {
                        TotalLocations++;
                        FileStatistics.incProcessedLocations();
                    }
                    ProcesarDirectorio.lockParaFichero.unlock();
                    if((TotalWords % InvertedIndex.M) == 0){
                        InvertedIndex.GlobalStatistics.getMostPopularWord();
                        FileStatistics.print(file.getName());
                        InvertedIndex.GlobalStatistics.addStatistics(FileStatistics);
                        ProcesarDirectorio.filesPhaser.arriveAndAwaitAdvance();
                        ProcesarDirectorio.filesPhaser.arriveAndAwaitAdvance();
                        FileStatistics = new Statistics("_");
                    }
                }
                if (Indexing.Verbose) System.out.println();
            }
        } catch (FileNotFoundException e) {
            System.err.printf("Fichero %s no encontrado.\n",file.getAbsolutePath());
            e.printStackTrace();
        } catch (IOException e) {
            System.err.printf("Error lectura fichero %s.\n",file.getAbsolutePath());
            e.printStackTrace();
        }

        FileStatistics.incProcessedFiles();
        FileStatistics.decProcessingFiles();
        InvertedIndex.mergeLines(IndexFilesLines);

        if(!((TotalWords % InvertedIndex.M) == 0)){
            InvertedIndex.GlobalStatistics.getMostPopularWord();
            FileStatistics.print(file.getName());
            InvertedIndex.GlobalStatistics.addStatistics(FileStatistics);
        }

        InvertedIndex.addLines(TotalLines);
        InvertedIndex.addWords(TotalWords);
        InvertedIndex.addKeys(TotalKeysFound);
        InvertedIndex.addLocations(TotalLocations);

        ProcesarDirectorio.filesPhaser.arriveAndDeregister();
    }


    private void addIndexFilesLine(Location loc, String line){
        IndexFilesLines.put(loc, line);
    }
}


# Concurrent Inverted Index – Synchronized Version

## Description
This project is a variant of the **Inverted Index** implementation in Java. While other versions may rely on high-level concurrent collections, this version focuses on the explicit use of **synchronization mechanisms** to manage concurrency and protect critical sections in a manual and controlled way.

The goal is to demonstrate fine-grained thread control using the `java.util.concurrent` package and locking primitives, orchestrating reading, indexing, saving, and loading of data in a coordinated manner.

## Main Features
The core of this project is **Explicit Synchronization**. The following mechanisms are implemented:

- **ReentrantLock:** Protects access to the shared data structure (`Hash`) inside `ProcesarFichero`, ensuring mutual exclusion during write operations.
- **Phaser:** A reusable synchronization barrier. It periodically stops all processing threads (every 1000 words) to compute and display global statistics in a coordinated way before continuing.
- **Semaphore:** Controls access to standard output (`System.out`) in the `Statistics` class, preventing console messages from different threads from interleaving.
- **CountDownLatch:** Used in the `saveIndex` method to wait until the three independent saving tasks (Index, Files, Lines) finish before completing execution.
- **Monitors (`synchronized`):** Synchronized methods for atomic updates of global counters (`TotalWords`, `TotalLines`, etc.) in the `InvertedIndex` class.
- **Condition Variables:** Use of `Condition` to manage specific waiting scenarios during task initialization.

## Project Structure

The source code is located in the `eps.scp` package.

### Main Classes (`src/eps/scp/`)
- **`Indexing.java`**  
  Entry point (`main`). Configures execution, launches index construction, and verifies results using assertions.

- **`InvertedIndex.java`**  
  Manager class. Contains the data structures (non-concurrent `HashMap` and `TreeMap`) and coordinates global phases using `Phaser` and `CountDownLatch`.

- **`ProcesarFichero.java`**  
  A (`Runnable`) task that processes each file. Uses a global `Lock` to safely insert words into the index and periodically reports to the `Phaser`.

- **`Statistics.java`**  
  Auxiliary class for collecting metrics. Uses a `Semaphore(1)` to ensure console output is readable and ordered.

- **`ProcesarDirectorio.java`**  
  Responsible for traversing directories and assigning files to processing threads.

## Requirements
- **Java Development Kit (JDK)** version 8 or higher.
- **Apache Commons IO** for file management.
- **JUnit 5** for testing and assertions.

## How to Run

### Generate the Index
The program processes text files and generates the indexed structure on disk.

**Command:**
```bash
java eps.scp.Indexing <Source_Directory> [<Index_Directory>]
```

**Example:**
```bash
java eps.scp.Indexing ./books ./OutputIndex
```

During execution, periodic statistics will be displayed on the console thanks to `Phaser` synchronization, showing which thread is processing which file and the most frequent words found so far.

## Synchronization Technical Details

Unlike the version based on *Virtual Threads* and concurrent collections, this implementation manages thread safety manually.

### Data Protection
The main map is a standard `TreeMap` (not *thread-safe*). Concurrent access is protected using a `ReentrantLock`:

```java
// In ProcesarFichero.java
ProcesarDirectorio.lockParaFichero.lock();
try {
    // Modification of the shared Hash
} finally {
    ProcesarDirectorio.lockParaFichero.unlock();
}
```

### Phase Barriers (Phaser)
Every M words (by default, 1000), threads synchronize at a barrier:

```java
ProcesarDirectorio.filesPhaser.arriveAndAwaitAdvance();
```

This allows the main thread to collect consistent partial statistics.

### Clean Output (Semaphore)
To prevent console output from being mixed due to concurrency:

```java
// In Statistics.java
semaforo.acquire();
// Print...
semaforo.release();
```

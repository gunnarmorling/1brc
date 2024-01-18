/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dev.morling.onebrc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CalculateAverage_lucasnog {


    public static void main(String[] args) throws IOException {

        int numProcessors = Runtime.getRuntime().availableProcessors();


        Path path = Path.of("measurements.txt");


        ExecutorService executorService = Executors.newFixedThreadPool(numProcessors);

        try (BufferedReader br = new BufferedReader(new FileReader(path.toFile()))) {
            TreeMap<String, Calculo> mapa = new TreeMap<>();

            long totalLinhas = br.lines().count();
            br.close();

            // Reabrir o BufferedReader para processar as linhas novamente
            BufferedReader novoBR = new BufferedReader(new FileReader(path.toFile()));

            // Dividir o trabalho entre as threads
            long linhasPorThread = totalLinhas / numProcessors;

            // Execução
            for (int i = 0; i < numProcessors; i++) {
                long inicio = i * linhasPorThread;
                long fim = (i == numProcessors - 1) ? totalLinhas : (i + 1) * linhasPorThread;

                Runnable worker = new ProcessadorThread(novoBR, mapa, inicio, fim);
                executorService.execute(worker);
            }

            executorService.shutdown();
            while (!executorService.isTerminated()) {
                // wait ExecutorService
            }

            // Print no console no map
            System.out.println(mapa);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class ProcessadorThread implements Runnable {
    private final BufferedReader br;
    private final Map<String, Calculo> mapa;
    private final long inicio;
    private final long fim;

    public ProcessadorThread(BufferedReader br, Map<String, Calculo> mapa, long inicio, long fim) {
        this.br = br;
        this.mapa = mapa;
        this.inicio = inicio;
        this.fim = fim;
    }

    @Override
    public void run() {
        String linha;
        long count = 0;

        try {
            while ((linha = br.readLine()) != null && count < fim) {
                if (count >= inicio) {
                    String[] partes = linha.split(";");
                    String chave = partes[0];
                    Double novoValor = Double.parseDouble(partes[1]);

                    synchronized (mapa) {
                        if (!mapa.containsKey(chave)) {
                            Calculo novoCalculo = new Calculo();
                            novoCalculo.setNumMin(novoValor);
                            novoCalculo.setNumMax(novoValor);
                            mapa.put(chave, novoCalculo);
                        } else {
                            Calculo calculo = mapa.get(chave);
                            calculo.setNumMax(Math.max(novoValor, calculo.getNumMax()));
                            calculo.setNumMin(Math.min(novoValor, calculo.getNumMin()));
                        }
                    }
                }

                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class Calculo {
    Double numMin, numMax, media;

    public Calculo() {
    }

    public String toString() {
        return getNumMin() + "/" + getMedia() + "/" + getNumMax();
    }

    public Double getNumMin() {
        return this.numMin;
    }

    public void setNumMin(Double numMin) {
        this.numMin = numMin;
    }

    public Double getNumMax() {
        return this.numMax;
    }

    public void setNumMax(Double numMax) {
        this.numMax = numMax;
    }

    public Double getMedia() {
        this.media = (this.numMax + this.numMin) / 2.0;
        DecimalFormat formato = new DecimalFormat("##.##");
        return Double.valueOf(formato.format(media).replace(',', '.'));
    }
}

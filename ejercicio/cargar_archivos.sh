#! /usr/bin/bash
hadoop fs -mkdir -p ejercicio/fecha
hadoop fs -mkdir -p ejercicio/paises
hadoop fs -mkdir -p ejercicio/retrasos
hadoop fs -mkdir -p ejercicio/vuelos

hadoop fs -put fecha.dat ejercicio/fecha
hadoop fs -put paises.ada ejercicio/paises
hadoop fs -put retrasos.dat ejercicio/retrasos
hadoop fs -put vuelos.dat ejercicio/vuelos
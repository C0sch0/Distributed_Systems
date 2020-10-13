package main

import (
  "encoding/csv"
  "fmt"
  "os"
  "bufio"
  "strconv"
  "log"
  "sync"
)

type Covid struct {
    Region      string `csv:"Region"` // .csv column headers
    CodigoRegion    string `csv:"Codigo region"`
    Comuna     string `csv:"Comuna"`
    CodigoComuna     string `csv:"Codigo comuna"`
    Poblacion     float64 `csv:"Poblacion"`
    Fecha     string `csv:"Fecha"`
    Casos     float64 `csv:"Casos confirmados"`
}

//func readCsvFile(filePath string) [][]string {
//}

func checkError(message string, err error) {
    if err != nil {
        log.Fatal(message, err)
    }
}


func Map_Select(datos [][]string) []Covid {
  list := []Covid{}

  for _, lista := range datos{
    poblacion, _ := strconv.ParseFloat(lista[4], 64)
    casos, _ := strconv.ParseFloat(lista[6], 64)

    list = append(list, Covid{
      Region: lista[0],
      CodigoRegion: lista[1],
      Comuna: lista[2],
      CodigoComuna: lista[3],
      Poblacion: poblacion,
      Fecha: lista[5],
      Casos: casos,
    })
  }
  return list
}


func Map_Projection(datos [][]string, col_pedidas []string) [][]string {
	filtered := [][]string{}

  for _, dato := range datos{
    new_line := []string{}
  	for _, word := range col_pedidas {
      if word == "Region" {
        new_line = append(new_line, dato[0])
      }
      if word == "Codigo region" {
        new_line = append(new_line, dato[1])
      }
      if word == "Comuna" {
        new_line = append(new_line, dato[2])
      }
      if word == "Codigo comuna" {
        new_line = append(new_line, dato[3])
      }
      if word == "Poblacion" {
        new_line = append(new_line, dato[4])
      }
      if word == "Fecha" {
        new_line = append(new_line, dato[5])
      }
      if word == "Casos confirmados" {
        new_line = append(new_line, dato[6])
      }
  	}
    filtered = append(filtered, new_line)
  }

  return filtered
}


func Reduce_GA(mapList chan map[string][]string, sendFinalValue chan map[string]float64, stdin_[]string){
  reduccion := map[string]float64{}
  operacion := stdin_[3]

  if operacion == "AVG" {
    for list := range mapList{
      for key, value := range list{
        var suma_total float64 = 0
        for _, valor := range value{
          cant, _ := strconv.ParseFloat(valor, 64)
          suma_total = suma_total + cant
        }
        total_numeros := float64(len(value))
        reduccion[key] = suma_total / total_numeros
        //fmt.Println(reduccion[key])
      }
    }
  }

  if operacion == "MIN" {
    for list := range mapList{
      for key, value := range list{
        var min_value float64
        for _, valor := range value{
          cant, _ := strconv.ParseFloat(valor, 64)
          if cant < min_value {
            min_value = cant
          }
        }
        reduccion[key] = min_value
        //fmt.Println(reduccion[key])
      }
    }
  }

  if operacion == "MAX" {
    for list := range mapList{
      for key, value := range list{
        var max_value float64
        for _, valor := range value{
          cant, _ := strconv.ParseFloat(valor, 64)
          if cant > max_value {
            max_value = cant
          }
        }
        reduccion[key] = max_value
        //fmt.Println(reduccion[key])
      }
    }
  }

  if operacion == "SUM" {
    for list := range mapList{
      for key, value := range list{
        var suma_tot float64
        for _, valor := range value{
          cant, _ := strconv.ParseFloat(valor, 64)
          suma_tot = suma_tot + cant
        }
        reduccion[key] = suma_tot
        //fmt.Println(reduccion[key])
      }
    }
  }

  sendFinalValue <- reduccion
  //fmt.Println(reduccion)
}


func Map_GA(datos [][]string, col_pedidas []string) map[string][]string {
  mapeo := map[string][]string{}

  //fmt.Println(col_pedidas)
  columna := col_pedidas[0]
  posicion := 0
  // Sacar columna para agrupar
  if columna == "Region" {
    posicion = 0
  }
  if columna == "Codigo region" {
    posicion = 1
  }
  if columna == "Comuna" {
    posicion = 2
  }
  if columna == "Codigo comuna" {
    posicion = 3
  }
  if columna == "Poblacion" {
    posicion = 4
  }
  if columna == "Fecha" {
    posicion = 5
  }
  if columna == "Casos confirmados" {
    posicion = 6
  }

  // Sacar columna para agregar
  col_agregacion := col_pedidas[2]
  posicion_2 := 0

  if col_agregacion == "Region" {
    posicion_2 = 0
  }
  if col_agregacion == "Codigo region" {
    posicion_2 = 1
  }
  if col_agregacion == "Comuna" {
    posicion_2 = 2
  }
  if col_agregacion == "Codigo comuna" {
    posicion_2 = 3
  }
  if col_agregacion == "Poblacion" {
    posicion_2 = 4
  }
  if col_agregacion == "Fecha" {
    posicion_2 = 5
  }
  if col_agregacion == "Casos confirmados" {
    posicion_2 = 6
  }

  //fmt.Println("mapeo")
  for _, dato := range datos{
    mapeo[dato[posicion]] = append(mapeo[dato[posicion]], dato[posicion_2])
  }
  //fmt.Println(mapeo)
  return mapeo
}



func Reducer_Projection(mapList chan [][]string, sendFinalValue chan [][]string){
  filtered := [][]string{}
  //valor_guardar := []string{}
  var found int = 1
  var found_2 int = 1
  for list := range mapList {
    // list: e.g.: [[Biobío Alto Biobio]]
    for _, value := range list {
      found = 1
      // value: e.g.: [Biobío Alto Biobio]
      for _, lista := range filtered {
        found_2 = 1
        for index := range lista{
          if lista[index] != value[index] {
            found_2 = 0
          }
        }
        if found_2 == 1 {
          found = 0
        }
      }
      if found == 1 {
        filtered = append(filtered, value)
      }
    }
  }
  //fmt.Println(filtered)
  sendFinalValue <- filtered
}

func Reducer_Select(mapList chan []Covid, sendFinalValue chan []Covid, data []string){
  final := []Covid{}

  col_name := data[0]
  filter := data[1]
  filter_value := data[2]

  for list := range mapList {
    for _, value := range list {
      switch col_name {
      // --------------------------- Comuna --------------------------------
      case "Comuna":
        switch filter {
        case "==":
          if value.Comuna == filter_value {
            final = append(final, value)
          }
        case "!=":
          if value.Comuna != filter_value {
            final = append(final, value)
          }
        }
      // --------------------------- Region --------------------------------
      case "Region":
        switch filter {
        case "==":
          if value.Region == filter_value {
            final = append(final, value)
          }
        case "!=":
          if value.Region != filter_value {
            final = append(final, value)
          }
        }

        // --------------------------- Casos --------------------------------
      case "Casos":
        var Casos_Int int = int(value.Casos)
        filtro_int, _ := strconv.Atoi(filter_value)
        switch filter {
        case "==":
          if Casos_Int ==  filtro_int {
            final = append(final, value)
          }
        case "!=":
          if Casos_Int != filtro_int {
            final = append(final, value)
          }
        case "<":
          if Casos_Int < filtro_int {
            final = append(final, value)
          }
        case ">":
          if Casos_Int > filtro_int {
            final = append(final, value)
          }
        case "<=":
          if Casos_Int <= filtro_int {
            final = append(final, value)
          }
        case ">=":
          if Casos_Int >= filtro_int {
            final = append(final, value)
          }
        }

        // --------------------------- Fecha --------------------------------
      case "Fecha":
        switch filter {
        case "==":
          if value.Fecha ==  filter_value {
            final = append(final, value)
          }
        case "!=":
          if value.Fecha != filter_value {
            final = append(final, value)
          }
        case "<":
          if value.Fecha < filter_value {
            final = append(final, value)
          }
        case ">":
          if value.Fecha > filter_value {
            final = append(final, value)
          }
        case "<=":
          if value.Fecha <= filter_value {
            final = append(final, value)
          }
        case ">=":
          if value.Fecha >= filter_value {
            final = append(final, value)
          }
        }
        // --------------------------- CodigoRegion --------------------------------
      case "CodigoRegion":
        switch filter {
        case "==":
          if value.CodigoRegion == filter_value {
            final = append(final, value)
          }
        case "!=":
          if value.CodigoRegion != filter_value {
            final = append(final, value)
          }
        }
        // --------------------------- CodigoComuna --------------------------------
      case "CodigoComuna":
        switch filter {
        case "==":
          if value.CodigoComuna == filter_value {
            final = append(final, value)
          }
        case "!=":
          if value.CodigoComuna != filter_value {
            final = append(final, value)
          }
        }
      }
    }
  }
  sendFinalValue <- final
}

func main() {
  numThreads_txt := os.Args[1]
  fmt.Println("Threads:", numThreads_txt)
  numThreads, _ := strconv.Atoi(numThreads_txt)

  // Read CSV
  in, err := os.Open("./Covid-19_std.csv")
  if err != nil {
      panic(err)
  }
  defer in.Close()

  lines, err := csv.NewReader(in).ReadAll()
  if err != nil {
    panic(err)
  }

  // Create the Channel
  //fmt.Println(records)
  // Leer input de STDin

  scanner := bufio.NewScanner(os.Stdin)
  fmt.Print("Comiense entregando instrucciones: \n")
  scanner.Scan()
  text := scanner.Text()
  var wg sync.WaitGroup

  wg.Add(numThreads)

  if text == "SELECT" {
    // Crear canal
    lists := make(chan []Covid)
    finalValue := make(chan []Covid)
    // Mapear

    for counter := 0; counter < numThreads; counter++ {
      if counter == numThreads - 1 {
        go func(dato [][]string){
          defer wg.Done()
          lists <- Map_Select(dato)
        }(lines[counter * (len(lines) / numThreads):])
      }

      if counter < numThreads - 1 {
        go func(dato [][]string){
          defer wg.Done()
          lists <- Map_Select(dato)
        }(lines[counter * (len(lines) / numThreads) : (counter + 1) * (len(lines) / numThreads)])
      }
    }

    fmt.Print("Ha elegido SELECT. Indique Col_Name, Filtro y Valor \n")
    // Manejo de inputs
    inputs_select := make([]string, 0)
    scanner.Scan()
    COL_NAME := scanner.Text()
    inputs_select = append(inputs_select, COL_NAME)

    scanner.Scan()
    FILTER := scanner.Text()
    inputs_select = append(inputs_select, FILTER)

    scanner.Scan()
    VALUE := scanner.Text()
    inputs_select = append(inputs_select, VALUE)

    // Reduce
    go Reducer_Select(lists, finalValue, inputs_select)

    // Esperar y cerrar canal
    wg.Wait()
    close(lists)
    fmt.Println(<-finalValue)

  }

  if text == "PROJECTION"{
    fmt.Print("Ha elegido PROJECTION. Indique N_Columnas y las N Columnas\n")

    lists := make(chan [][]string)
    finalValue := make(chan [][]string)
    // var wg sync.WaitGroup
    //wg.Add(len(lines))

    inputs_projection := make([]string, 0)
    scanner.Scan()
    N_COL := scanner.Text()
    N_COL_INT, _ := strconv.Atoi(N_COL)

    for i := 0; i < N_COL_INT; i++ {
      scanner.Scan()
      COL_N := scanner.Text()
      inputs_projection = append(inputs_projection, COL_N)
    }

    //fmt.Println(lines[1 * (len(lines) / 2):])
    for counter := 0; counter < numThreads; counter++ {
      //fmt.Println("Hola")
      if counter == numThreads - 1 {
        go func(datos [][]string){
          defer wg.Done()
          lists <- Map_Projection(datos, inputs_projection)
        }(lines[counter * (len(lines) / numThreads):])
      }

      if counter < numThreads - 1 {
        go func(datos [][]string){
          defer wg.Done()
          //fmt.Println(datos)
          lists <- Map_Projection(datos, inputs_projection)
        }(lines[counter * (len(lines) / numThreads) : (counter + 1) * (len(lines) / numThreads)])
      }
    }

    go Reducer_Projection(lists, finalValue)
    wg.Wait()
    close(lists)
    fmt.Println(<-finalValue)
  }

  if text == "GROUP"{
    fmt.Print("Ha elegido GROUP. Indique COL_0 / AGGREGATE / COL_1 / FUNCTION\n")
    inputs_group := make([]string, 0)
    lista_mapeo := make(chan map[string][]string)
    finalValue := make(chan map[string]float64)

    scanner.Scan()
    COL_NAME_0 := scanner.Text()

    scanner.Scan()
    AGGREGATE := scanner.Text()

    scanner.Scan()
    COL_NAME_1 := scanner.Text()

    scanner.Scan()
    FUNCTION := scanner.Text() // MIN, MAX,AVG,SUM.

    inputs_group = append(inputs_group, COL_NAME_0)
    inputs_group = append(inputs_group, AGGREGATE)
    inputs_group = append(inputs_group, COL_NAME_1)
    inputs_group = append(inputs_group, FUNCTION)

    //fmt.Println(lines[1 * (len(lines) / 2):])
    for counter := 0; counter < numThreads; counter++ {
      if counter == numThreads - 1 {
        go func(datos [][]string){
          defer wg.Done()
          lista_mapeo <- Map_GA(datos, inputs_group)
        }(lines[counter * (len(lines) / numThreads):])
      }

      if counter < numThreads - 1 {
        go func(datos [][]string){
          defer wg.Done()
          //fmt.Println(datos)
          lista_mapeo <- Map_GA(datos, inputs_group)
        }(lines[counter * (len(lines) / numThreads) : (counter + 1) * (len(lines) / numThreads)])
      }
    }

    go Reduce_GA(lista_mapeo, finalValue, inputs_group)
    wg.Wait()
    close(lista_mapeo)
    // write csv

  }




  // Guardamos resultado a CSV

  //file, err := os.Create("result.csv")
//  checkError("Cannot create file", err)
  //defer file.Close()


  //writer := csv.NewWriter(file)
//  defer writer.Flush()

  //fmt.Println(<-finalValue)
//  for _, value := range <-finalValue {
  //  fmt.Println(value)
  //  err := writer.Write(value)
  //  checkError("Cannot write to file", err)
//  }
}



// Read CSV borrowed from https://stackoverflow.com/questions/24999079/reading-csv-file-in-go
// Read STDin from https://stackoverflow.com/questions/20895552/how-to-read-from-standard-input-in-the-console
// MapReduce example from
// Example N-2

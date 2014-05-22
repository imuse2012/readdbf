package main

import(
  "fmt"
  "code.google.com/p/go-dbf/godbf"
)


func main(){

    file1 := "show2003.dbf"
//    file1 := "sjshq.dbf"

    dbfTable, e := godbf.NewFromFile(file1, "GB2312") 
    if e != nil {

        fmt.Println(e)
    }
    fields := dbfTable.Fields()

    fmt.Println(fields)

    for _,v := range fields {
        fmt.Println(v.FieldName())
    }

    for i := 0; i< dbfTable.NumberOfRecords(); i++{

        row := dbfTable.GetRowAsSlice(i)
        fmt.Println(row)

    }

    fmt.Println(dbfTable.NumberOfRecords())
}

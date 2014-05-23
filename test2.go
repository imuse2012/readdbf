package main

import(
  "fmt"
  "errors"
  "code.google.com/p/go-dbf/godbf"
  "github.com/garyburd/redigo/redis"
  "time"
  "strings"
  "os"
  "github.com/howeyc/fsnotify"
)

// s* 对应的key
func getFields(key string) (string, error){
    maps := map[string]string{
        "S1":"HQZQDM",
        "S2":"HQZQJC",
        "S4":"HQJRKP",
        "S3":"HQZRSP",
        "S8":"HQZJCJ",
        "S6":"HQZGCJ",
        "S7":"HQZDCJ",
        "S9":"HQBJW1",
        "S10":"HQSJW1",
        "S11":"HQCJSL",
        "S5":"HQCJJE",
        "S15":"HQBSL1",
        "S17":"HQBSL2",
        "S16":"HQBJW2",
        "S19":"HQBSL3",
        "S18":"HQBJW3",
        "S27":"HQBSL4",
        "S26":"HQBJW4",
        "S29":"HQBSL5",
        "S28":"HQBJW5",
        "S21":"HQSSL1",
        "S23":"HQSSL2",
        "S22":"HQSJW2",
        "S25":"HQSSL3",
        "S24":"HQSJW3",
        "S31":"HQSSL4",
        "S30":"HQSJW4",
        "S33":"HQSSL5",
        "S32":"HQSJW5",
        "S13":"HQSYL1",
    }

    if v, ok := maps[key]; ok {
        return v, nil

    }

    return "", errors.New("key not exists! :" + key)
}

var buildPeriod time.Time
var redisPool *redis.Pool
var	eventTime   = make(map[string]int64) 

func init(){
    redisPool = &redis.Pool{
        MaxIdle: 3,
        MaxActive: 200, // max number of connections
        Dial: func() (redis.Conn, error) {
                c, err := redis.Dial("tcp", "127.0.0.1:6379")
                if err != nil {
                        panic(err.Error())
                }
                return c, err
        },
    }
}


func main(){
    // sfnotify
    watcher, err := fsnotify.NewWatcher()
    if err != nil {

        fmt.Println("[ERROR] Fail to create new Watcher :", err)
        os.Exit(2)
    }

    shfile := "show2003.dbf"
    szfile := "sjshq.dbf"


    var ch = make(chan int)

    go func(){
      for {
        select {
        case event := <-watcher.Event:
                isbuild := true
				if buildPeriod.Add(1 * time.Second).After(time.Now()) {
					continue
				}
				buildPeriod = time.Now()

                mt := getFileModTime(event.Name)
                if t := eventTime[event.Name]; mt == t {
                    isbuild = false
                }

                eventTime[event.Name] = mt

                if isbuild {
                    fmt.Println("[EVENT] ", event)
                    if event.Name == shfile {
                        fmt.Println("read sh")
                        ReadDBF(shfile, true)
                    }
                    
                    if event.Name == szfile{
                        fmt.Println("read sz")
                        ReadDBF(szfile, false)
                    }
                }

        case werr := <-watcher.Error:
            fmt.Println("[Error] Fail to watch file", werr)

        }
      }
    }()


    watcher.Watch(shfile)
    watcher.Watch(szfile)

    <-ch

    watcher.Close()

    start := time.Now().UnixNano()

    /*
    var ch1,ch2 chan int

    go func(ch chan int){
        ReadDBF(shfile, true)
        ch1<-0
    }(ch1)
    go func(ch chan int){
        ReadDBF(szfile, false)
        ch1<-0
    }(ch2)

    a := <-ch1
    b := <-ch2
    fmt.Println(a,b)
    */


    end := time.Now().UnixNano()

    fmt.Println(fmt.Sprintf("用时: %d 毫秒", (end - start)/1000000))
}

func ReadDBF(file string, isSH bool)(){
    dbfTable, e := godbf.NewFromFile(file, "GB2312") 
    if e != nil {
        fmt.Println(e)
        return
    }
    fields := dbfTable.Fields()

    fieldNames := make([]string, 0)
    for _,field := range fields{
//    fmt.Println(k,dbfTable.FieldValue(1, k))
        if isSH {
            szName, err := getFields(field.FieldName())
            if err != nil {
                fmt.Println(err, field.FieldName())
                continue
            }
            fieldNames = append(fieldNames, szName)
        } else {
            fieldNames = append(fieldNames, field.FieldName())
        }
    }
    fieldNames = append(fieldNames, "time")

    // update time
    var updateDate, updateTime string
    if isSH {
        updateDate = dbfTable.FieldValue(0, 5)
        updateTime = dbfTable.FieldValue(0, 1)
    } else {
        updateDate = dbfTable.FieldValue(0, 1)
        updateTime = dbfTable.FieldValue(0, 7)
    }

//    fmt.Println(updateDate, updateTime)

    timeparsed, _ := time.Parse("20060102150405", updateDate+updateTime)
    timeUnix := fmt.Sprintf("%d",timeparsed.Unix())

    for i := 1; i< dbfTable.NumberOfRecords(); i++{

        row := dbfTable.GetRowAsSlice(i)


        if isSH {
            row[0] = fmt.Sprintf("sh%s", row[0])
        } else {
            row[0]  = fmt.Sprintf("sz%s", row[0])
        }

        row = append(row, timeUnix)

        save2redis(row, fieldNames)
        //go save2redis(row)

    }
}

func save2redis(row, fields []string){
//    fmt.Println(row)
    if len(row) != len(fields){
        fmt.Println("Error: row fields not fix:", len(row), len(fields))
        return

    }

    key := fmt.Sprintf("stock-data:%s", row[0])

    args := []interface{}{key}

    for k,v := range row {

        v = strings.Replace(v, "-", "0", -1)

        args = append(args, fields[k], v)
    }

    client := redisPool.Get()
    if _, err := client.Do("HMSET", args...); err != nil {
        fmt.Println(err)
        return
    }
    defer client.Close()

    //fmt.Println("save ok")

}

// getFileModTime retuens unix timestamp of `os.File.ModTime` by given path.
func getFileModTime(path string) int64 {
	path = strings.Replace(path, "\\", "/", -1)
	f, err := os.Open(path)
	if err != nil {
		fmt.Println("[ERRO] Fail to open file[ %s ]\n", err)
		return time.Now().Unix()
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		fmt.Println("[ERRO] Fail to get file information[ %s ]\n", err)
		return time.Now().Unix()
	}

	return fi.ModTime().Unix()
}

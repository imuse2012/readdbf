package main

import(
    "errors"
    "fmt"
    "time"
    "os"
    "os/exec"
    "path"
    "path/filepath"
    "strings"
    "github.com/Unknwon/goconfig"
    "github.com/garyburd/redigo/redis"
    "github.com/howeyc/fsnotify"
    "code.google.com/p/go-dbf/godbf"
   _ "github.com/go-sql-driver/mysql"
   "github.com/go-xorm/xorm"
)

var(
    Cfg *goconfig.ConfigFile
    redisPool *redis.Pool
    WorkDir string
    ConfigFile string = "conf.ini"
    Engine *xorm.Engine
    usemysql bool
    buildPeriod time.Time
    eventTime   = make(map[string]int64)
    DEBUG bool
)

func init(){
    var err error

    // 当前目录
    WorkDir, err = ExecDir()
    if err != nil {
        panic(err.Error())
    }

    // 配置文件
    cfgPath := filepath.Join(WorkDir, ConfigFile)
    Cfg, err = goconfig.LoadConfigFile(cfgPath)
    if err != nil {
        panic(err.Error())
    }

    DEBUG = Cfg.MustBool("base", "debug", false)

    // redis链接
    host := Cfg.MustValue("redis", "host")
    port := Cfg.MustValue("redis", "port", "6379")
    if host == "" {
        panic(errors.New("pleace config redis host first!"))
    }

    initRedis(fmt.Sprintf("%s:%s", host, port), "")

    // mysql
    useMysql := Cfg.MustBool("base", "usemysql")

    if useMysql {
        // mysql链接
        user := Cfg.MustValue("mysql", "user")
        pwd := Cfg.MustValue("mysql", "password")
        host = Cfg.MustValue("mysql", "host")
        port = Cfg.MustValue("mysql", "port")
        dbname := Cfg.MustValue("mysql", "dbname")
        charset := Cfg.MustValue("mysql", "charset")
        
        dns := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=%s", user, pwd, host, port, dbname, charset)
    
        Engine, err = xorm.NewEngine("mysql", dns)
        if err != nil {
            panic(err.Error())
        }

        fmt.Println("mysql Engine OK !")
    }

    // 同步数据库

}

func initRedis(server, password string){
    redisPool = &redis.Pool{
        MaxIdle:     3,
        IdleTimeout: 240 * time.Second,
        Dial: func() (redis.Conn, error) {
            c, err := redis.Dial("tcp", server)
            if err != nil {
                return nil, err
            }
            if password != "" {
                if _, err := c.Do("AUTH", password); err != nil {
                    c.Close()
                    return nil, err
                }
            }
            return c, err
        },
        TestOnBorrow: func(c redis.Conn, t time.Time) error {
            _, err := c.Do("PING")
            return err
        },
    }
}

func main(){
    var err error

    shfile := Cfg.MustValue("file", "shfile")
    szfile := Cfg.MustValue("file", "szfile")

    if shfile == "" {
        panic("shfile config is empty")
    }
    if szfile == "" {
        panic("szfile config is empty")
    }

    /*
    shfile = filepath.Join(WorkDir, shfile)
    szfile = filepath.Join(WorkDir, szfile)
    */

    // 启动时先读一遍
    err = readNsave(shfile, true)
    if err != nil {
        panic(err.Error())
    }
    err = readNsave(szfile, false)
    if err != nil {
        panic(err.Error())
    }

    // 开始监控
    watcher, err := fsnotify.NewWatcher()
    if err != nil {
        panic(err.Error())
    }

    var exit = make(chan int)

    go func(){
        for{
            select{
                case event := <-watcher.Event:
                    // 每秒测一次
        				if buildPeriod.Add(1 * time.Second).After(time.Now()) {
        					continue
        				}
        				buildPeriod = time.Now()
        
                        //   更新时间判断
        				mt := getFileModTime(event.Name)
                        t := eventTime[event.Name]
        				if mt == t {
                            continue
        				}
        
                        show("更新时间对比",t, mt)
                        eventTime[event.Name] = mt

                        st := time.Now().UnixNano()
                        if event.Name == shfile { 
                            show("INFO : file change :", event.Name)
                            err = readNsave(shfile, true)
                            if err != nil {
                                show(err)
                            } else {
                                show("read and save done!", event.Name)
                            }
                        }

                        if event.Name == szfile {
                            show("INFO : file change :", event.Name)
                            err = readNsave(szfile, false)
                            if err != nil {
                                show(err)
                            } else {
                                show("read and save done!", event.Name)
                            }
                        }
                        et := time.Now().UnixNano() - st
                        fmt.Println(fmt.Sprintf("用时毫秒", float64(et/1000000000)))
                case watcher_err := <-watcher.Error:
                    // @todo 错误处理
                    show("ERROR Fail to watch file", watcher_err)
            }
        }
    }()


    // 监控文件
    watcher.Watch(shfile)
    watcher.Watch(szfile)
    
    <-exit

    defer watcher.Close()
}


// 限制 goroutine

// 存到mysql
func save2mysql(row, fields []string) (err error) {
    keys := make([]string, 0)
    vals := make([]string, 0)

    arrLen := len(row)
    for i := 0; i < arrLen; i++ {
        keys = append(keys, fields[i])
        vals = append(vals, strings.Replace(row[i], "-", "0", -1))
    }

    checkSql := "SELECT `id` FROM `sjshq` WHERE `HQZQDM`=?"
    res, err := Engine.Query(checkSql, vals[0])
    if err != nil {
        fmt.Println(err)
        return
    }

    if len(res)  == 0 {
        fieldStr := ""
        valueStr := ""
        for i := 0; i<arrLen; i++{
            fieldStr += "`"+keys[i]+"`"
            valueStr += "\""+vals[i]+"\""
            if i < arrLen - 1 {
                fieldStr +=", "
                valueStr +=", "
            }
        }
        inserSql := fmt.Sprintf("INSERT INTO `sjshq`(%s) VALUES(%s)", fieldStr, valueStr)
        _, err := Engine.Exec(inserSql)
        if err != nil {
            return err
        }
    } else {
        setStr := ""
        for i := 1; i<arrLen; i++{
            setStr += "`"+keys[i]+"`=\""+vals[i]+"\""
            if i < arrLen - 1 {
                setStr +=", "
            }
        }
        updateSql := fmt.Sprintf("UPDATE `sjshq` SET %s WHERE `HQZQDM`=\"%s\"", setStr, vals[0])
        _, err := Engine.Exec(updateSql)
        if err != nil {
            return err
        }
    }

    return
}

// 读dbf
func ReadDBF(file string, isSH bool)( rows [][]string, fieldNames []string , err error){
    dbfTable, err := godbf.NewFromFile(file, "GB2312") 
    if err != nil {
        return
    }
    fields := dbfTable.Fields()

    fieldNames = make([]string, 0)
    var szName string
    for _,field := range fields{
        if isSH {
            szName, err = getFields(field.FieldName())
            if err != nil {
                return
                /*
                fmt.Println(err, field.FieldName())
                continue
                */
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


        // 读数据返回
        rows = append(rows, row)
        //save2redis(row, fieldNames)
        //go save2redis(row)

    }

    return
}

// 保存到redis
func save2redis(row, fields []string) (err error){
    key := fmt.Sprintf("stock-data:%s", row[0])

    args := []interface{}{key}

    for k,v := range row {

        v = strings.Replace(v, "-", "0", -1)

        args = append(args, fields[k], v)
    }


    client := redisPool.Get()
    defer client.Close()

    _, err = client.Do("HMSET", args...)
    if err != nil {
        return
    }

    return
}

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

// 读配置文件 
func newConfig(cfgFile string) (cfg *goconfig.ConfigFile, err error) {
    workDir, err := ExecDir()
    if err != nil {
        return 
    }

    cfgPath := filepath.Join(workDir, cfgFile)
    cfg, err = goconfig.LoadConfigFile(cfgPath)

    return
}

// 执行文件所在目录
func ExecDir() (string, error) {
	file, err := exec.LookPath(os.Args[0])
	if err != nil {
		return "", err
	}
	p, err := filepath.Abs(file)
	if err != nil {
		return "", err
	}
	return path.Dir(strings.Replace(p, "\\", "/", -1)), nil
}


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

func show(args... interface{}) {
    if DEBUG {
        fmt.Println(args)
    }
}

// 保存到数据库 
func readNsave(file string, isSH bool) (err error){
    rows, fields, err := ReadDBF(file, isSH) 
    if err != nil {
        return
    }

    rowLen := len(rows)
    for i := 0; i<rowLen; i++ {
      if len(rows[i]) != len(fields){
        return errors.New(fmt.Sprintf("Error: row fields not fix: row-keys-num(%d) for fields num(%d)", len(rows[i]), len(fields)))
      }
      err = save2redis(rows[i], fields)
      if err !=nil {
          return
      }

      if usemysql {
          err = save2mysql(rows[i], fields)
          if err != nil {
            return
          }
      }
      // go error mysql ERROR 1040 (00000): Too many connections
      //go save2mysql(rows[i], fields)
    }

    return
}

package main

import (
    "encoding/json"
    "errors"
    "flag"
    "fmt"
    "github.com/qiniu/api/auth/digest"
    "github.com/qiniu/api/rs"
    "github.com/qiniu/api/url"
    "github.com/qiniu/log"
    "io"
    "io/ioutil"
    "net/http"
    "os"
    "path"
    "strconv"
    "strings"
    "sync"
    "time"
)

type Config struct {
    Bucket    string
    Domain    string
    BaseDir   string
    AccessKey string
    SecretKey string
    IPs       []string
}

type editLog struct {
    *Config
    keysLog     string
    historyLog  string
    dataDir     string
    backupCount int64
    lock        *sync.Mutex
}

func (e *editLog) init() (err error) {
    err = e.initLog()
    if err != nil {
        return
    }
    e.dataDir = path.Join(e.BaseDir, e.Bucket, "data")
    err = os.MkdirAll(e.dataDir, 0700)
    return
}

func (e *editLog) initLog() (err error) {
    logDir := path.Join(e.BaseDir, e.Bucket, "log")
    err = os.MkdirAll(logDir, 0700)
    if err != nil {
        log.Errorf("Error init log dir: %s", logDir)
        return
    }

    keysLog := path.Join(logDir, "keys.log")
    fKeys, err := os.OpenFile(keysLog, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0660)
    if err != nil {
        log.Errorf("Error init keys log file: %s", keysLog)
        return
    }
    fKeys.Close()

    historyLog := path.Join(logDir, "history.log")
    fHistory, err := os.OpenFile(historyLog, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0660)
    if err != nil {
        log.Errorf("Error init history log file: %s", keysLog)
        return
    }
    fHistory.Close()

    e.keysLog = keysLog
    e.historyLog = historyLog
    return nil
}

func (e *editLog) putKey(key string) (err error) {
    fKey, err := os.OpenFile(e.keysLog, os.O_RDWR|os.O_APPEND, 0660)
    if err != nil {
        return
    }
    defer fKey.Close()
    item := fmt.Sprintf("%s:%d\n", e.escape(key), time.Now().UnixNano())
    _, err = fKey.WriteString(item)
    return
}

func (e *editLog) getKeys() (keysMap map[string]string, err error) {
    b, err := ioutil.ReadFile(e.keysLog)
    if err != nil {
        return
    }
    keysMap = make(map[string]string)
    keysList := strings.Split(string(b), "\n")
    for _, item := range keysList {
        if item == "" {
            continue
        }
        splitedItem := strings.Split(item, ":")
        if len(splitedItem) == 2 {
            keysMap[splitedItem[0]] = splitedItem[1]
        }
    }
    return
}

func (e *editLog) putHistory(keyWithTime, etag string, modTime, downloaded, fullSize int64) (err error) {
    fHistory, err := os.OpenFile(e.historyLog, os.O_RDWR|os.O_APPEND, 0660)
    if err != nil {
        return
    }
    defer fHistory.Close()
    item := fmt.Sprintf("%s %s %d %d %d\n", keyWithTime, etag, modTime, downloaded, fullSize)
    _, err = fHistory.WriteString(item)
    return
}

func (e *editLog) getHistory() (historyMap map[string]string, err error) {
    b, err := ioutil.ReadFile(e.historyLog)
    if err != nil {
        return
    }
    historyMap = make(map[string]string)
    historyList := strings.Split(string(b), "\n")
    for _, item := range historyList {
        if item == "" {
            continue
        }
        splitedItem := strings.SplitN(item, " ", 2)
        if len(splitedItem) == 2 {
            historyMap[splitedItem[0]] = splitedItem[1]
        }
    }
    return
}

func (e *editLog) startBackup() (err error) {
    log.Info("Starting backup tasks")
    tasks, redos, err := e.makeTasks()
    if err != nil {
        log.Error("Error making tasklists")
        return
    }
    go e.doBackup(tasks, redos)
    return nil
}

func (e *editLog) doBackup(tasks []string, redos map[string]int64) {
    e.lock.Lock()
    defer e.lock.Unlock()
    //New tasks
    log.Infof("##### %d file(s) to download #####", len(tasks))
    ctNewSucceeded, ctNewFailed := 0, 0
    for _, keyWithTime := range tasks {
        err := e.doTask(keyWithTime, 0)
        if err != nil {
            ctNewFailed += 1
        } else {
            ctNewSucceeded += 1
        }
    }
    log.Infof("##### Tasks ended with %d succeeded, %d failed #####", ctNewSucceeded, ctNewFailed)

    //Redos
    log.Infof("##### %d file(s) to redo #####", len(redos))
    ctRedoSucceeded, ctRedoFailed := 0, 0
    for keyWithTime, start := range redos {
        err := e.doTask(keyWithTime, start)
        if err != nil {
            ctRedoFailed += 1
        } else {
            ctRedoSucceeded += 1
        }
    }
    log.Infof("##### Redos ended with %d succeeded, %d failed #####", ctRedoSucceeded, ctRedoFailed)

}

func (e *editLog) makeTasks() (taskList []string, redoMap map[string]int64, err error) {
    keysMap, err := e.getKeys()
    if err != nil {
        return
    }
    historyMap, err := e.getHistory()
    if err != nil {
        return
    }
    redoMap = make(map[string]int64)
    for key, ts := range keysMap {
        keyWithTime := fmt.Sprintf("%s:%s", key, ts)
        if detail, ok := historyMap[keyWithTime]; !ok {
            taskList = append(taskList, keyWithTime)
        } else {
            splitedDetail := strings.Split(detail, " ")
            if len(splitedDetail) != 4 {
                text := fmt.Sprintf("Error spliting history detail: %s", detail)
                log.Debug(text)
                continue
            }
            downloaded, err1 := strconv.Atoi(splitedDetail[2])
            fullSzie, err2 := strconv.Atoi(splitedDetail[3])
            if err1 != nil || err2 != nil {
                text := fmt.Sprintf("Error converting detail to int: %s", detail)
                log.Debug(text)
                continue
            }
            if downloaded < fullSzie {
                redoMap[keyWithTime] = int64(downloaded)
            }
        }
    }
    return
}

func (e *editLog) doTask(keyWithTime string, start int64) (err error) {
    splited := strings.Split(keyWithTime, ":")
    if len(splited) != 2 {
        text := fmt.Sprintf("Error spliting key: %s", keyWithTime)
        log.Error(text)
        err = errors.New(text)
        return
    }
    key, _ := splited[0], splited[1]
    unescapedKey, err := e.unescapte(key)
    entry := fmt.Sprintf("%s:%s", e.Bucket, unescapedKey)
    if err != nil {
        log.Errorf("Error unescaping %s", key)
    }
    log.Infof("Downloading %s", entry)
    _, downloaded, fullSize, modTime, etag, err := e.download(unescapedKey, start)
    //todo retry and resume
    if err != nil {
        log.Errorf("Failed downloading %s : %s", entry, err)
        return
    }
    log.Infof("Succeeded downloading %s", entry)
    err = e.putHistory(keyWithTime, etag, modTime, downloaded, fullSize)
    if err != nil {
        log.Errorf("Failed logging %s to history", entry)
    }
    log.Infof("Succeeded logging %s to history", entry)
    return
}

func (e *editLog) download(key string, start int64) (code int, downloaded, fullSize, modTime int64, etag string, err error) {
    downloaded = -1
    baseUrl := e.makeBaseUrl(e.escape(key))
    fUrl := e.makeFullUrl(baseUrl)
    client := &http.Client{}
    req, err := http.NewRequest("GET", fUrl, nil)
    header := http.Header{}
    header.Set("Accept-Encoding", "identity")
    header.Set("Range", fmt.Sprintf("bytes=%d-", start))
    req.Header = header
    req.Close = true
    if err != nil {
        return
    }
    resp, err := client.Do(req)
    if err != nil {
        log.Debug(err)
        return
    }
    defer resp.Body.Close()
    reqId := resp.Header.Get("X-Reqid")
    code = resp.StatusCode
    log.Debugf("ReqId: %s", reqId)
    log.Debugf("Code: %d", code)
    if code/100 != 2 {
        msg := fmt.Sprintf("Code: %d", code)
        err = errors.New(msg)
        return
    }
    size, err := strconv.Atoi(resp.Header.Get("Content-Length"))
    if err != nil {
        msg := "No Content-Length in response header"
        err = errors.New(msg)
        return
    }
    fullSize = int64(size) + start
    etag = resp.Header.Get("Etag")
    if start > 0 {
        downloaded, modTime, err = e.append(key, resp.Body)
    } else {
        downloaded, modTime, err = e.save(key, resp.Body)
    }
    if downloaded < fullSize {
        err = io.ErrUnexpectedEOF
    }
    //checkEtag := true
    return
}

func (e *editLog) save(key string, r io.Reader) (downloaded, modTime int64, err error) {
    if key == "" {
        key = "_empty"
    }
    fPath := path.Join(e.dataDir, key)
    fDir := path.Dir(fPath)
    err = os.MkdirAll(fDir, 0700)
    if err != nil {
        log.Errorf("Error making folder: %s", fDir)
        return
    }
    f, err := os.Create(fPath)
    if err != nil {
        log.Errorf("Error creating file: %s", fPath)
        return
    }
    defer f.Close()
    _, err = io.Copy(f, r)
    if err != nil && err != io.ErrUnexpectedEOF {
        return
    }
    fi, err := f.Stat()
    if err != nil {
        log.Errorf("Error stating file: %s", fPath)
        return
    }
    modTime = fi.ModTime().UnixNano()
    downloaded = fi.Size()
    return
}

func (e *editLog) append(key string, r io.Reader) (downloaded, modTime int64, err error) {
    if key == "" {
        key = "_empty"
    }
    fPath := path.Join(e.dataDir, key)
    fDir := path.Dir(fPath)
    err = os.MkdirAll(fDir, 0700)
    if err != nil {
        log.Errorf("Error making folder: %s", fDir)
        return
    }
    f, err := os.OpenFile(fPath, os.O_RDWR|os.O_APPEND, 0660)
    if err != nil {
        log.Errorf("Error opening file: %s", fPath)
        return
    }
    defer f.Close()
    _, err = io.Copy(f, r)
    if err != nil && err != io.ErrUnexpectedEOF {
        return
    }
    fi, err := f.Stat()
    if err != nil {
        log.Errorf("Error stating file: %s", fPath)
        return
    }
    modTime = fi.ModTime().UnixNano()
    downloaded = fi.Size()
    return
}

func (e *editLog) makeBaseUrl(key string) string {
    return fmt.Sprintf("http://%s/%s", e.Domain, key)
}

func (e *editLog) makeFullUrl(baseUrl string) string {
    policy := rs.GetPolicy{}
    mac := &digest.Mac{e.AccessKey, []byte(e.SecretKey)}
    return policy.MakeRequest(baseUrl, mac)
}

func (e *editLog) escape(text string) (esced string) {
    esced = strings.Replace(url.Escape(text), ":", "%3A", -1)
    return
}

func (e *editLog) unescapte(text string) (unesced string, err error) {
    unesced, err = url.Unescape(text)
    return
}

func NewEditLog(confPath string) (e *editLog, err error) {
    r, err := os.Open(confPath)
    if err != nil {
        log.Errorf("Error loading config: %s", confPath)
        return
    }
    conf := &Config{}
    decoder := json.NewDecoder(r)
    err = decoder.Decode(conf)

    if err != nil {
        log.Error("Error decode config content")
        return
    }
    text := fmt.Sprintf("\n### Config information ###\nBucket: %s;\nDomain: %s\nBaseDir: %s\nAccessKey: %s\nSecret Key: %s\n",
        conf.Bucket, conf.Domain, conf.BaseDir, strings.Repeat("*", len(conf.AccessKey)), strings.Repeat("*", len(conf.SecretKey)))
    log.Info(text)
    if conf.Bucket == "" || conf.Domain == "" || conf.BaseDir == "" || conf.AccessKey == "" || conf.SecretKey == "" {
        text := "Error not enough parameters"
        log.Error(text)
        err = errors.New(text)
        return
    }
    lock := &sync.Mutex{}
    e = &editLog{
        Config: conf,
        lock:   lock,
    }
    err = e.init()
    return
}

//server
type Server struct {
    el *editLog
}

func NewServer(el *editLog) Server {
    return Server{el}
}

func (s *Server) PutKeyHandler(rw http.ResponseWriter, req *http.Request) {
    if len(s.el.IPs) != 0 {
        remoteIp := strings.Split(req.RemoteAddr, ":")[0]
        allowed := checkIp(remoteIp, s.el.IPs)
        if !allowed {
            log.Errorf("IP %s is not allowed", remoteIp)
            rw.WriteHeader(496)
            return
        }
    }
    req.ParseForm()
    key := req.Form.Get("key")
    if key == "" {
        log.Error("No key to put")
        rw.WriteHeader(497)
        return
    }
    err := s.el.putKey(key)
    if err != nil {
        log.Errorf("Error with the key : %s", key)
        rw.WriteHeader(498)
        return
    }
    log.Infof("Success with the key : %s", key)
    return
}

func (s *Server) BackupHandler(rw http.ResponseWriter, req *http.Request) {
    if len(s.el.IPs) != 0 {
        remoteIp := strings.Split(req.RemoteAddr, ":")[0]
        allowed := checkIp(remoteIp, s.el.IPs)
        if !allowed {
            log.Errorf("IP %s is not allowed", remoteIp)
            rw.WriteHeader(496)
            return
        }
    }
    err := s.el.startBackup()
    if err != nil {
        log.Error("Error starting backup")
        rw.WriteHeader(495)
        return
    }
    return
}

func checkIp(ip string, ipList []string) bool {
    for _, v := range ipList {
        if v == ip {
            return true
        }
    }
    return false
}

//flag
var confPath = flag.String("c", "", "-conf <path to config file>")
var port = flag.Int("s", -1, "-s <port> , start a simplet server")
var put = flag.String("p", "", "-p <key> , put a key to log")
var backup = flag.Bool("b", false, "-b , start a backup task")
var verbose = flag.Bool("v", false, "-v , verbose model")

//main
func useage() {
    text := `使用方法: backup -c <path to config file> {-s <port> | -p <key> | -b }
-s <port> : 启动一个监听 <port> 端口的简易服务器，
            通过请求 http://localhost:port/addkey?key=somekey 来新增文件记录;
-p <key>  : 新增一个文件记录
-b  v      : 开始备份
-v        : 详情模式
#####
config 格式: 
{
    "ips": [],
    "bucket": "",
    "domain": "",
    "baseDir": "",
    "accessKey": "",
    "secretKey": ""
}

    `
    fmt.Print(text)
}

func main() {
    flag.Usage = useage
    flag.Parse()

    logLevel := log.Linfo
    if *verbose {
        logLevel = log.Ldebug
    }
    log.SetOutputLevel(logLevel)
    log.SetFlags(log.Llevel | log.Lshortfile | log.LstdFlags)

    if *confPath == "" {
        useage()
        os.Exit(1)
    }

    el, err := NewEditLog(*confPath)
    if err != nil {
        log.Fatal("Error initializing editLog")
        fmt.Println(err)
        os.Exit(1)
    }

    if *port >= 0 {
        log.Infof("Start a put server for bucket: %s", el.Bucket)
        ser := NewServer(el)
        mux := http.NewServeMux()

        mux.HandleFunc("/addkey", func(rw http.ResponseWriter, req *http.Request) {
            ser.PutKeyHandler(rw, req)
        })
        mux.HandleFunc("/backup", func(rw http.ResponseWriter, req *http.Request) {
            ser.BackupHandler(rw, req)
        })
        http.ListenAndServe(fmt.Sprintf(":%d", *port), mux)
        os.Exit(0)
    }

    if *backup {
        err = el.startBackup()
        if err != nil {
            log.Error("Backup task failed")
        }
        os.Exit(0)
    }

    if *put != "" {
        err = el.putKey(*put)
        if err != nil {
            log.Errorf("Error put key : %s", *put)
            os.Exit(1)
        }
        log.Infof("Success put key : %s", *put)
        os.Exit(0)
    }

    useage()
    os.Exit(1)
}

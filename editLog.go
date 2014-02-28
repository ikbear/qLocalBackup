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
    "time"
)

type Config struct {
    Bucket    string
    Domain    string
    BaseDir   string
    AccessKey string
    SecretKey string
}

type editLog struct {
    *Config
    keysLog    string
    historyLog string
    dataDir    string
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
    log.Info("Starting a backup task")
    tasks, err := e.makeTasks()
    if err != nil {
        return
    }
    log.Infof("%d file(s) to download", len(tasks))
    ctSucceeded, ctFailed := 0, 0
    for _, keyWithTime := range tasks {
        err = e.doTask(keyWithTime)
        if err != nil {
            ctFailed += 1
        } else {
            ctSucceeded += 1
        }
    }
    log.Infof("Task ended with %d succeeded, %d failed ", ctSucceeded, ctFailed)
    return nil
}

func (e *editLog) makeTasks() (taskList []string, err error) {
    keysMap, err := e.getKeys()
    if err != nil {
        return
    }
    historyMap, err := e.getHistory()
    if err != nil {
        return
    }
    for key, ts := range keysMap {
        keyWithTime := fmt.Sprintf("%s:%s", key, ts)
        if _, ok := historyMap[keyWithTime]; !ok {
            taskList = append(taskList, keyWithTime)
        }
    }
    return
}

func (e *editLog) doTask(keyWithTime string) (err error) {
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
    code, downloaded, fullSize, modTime, etag, err := e.download(unescapedKey)
    //todo retry and resume
    if code == 404 {
        log.Debugf("File %s not found", entry)
    }
    if err != nil {
        log.Errorf("Failed downloading %s: %s", entry, err)
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

func (e *editLog) download(key string) (code int, downloaded, fullSize, modTime int64, etag string, err error) {
    downloaded = -1
    baseUrl := e.makeBaseUrl(e.escape(key))
    fUrl := e.makeFullUrl(baseUrl)
    client := &http.Client{}
    req, err := http.NewRequest("GET", fUrl, nil)
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
    code = resp.StatusCode
    if code/100 != 2 {
        msg := fmt.Sprintf("Get Error: %s Code %d", baseUrl, code)
        log.Debug(msg)
        err = errors.New(msg)
        return
    }
    size, _ := strconv.Atoi(resp.Header.Get("Content-Length"))
    fullSize = int64(size)
    etag = resp.Header.Get("Etag")
    reqId := resp.Header.Get("X-Reqid")
    log.Debugf("Response Reqid: %s", reqId)
    downloaded, modTime, err = e.save(key, resp.Body)
    //checkSize := true
    //checkEtag := true
    return
}

func (e *editLog) save(key string, r io.Reader) (written, modTime int64, err error) {
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
    written, err = io.Copy(f, r)
    if err != nil {
        return
    }
    fi, err := os.Stat(fPath)
    if err != nil {
        log.Errorf("Error stating file: %s", fPath)
        return
    }
    modTime = fi.ModTime().UnixNano()
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
    if conf.Bucket == "" || conf.Domain == "" || conf.BaseDir == "" || conf.AccessKey == "" || conf.SecretKey == "" {
        text := "Error not enough parameters"
        log.Error(text)
        err = errors.New(text)
        return
    }
    e = &editLog{
        Config: conf,
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

func (s *Server) PutKeyHandler(w http.ResponseWriter, req *http.Request) {
    req.ParseForm()
    key := req.Form.Get("key")
    if key == "" {
        log.Error("No key to put")
        return
    }
    err := s.el.putKey(key)
    if err != nil {
        log.Errorf("Error put key : %s", key)
    }
    log.Infof("Success put key : %s", key)
    return
}

//flag
var confPath = flag.String("c", "", "-conf <path to config file>")
var port = flag.Int("s", -1, "-s <port> , start a simplet server")
var put = flag.String("p", "", "-p <key> , put a key to log")
var backup = flag.Bool("b", false, "-b , start a backup task")

//main
func useage() {
    text := `
Usage: backup -c <path to config file> {-s <port> | -p <key> | -b }
    `
    fmt.Print(text)
}

func main() {
    log.SetOutputLevel(log.Linfo)
    log.SetFlags(log.Llevel | log.Lshortfile | log.LstdFlags)
    flag.Parse()

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
        log.Info("Start a put server")
        ser := NewServer(el)
        mux := http.NewServeMux()

        mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
            ser.PutKeyHandler(w, req)
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

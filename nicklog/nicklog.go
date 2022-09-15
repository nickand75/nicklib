package nicklog

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/gomail.v2"
)

const (
	cTimeFormat     = "2006-01-02_15-04-05.000"
	cMailMsgBufSize = 10024
)

type MailServer struct {
	Host     string
	Port     int
	UserName string
	Sender   string
	Password string
	IsTLS    bool
}

type Logger struct {
	fileName   string
	fileExt    string
	dir        string
	arcDir     string
	maxSize    int64
	maxFiles   int
	file       *os.File
	lock       sync.Mutex
	size       int64
	mailSender *MailSender
}

func NewLogger(dir string, fileName string, maxSize int64, maxFiles int, arcDir string,
	mailServers []MailServer, mailRcpts []string, mailSubj string, maxMsgSize int, sendMsgPeriod time.Duration) (l *Logger, err error) {

	if maxSize < 100 {
		return nil, errors.New("maxSize is less 100")
	}
	if maxFiles < 1 {
		return nil, errors.New("maxFiles is less 1")
	}

	if len(fileName) == 0 {
		return nil, errors.New("FileName is not set")
	}

	if len(dir) == 0 {
		return nil, errors.New("dir is not set")
	}

	if len(arcDir) == 0 {
		arcDir = dir
	}

	if len(mailSubj) == 0 {
		mailSubj = "Fatal error!"
	}

	l = &Logger{
		dir:      dir,
		fileName: fileName,
		fileExt:  filepath.Ext(fileName),
		maxSize:  maxSize * 1024,
		maxFiles: maxFiles,
		arcDir:   arcDir,
	}

	ss := strings.Split(fileName, ".")
	prefix := ""
	if len(ss) > 0 {
		prefix = ss[0]
	}

	if len(mailServers) > 0 && len(mailRcpts) > 0 {
		l.mailSender, err = NewMailSender(prefix, mailServers, mailRcpts, mailSubj, filepath.Join(dir, "maillog"), maxMsgSize, sendMsgPeriod)
		if err != nil {
			return nil, err
		}
	}

	if err := l.delOld(); err != nil {
		return nil, err
	}

	// open log file...
	if fileInfo, err := os.Stat(filepath.Join(l.dir, l.fileName)); err != nil {
		if err = l.createFile(); err != nil {
			return nil, err
		}
	} else {
		// file exists, check size...
		if !fileInfo.IsDir() && fileInfo.Size() < l.maxSize {
			// open for read-write
			if l.file, err = os.OpenFile(filepath.Join(l.dir, l.fileName), os.O_RDWR, 0666); err != nil {
				return nil, err
			}
			l.file.Seek(0, 2)
			l.size = fileInfo.Size()
		} else {
			if err := l.rotate(); err != nil {
				return nil, err
			}
		}
	}

	rand.Seed(time.Now().Unix())

	return
}

func (l *Logger) GetWriter() (w io.Writer) {
	return l.file
}

func (l *Logger) createFile() (err error) {
	if l.file, err = os.Create(filepath.Join(l.dir, l.fileName)); err != nil {
		return
	}
	l.size = 0
	return
}

func (l *Logger) rotate() (err error) {
	// delete old
	if err = l.delOld(); err != nil {
		return
	}
	// close already opened
	if err = l.close(); err != nil {
		return
	}

	// rename current...
	newFileName := l.fileName[:len(l.fileName)-len(l.fileExt)] + "_" + time.Now().Format(cTimeFormat) + l.fileExt
	if err = os.Rename(filepath.Join(l.dir, l.fileName), filepath.Join(l.arcDir, newFileName)); err != nil {
		return
	}

	// open new...
	return l.createFile()
}

func (l *Logger) close() (err error) {
	if l.file != nil {
		err = l.file.Close()
	}
	return
}

func (l *Logger) delOld() (err error) {
	for {
		ok, err := l.delFile()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
}

func (l *Logger) delFile() (deleted bool, err error) {

	files, err := ioutil.ReadDir(l.dir)
	if err != nil {
		return false, err
	}

	if len(files) < l.maxFiles {
		return false, nil
	}

	// lets find the oldiest and kill'em
	minId := -1
	cnt := 0
	minDateTime := time.Time{}
	for i := 0; i < len(files); i++ {
		fileName := files[i].Name()
		if len(fileName) == (len(l.fileName) + len(cTimeFormat) + 1) {
			if fileName[:len(l.fileName)-len(l.fileExt)] == l.fileName[:len(l.fileName)-len(l.fileExt)] && filepath.Ext(fileName) == l.fileExt {
				cnt++
				if fileDateTime, err := time.Parse(cTimeFormat, fileName[len(l.fileName)-len(l.fileExt)+1:len(l.fileName)-len(l.fileExt)+len(cTimeFormat)+1]); err == nil {
					if minDateTime.IsZero() || fileDateTime.Before(minDateTime) {
						minDateTime = fileDateTime
						minId = i
					}
				}
			}
		}
	}

	if minDateTime.IsZero() || cnt < (l.maxFiles-1) {
		return false, nil
	}

	// delete old...
	if err := os.Remove(filepath.Join(l.dir, files[minId].Name())); err != nil {
		return false, err
	}

	return true, nil
}

func (l *Logger) Printf(format string, args ...interface{}) {

	fmt.Fprintf(l, format, args...)

}

func (l *Logger) Println(a ...interface{}) (n int, err error) {

	var args []interface{}
	args = append(args, time.Now().Format("2006-01-02 15:04:05"))
	args = append(args, a...)

	n, err = fmt.Fprintln(l, args...)

	return
}

func (l *Logger) Print(a ...interface{}) (n int, err error) {

	var args []interface{}
	args = append(args, time.Now().Format("2006-01-02 15:04:05"))
	args = append(args, a...)

	n, err = fmt.Fprint(l, args...)

	return
}

func (l *Logger) Write(p []byte) (n int, err error) {

	l.lock.Lock()
	defer l.lock.Unlock()

	if (l.size + int64(len(p))) > l.maxSize {
		if err := l.rotate(); err != nil {
			panic("Cannot rotatate: " + err.Error())
		}
	}

	// write to file
	n, err = l.file.Write(p)
	l.size += int64(n)

	if l.mailSender != nil {
		l.mailSender.Write(p)
	}

	return n, err
}

// Close implements io.Closer, and closes the current logfile.
func (l *Logger) Close() error {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.close()
}

type MailSender struct {
	prefix        string
	servers       []MailServer
	rcpts         []string
	subj          string
	maxMsgSize    int64
	sendMsgPeriod time.Duration
	last          string
	tmpDir        string
	outDir        string
	file          *os.File
	lock          sync.RWMutex
	saveTime      time.Time
	curSize       int64
}

func NewMailSender(prefix string, servers []MailServer, rcpts []string, subj string, dir string, maxMsgSize int, sendMsgPeriod time.Duration) (s *MailSender, err error) {

	if len(dir) == 0 {
		return nil, errors.New("dir is not set")
	}

	tmpDir := filepath.Join(dir, "tmp")
	outDir := filepath.Join(dir, "out")

	// create dirs...
	if err = os.MkdirAll(tmpDir, os.ModeDir|0755); err != nil {
		return
	}

	if err = os.MkdirAll(outDir, os.ModeDir|0755); err != nil {
		return
	}

	s = &MailSender{
		prefix:        prefix,
		servers:       servers,
		rcpts:         rcpts,
		subj:          subj,
		maxMsgSize:    int64(maxMsgSize),
		sendMsgPeriod: sendMsgPeriod,
		last:          filepath.Join(dir, "last"),
		tmpDir:        tmpDir,
		outDir:        outDir,
	}

	if err = s.save(); err != nil {
		return
	}

	go s.checkSave()
	go s.sendFiles()

	return
}

func (s *MailSender) move2Out(curFileName string) (err error) {

	files, err := ioutil.ReadDir(s.tmpDir)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	for i := 0; i < len(files); i++ {

		fileName := files[i].Name()

		if !files[i].IsDir() && fileName != curFileName && len(fileName) >= len(s.prefix) && fileName[:len(s.prefix)] == s.prefix && fileName[len(fileName)-4:] == ".txt" {
			if files[i].Size() > 0 {
				if err = os.Rename(filepath.Join(s.tmpDir, fileName), filepath.Join(s.outDir, fileName)); err != nil {
					log.Println(err.Error())
					continue
				}
			} else {
				if err = os.Remove(filepath.Join(s.tmpDir, fileName)); err != nil {
					log.Println(err.Error())
					continue
				}
			}
		}
	}
	return
}

func (s *MailSender) save() error {

	// create new file...
	newCurFileName := s.prefix + strconv.FormatInt(time.Now().UnixNano(), 10) + ".txt"
	newFile, err := os.Create(filepath.Join(s.tmpDir, newCurFileName))
	if err != nil {
		log.Println(err.Error())
		return err
	}

	s.lock.Lock()
	// close old...
	if s.file != nil {
		s.file.Close()
	}
	s.file = newFile

	s.curSize = 0
	s.saveTime = time.Now().Add(s.sendMsgPeriod)
	s.lock.Unlock()

	return s.move2Out(newCurFileName)
}

func (s *MailSender) checkSave() {

	for {
		curSize := atomic.LoadInt64(&s.curSize)

		if curSize > 0 && (curSize >= s.maxMsgSize || s.saveTime.Before(time.Now())) {
			s.save()
		}

		time.Sleep(1 * time.Second)
	}
}

func (s *MailSender) sendFiles() {
	for {

		time.Sleep(1 * time.Minute)

		if _, err := os.Stat(s.last); err == nil {
			time.Sleep(1 * time.Minute)
			s.delLast()
		}

		fs, err := ioutil.ReadDir(s.outDir)
		if err != nil {
			log.Println(err.Error())
			return
		}

		var files sort.StringSlice
		for i := 0; i < len(fs); i++ {
			fileName := fs[i].Name()

			if !fs[i].IsDir() && len(fileName) >= len(s.prefix) && fileName[:len(s.prefix)] == s.prefix && fileName[len(fileName)-4:] == ".txt" {
				files = append(files, fileName)
			}
		}
		if len(files) == 0 {
			continue
		}

		files.Sort()
		if len(files) == 1 {
			if err = s.sendText(files[0]); err != nil {
				log.Println(err.Error())
			} else {
				if err := os.Remove(filepath.Join(s.outDir, files[0])); err != nil {
					log.Println(err.Error())
				}
			}
		} else {
			cnt := len(files)
			if cnt > 10 {
				cnt = 10
				files = files[:10]
			}

			if err := s.sendAttach(files, cnt); err != nil {
				log.Println(err.Error())
			} else {
				for j := 0; j < cnt; j++ {
					if err := os.Remove(filepath.Join(s.outDir, files[j])); err != nil {
						log.Println(err.Error())
					}
				}
			}

		}
	}
}

func (s *MailSender) createLast() {
	if file, err := os.Create(s.last); err != nil {
		log.Println(err.Error())
	} else {
		file.Close()
	}
}

func (s *MailSender) delLast() {
	if err := os.Remove(s.last); err != nil {
		log.Println(err.Error())
	}
}

func (s *MailSender) sendAttach(files []string, cnt int) (err error) {

	s.createLast()
	defer s.delLast()

	m := gomail.NewMessage()

	m.SetHeader("To", s.rcpts...)
	m.SetHeader("Subject", s.subj)
	m.SetBody("text/plian", "Logs in attachment ("+time.Now().UTC().Format("2006-01-02 15:04:05")+")\n")

	if cnt > len(files) {
		cnt = len(files)
	}

	for i := 0; i < cnt; i++ {
		m.Attach(filepath.Join(s.outDir, files[i]))
	}

	for i := 0; i < len(s.servers); i++ {
		m.SetHeader("From", s.servers[i].Sender)
		if err = gomail.NewDialer(s.servers[i].Host, s.servers[i].Port, s.servers[i].UserName, s.servers[i].Password).DialAndSend(m); err == nil {
			break
		}
	}
	return
}

func (s *MailSender) sendText(fileName string) (err error) {

	s.createLast()
	defer s.delLast()

	file, err := ioutil.ReadFile(filepath.Join(s.outDir, fileName))
	if err != nil {
		return
	}

	m := gomail.NewMessage()

	m.SetHeader("To", s.rcpts...)
	m.SetHeader("Subject", s.subj)
	m.SetBody("text/plain", "Log ("+time.Now().UTC().Format("2006-01-02 15:04:05")+"):\n"+string(file))

	for i := 0; i < len(s.servers); i++ {
		m.SetHeader("From", s.servers[i].Sender)
		if err = gomail.NewDialer(s.servers[i].Host, s.servers[i].Port, s.servers[i].UserName, s.servers[i].Password).DialAndSend(m); err == nil {
			break
		}
	}
	return
}

func (s *MailSender) Write(p []byte) (n int, err error) {

	s.lock.RLock()
	defer s.lock.RUnlock()

	// write to file
	n, err = s.file.Write(p)
	if n > 0 {
		atomic.AddInt64(&s.curSize, int64(n))
	}

	return n, err
}

package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"github.com/bmizerany/pat"
	"github.com/fzzbt/radix/redis"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"
	"crypto/tls"
	"image"
	"image/png"
	_ "image/gif"
	_ "image/jpeg"
	"bytes"
	"math"
)

// HTTP headers struct
type Headers struct {
	contentType  string
	lastModified string
	cacheControl string
}

// The URL for the default avatar
const defaultAvatarUrl = "//linuxfr.org/images/default-avatar.png"

// The maximal size for an image is 5MB
const maxSize = 5 * (1 << 20)

// The directory for caching files
var directory string

// The connection to redis
var connection *redis.Client

// Check if an URL is valid and not temporary in error
func urlStatus(uri string) error {

	str, err := connection.Get("img/err/" + uri).Str()
	if err == nil {
		return errors.New(str)
	}

	return nil
}

// Generate a key for cache from a string
func generateKeyForCache(s string) string {
	h := sha1.New()
	io.WriteString(h, s)
	key := h.Sum(nil)

	// Use 3 levels of hasing to avoid having too many files in the same directory
	return fmt.Sprintf("%s/%x/%x/%x/%x", directory, key[0:1], key[1:2], key[2:3], key[3:])
}

// Fetch image from cache
func fetchImageFromCache(uri, variation string) (headers Headers, body []byte, ok bool) {
	ok = false

	contentType, err := connection.Hget("img/"+variation+"/"+uri, "type").Str()
	if err != nil {
		return
	}

	filename := generateKeyForCache(uri)
	stat, err := os.Stat(filename)
	if err != nil {
		return
	}

	headers.contentType = contentType
	headers.lastModified = stat.ModTime().Format(time.RFC1123)

	body, err = ioutil.ReadFile(filename)
	ok = err == nil

	return
}

// Save the body and the content-type header in cache
func saveImageInCache(uri, variation string, headers Headers, body []byte) {
	go func() {
		filename := generateKeyForCache(variation+":"+uri)
		dirname := path.Dir(filename)
		err := os.MkdirAll(dirname, 0755)
		if err != nil {
			return
		}

		// Save the body on disk
		err = ioutil.WriteFile(filename, body, 0644)
		if err != nil {
			log.Printf("Error while writing %s\n", filename)
			return
		}

		// And other infos in redis
		connection.Hset("img/"+variation+"/"+uri, "type", headers.contentType)
	}()
}

// Save the error in redis for 10 minutes
func saveErrorInCache(uri string, err error) {
	go func() {
		connection.Set("img/err/"+uri, err.Error())
		connection.Expire("img/err/"+uri, 600)
	}()
}

// Fetch the image from the distant server
func fetchImageFromServer(uri string) (headers Headers, body []byte, err error) {
	// Accepts any certificate in HTTPS
	cfg := &tls.Config{InsecureSkipVerify: true}
	tr := &http.Transport{TLSClientConfig: cfg}
	client := &http.Client{Transport: tr}
	res, err := client.Get(uri)
	if err != nil {
		return
	}
	if res.StatusCode != 200 {
		log.Printf("Status code of %s is: %d\n", uri, res.StatusCode)
		err = errors.New("Unexpected status code")
		saveErrorInCache(uri, err)
		return
	}

	defer res.Body.Close()
	body, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}
	if res.ContentLength > maxSize {
		log.Printf("Exceeded max size for %s: %d\n", uri, res.ContentLength)
		err = errors.New("Exceeded max size")
		saveErrorInCache(uri, err)
		return
	}
	contentType := res.Header.Get("Content-Type")
	if contentType[0:5] != "image" {
		log.Printf("%s has an invalid content-type: %s\n", uri, contentType)
		err = errors.New("Invalid content-type")
		saveErrorInCache(uri, err)
		return
	}
	log.Printf("Fetch %s (%s)\n", uri, contentType)

	headers.contentType = contentType
	headers.lastModified = time.Now().Format(time.RFC1123)
	if urlStatus(uri) == nil {
		saveImageInCache(uri, "orig", headers, body)
	}
	return
}

// Fetch image from cache if available, or from the server
func fetchImage(uri string) (headers Headers, body []byte, err error) {
	err = urlStatus(uri)
	if err != nil {
		return
	}

	headers, body, ok := fetchImageFromCache(uri, "orig")
	if !ok {
		headers, body, err = fetchImageFromServer(uri)
	}

	headers.cacheControl = "public, max-age=600"

	return
}

func fetchResizedImage(uri string, width, height int) (headers Headers, body []byte, err error) {

	variation := fmt.Sprintf("resize/%d/%d", width, height)
	if err != nil {
		return
	}
	
	headers, body, ok := fetchImageFromCache(uri, variation)

	if ok {
		return
	}

	headers, body, err = fetchImage(uri)
	if err != nil {
		return
	}

	headers, body, err = resizeImage(uri, string(body), headers, width, height)
	if (err != nil) {
		return
	}

	saveImageInCache(uri, variation, headers, body)

	return
}

func resizeImage(uri, origBody string, origHeaders Headers, width, height int) (headers Headers, body []byte, err error) {

	m, _, err := image.Decode(strings.NewReader(origBody))

	if err != nil {
		return
	}

	bounds := m.Bounds()
	origWidth, origHeight := bounds.Dx(), bounds.Dy()

	if width >= origWidth && height >= origHeight {
		headers = origHeaders
		body = []byte(origBody)
		return
	}

	ratio := math.Max(float64(origWidth), float64(origHeight)) / math.Min(float64(width), float64(height))

	newWidth := int(math.Floor(float64(origWidth) / ratio))
	newHeight := int(math.Floor(float64(origHeight) / ratio))

	log.Printf("Resize: %s to %vx%v: orig: %vx%v; new: %vx%v; ratio: %v\n", uri, width, height, origWidth, origHeight, newWidth, newHeight, ratio)

	m = Resample(m, m.Bounds(), newWidth, newHeight)
	writter := new(bytes.Buffer)

	err = png.Encode(writter, m)

	if err != nil {
		return
	}

	body = []byte(writter.String())

	headers = origHeaders
	headers.contentType = "image/png"

	return
}


// Receive an HTTP request, fetch the image and respond with it
func Image(w http.ResponseWriter, r *http.Request, fn func()) {
	query := r.URL.Query()
	encoded_url := query.Get(":encoded_url")

	strWidth, strHeight := query.Get(":width"), query.Get(":height")

	width, err := strconv.ParseInt(strWidth, 10, 32)
	if err != nil {
		log.Printf("Invalid width %s\n", strWidth)
		http.Error(w, "Invalid parameters", 400)
		return
	}

	height, err := strconv.ParseInt(strHeight, 10, 32)
	if err != nil {
		log.Printf("Invalid width %s\n", strHeight)
		http.Error(w, "Invalid parameters", 400)
		return
	}

	if (width * height > maxSize) {
		log.Printf("Requested resized image exceeds max size\n")
		http.Error(w, "Requested resized image exceeds max size", 400)
		return
	}

	chars, err := hex.DecodeString(encoded_url)
	if err != nil {
		log.Printf("Invalid URL %s\n", encoded_url)
		http.Error(w, "Invalid parameters", 400)
		return
	}
	uri := string(chars)

	headers, body, err := fetchResizedImage(uri, int(width), int(height))
	if err != nil {
		fn()
		return
	}

	if headers.lastModified == r.Header.Get("If-Modified-Since") {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	w.Header().Add("Content-Type", headers.contentType)
	w.Header().Add("Last-Modified", headers.lastModified)
	w.Header().Add("Cache-Control", headers.cacheControl)
	w.Write(body)
}

// Receive an HTTP request for an image and respond with it
func Img(w http.ResponseWriter, r *http.Request) {
	fn := func() {
		http.NotFound(w, r)
	}
	Image(w, r, fn)
}

// Returns 200 OK if the server is running (for monitoring)
func Status(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "OK")
}

func main() {
	// Parse the command-line
	var addr string
	var logs string
	var conn string
	flag.StringVar(&addr, "a", "127.0.0.1:8000", "Bind to this address:port")
	flag.StringVar(&logs, "l", "-", "Use this file for logs")
	flag.StringVar(&conn, "r", "localhost:6379/0", "The redis database to use for caching meta")
	flag.StringVar(&directory, "d", "cache", "The directory for the caching files")
	flag.Parse()

	// Logging
	if logs != "-" {
		f, err := os.OpenFile(logs, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatal("OpenFile: ", err)
		}
		syscall.Dup2(int(f.Fd()), int(os.Stdout.Fd()))
		syscall.Dup2(int(f.Fd()), int(os.Stderr.Fd()))
	}

	// Redis
	parts := strings.Split(conn, "/")
	host := parts[0]
	db := 0
	if len(parts) >= 2 {
		db, _ = strconv.Atoi(parts[1])
	}
	cfg := redis.Config{Database: db, Address: host, PoolCapacity: 4}
	connection = redis.NewClient(cfg)
	defer connection.Close()

	// Routing
	m := pat.New()
	m.Get("/status", http.HandlerFunc(Status))
	m.Get("/resize/:encoded_url/:width/:height", http.HandlerFunc(Img))
	http.Handle("/", m)

	// Start the HTTP server
	log.Printf("Listening on http://%s/\n", addr)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

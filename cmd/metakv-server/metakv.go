package main

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
    "io/ioutil"
    "fmt"
)

type KvEntry struct {
	Path  string
	Value []byte
	Rev   []byte
}

type dataEntry struct {
	K KvEntry
	w *http.ResponseWriter
}

var store map[string]*dataEntry
var list []dataEntry

func sendAllData(w http.ResponseWriter, p string) {
	for k, v := range store {
		if strings.HasPrefix(k, p) {
		    val, _ := json.Marshal(v.K)
			w.Write(val)
		}
	}
}

func checkList(path string, body []byte) {
	for _, val := range list {
        if strings.HasPrefix(val.K.Path, path) {
            (*(val.w)).Write(body)
		}
	}
}

func viewHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "PUT" {
		body, _ := ioutil.ReadAll(r.Body)
		fmt.Println("got body %s", body)
		en := KvEntry{
			Path:  r.URL.Path,
			Value: body,
			Rev:   []byte("1"),
		}
		store[r.URL.Path] = &dataEntry{K: en,}
		w.WriteHeader(http.StatusOK)
		val, _ := json.Marshal(en)
		checkList(r.URL.Path, val)
		return
	}
	query := r.URL.Query()
	for _, v := range query["feed"] {
		if v == "continuous" {
			var g *dataEntry
			if g = store[r.URL.Path]; g != nil {
				g.w = &w
			} else {
				g = &dataEntry{}
				g.w = &w
				store[r.URL.Path] = g
			}
			list = append(list, *g)
			sendAllData(*g.w, r.URL.Path)
		}
		return
	}
	v := store[r.URL.Path]
	val, _ := json.Marshal(v.K)
	w.Write(val)
}

func main() {
	os.Setenv("CBAUTH_REVRPC_URL", "http://localhost:9000/test")
	http.HandleFunc("/", viewHandler)
	http.ListenAndServe(":9000", nil)
}

package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

type HTTPServer struct {
	Log *Log
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset Offset `json:"offset"`
}

type ConsumeRequest struct {
	Offset Offset `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

func (s *HTTPServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	// unmarshall a request body into a ProduceRequest struct
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	// append to log

	offset, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// initialize a response struct to encode to JSON
	res := ProduceResponse{Offset: offset}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *HTTPServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	record, err := s.Log.Read(req.Offset)
	if err == ErrOffsetNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ConsumeResponse{Record: record}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func NewHTTPServer(addr string) *http.Server {
	s := HTTPServer{Log: NewLog()}
	r := mux.NewRouter()
	r.HandleFunc("/", s.handleProduce).Methods("POST")
	r.HandleFunc("/", s.handleConsume).Methods("GET")
	server := &http.Server{
		Addr:    addr,
		Handler: r,
	}
	return server
}

package web

import (
	"fmt"
	"github.com/gorilla/mux"
	"github.com/hauke96/sigolo/v2"
	"io"
	"net/http"
	"soq/index"
	ownIo "soq/io"
	"soq/query"
)

func StartServer(port string, indexBaseFolder string, defaultCellSize float64, checkFeatureValidity bool) {
	r := initRouter(indexBaseFolder, defaultCellSize, checkFeatureValidity)
	sigolo.Infof("Start server with TLS support on port %s", port)
	err := http.ListenAndServe(":"+port, r)
	sigolo.FatalCheck(err)
}

func StartServerTls(port string, certFile string, keyFile string, indexBaseFolder string, defaultCellSize float64, checkFeatureValidity bool) {
	r := initRouter(indexBaseFolder, defaultCellSize, checkFeatureValidity)
	sigolo.Infof("Start server without TLS support on port %s", port)
	err := http.ListenAndServeTLS(":"+port, certFile, keyFile, r)
	sigolo.FatalCheck(err)
}

func initRouter(indexBaseFolder string, defaultCellSize float64, checkFeatureValidity bool) *mux.Router {
	tagIndex, err := index.LoadTagIndex(indexBaseFolder)
	sigolo.FatalCheck(err)
	geometryIndex := index.LoadGridIndex(indexBaseFolder, defaultCellSize, defaultCellSize, checkFeatureValidity, tagIndex)

	r := mux.NewRouter()
	r.HandleFunc("/app", func(writer http.ResponseWriter, request *http.Request) {
		sigolo.Infof("Serve index.html")
		http.ServeFile(writer, request, "./web/index.html")
	})
	r.HandleFunc("/query", func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Access-Control-Allow-Origin", "*")

		queryBytes, err := io.ReadAll(request.Body)
		if err != nil {
			sigolo.Errorf("Error reding HTTP body of request to '/query': %+v", err)
			writer.WriteHeader(http.StatusInternalServerError)
			_, err = writer.Write([]byte("Error reading HTTP body."))
			if err != nil {
				sigolo.Errorf("Error writing error response: %+v", err)
			}
			return
		}

		queryString := string(queryBytes)

		trimmedQueryString := queryString
		queryRunes := []rune(queryString)
		maxLengthOfPrintedQuery := 10000
		if len(queryRunes) > maxLengthOfPrintedQuery {
			trimmedQueryString = string(queryRunes[:maxLengthOfPrintedQuery]) + "... [truncated]"
		}
		sigolo.Infof("Query:\n%s", trimmedQueryString)

		queryObj, err := query.ParseQueryString(queryString, tagIndex, geometryIndex)
		if err != nil {
			sigolo.Errorf("Error parsing query: %+v", err)
			writer.WriteHeader(http.StatusBadRequest)
			_, err = writer.Write([]byte(fmt.Sprintf("Error parsing query: %+v", err)))
			if err != nil {
				sigolo.Errorf("Error writing error response: %+v", err)
			}
			return
		}

		features, err := queryObj.Execute(geometryIndex)
		if err != nil {
			sigolo.Errorf("Error executing query: %+v", err)
			writer.WriteHeader(http.StatusInternalServerError)
			_, err = writer.Write([]byte(fmt.Sprintf("Error executing query: %+v", err)))
			if err != nil {
				sigolo.Errorf("Error writing error response: %+v", err)
			}
			return
		}

		sigolo.Debugf("Found %d features", len(features))

		err = ownIo.WriteFeaturesAsGeoJson(features, tagIndex, writer)
		if err != nil {
			sigolo.Errorf("Error writing query result: %+v", err)
			writer.WriteHeader(http.StatusInternalServerError)
			_, err = writer.Write([]byte(fmt.Sprintf("Error writing query result: %+v", err)))
			if err != nil {
				sigolo.Errorf("Error writing error response: %+v", err)
			}
			return
		}
	}).Methods(http.MethodPost)

	return r
}

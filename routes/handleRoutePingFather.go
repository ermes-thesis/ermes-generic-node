package routes

import (
	"fmt"
	"info"
	"io"
	"net/http"
	"os"
)

func handleRoutePingFather(logger *io.Writer, serverinfo *info.Serverinfo) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(*logger, "got /pingFather \n")

			fathername := info.GetServerInfoFather(serverinfo)

			reqUrl := fmt.Sprintf("http://localhost:%s", fathername)

			fmt.Fprintf(*logger, "Sending a request to the father, located at %s \n", reqUrl)

			req, err := http.NewRequest(http.MethodGet, reqUrl, nil)

			if err != nil {
				fmt.Fprintf(os.Stderr, "Error when creating the http request: %s \n", err)

				w.WriteHeader(http.StatusBadRequest)

				return
			}

			res, err := http.DefaultClient.Do(req)

			if err != nil {
				fmt.Fprintf(os.Stderr, "Error when contacting the father: %s \n", err)

				w.WriteHeader(http.StatusInternalServerError)

				return
			}

			fmt.Fprintf(*logger, "Got a response from the father! \n")
			fmt.Fprintf(*logger, "Code returned: %d \n", res.StatusCode)

			w.WriteHeader(http.StatusOK)

		},
	)
}
